//! A SQLite-backed [`Filesystem`] for Spin's filesystem factor.
//!
//! One database file holds one filesystem: an `nodes` table of inodes (kind,
//! timestamps, link count, and content as a BLOB) and a `dirents` table of
//! `(parent, name) -> inode` edges. That inode shape is what buys full POSIX
//! semantics - hard links, symlinks, rename that keeps open handles valid,
//! and unlink of open files - so this backend passes the factor's complete
//! conformance suite, portable and posix tiers alike.
//!
//! Every operation runs as one transaction on a blocking thread, so multi-step
//! operations (rename with clobber, link-count maintenance) are atomic even
//! with several Spin processes pointed at the same database file: SQLite's
//! locking is real cross-process locking. WAL mode keeps readers and the
//! writer out of each other's way.
//!
//! What this is for: durable app state on a single host with real
//! transactional integrity, e.g. a git server whose repositories must survive
//! restarts - without granting the app any host directory. For multi-tenant
//! deployments, give each label its own database file; nothing is shared
//! between databases.

#![deny(missing_docs)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use spin_factor_filesystem::runtime_config::spin::MakeFilesystem;
use spin_factor_filesystem::{
    DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, Filesystem, FilesystemDefinition,
    FsPath, FsPathBuf, FsResult, MetadataHash, NewTimestamp, ObjectId, OpenFlags, OpenOptions,
    Opened, SetTimes, Stat,
};

/// The inode of the root directory.
const ROOT: i64 = 1;

/// How many symbolic links may be traversed while resolving one path,
/// matching the factor's other backends.
const SYMLINK_LIMIT: usize = 32;

/// The longest permitted name component, as POSIX `NAME_MAX`.
const NAME_MAX: usize = 255;

/// Node kinds as stored in the `kind` column.
const KIND_FILE: i64 = 0;
const KIND_DIR: i64 = 1;
const KIND_SYMLINK: i64 = 2;

/// A filesystem stored in a SQLite database.
///
/// Cloning is shallow: clones share the same database handle.
#[derive(Clone)]
pub struct SqliteFilesystem {
    db: Arc<Db>,
}

struct Db {
    conn: Mutex<Connection>,
    /// Live [`SqliteFile`] handles per inode. An unlinked inode's row is kept
    /// until its last handle drops, which is what makes unlink-while-open
    /// behave like POSIX.
    open_handles: Mutex<HashMap<i64, u64>>,
    summary: String,
}

impl SqliteFilesystem {
    /// Opens (creating if absent) a filesystem in the database at `path`.
    pub fn open_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite database {}", path.display()))?;
        // WAL keeps readers unblocked by the writer and survives crashes.
        conn.pragma_update(None, "journal_mode", "wal")?;
        Self::init(conn, format!("sqlite {}", path.display()))
    }

    /// Opens a filesystem in a fresh in-memory database. Intended for tests.
    pub fn in_memory() -> anyhow::Result<Self> {
        Self::init(Connection::open_in_memory()?, "sqlite :memory:".into())
    }

    /// Caps the database at roughly `bytes` by limiting its page count.
    /// Growth beyond it fails with `insufficient-space`.
    pub fn set_maximum_size(&self, bytes: u64) -> anyhow::Result<()> {
        let conn = self.db.conn.lock().unwrap_or_else(|e| e.into_inner());
        let page_size: u64 = conn.pragma_query_value(None, "page_size", |r| r.get(0))?;
        let pages = bytes.div_ceil(page_size.max(1)).max(1);
        conn.pragma_update(None, "max_page_count", pages)?;
        Ok(())
    }

    fn init(conn: Connection, summary: String) -> anyhow::Result<Self> {
        conn.pragma_update(None, "busy_timeout", 5_000)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS nodes (
                ino INTEGER PRIMARY KEY AUTOINCREMENT,
                kind INTEGER NOT NULL,
                nlink INTEGER NOT NULL DEFAULT 0,
                atime_s INTEGER NOT NULL, atime_ns INTEGER NOT NULL,
                mtime_s INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
                ctime_s INTEGER NOT NULL, ctime_ns INTEGER NOT NULL,
                symlink_target TEXT,
                data BLOB
            );
            CREATE TABLE IF NOT EXISTS dirents (
                parent INTEGER NOT NULL,
                name TEXT NOT NULL,
                ino INTEGER NOT NULL,
                PRIMARY KEY (parent, name)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS dirents_by_ino ON dirents(ino);",
        )?;
        // Rows orphaned by a crash while they were unlinked-but-open have no
        // handles any more; reclaim them.
        conn.execute("DELETE FROM nodes WHERE nlink = 0", [])?;
        // The root directory, exactly once. Like the other backends, its
        // link count is fixed: one name, forever.
        let (s, ns) = encode_time(SystemTime::now());
        conn.execute(
            "INSERT OR IGNORE INTO nodes
                 (ino, kind, nlink, atime_s, atime_ns, mtime_s, mtime_ns, ctime_s, ctime_ns)
             VALUES (?1, ?2, 1, ?3, ?4, ?3, ?4, ?3, ?4)",
            params![ROOT, KIND_DIR, s, ns],
        )?;
        Ok(Self {
            db: Arc::new(Db {
                conn: Mutex::new(conn),
                open_handles: Mutex::new(HashMap::new()),
                summary,
            }),
        })
    }

    /// Runs `f` inside one transaction on a blocking thread.
    async fn run<T, F>(&self, f: F) -> FsResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Tx<'_>) -> FsResult<T> + Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = db.conn.lock().unwrap_or_else(|e| e.into_inner());
            let tx = conn.transaction().map_err(db_err)?;
            let tx = Tx { tx, db: &db };
            let out = f(&tx)?;
            tx.tx.commit().map_err(db_err)?;
            Ok(out)
        })
        .await
        .map_err(|_| ErrorCode::Io)?
    }
}

/// A transaction plus the shared handle bookkeeping.
struct Tx<'a> {
    tx: Transaction<'a>,
    db: &'a Db,
}

/// The result of a [`Tx::walk`].
enum Lookup {
    Exists(i64),
    Missing { dir: i64, name: String },
}

/// A node row's identity: kind plus symlink target when relevant.
struct NodeRow {
    kind: i64,
    target: Option<String>,
}

impl Tx<'_> {
    fn node(&self, ino: i64) -> FsResult<NodeRow> {
        self.tx
            .query_row(
                "SELECT kind, symlink_target FROM nodes WHERE ino = ?1",
                [ino],
                |row| {
                    Ok(NodeRow {
                        kind: row.get(0)?,
                        target: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(db_err)?
            .ok_or(ErrorCode::NoEntry)
    }

    fn child(&self, dir: i64, name: &str) -> FsResult<Option<i64>> {
        if self.node(dir)?.kind != KIND_DIR {
            return Err(ErrorCode::NotDirectory);
        }
        self.tx
            .query_row(
                "SELECT ino FROM dirents WHERE parent = ?1 AND name = ?2",
                params![dir, name],
                |row| row.get(0),
            )
            .optional()
            .map_err(db_err)
    }

    /// Walks `components` from the root; the algorithm is the memory
    /// backend's, with directory-entry lookups going to SQL. See that
    /// backend for the reasoning about `..`, symlink splicing, and budgets.
    fn walk(&self, components: &[&str], follow_final: bool, create: bool) -> FsResult<Lookup> {
        let mut stack = vec![ROOT];
        let mut queue: Vec<String> = components.iter().rev().map(|c| (*c).to_owned()).collect();
        let mut budget = SYMLINK_LIMIT;

        while let Some(component) = queue.pop() {
            let current = *stack.last().expect("stack always holds the root");

            if component == ".." {
                if self.node(current)?.kind != KIND_DIR {
                    return Err(ErrorCode::NotDirectory);
                }
                if stack.len() == 1 {
                    return Err(ErrorCode::NotPermitted);
                }
                stack.pop();
                continue;
            }

            let is_final = queue.is_empty();
            let Some(child) = self.child(current, &component)? else {
                if is_final && create {
                    return Ok(Lookup::Missing {
                        dir: current,
                        name: component,
                    });
                }
                return Err(ErrorCode::NoEntry);
            };

            let node = self.node(child)?;
            if node.kind == KIND_SYMLINK && (!is_final || follow_final) {
                let target = node.target.unwrap_or_default();
                if target.starts_with('/') {
                    return Err(ErrorCode::NotPermitted);
                }
                if budget == 0 {
                    return Err(ErrorCode::Loop);
                }
                budget -= 1;
                for part in target
                    .split('/')
                    .filter(|c| !c.is_empty() && *c != ".")
                    .rev()
                {
                    queue.push(part.to_owned());
                }
                continue;
            }

            stack.push(child);
        }

        Ok(Lookup::Exists(
            *stack.last().expect("stack always holds the root"),
        ))
    }

    fn resolve(&self, path: &FsPath, follow_final: bool) -> FsResult<i64> {
        let components: Vec<&str> = path.components().collect();
        match self.walk(&components, follow_final, false)? {
            Lookup::Exists(ino) => {
                if path.requires_directory() && self.node(ino)?.kind != KIND_DIR {
                    return Err(ErrorCode::NotDirectory);
                }
                Ok(ino)
            }
            Lookup::Missing { .. } => Err(ErrorCode::NoEntry),
        }
    }

    fn resolve_parent(&self, path: &FsPath) -> FsResult<(i64, String)> {
        let components: Vec<&str> = path.components().collect();
        let Some((name, parents)) = components.split_last() else {
            return Err(ErrorCode::NotPermitted);
        };
        if *name == ".." {
            return Err(ErrorCode::NotPermitted);
        }
        let parent = match self.walk(parents, true, false)? {
            Lookup::Exists(ino) => ino,
            Lookup::Missing { .. } => return Err(ErrorCode::NoEntry),
        };
        if self.node(parent)?.kind != KIND_DIR {
            return Err(ErrorCode::NotDirectory);
        }
        Ok((parent, (*name).to_owned()))
    }

    /// Inserts a node row, returning its inode.
    fn alloc(&self, kind: i64, target: Option<&str>, data: Option<&[u8]>) -> FsResult<i64> {
        let (s, ns) = encode_time(SystemTime::now());
        self.tx
            .execute(
                "INSERT INTO nodes
                     (kind, nlink, atime_s, atime_ns, mtime_s, mtime_ns, ctime_s, ctime_ns,
                      symlink_target, data)
                 VALUES (?1, 0, ?2, ?3, ?2, ?3, ?2, ?3, ?4, ?5)",
                params![kind, s, ns, target, data],
            )
            .map_err(db_err)?;
        Ok(self.tx.last_insert_rowid())
    }

    /// Adds a directory entry and bumps the target's link count.
    fn link(&self, dir: i64, name: &str, ino: i64) -> FsResult<()> {
        self.attach(dir, name, ino)?;
        self.tx
            .execute("UPDATE nodes SET nlink = nlink + 1 WHERE ino = ?1", [ino])
            .map_err(db_err)?;
        Ok(())
    }

    /// Removes a directory entry and drops the target's link count, freeing
    /// the row once nothing refers to it.
    fn unlink(&self, dir: i64, name: &str) -> FsResult<()> {
        let ino = self.detach(dir, name)?;
        self.tx
            .execute(
                "UPDATE nodes SET nlink = MAX(nlink - 1, 0) WHERE ino = ?1",
                [ino],
            )
            .map_err(db_err)?;
        self.collect(ino)?;
        Ok(())
    }

    /// Adds a directory entry without touching link counts.
    fn attach(&self, dir: i64, name: &str, ino: i64) -> FsResult<()> {
        if name.len() > NAME_MAX {
            return Err(ErrorCode::NameTooLong);
        }
        if self.node(dir)?.kind != KIND_DIR {
            return Err(ErrorCode::NotDirectory);
        }
        self.tx
            .execute(
                "INSERT OR REPLACE INTO dirents (parent, name, ino) VALUES (?1, ?2, ?3)",
                params![dir, name, ino],
            )
            .map_err(db_err)?;
        self.touch_parent(dir)
    }

    /// Removes a directory entry without touching link counts.
    fn detach(&self, dir: i64, name: &str) -> FsResult<i64> {
        let ino = self.child(dir, name)?.ok_or(ErrorCode::NoEntry)?;
        self.tx
            .execute(
                "DELETE FROM dirents WHERE parent = ?1 AND name = ?2",
                params![dir, name],
            )
            .map_err(db_err)?;
        self.touch_parent(dir)?;
        Ok(ino)
    }

    fn touch_parent(&self, dir: i64) -> FsResult<()> {
        let (s, ns) = encode_time(SystemTime::now());
        self.tx
            .execute(
                "UPDATE nodes SET mtime_s = ?1, mtime_ns = ?2, ctime_s = ?1, ctime_ns = ?2
                 WHERE ino = ?3",
                params![s, ns, dir],
            )
            .map_err(db_err)?;
        Ok(())
    }

    /// Frees a node row once it has no names and no open handles.
    fn collect(&self, ino: i64) -> FsResult<()> {
        let handles = self
            .db
            .open_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if handles.get(&ino).copied().unwrap_or(0) == 0 {
            self.tx
                .execute("DELETE FROM nodes WHERE ino = ?1 AND nlink = 0", [ino])
                .map_err(db_err)?;
        }
        Ok(())
    }

    /// True if `inode` is inside the directory subtree rooted at `ancestor`
    /// (inclusive).
    fn is_ancestor(&self, ancestor: i64, inode: i64) -> FsResult<bool> {
        self.tx
            .query_row(
                "WITH RECURSIVE sub(i) AS (
                     VALUES (?1)
                     UNION
                     SELECT d.ino FROM dirents d JOIN sub ON d.parent = sub.i
                 )
                 SELECT EXISTS (SELECT 1 FROM sub WHERE i = ?2)",
                params![ancestor, inode],
                |row| row.get(0),
            )
            .map_err(db_err)
    }

    fn stat(&self, ino: i64) -> FsResult<Stat> {
        self.tx
            .query_row(
                "SELECT kind, nlink,
                        CASE kind
                            WHEN 0 THEN LENGTH(COALESCE(data, x''))
                            WHEN 2 THEN LENGTH(COALESCE(symlink_target, ''))
                            ELSE 0
                        END,
                        atime_s, atime_ns, mtime_s, mtime_ns, ctime_s, ctime_ns
                 FROM nodes WHERE ino = ?1",
                [ino],
                |row| {
                    Ok(Stat {
                        type_: descriptor_type(row.get(0)?),
                        link_count: row.get::<_, i64>(1)?.max(0) as u64,
                        size: row.get::<_, i64>(2)?.max(0) as u64,
                        data_access_timestamp: Some(decode_time(row.get(3)?, row.get(4)?)),
                        data_modification_timestamp: Some(decode_time(row.get(5)?, row.get(6)?)),
                        status_change_timestamp: Some(decode_time(row.get(7)?, row.get(8)?)),
                    })
                },
            )
            .optional()
            .map_err(db_err)?
            .ok_or(ErrorCode::BadDescriptor)
    }

    fn apply_times(&self, ino: i64, times: SetTimes) -> FsResult<()> {
        // Confirm the row exists so a stale handle reports `bad-descriptor`.
        self.node(ino)?;
        for (timestamp, s_col, ns_col) in [
            (times.access, "atime_s", "atime_ns"),
            (times.modification, "mtime_s", "mtime_ns"),
        ] {
            let time = match timestamp {
                NewTimestamp::NoChange => continue,
                NewTimestamp::Now => SystemTime::now(),
                NewTimestamp::Timestamp(time) => time,
            };
            let (s, ns) = encode_time(time);
            self.tx
                .execute(
                    &format!("UPDATE nodes SET {s_col} = ?1, {ns_col} = ?2 WHERE ino = ?3"),
                    params![s, ns, ino],
                )
                .map_err(db_err)?;
        }
        // Changing metadata is itself a status change.
        let (s, ns) = encode_time(SystemTime::now());
        self.tx
            .execute(
                "UPDATE nodes SET ctime_s = ?1, ctime_ns = ?2 WHERE ino = ?3",
                params![s, ns, ino],
            )
            .map_err(db_err)?;
        Ok(())
    }

    fn file_len(&self, ino: i64) -> FsResult<usize> {
        let row = self
            .tx
            .query_row(
                "SELECT kind, LENGTH(COALESCE(data, x'')) FROM nodes WHERE ino = ?1",
                [ino],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(db_err)?;
        match row {
            Some((KIND_FILE, len)) => Ok(len.max(0) as usize),
            Some(_) => Err(ErrorCode::BadDescriptor),
            None => Err(ErrorCode::BadDescriptor),
        }
    }

    fn read_data(&self, ino: i64) -> FsResult<Vec<u8>> {
        self.file_len(ino)?;
        self.tx
            .query_row(
                "SELECT COALESCE(data, x'') FROM nodes WHERE ino = ?1",
                [ino],
                |row| row.get(0),
            )
            .map_err(db_err)
    }

    fn write_data(&self, ino: i64, data: &[u8]) -> FsResult<()> {
        let (s, ns) = encode_time(SystemTime::now());
        self.tx
            .execute(
                "UPDATE nodes SET data = ?1, mtime_s = ?2, mtime_ns = ?3,
                                  ctime_s = ?2, ctime_ns = ?3
                 WHERE ino = ?4",
                params![data, s, ns, ino],
            )
            .map_err(db_err)?;
        Ok(())
    }
}

#[async_trait]
impl Filesystem for SqliteFilesystem {
    fn summary(&self) -> String {
        self.db.summary.clone()
    }

    async fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Opened> {
        let path = path.to_owned();
        let db = self.db.clone();
        let ino = self
            .run(move |tx| {
                let creating = opts.open_flags.contains(OpenFlags::CREATE);
                let want_dir_spelling = path.requires_directory();
                let ino = if creating {
                    let exclusive = opts.open_flags.contains(OpenFlags::EXCLUSIVE);
                    let follow_final = !exclusive && opts.follow_symlinks;
                    let components: Vec<&str> = path.components().collect();
                    match tx.walk(&components, follow_final, true)? {
                        Lookup::Exists(_) if exclusive => return Err(ErrorCode::Exist),
                        Lookup::Exists(ino) => ino,
                        Lookup::Missing { dir, name } => {
                            if opts.open_flags.contains(OpenFlags::DIRECTORY) {
                                return Err(ErrorCode::NoEntry);
                            }
                            if want_dir_spelling {
                                return Err(ErrorCode::IsDirectory);
                            }
                            let ino = tx.alloc(KIND_FILE, None, Some(&[]))?;
                            tx.link(dir, &name, ino)?;
                            ino
                        }
                    }
                } else {
                    tx.resolve(&path, opts.follow_symlinks)?
                };

                let node = tx.node(ino)?;
                match node.kind {
                    KIND_DIR => {
                        if creating
                            || opts.flags.contains(DescriptorFlags::WRITE)
                            || opts.open_flags.contains(OpenFlags::TRUNCATE)
                        {
                            return Err(ErrorCode::IsDirectory);
                        }
                        Ok(None)
                    }
                    _ if want_dir_spelling => Err(ErrorCode::NotDirectory),
                    KIND_SYMLINK => Err(ErrorCode::Loop),
                    _ => {
                        if opts.open_flags.contains(OpenFlags::DIRECTORY) {
                            return Err(ErrorCode::NotDirectory);
                        }
                        if opts.open_flags.contains(OpenFlags::TRUNCATE) {
                            tx.write_data(ino, &[])?;
                        }
                        Ok(Some(ino))
                    }
                }
            })
            .await?;

        match ino {
            None => Ok(Opened::Dir),
            Some(ino) => {
                *db.open_handles
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(ino)
                    .or_insert(0) += 1;
                Ok(Opened::File(Arc::new(SqliteFile {
                    fs: self.clone(),
                    ino,
                })))
            }
        }
    }

    async fn stat_at(&self, path: &FsPath, follow: bool) -> FsResult<Stat> {
        let path = path.to_owned();
        self.run(move |tx| tx.stat(tx.resolve(&path, follow)?))
            .await
    }

    async fn set_times_at(&self, path: &FsPath, follow: bool, times: SetTimes) -> FsResult<()> {
        let path = path.to_owned();
        self.run(move |tx| tx.apply_times(tx.resolve(&path, follow)?, times))
            .await
    }

    async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let path = path.to_owned();
        self.run(move |tx| {
            let ino = tx.resolve(&path, true)?;
            if tx.node(ino)?.kind != KIND_DIR {
                return Err(ErrorCode::NotDirectory);
            }
            let mut stmt = tx
                .tx
                .prepare(
                    "SELECT d.name, n.kind FROM dirents d
                     JOIN nodes n ON n.ino = d.ino
                     WHERE d.parent = ?1 ORDER BY d.name",
                )
                .map_err(db_err)?;
            let entries = stmt
                .query_map([ino], |row| {
                    Ok(DirEntry {
                        name: row.get(0)?,
                        type_: descriptor_type(row.get(1)?),
                    })
                })
                .map_err(db_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db_err)?;
            Ok(entries)
        })
        .await
    }

    async fn create_dir(&self, path: &FsPath) -> FsResult<()> {
        let path = path.to_owned();
        self.run(move |tx| {
            let (parent, name) = tx.resolve_parent(&path)?;
            if tx.child(parent, &name)?.is_some() {
                return Err(ErrorCode::Exist);
            }
            let ino = tx.alloc(KIND_DIR, None, None)?;
            tx.link(parent, &name, ino)
        })
        .await
    }

    async fn remove_dir(&self, path: &FsPath) -> FsResult<()> {
        let path = path.to_owned();
        self.run(move |tx| {
            let (parent, name) = tx.resolve_parent(&path)?;
            let ino = tx.child(parent, &name)?.ok_or(ErrorCode::NoEntry)?;
            if tx.node(ino)?.kind != KIND_DIR {
                return Err(ErrorCode::NotDirectory);
            }
            let occupied: bool = tx
                .tx
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM dirents WHERE parent = ?1)",
                    [ino],
                    |row| row.get(0),
                )
                .map_err(db_err)?;
            if occupied {
                return Err(ErrorCode::NotEmpty);
            }
            tx.unlink(parent, &name)
        })
        .await
    }

    async fn unlink(&self, path: &FsPath) -> FsResult<()> {
        let path = path.to_owned();
        self.run(move |tx| {
            let (parent, name) = tx.resolve_parent(&path)?;
            let ino = tx.child(parent, &name)?.ok_or(ErrorCode::NoEntry)?;
            if tx.node(ino)?.kind == KIND_DIR {
                return Err(ErrorCode::IsDirectory);
            }
            tx.unlink(parent, &name)
        })
        .await
    }

    async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from = from.to_owned();
        let to = to.to_owned();
        self.run(move |tx| {
            let (from_parent, from_name) = tx.resolve_parent(&from)?;
            let (to_parent, to_name) = tx.resolve_parent(&to)?;
            let ino = tx
                .child(from_parent, &from_name)?
                .ok_or(ErrorCode::NoEntry)?;

            // POSIX: renaming a name onto another name for the same object
            // does nothing.
            if tx.child(to_parent, &to_name)? == Some(ino) {
                return Ok(());
            }

            // Refuse to move a directory inside itself. Must precede every
            // mutation: a refused rename may not have side effects.
            let source_is_dir = tx.node(ino)?.kind == KIND_DIR;
            if source_is_dir && tx.is_ancestor(ino, to_parent)? {
                return Err(ErrorCode::Invalid);
            }

            if let Some(existing) = tx.child(to_parent, &to_name)? {
                match tx.node(existing)?.kind {
                    KIND_DIR => {
                        if !source_is_dir {
                            return Err(ErrorCode::IsDirectory);
                        }
                        let occupied: bool = tx
                            .tx
                            .query_row(
                                "SELECT EXISTS (SELECT 1 FROM dirents WHERE parent = ?1)",
                                [existing],
                                |row| row.get(0),
                            )
                            .map_err(db_err)?;
                        if occupied {
                            return Err(ErrorCode::NotEmpty);
                        }
                    }
                    _ if source_is_dir => return Err(ErrorCode::NotDirectory),
                    _ => {}
                }
                tx.unlink(to_parent, &to_name)?;
            }

            tx.detach(from_parent, &from_name)?;
            tx.attach(to_parent, &to_name, ino)?;
            Ok(())
        })
        .await
    }

    async fn symlink(&self, target: &str, link: &FsPath) -> FsResult<()> {
        if target.starts_with('/') {
            // WASI: a rooted target is refused at creation.
            return Err(ErrorCode::NotPermitted);
        }
        let target = target.to_owned();
        let link = link.to_owned();
        self.run(move |tx| {
            let (parent, name) = tx.resolve_parent(&link)?;
            if tx.child(parent, &name)?.is_some() {
                return Err(ErrorCode::Exist);
            }
            let ino = tx.alloc(KIND_SYMLINK, Some(&target), None)?;
            tx.link(parent, &name, ino)
        })
        .await
    }

    async fn readlink(&self, path: &FsPath) -> FsResult<String> {
        let path = path.to_owned();
        self.run(move |tx| {
            let node = tx.node(tx.resolve(&path, false)?)?;
            match node.kind {
                KIND_SYMLINK => Ok(node.target.unwrap_or_default()),
                _ => Err(ErrorCode::Invalid),
            }
        })
        .await
    }

    async fn hard_link(&self, from: &FsPath, follow: bool, to: &FsPath) -> FsResult<()> {
        let from = from.to_owned();
        let to = to.to_owned();
        self.run(move |tx| {
            let ino = tx.resolve(&from, follow)?;
            if tx.node(ino)?.kind == KIND_DIR {
                return Err(ErrorCode::NotPermitted);
            }
            let (parent, name) = tx.resolve_parent(&to)?;
            if tx.child(parent, &name)?.is_some() {
                return Err(ErrorCode::Exist);
            }
            tx.link(parent, &name, ino)
        })
        .await
    }

    async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash> {
        let path = path.to_owned();
        self.run(move |tx| {
            Ok(MetadataHash::from_stat(
                &tx.stat(tx.resolve(&path, follow)?)?,
            ))
        })
        .await
    }

    async fn object_id_at(&self, path: &FsPath, follow: bool) -> FsResult<ObjectId> {
        let path = path.to_owned();
        self.run(move |tx| Ok(ObjectId(tx.resolve(&path, follow)? as u128)))
            .await
    }
}

/// An open file in a [`SqliteFilesystem`].
struct SqliteFile {
    fs: SqliteFilesystem,
    ino: i64,
}

impl Drop for SqliteFile {
    fn drop(&mut self) {
        let mut handles = self
            .fs
            .db
            .open_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let remaining = handles
            .entry(self.ino)
            .and_modify(|n| *n = n.saturating_sub(1))
            .or_insert(0);
        if *remaining == 0 {
            handles.remove(&self.ino);
            drop(handles);
            // Reclaim the row if the file was unlinked while open. Local
            // SQLite deletes are quick; contention just waits on the mutex.
            let conn = self.fs.db.conn.lock().unwrap_or_else(|e| e.into_inner());
            let _ = conn.execute("DELETE FROM nodes WHERE ino = ?1 AND nlink = 0", [self.ino]);
        }
    }
}

#[async_trait]
impl File for SqliteFile {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> FsResult<usize> {
        let ino = self.ino;
        let wanted = buf.len();
        let data: Vec<u8> = self
            .fs
            .run(move |tx| {
                let len = tx.file_len(ino)?;
                let Ok(offset) = usize::try_from(offset) else {
                    return Ok(Vec::new());
                };
                if offset >= len || wanted == 0 {
                    return Ok(Vec::new());
                }
                let take = wanted.min(len - offset);
                // substr is 1-indexed and clamps for us; the arithmetic above
                // only bounds the copy.
                tx.tx
                    .query_row(
                        "SELECT substr(data, ?1, ?2) FROM nodes WHERE ino = ?3",
                        params![(offset + 1) as i64, take as i64, ino],
                        |row| row.get(0),
                    )
                    .map_err(db_err)
            })
            .await?;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);
        Ok(n)
    }

    async fn write_at(&self, buf: &[u8], offset: u64) -> FsResult<usize> {
        if buf.is_empty() {
            // POSIX: a zero-length positional write is a no-op.
            let ino = self.ino;
            return self.fs.run(move |tx| tx.file_len(ino).map(|_| 0)).await;
        }
        let ino = self.ino;
        let buf = buf.to_vec();
        self.fs
            .run(move |tx| {
                let mut data = tx.read_data(ino)?;
                let offset = usize::try_from(offset).map_err(|_| ErrorCode::FileTooLarge)?;
                let end = offset
                    .checked_add(buf.len())
                    .ok_or(ErrorCode::FileTooLarge)?;
                if end > data.len() {
                    data.resize(end, 0);
                }
                data[offset..end].copy_from_slice(&buf);
                tx.write_data(ino, &data)?;
                Ok(buf.len())
            })
            .await
    }

    async fn append(&self, buf: &[u8]) -> FsResult<usize> {
        let ino = self.ino;
        let buf = buf.to_vec();
        // Read-modify-write rather than SQL `||`: SQLite's concatenation is
        // string concatenation, which coerces BLOBs to TEXT.
        self.fs
            .run(move |tx| {
                let mut data = tx.read_data(ino)?;
                data.extend_from_slice(&buf);
                tx.write_data(ino, &data)?;
                Ok(buf.len())
            })
            .await
    }

    async fn stat(&self) -> FsResult<Stat> {
        let ino = self.ino;
        self.fs.run(move |tx| tx.stat(ino)).await
    }

    async fn set_size(&self, size: u64) -> FsResult<()> {
        let ino = self.ino;
        self.fs
            .run(move |tx| {
                let mut data = tx.read_data(ino)?;
                let size = usize::try_from(size).map_err(|_| ErrorCode::FileTooLarge)?;
                data.resize(size, 0);
                tx.write_data(ino, &data)
            })
            .await
    }

    async fn set_times(&self, times: SetTimes) -> FsResult<()> {
        let ino = self.ino;
        self.fs.run(move |tx| tx.apply_times(ino, times)).await
    }

    async fn sync(&self) -> FsResult<()> {
        // Transactions are durable at commit; nothing further to flush.
        Ok(())
    }

    async fn sync_data(&self) -> FsResult<()> {
        Ok(())
    }

    async fn metadata_hash(&self) -> FsResult<MetadataHash> {
        let ino = self.ino;
        self.fs
            .run(move |tx| Ok(MetadataHash::from_stat(&tx.stat(ino)?)))
            .await
    }

    async fn object_id(&self) -> FsResult<ObjectId> {
        Ok(ObjectId(self.ino as u128))
    }
}

fn descriptor_type(kind: i64) -> DescriptorType {
    match kind {
        KIND_DIR => DescriptorType::Directory,
        KIND_SYMLINK => DescriptorType::SymbolicLink,
        _ => DescriptorType::RegularFile,
    }
}

/// Splits a [`SystemTime`] into floored seconds and forward-counting
/// nanoseconds relative to the epoch, so pre-epoch times store losslessly.
fn encode_time(time: SystemTime) -> (i64, i64) {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(after) => (
            i64::try_from(after.as_secs()).unwrap_or(i64::MAX),
            after.subsec_nanos() as i64,
        ),
        Err(err) => {
            let before = err.duration();
            let secs = i64::try_from(before.as_secs()).unwrap_or(i64::MAX);
            if before.subsec_nanos() == 0 {
                (-secs, 0)
            } else {
                (
                    -(secs.saturating_add(1)),
                    (1_000_000_000 - before.subsec_nanos()) as i64,
                )
            }
        }
    }
}

fn decode_time(s: i64, ns: i64) -> SystemTime {
    let ns = ns.clamp(0, 999_999_999) as u32;
    if s >= 0 {
        SystemTime::UNIX_EPOCH + Duration::new(s as u64, ns)
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_secs(s.unsigned_abs()) + Duration::new(0, ns)
    }
}

fn db_err(err: rusqlite::Error) -> ErrorCode {
    if let rusqlite::Error::SqliteFailure(e, _) = &err
        && e.code == rusqlite::ErrorCode::DiskFull
    {
        // SQLITE_FULL covers both a full disk and a reached max_page_count.
        return ErrorCode::InsufficientSpace;
    }
    tracing::warn!("sqlite filesystem error: {err}");
    ErrorCode::Io
}

/// The `sqlite` filesystem type for runtime config: a database file path,
/// resolved like the `host` type's directory path.
pub struct SqliteFilesystemMaker {
    base_path: Option<PathBuf>,
}

impl SqliteFilesystemMaker {
    /// Creates a maker resolving relative `path`s against `base_path`,
    /// conventionally the runtime config directory.
    pub fn new(base_path: Option<PathBuf>) -> Self {
        Self { base_path }
    }
}

/// Configuration for a `type = "sqlite"` filesystem.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteFilesystemRuntimeConfig {
    /// The database file holding the filesystem. Created if absent.
    pub path: PathBuf,
    /// Whether mounts may mutate the filesystem. Defaults to read-only.
    #[serde(default)]
    pub writable: bool,
    /// A cap, in bytes, on the database size; growth past it reports
    /// `insufficient-space` to the guest.
    #[serde(default)]
    pub maximum_size: Option<u64>,
}

impl MakeFilesystem for SqliteFilesystemMaker {
    const RUNTIME_CONFIG_TYPE: &'static str = "sqlite";
    type RuntimeConfig = SqliteFilesystemRuntimeConfig;

    fn make_filesystem(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<FilesystemDefinition> {
        let path = match &self.base_path {
            Some(base) if runtime_config.path.is_relative() => base.join(&runtime_config.path),
            _ => runtime_config.path.clone(),
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create parent directory {}", parent.display())
            })?;
        }
        let filesystem = SqliteFilesystem::open_file(&path)?;
        if let Some(bytes) = runtime_config.maximum_size {
            filesystem.set_maximum_size(bytes)?;
        }
        Ok(FilesystemDefinition {
            filesystem: Arc::new(filesystem),
            writable: runtime_config.writable,
        })
    }
}

// Keep FsPathBuf referenced: `run` closures own their paths.
const _: fn(&FsPath) -> FsPathBuf = FsPath::to_owned;
