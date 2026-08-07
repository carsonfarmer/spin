//! An object-store-backed [`Filesystem`] for Spin's filesystem factor, with
//! Amazon S3 (and S3-compatible endpoints) wired up as runtime-config type
//! `s3`.
//!
//! # Layout
//!
//! A mount maps onto a bucket under a fixed key prefix. A file at guest path
//! `a/b.txt` is the object `<prefix>/a/b.txt`; a directory is the marker
//! object `<prefix>/a/dir/.spin-dir` (so empty directories exist) plus
//! whatever lives under its key prefix. Listings use delimited list requests;
//! reads use ranged GETs; every mutation writes whole objects, so a file is
//! always a single, atomically-replaced object - the natural unit of
//! consistency an object store offers.
//!
//! # Multi-tenant isolation
//!
//! The mount's boundary is `(credentials, bucket, prefix)`, and the design
//! assumes one tenant per mount:
//!
//! - Every key this backend constructs starts with the configured prefix, and
//!   path resolution refuses `..` past the mount root before any request is
//!   made, so one mount cannot name another mount's keys.
//! - Per-mount credentials mean the *store* enforces the same boundary: scope
//!   each tenant's credentials to its prefix (see the crate README for an IAM
//!   policy shape) and the isolation holds at the service even if a future
//!   code path misbehaves - two independent walls, either sufficient.
//! - Nothing is cached across requests, so revoking credentials or moving a
//!   prefix takes effect at the store's own consistency, not ours.
//!
//! # Semantics
//!
//! This backend meets the factor's `core` conformance tier: files,
//! directories, rename (including clobber and cycle refusal), and sandbox
//! confinement. Object stores have no hard links, symlinks, or settable
//! timestamps, so those operations report `unsupported`; unlinking a file
//! that a handle still has open deletes the object, so later reads through
//! the handle report `no-entry` (unlike the inode-based backends). Renaming
//! a directory moves every object under its prefix, one copy at a time -
//! correct, but not atomic and proportional to the subtree, exactly as `aws
//! s3 mv --recursive` behaves. Concurrent writers to one file follow
//! last-put-wins.

#![deny(missing_docs)]

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::TryStreamExt as _;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use spin_factor_filesystem::{
    DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, Filesystem, FsPath, FsResult,
    MetadataHash, ObjectId, OpenFlags, OpenOptions, Opened, SetTimes, Stat,
};

mod s3;
pub use s3::{S3FilesystemMaker, S3FilesystemRuntimeConfig};

/// The marker object that keeps an otherwise-empty directory in existence.
/// Invisible in listings and unaddressable as a path component.
const DIR_MARKER: &str = ".spin-dir";

/// The longest permitted name component, as POSIX `NAME_MAX`.
const NAME_MAX: usize = 255;

/// A filesystem over any [`ObjectStore`] implementation.
///
/// Cloning is shallow: clones share the same store handle.
#[derive(Clone)]
pub struct ObjectStoreFilesystem {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<dyn ObjectStore>,
    /// The key prefix under which this filesystem lives; every key is built
    /// beneath it. Empty parts are not representable in [`StorePath`], so
    /// this is the whole containment story on our side.
    prefix: StorePath,
    summary: String,
}

/// What a key currently is, as far as the store knows.
enum EntryKind {
    Missing,
    File,
    Dir,
}

impl ObjectStoreFilesystem {
    /// Creates a filesystem over `store`, rooted at `prefix` (`""` for the
    /// bucket root).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str, summary: String) -> anyhow::Result<Self> {
        // `StorePath::parse` refuses what S3 keys cannot hold and normalizes
        // separators; an invalid prefix is a configuration error.
        let prefix = StorePath::parse(prefix)?;
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                prefix,
                summary,
            }),
        })
    }

    fn store(&self) -> &dyn ObjectStore {
        self.inner.store.as_ref()
    }

    /// The store key for resolved path components.
    fn key(&self, components: &[String]) -> StorePath {
        let mut key = self.inner.prefix.clone();
        for component in components {
            key = key.child(component.as_str());
        }
        key
    }

    /// Resolves `path` lexically against the mount root, verifying every
    /// intermediate component is an existing directory, and returns the
    /// final component list (which may or may not exist).
    ///
    /// `..` never rises above the root (`not-permitted`), matching the other
    /// backends; a component that exists as a file mid-path is
    /// `not-directory`; a missing intermediate is `no-entry`. The marker
    /// object's name is unaddressable.
    async fn resolve(&self, path: &FsPath) -> FsResult<Vec<String>> {
        let components: Vec<&str> = path.components().collect();
        let mut stack: Vec<String> = Vec::new();
        for (index, component) in components.iter().enumerate() {
            if *component == ".." {
                if stack.pop().is_none() {
                    return Err(ErrorCode::NotPermitted);
                }
                continue;
            }
            if component.len() > NAME_MAX {
                return Err(ErrorCode::NameTooLong);
            }
            if *component == DIR_MARKER {
                return Err(ErrorCode::NoEntry);
            }
            let is_final = index + 1 == components.len();
            if !is_final {
                stack.push((*component).to_string());
                match self.entry_kind(&self.key(&stack)).await? {
                    EntryKind::Dir => {}
                    EntryKind::File => return Err(ErrorCode::NotDirectory),
                    EntryKind::Missing => return Err(ErrorCode::NoEntry),
                }
            } else {
                stack.push((*component).to_string());
            }
        }
        Ok(stack)
    }

    /// Like [`Self::resolve`], but requires the final component to exist,
    /// honouring a trailing-slash directory requirement.
    async fn resolve_existing(&self, path: &FsPath) -> FsResult<(Vec<String>, EntryKind)> {
        let components = self.resolve(path).await?;
        let kind = if components.is_empty() {
            EntryKind::Dir // The root always exists.
        } else {
            self.entry_kind(&self.key(&components)).await?
        };
        match kind {
            EntryKind::Missing => Err(ErrorCode::NoEntry),
            EntryKind::File if path.requires_directory() => Err(ErrorCode::NotDirectory),
            kind => Ok((components, kind)),
        }
    }

    /// Splits `path` into an existing parent directory plus final name.
    async fn resolve_parent(&self, path: &FsPath) -> FsResult<(Vec<String>, String)> {
        let mut components = self.resolve(path).await?;
        let Some(name) = components.pop() else {
            return Err(ErrorCode::NotPermitted);
        };
        if !components.is_empty() {
            match self.entry_kind(&self.key(&components)).await? {
                EntryKind::Dir => {}
                EntryKind::File => return Err(ErrorCode::NotDirectory),
                EntryKind::Missing => return Err(ErrorCode::NoEntry),
            }
        }
        Ok((components, name))
    }

    /// What `key` names right now: an object, a directory (marker or
    /// non-empty prefix), or nothing.
    async fn entry_kind(&self, key: &StorePath) -> FsResult<EntryKind> {
        match self.store().head(key).await {
            Ok(_) => return Ok(EntryKind::File),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(store_err(err)),
        }
        if self.dir_exists(key).await? {
            Ok(EntryKind::Dir)
        } else {
            Ok(EntryKind::Missing)
        }
    }

    async fn dir_exists(&self, key: &StorePath) -> FsResult<bool> {
        match self.store().head(&key.child(DIR_MARKER)).await {
            Ok(_) => return Ok(true),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(store_err(err)),
        }
        // No marker; a directory still exists implicitly if anything lives
        // under its prefix (content written by other tools).
        let mut listing = self.store().list(Some(key));
        match listing.try_next().await {
            Ok(entry) => Ok(entry.is_some()),
            Err(err) => Err(store_err(err)),
        }
    }

    /// True if a directory's prefix holds anything besides its own marker.
    async fn dir_occupied(&self, key: &StorePath) -> FsResult<bool> {
        let marker = key.child(DIR_MARKER);
        let mut listing = self.store().list(Some(key));
        while let Some(meta) = listing.try_next().await.map_err(store_err)? {
            if meta.location != marker {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn read_object(&self, key: &StorePath) -> FsResult<Vec<u8>> {
        let result = self.store().get(key).await.map_err(store_err)?;
        Ok(result.bytes().await.map_err(store_err)?.to_vec())
    }

    async fn put_object(&self, key: &StorePath, data: Vec<u8>) -> FsResult<()> {
        self.store()
            .put(key, PutPayload::from(data))
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

#[async_trait]
impl Filesystem for ObjectStoreFilesystem {
    fn summary(&self) -> String {
        self.inner.summary.clone()
    }

    async fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Opened> {
        let creating = opts.open_flags.contains(OpenFlags::CREATE);
        let exclusive = opts.open_flags.contains(OpenFlags::EXCLUSIVE);
        let want_dir = opts.open_flags.contains(OpenFlags::DIRECTORY);
        let want_write = opts.flags.contains(DescriptorFlags::WRITE)
            || opts.open_flags.contains(OpenFlags::TRUNCATE);

        let components = self.resolve(path).await?;
        let kind = if components.is_empty() {
            EntryKind::Dir
        } else {
            self.entry_kind(&self.key(&components)).await?
        };
        let key = self.key(&components);

        match kind {
            EntryKind::Dir => {
                // POSIX: `O_CREAT|O_EXCL` on an existing directory is
                // `EEXIST`; plain `O_CREAT` is `EISDIR`; and a directory
                // cannot be opened for byte I/O.
                if creating && exclusive {
                    return Err(ErrorCode::Exist);
                }
                if creating || want_write {
                    return Err(ErrorCode::IsDirectory);
                }
                Ok(Opened::Dir)
            }
            EntryKind::File => {
                if creating && exclusive {
                    return Err(ErrorCode::Exist);
                }
                if want_dir || path.requires_directory() {
                    return Err(ErrorCode::NotDirectory);
                }
                if opts.open_flags.contains(OpenFlags::TRUNCATE) {
                    self.put_object(&key, Vec::new()).await?;
                }
                Ok(Opened::File(Arc::new(ObjectStoreFile {
                    fs: self.clone(),
                    key,
                })))
            }
            EntryKind::Missing => {
                if !creating {
                    return Err(ErrorCode::NoEntry);
                }
                if want_dir {
                    return Err(ErrorCode::NoEntry);
                }
                if path.requires_directory() {
                    return Err(ErrorCode::IsDirectory);
                }
                // The parent must already exist as a directory.
                let mut parent = components.clone();
                parent.pop();
                if !parent.is_empty() && !self.dir_exists(&self.key(&parent)).await? {
                    return Err(ErrorCode::NoEntry);
                }
                // With `exclusive`, creation is atomic at the store: put
                // with if-none-match semantics refuses a concurrent winner.
                let options = PutOptions {
                    mode: if exclusive {
                        PutMode::Create
                    } else {
                        PutMode::Overwrite
                    },
                    ..Default::default()
                };
                match self
                    .store()
                    .put_opts(&key, PutPayload::default(), options)
                    .await
                {
                    Ok(_) => {}
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        return Err(ErrorCode::Exist);
                    }
                    Err(err) => return Err(store_err(err)),
                }
                Ok(Opened::File(Arc::new(ObjectStoreFile {
                    fs: self.clone(),
                    key,
                })))
            }
        }
    }

    async fn stat_at(&self, path: &FsPath, _follow: bool) -> FsResult<Stat> {
        let (components, kind) = self.resolve_existing(path).await?;
        match kind {
            EntryKind::Dir => Ok(Stat {
                type_: DescriptorType::Directory,
                link_count: 1,
                size: 0,
                data_access_timestamp: None,
                data_modification_timestamp: None,
                status_change_timestamp: None,
            }),
            _ => {
                let meta = self
                    .store()
                    .head(&self.key(&components))
                    .await
                    .map_err(store_err)?;
                Ok(object_stat(&meta))
            }
        }
    }

    async fn set_times_at(&self, path: &FsPath, _follow: bool, _times: SetTimes) -> FsResult<()> {
        // Object stores own their timestamps; there is nothing to set.
        self.resolve_existing(path).await?;
        Err(ErrorCode::Unsupported)
    }

    async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let (components, kind) = self.resolve_existing(path).await?;
        if !matches!(kind, EntryKind::Dir) {
            return Err(ErrorCode::NotDirectory);
        }
        let key = self.key(&components);
        let listing = self
            .store()
            .list_with_delimiter(
                if components.is_empty() && self.inner.prefix.as_ref().is_empty() {
                    None
                } else {
                    Some(&key)
                },
            )
            .await
            .map_err(store_err)?;

        let mut entries = Vec::new();
        for prefix in listing.common_prefixes {
            if let Some(name) = last_part(&prefix) {
                entries.push(DirEntry {
                    type_: DescriptorType::Directory,
                    name,
                });
            }
        }
        for object in listing.objects {
            let Some(name) = last_part(&object.location) else {
                continue;
            };
            if name == DIR_MARKER {
                continue;
            }
            entries.push(DirEntry {
                type_: DescriptorType::RegularFile,
                name,
            });
        }
        Ok(entries)
    }

    async fn create_dir(&self, path: &FsPath) -> FsResult<()> {
        let (parent, name) = self.resolve_parent(path).await?;
        let mut components = parent;
        components.push(name);
        let key = self.key(&components);
        if !matches!(self.entry_kind(&key).await?, EntryKind::Missing) {
            return Err(ErrorCode::Exist);
        }
        self.put_object(&key.child(DIR_MARKER), Vec::new()).await
    }

    async fn remove_dir(&self, path: &FsPath) -> FsResult<()> {
        let (components, kind) = self.resolve_existing(path).await?;
        if components.is_empty() {
            // The mount root itself cannot be removed.
            return Err(ErrorCode::NotPermitted);
        }
        if !matches!(kind, EntryKind::Dir) {
            return Err(ErrorCode::NotDirectory);
        }
        let key = self.key(&components);
        if self.dir_occupied(&key).await? {
            return Err(ErrorCode::NotEmpty);
        }
        match self.store().delete(&key.child(DIR_MARKER)).await {
            Ok(()) => Ok(()),
            // An implicit directory (content but no marker) that emptied out
            // has nothing to delete; removing it is fine.
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(store_err(err)),
        }
    }

    async fn unlink(&self, path: &FsPath) -> FsResult<()> {
        let (components, kind) = self.resolve_existing(path).await?;
        match kind {
            EntryKind::Dir => Err(ErrorCode::IsDirectory),
            EntryKind::Missing => Err(ErrorCode::NoEntry),
            EntryKind::File => {
                let key = self.key(&components);
                match self.store().delete(&key).await {
                    Ok(()) => Ok(()),
                    Err(object_store::Error::NotFound { .. }) => Err(ErrorCode::NoEntry),
                    Err(err) => Err(store_err(err)),
                }
            }
        }
    }

    async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let (from_parent, from_name) = self.resolve_parent(from).await?;
        let (to_parent, to_name) = self.resolve_parent(to).await?;
        let mut from_components = from_parent;
        from_components.push(from_name);
        let mut to_components = to_parent;
        to_components.push(to_name);

        let from_key = self.key(&from_components);
        let to_key = self.key(&to_components);
        if from_key == to_key {
            return Ok(());
        }

        let from_kind = self.entry_kind(&from_key).await?;
        let to_kind = self.entry_kind(&to_key).await?;

        match from_kind {
            EntryKind::Missing => Err(ErrorCode::NoEntry),
            EntryKind::File => {
                if matches!(to_kind, EntryKind::Dir) {
                    return Err(ErrorCode::IsDirectory);
                }
                // Clobbering rename of one object: copy then delete, the
                // store's native shape for a move.
                self.store()
                    .rename(&from_key, &to_key)
                    .await
                    .map_err(store_err)
            }
            EntryKind::Dir => {
                // A directory cannot move inside its own subtree. The check
                // is lexical: object keys are the whole hierarchy here.
                if to_components.starts_with(&from_components) {
                    return Err(ErrorCode::Invalid);
                }
                match to_kind {
                    EntryKind::File => return Err(ErrorCode::NotDirectory),
                    EntryKind::Dir => {
                        if self.dir_occupied(&to_key).await? {
                            return Err(ErrorCode::NotEmpty);
                        }
                        // Replace the empty destination wholesale.
                        match self.store().delete(&to_key.child(DIR_MARKER)).await {
                            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                            Err(err) => return Err(store_err(err)),
                        }
                    }
                    EntryKind::Missing => {}
                }
                // Move every object under the prefix, marker included. Not
                // atomic - object stores have no multi-key transactions - and
                // proportional to the subtree, like `aws s3 mv --recursive`.
                let objects: Vec<StorePath> = self
                    .store()
                    .list(Some(&from_key))
                    .map_ok(|meta| meta.location)
                    .try_collect()
                    .await
                    .map_err(store_err)?;
                let from_str = from_key.as_ref();
                for location in objects {
                    let suffix = location
                        .as_ref()
                        .strip_prefix(from_str)
                        .and_then(|s| s.strip_prefix('/'))
                        .unwrap_or("");
                    let destination = StorePath::parse(format!("{to_key}/{suffix}"))
                        .map_err(|_| ErrorCode::Io)?;
                    self.store()
                        .rename(&location, &destination)
                        .await
                        .map_err(store_err)?;
                }
                // An implicit source directory has no marker; guarantee the
                // destination exists even then.
                if !self.dir_exists(&to_key).await? {
                    self.put_object(&to_key.child(DIR_MARKER), Vec::new())
                        .await?;
                }
                Ok(())
            }
        }
    }

    async fn symlink(&self, _target: &str, _link: &FsPath) -> FsResult<()> {
        Err(ErrorCode::Unsupported)
    }

    async fn readlink(&self, path: &FsPath) -> FsResult<String> {
        // No symlinks exist, so a resolvable path is "not a symlink" and an
        // unresolvable one is `no-entry`.
        self.resolve_existing(path).await?;
        Err(ErrorCode::Invalid)
    }

    async fn hard_link(&self, _from: &FsPath, _follow: bool, _to: &FsPath) -> FsResult<()> {
        Err(ErrorCode::Unsupported)
    }

    async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash> {
        Ok(MetadataHash::from_stat(&self.stat_at(path, follow).await?))
    }

    async fn object_id_at(&self, path: &FsPath, _follow: bool) -> FsResult<ObjectId> {
        let (components, _) = self.resolve_existing(path).await?;
        Ok(key_id(&self.key(&components)))
    }
}

/// An open file: a key, read and written whole-object.
struct ObjectStoreFile {
    fs: ObjectStoreFilesystem,
    key: StorePath,
}

#[async_trait]
impl File for ObjectStoreFile {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> FsResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let meta = match self.fs.store().head(&self.key).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Err(ErrorCode::NoEntry),
            Err(err) => return Err(store_err(err)),
        };
        if offset >= meta.size {
            return Ok(0);
        }
        let end = meta.size.min(offset.saturating_add(buf.len() as u64));
        let bytes = self
            .fs
            .store()
            .get_range(&self.key, offset..end)
            .await
            .map_err(store_err)?;
        let n = bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&bytes[..n]);
        Ok(n)
    }

    async fn write_at(&self, buf: &[u8], offset: u64) -> FsResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Whole-object read-modify-write, written through immediately: the
        // object is always a complete, consistent value at the store.
        let mut data = self.fs.read_object(&self.key).await?;
        let offset = usize::try_from(offset).map_err(|_| ErrorCode::FileTooLarge)?;
        let end = offset
            .checked_add(buf.len())
            .ok_or(ErrorCode::FileTooLarge)?;
        if end > data.len() {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(buf);
        self.fs.put_object(&self.key, data).await?;
        Ok(buf.len())
    }

    async fn append(&self, buf: &[u8]) -> FsResult<usize> {
        let mut data = self.fs.read_object(&self.key).await?;
        data.extend_from_slice(buf);
        self.fs.put_object(&self.key, data).await?;
        Ok(buf.len())
    }

    async fn stat(&self) -> FsResult<Stat> {
        let meta = match self.fs.store().head(&self.key).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Err(ErrorCode::NoEntry),
            Err(err) => return Err(store_err(err)),
        };
        Ok(object_stat(&meta))
    }

    async fn set_size(&self, size: u64) -> FsResult<()> {
        let mut data = self.fs.read_object(&self.key).await?;
        let size = usize::try_from(size).map_err(|_| ErrorCode::FileTooLarge)?;
        data.resize(size, 0);
        self.fs.put_object(&self.key, data).await
    }

    async fn set_times(&self, _times: SetTimes) -> FsResult<()> {
        Err(ErrorCode::Unsupported)
    }

    async fn sync(&self) -> FsResult<()> {
        // Writes go through on every mutation; there is nothing buffered.
        Ok(())
    }

    async fn sync_data(&self) -> FsResult<()> {
        Ok(())
    }

    async fn metadata_hash(&self) -> FsResult<MetadataHash> {
        Ok(MetadataHash::from_stat(&self.stat().await?))
    }

    async fn object_id(&self) -> FsResult<ObjectId> {
        Ok(key_id(&self.key))
    }
}

fn object_stat(meta: &object_store::ObjectMeta) -> Stat {
    let modified: SystemTime = meta.last_modified.into();
    Stat {
        type_: DescriptorType::RegularFile,
        link_count: 1,
        size: meta.size,
        data_access_timestamp: None,
        data_modification_timestamp: Some(modified),
        status_change_timestamp: Some(modified),
    }
}

/// A stable identity for a key: two independent hashes of the full key
/// string. Renames change a file's identity here - the key *is* the object.
fn key_id(key: &StorePath) -> ObjectId {
    use std::hash::{Hash as _, Hasher as _};
    let mut low = std::hash::DefaultHasher::new();
    key.as_ref().hash(&mut low);
    let mut high = std::hash::DefaultHasher::new();
    (key.as_ref(), 1u8).hash(&mut high);
    ObjectId(((high.finish() as u128) << 64) | low.finish() as u128)
}

fn last_part(path: &StorePath) -> Option<String> {
    path.parts().last().map(|part| part.as_ref().to_string())
}

fn store_err(err: object_store::Error) -> ErrorCode {
    match err {
        object_store::Error::NotFound { .. } => ErrorCode::NoEntry,
        object_store::Error::AlreadyExists { .. } => ErrorCode::Exist,
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => ErrorCode::Access,
        err => {
            tracing::warn!("object store error: {err}");
            ErrorCode::Io
        }
    }
}
