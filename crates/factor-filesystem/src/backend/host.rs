//! A filesystem backed by a real directory on the host.
//!
//! This is the backend behind `files = [...]` mounts, so it has to be
//! semantically identical to what Spin does today. It is therefore built on
//! `cap-std` - the same primitive `wasmtime-wasi` uses - which does the
//! sandboxing for us: `cap_std::fs::Dir` refuses to resolve a path outside
//! itself, including through `..` and symbolic links, so the invariant
//! [`Filesystem`] asks backends to uphold is enforced by the same
//! well-audited code that enforces it for `wasmtime up` today.
//!
//! Blocking syscalls run on `tokio`'s blocking pool, again matching
//! `wasmtime-wasi`.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt, SystemTimeSpec};
use cap_std::fs::{Dir, FileExt, OpenOptions as CapOpenOptions};

use crate::spi::{
    DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, Filesystem, FsPath, FsResult,
    MetadataHash, NewTimestamp, ObjectId, OpenFlags, OpenOptions, Opened, SetTimes, Stat,
};

/// A filesystem rooted at a directory on the host.
pub struct HostFilesystem {
    dir: Arc<Dir>,
    /// The host path this was opened from. Only used for diagnostics; all
    /// access goes through `dir`.
    display_path: PathBuf,
}

impl HostFilesystem {
    /// Opens `path` on the host as a filesystem root.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let display_path = path.into();
        let dir = Dir::open_ambient_dir(&display_path, cap_std::ambient_authority())?;
        Ok(Self {
            dir: Arc::new(dir),
            display_path,
        })
    }

    /// Wraps an already-open `cap-std` directory.
    pub fn from_dir(dir: Dir, display_path: impl Into<PathBuf>) -> Self {
        Self {
            dir: Arc::new(dir),
            display_path: display_path.into(),
        }
    }

    /// Runs a blocking filesystem operation off the async runtime's worker
    /// threads.
    async fn blocking<F, R>(&self, f: F) -> FsResult<R>
    where
        F: FnOnce(&Dir) -> io::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let dir = Arc::clone(&self.dir);
        spawn_blocking(move || f(&dir)).await
    }
}

/// Runs `f` on the blocking pool, mapping both its error and a panic/cancel of
/// the pool task into an [`ErrorCode`].
async fn spawn_blocking<F, R>(f: F) -> FsResult<R>
where
    F: FnOnce() -> io::Result<R> + Send + 'static,
    R: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(Into::into),
        Err(err) if err.is_cancelled() => Err(ErrorCode::Interrupted),
        Err(err) => std::panic::resume_unwind(err.into_panic()),
    }
}

#[async_trait]
impl Filesystem for HostFilesystem {
    fn summary(&self) -> String {
        format!("host: {}", self.display_path.display())
    }

    async fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Opened> {
        let path = path.as_std_path().to_owned();

        let mut cap_opts = CapOpenOptions::new();
        cap_opts
            .read(opts.flags.contains(DescriptorFlags::READ))
            .follow(if opts.follow_symlinks {
                FollowSymlinks::Yes
            } else {
                FollowSymlinks::No
            });
        if opts.flags.contains(DescriptorFlags::WRITE) {
            cap_opts.write(true);
        } else if opts.open_flags.contains(OpenFlags::TRUNCATE)
            || opts.open_flags.contains(OpenFlags::CREATE)
        {
            // `O_TRUNC` and `O_CREAT` need write access at the OS level even
            // when the guest only asked to read the file afterwards -
            // `O_CREAT|O_RDONLY` is the most common creating open wasi-libc
            // emits. What the guest may then do with the descriptor is the
            // descriptor layer's business, not the OS handle's.
            cap_opts.write(true);
        }
        cap_opts
            .create(opts.open_flags.contains(OpenFlags::CREATE))
            .create_new(
                opts.open_flags.contains(OpenFlags::CREATE)
                    && opts.open_flags.contains(OpenFlags::EXCLUSIVE),
            )
            .truncate(opts.open_flags.contains(OpenFlags::TRUNCATE));

        let want_dir = opts.open_flags.contains(OpenFlags::DIRECTORY);
        let want_write = opts.flags.contains(DescriptorFlags::WRITE)
            || opts.open_flags.contains(OpenFlags::TRUNCATE);
        let create = opts.open_flags.contains(OpenFlags::CREATE);
        let create_new = create && opts.open_flags.contains(OpenFlags::EXCLUSIVE);
        let follow = opts.follow_symlinks;

        let opened = self
            .blocking(move |dir| {
                // Decide file-vs-directory up front: opening a directory with
                // the file options above fails on some platforms and succeeds
                // uselessly on others.
                let meta = if follow {
                    dir.metadata(&path)
                } else {
                    dir.symlink_metadata(&path)
                };

                // POSIX: `O_NOFOLLOW` on a trailing symlink is `ELOOP`, and
                // it wins over `O_DIRECTORY`'s `ENOTDIR` - matching Linux and
                // wasmtime-wasi. (With `create`, `O_NOFOLLOW` still refuses
                // the link rather than creating anything.) Reported as a
                // typed error because `std` has no stable `ELOOP` spelling.
                if !follow && meta.as_ref().is_ok_and(|m| m.file_type().is_symlink()) {
                    return Ok(Err(ErrorCode::Loop));
                }

                let is_dir = match &meta {
                    Ok(meta) => meta.is_dir(),
                    // The target may not exist yet, in which case `create`
                    // makes a regular file.
                    Err(_) => false,
                };

                if is_dir {
                    // POSIX: `O_CREAT|O_EXCL` on an existing directory is
                    // `EEXIST`; plain `O_CREAT` is `EISDIR`.
                    if create_new {
                        return Ok(Err(ErrorCode::Exist));
                    }
                    if create {
                        return Err(io::Error::from(io::ErrorKind::IsADirectory));
                    }
                    if want_write {
                        return Err(io::Error::from(io::ErrorKind::IsADirectory));
                    }
                    // A directory descriptor is just `(filesystem, path)`, so
                    // this open is purely a permission and existence check.
                    if follow {
                        dir.open_dir(&path)?;
                    } else {
                        dir.open_dir_nofollow(&path)?;
                    }
                    Ok(Ok(None))
                } else {
                    if want_dir {
                        // Report a missing path as missing rather than as "not
                        // a directory".
                        meta?;
                        return Err(io::Error::from(io::ErrorKind::NotADirectory));
                    }
                    Ok(Ok(Some(dir.open_with(&path, &cap_opts)?)))
                }
            })
            .await??;

        Ok(match opened {
            None => Opened::Dir,
            Some(file) => Opened::File(Arc::new(HostFile {
                file: Arc::new(file),
            })),
        })
    }

    async fn stat_at(&self, path: &FsPath, follow: bool) -> FsResult<Stat> {
        let path = path.as_std_path().to_owned();
        self.blocking(move |dir| {
            let meta = if follow {
                dir.metadata(&path)?
            } else {
                dir.symlink_metadata(&path)?
            };
            Ok(stat_from_metadata(&meta))
        })
        .await
    }

    async fn set_times_at(&self, path: &FsPath, follow: bool, times: SetTimes) -> FsResult<()> {
        if times.is_noop() {
            return Ok(());
        }
        let path = path.as_std_path().to_owned();
        let atime = time_spec(times.access);
        let mtime = time_spec(times.modification);
        self.blocking(move |dir| {
            if follow {
                dir.set_times(&path, atime, mtime)
            } else {
                dir.set_symlink_times(&path, atime, mtime)
            }
        })
        .await
    }

    async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let path = path.as_std_path().to_owned();
        self.blocking(move |dir| {
            let entries = dir.read_dir(&path)?;
            let mut out = Vec::new();
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    // Windows surfaces entries whose metadata cannot be read
                    // (`C:\DumpStack.log.tmp` and friends). Skipping them
                    // matches what `wasmtime-wasi` does.
                    Err(err) if is_transient_windows_error(&err) => continue,
                    Err(err) => return Err(err),
                };
                let Ok(name) = entry.file_name().into_string() else {
                    // `wasi:filesystem` names are UTF-8; a name that is not
                    // is `illegal-byte-sequence`, as wasmtime-wasi reports.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "directory entry name is not valid UTF-8",
                    ));
                };
                let type_ = entry.metadata()?.file_type();
                out.push(DirEntry {
                    type_: descriptor_type(&type_),
                    name,
                });
            }
            Ok(out)
        })
        .await
    }

    async fn create_dir(&self, path: &FsPath) -> FsResult<()> {
        let path = path.as_std_path().to_owned();
        self.blocking(move |dir| dir.create_dir(&path)).await
    }

    async fn remove_dir(&self, path: &FsPath) -> FsResult<()> {
        let path = path.as_std_path().to_owned();
        self.blocking(move |dir| dir.remove_dir(&path)).await
    }

    async fn unlink(&self, path: &FsPath) -> FsResult<()> {
        let path = path.as_std_path().to_owned();
        self.blocking(move |dir| dir.remove_file_or_symlink(&path))
            .await
    }

    async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from = from.as_std_path().to_owned();
        let to = to.as_std_path().to_owned();
        self.blocking(move |dir| dir.rename(&from, dir, &to)).await
    }

    async fn symlink(&self, target: &str, link: &FsPath) -> FsResult<()> {
        if target.starts_with('/') {
            // WASI: "If `old-path` starts with `/`, the function fails with
            // `error-code::not-permitted`."
            return Err(ErrorCode::NotPermitted);
        }
        let target = target.to_owned();
        let link = link.as_std_path().to_owned();
        self.blocking(move |dir| dir.symlink(&target, &link)).await
    }

    async fn readlink(&self, path: &FsPath) -> FsResult<String> {
        let path = path.as_std_path().to_owned();
        let target = self
            .blocking(move |dir| dir.read_link(&path))
            .await?
            .into_os_string()
            .into_string()
            .map_err(|_| ErrorCode::IllegalByteSequence)?;
        Ok(target)
    }

    async fn hard_link(&self, from: &FsPath, follow: bool, to: &FsPath) -> FsResult<()> {
        if follow {
            // `cap-std` only exposes `linkat` without `AT_SYMLINK_FOLLOW`
            // (its `hard_link` links the final path object itself, symlink or
            // not). Following the source first cannot be done atomically, so
            // report `invalid` - which is also what wasmtime-wasi does, and
            // harmless in practice: wasi-libc's `link()` always passes
            // `follow: false`.
            return Err(ErrorCode::Invalid);
        }
        let from = from.as_std_path().to_owned();
        let to = to.as_std_path().to_owned();
        self.blocking(move |dir| dir.hard_link(&from, dir, &to))
            .await
    }

    async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash> {
        Ok(MetadataHash::from_stat(&self.stat_at(path, follow).await?))
    }

    async fn object_id_at(&self, path: &FsPath, follow: bool) -> FsResult<ObjectId> {
        let path = path.as_std_path().to_owned();
        self.blocking(move |dir| {
            let meta = if follow {
                dir.metadata(&path)?
            } else {
                dir.symlink_metadata(&path)?
            };
            Ok(object_id(&meta))
        })
        .await
    }
}

/// An open file on the host.
struct HostFile {
    file: Arc<cap_std::fs::File>,
}

impl HostFile {
    async fn blocking<F, R>(&self, f: F) -> FsResult<R>
    where
        F: FnOnce(&cap_std::fs::File) -> io::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let file = Arc::clone(&self.file);
        spawn_blocking(move || f(&file)).await
    }
}

#[async_trait]
impl File for HostFile {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> FsResult<usize> {
        // The blocking pool needs an owned buffer, so read into one and copy
        // back. Callers read in `DEFAULT_BUFFER_CAPACITY`-sized chunks, so this
        // is one allocation per chunk rather than per byte.
        let len = buf.len();
        let mut owned = vec![0u8; len];
        let (n, owned) = self
            .blocking(move |file| {
                let n = file.read_at(&mut owned, offset)?;
                Ok((n, owned))
            })
            .await?;
        buf[..n].copy_from_slice(&owned[..n]);
        Ok(n)
    }

    async fn write_at(&self, buf: &[u8], offset: u64) -> FsResult<usize> {
        let buf = buf.to_vec();
        self.blocking(move |file| file.write_at(&buf, offset)).await
    }

    async fn append(&self, buf: &[u8]) -> FsResult<usize> {
        let buf = buf.to_vec();
        self.blocking(move |file| {
            // The handle was not opened with `O_APPEND` - the guest asks for
            // appends per-call, not per-descriptor - so find the end and write
            // there. Not atomic against a concurrent writer, which is the same
            // caveat every non-kernel filesystem carries.
            let end = file.metadata()?.len();
            file.write_at(&buf, end)
        })
        .await
    }

    async fn stat(&self) -> FsResult<Stat> {
        self.blocking(|file| Ok(stat_from_metadata(&file.metadata()?)))
            .await
    }

    async fn set_size(&self, size: u64) -> FsResult<()> {
        self.blocking(move |file| file.set_len(size)).await
    }

    async fn set_times(&self, times: SetTimes) -> FsResult<()> {
        if times.is_noop() {
            return Ok(());
        }
        self.blocking(move |file| {
            let now = SystemTime::now();
            let mut file_times = std::fs::FileTimes::new();
            match times.access {
                NewTimestamp::NoChange => {}
                NewTimestamp::Now => file_times = file_times.set_accessed(now),
                NewTimestamp::Timestamp(t) => file_times = file_times.set_accessed(t),
            }
            match times.modification {
                NewTimestamp::NoChange => {}
                NewTimestamp::Now => file_times = file_times.set_modified(now),
                NewTimestamp::Timestamp(t) => file_times = file_times.set_modified(t),
            }
            // `std::fs::File::set_times` is the portable spelling of
            // `futimens`; getting there from a `cap-std` handle costs one
            // `dup`, which is fine for an operation this rare.
            file.try_clone()?.into_std().set_times(file_times)
        })
        .await
    }

    async fn sync(&self) -> FsResult<()> {
        self.blocking(|file| file.sync_all()).await
    }

    async fn sync_data(&self) -> FsResult<()> {
        self.blocking(|file| file.sync_data()).await
    }

    async fn metadata_hash(&self) -> FsResult<MetadataHash> {
        Ok(MetadataHash::from_stat(&self.stat().await?))
    }

    async fn object_id(&self) -> FsResult<ObjectId> {
        self.blocking(|file| Ok(object_id(&file.metadata()?))).await
    }
}

fn descriptor_type(ft: &cap_std::fs::FileType) -> DescriptorType {
    use cap_fs_ext::FileTypeExt as _;
    if ft.is_dir() {
        DescriptorType::Directory
    } else if ft.is_symlink() {
        DescriptorType::SymbolicLink
    } else if ft.is_block_device() {
        DescriptorType::BlockDevice
    } else if ft.is_char_device() {
        DescriptorType::CharacterDevice
    } else if ft.is_file() {
        DescriptorType::RegularFile
    } else {
        DescriptorType::Unknown
    }
}

fn stat_from_metadata(meta: &cap_std::fs::Metadata) -> Stat {
    Stat {
        type_: descriptor_type(&meta.file_type()),
        link_count: meta.nlink(),
        size: meta.len(),
        data_access_timestamp: meta.accessed().map(|t| t.into_std()).ok(),
        data_modification_timestamp: meta.modified().map(|t| t.into_std()).ok(),
        status_change_timestamp: meta.created().map(|t| t.into_std()).ok(),
    }
}

fn object_id(meta: &cap_std::fs::Metadata) -> ObjectId {
    // `(dev, ino)` is the POSIX definition of object identity and is exactly
    // what `is-same-object` is asking about.
    ObjectId(((meta.dev() as u128) << 64) | meta.ino() as u128)
}

fn time_spec(time: NewTimestamp) -> Option<SystemTimeSpec> {
    match time {
        NewTimestamp::NoChange => None,
        NewTimestamp::Now => Some(SystemTimeSpec::SymbolicNow),
        NewTimestamp::Timestamp(t) => Some(SystemTimeSpec::Absolute(system_time(t))),
    }
}

fn system_time(time: SystemTime) -> cap_std::time::SystemTime {
    cap_std::time::SystemTime::from_std(time)
}

/// Windows surfaces directory entries whose metadata cannot be read
/// (`C:\DumpStack.log.tmp` and friends). Skipping them matches what
/// `wasmtime-wasi` does; on other platforms nothing is skipped.
#[cfg(windows)]
fn is_transient_windows_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::PermissionDenied | io::ErrorKind::ResourceBusy
    )
}

#[cfg(not(windows))]
fn is_transient_windows_error(_err: &io::Error) -> bool {
    false
}
