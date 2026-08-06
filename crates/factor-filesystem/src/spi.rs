//! The service-provider interface for pluggable filesystems.
//!
//! This module is deliberately free of `wasmtime`, WIT, and bindgen types. It
//! describes a filesystem the way a storage author thinks about one, and the
//! binding layers in [`crate::p2`] and [`crate::p3`] are responsible for
//! translating between it and whichever version of `wasi:filesystem` a guest
//! happens to import.
//!
//! The two traits split responsibilities the same way `wasi:filesystem` does:
//!
//! - [`Filesystem`] owns the *namespace*. Every operation on it names a path
//!   relative to the filesystem's root, mirroring the `*-at` operations that
//!   `wasi:filesystem` performs against a directory descriptor.
//! - [`File`] is an *open regular file*. It has no cursor - all I/O is
//!   positional - so a handle is cheap to share and safe to use concurrently.
//!
//! Directories need no handle type: a directory descriptor is just a
//! filesystem plus a path.

use std::fmt::{self, Debug, Display};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

/// The result of a filesystem operation.
pub type FsResult<T> = Result<T, ErrorCode>;

/// A pluggable filesystem backend.
///
/// # Sandboxing
///
/// An implementation *is* a root. It must never resolve a path to an object
/// outside itself, whether through `..` components or through symbolic links.
///
/// The factor rejects absolute paths before they reach a backend (as
/// `wasi:filesystem` requires), but it deliberately does not pre-resolve `..`,
/// because `a/link/../b` cannot be resolved without knowing where `link`
/// points. Symlink resolution therefore belongs to the backend - it is the only
/// thing that knows what its own links mean.
///
/// Backends that do not support symbolic links can ignore the question
/// entirely: return [`ErrorCode::Unsupported`] from [`Filesystem::symlink`],
/// [`ErrorCode::Invalid`] from [`Filesystem::readlink`], and resolve paths
/// lexically.
///
/// # Errors
///
/// Operations return [`ErrorCode`], which is `wasi:filesystem`'s `error-code`.
/// A backend only ever needs to return the codes that are meaningful for it;
/// nothing requires an object store to have an opinion about `ETXTBSY`.
#[async_trait]
pub trait Filesystem: Send + Sync + 'static {
    /// A short human-readable description, used in diagnostics.
    ///
    /// For example `"host: /var/lib/spin/repos"` or `"memory"`.
    fn summary(&self) -> String;

    /// Opens the object at `path`.
    ///
    /// The permission checks that `wasi:filesystem` specifies against the
    /// *base descriptor* have already been applied by the caller; an
    /// implementation is responsible only for the permissions of the object
    /// itself (for example, refusing to open a read-only backend for writing).
    async fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Opened>;

    /// Returns the attributes of the object at `path`.
    async fn stat_at(&self, path: &FsPath, follow: bool) -> FsResult<Stat>;

    /// Sets the timestamps of the object at `path`.
    async fn set_times_at(&self, path: &FsPath, follow: bool, times: SetTimes) -> FsResult<()>;

    /// Lists the entries of the directory at `path`.
    ///
    /// The `.` and `..` entries must be omitted.
    ///
    /// Entries are returned all at once rather than streamed. Directories big
    /// enough for that to matter are rare, and the simplification is worth a
    /// great deal to backend authors; a streaming variant can be added later
    /// without breaking implementations.
    async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>>;

    /// Creates a directory at `path`.
    async fn create_dir(&self, path: &FsPath) -> FsResult<()>;

    /// Removes the (empty) directory at `path`.
    async fn remove_dir(&self, path: &FsPath) -> FsResult<()>;

    /// Removes the non-directory object at `path`.
    async fn unlink(&self, path: &FsPath) -> FsResult<()>;

    /// Renames `from` to `to`, both relative to this filesystem's root.
    ///
    /// The caller guarantees that both descriptors involved belong to *this*
    /// filesystem; renames that would cross a mount boundary fail with
    /// [`ErrorCode::CrossDevice`] before reaching a backend.
    async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()>;

    /// Creates a symbolic link at `link` whose contents are `target`.
    ///
    /// `target` is uninterpreted text. It is resolved, if ever, at lookup time.
    async fn symlink(&self, target: &str, link: &FsPath) -> FsResult<()>;

    /// Reads the contents of the symbolic link at `path`.
    async fn readlink(&self, path: &FsPath) -> FsResult<String>;

    /// Creates a hard link at `to` referring to the same object as `from`.
    ///
    /// As with [`Filesystem::rename`], both paths are known to belong to this
    /// filesystem.
    async fn hard_link(&self, from: &FsPath, follow: bool, to: &FsPath) -> FsResult<()>;

    /// Returns a hash of the metadata of the object at `path`.
    ///
    /// See [`File::metadata_hash`] for what this is expected to capture.
    async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash>;

    /// Returns a stable identity for the object at `path`.
    ///
    /// Two paths that yield the same [`ObjectId`] within the same filesystem
    /// refer to the same object; this backs `wasi:filesystem`'s
    /// `is-same-object`. Backends with no notion of object identity may return
    /// [`ErrorCode::Unsupported`], in which case `is-same-object` falls back to
    /// comparing paths.
    async fn object_id_at(&self, path: &FsPath, follow: bool) -> FsResult<ObjectId>;
}

/// The result of [`Filesystem::open`].
pub enum Opened {
    /// A regular file, with a handle for subsequent I/O.
    File(Arc<dyn File>),
    /// A directory. Directory descriptors carry no state beyond their path, so
    /// there is nothing to return.
    Dir,
}

/// An open regular file.
///
/// Files have no cursor: all reads and writes name an explicit offset, which
/// is what `wasi:filesystem` exposes to guests and what makes it safe for
/// several streams to be active on one file at once.
#[async_trait]
pub trait File: Send + Sync + 'static {
    /// Reads into `buf` starting at `offset`, returning the number of bytes
    /// read. A return value of `0` means end of file.
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> FsResult<usize>;

    /// Writes `buf` starting at `offset`, returning the number of bytes
    /// written.
    ///
    /// Writing past the end of the file extends it, zero-filling any gap.
    async fn write_at(&self, buf: &[u8], offset: u64) -> FsResult<usize>;

    /// Appends `buf` to the end of the file, returning the number of bytes
    /// written.
    async fn append(&self, buf: &[u8]) -> FsResult<usize>;

    /// Returns this file's attributes.
    async fn stat(&self) -> FsResult<Stat>;

    /// Truncates or extends the file to `size`. Extension zero-fills.
    async fn set_size(&self, size: u64) -> FsResult<()>;

    /// Sets this file's timestamps.
    async fn set_times(&self, times: SetTimes) -> FsResult<()>;

    /// Flushes data and metadata to durable storage.
    async fn sync(&self) -> FsResult<()>;

    /// Flushes data (but not necessarily metadata) to durable storage.
    async fn sync_data(&self) -> FsResult<()>;

    /// Returns a hash of this file's metadata.
    ///
    /// It should change when the file is modified or replaced and should
    /// usually not change otherwise. It is not required to be
    /// cryptographically anything; guests use it to invalidate caches.
    async fn metadata_hash(&self) -> FsResult<MetadataHash>;

    /// Returns a stable identity for this file. See
    /// [`Filesystem::object_id_at`].
    async fn object_id(&self) -> FsResult<ObjectId>;

    /// Passes an access-pattern hint to the backend.
    ///
    /// Purely advisory; the default implementation ignores it.
    async fn advise(&self, offset: u64, len: u64, advice: Advice) -> FsResult<()> {
        let _ = (offset, len, advice);
        Ok(())
    }
}

/// A path relative to the root of a [`Filesystem`].
///
/// An `FsPath` is always relative, always UTF-8, and always `/`-separated,
/// regardless of host platform. It may still contain `.` and `..` components:
/// resolving those is the backend's job, since it cannot be done correctly
/// without following symbolic links.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct FsPath(str);

impl FsPath {
    /// Validates `path` as a filesystem-relative path.
    ///
    /// Fails with [`ErrorCode::NotPermitted`] for absolute paths, which is what
    /// `wasi:filesystem` specifies for a path that "starts with `/`".
    pub fn new(path: &str) -> FsResult<&Self> {
        if path.starts_with('/') {
            return Err(ErrorCode::NotPermitted);
        }
        if path.contains('\0') {
            return Err(ErrorCode::Invalid);
        }
        // SAFETY: `FsPath` is a `#[repr(transparent)]` wrapper around `str`.
        Ok(unsafe { &*(path as *const str as *const FsPath) })
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns true if this path refers to the filesystem root.
    ///
    /// Both the empty string and `"."` name the root.
    pub fn is_root(&self) -> bool {
        self.0.is_empty() || &self.0 == "."
    }

    /// The non-empty, non-`.` components of this path, in order.
    ///
    /// `..` components are preserved; it is up to the caller to give them
    /// meaning.
    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|c| !c.is_empty() && *c != ".")
    }

    /// True if the path's spelling requires the final object to be a
    /// directory: a trailing `/`, `/.`, or a lone `.`.
    ///
    /// POSIX gives `ENOTDIR` when such a path names a regular file. Backends
    /// that resolve paths themselves check this after their walk; the host
    /// backend's operating system enforces it natively.
    pub fn requires_directory(&self) -> bool {
        self.0.ends_with('/') || self.0.ends_with("/.") || &self.0 == "."
    }

    /// This path as a relative [`std::path::Path`].
    pub fn as_std_path(&self) -> &std::path::Path {
        std::path::Path::new(if self.is_root() { "." } else { &self.0 })
    }

    /// Copies this path into an owned [`FsPathBuf`].
    pub fn to_owned(&self) -> FsPathBuf {
        FsPathBuf(self.0.to_owned())
    }
}

impl Display for FsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for FsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

/// An owned [`FsPath`].
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FsPathBuf(String);

impl FsPathBuf {
    /// Validates and takes ownership of `path`. See [`FsPath::new`].
    pub fn new(path: impl Into<String>) -> FsResult<Self> {
        let path = path.into();
        FsPath::new(&path)?;
        Ok(Self(path))
    }

    /// The root of a filesystem.
    pub fn root() -> Self {
        Self(String::new())
    }

    /// Borrows this path.
    pub fn as_path(&self) -> &FsPath {
        // SAFETY: `FsPath` is a `#[repr(transparent)]` wrapper around `str`,
        // and the contents were validated on construction.
        unsafe { &*(self.0.as_str() as *const str as *const FsPath) }
    }
}

impl std::ops::Deref for FsPathBuf {
    type Target = FsPath;

    fn deref(&self) -> &FsPath {
        self.as_path()
    }
}

impl Display for FsPathBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// How a path should be opened.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenOptions {
    /// `O_CREAT`/`O_EXCL`/`O_TRUNC`/`O_DIRECTORY` equivalents.
    pub open_flags: OpenFlags,
    /// The access the resulting descriptor should have.
    pub flags: DescriptorFlags,
    /// Whether a symbolic link at the final path component should be followed.
    pub follow_symlinks: bool,
}

impl OpenOptions {
    /// Options for opening an existing object read-only, following symlinks.
    pub fn read() -> Self {
        Self {
            open_flags: OpenFlags::empty(),
            flags: DescriptorFlags::READ,
            follow_symlinks: true,
        }
    }

    /// True if the caller asked for write access to the opened object.
    pub fn is_write(&self) -> bool {
        self.flags.contains(DescriptorFlags::WRITE)
            || self.flags.contains(DescriptorFlags::MUTATE_DIRECTORY)
            || self.open_flags.contains(OpenFlags::CREATE)
            || self.open_flags.contains(OpenFlags::TRUNCATE)
    }
}

macro_rules! bitflags {
    (
        $(#[$outer:meta])*
        pub struct $name:ident: $repr:ty {
            $($(#[$inner:meta])* const $flag:ident = $value:expr;)*
        }
    ) => {
        $(#[$outer])*
        #[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
        pub struct $name($repr);

        impl $name {
            $($(#[$inner])* pub const $flag: Self = Self($value);)*

            /// The empty set of flags.
            pub const fn empty() -> Self {
                Self(0)
            }

            /// Every flag set.
            pub const fn all() -> Self {
                Self(0 $(| $value)*)
            }

            /// True if every flag in `other` is set in `self`.
            pub const fn contains(self, other: Self) -> bool {
                self.0 & other.0 == other.0
            }

            /// True if no flags are set.
            pub const fn is_empty(self) -> bool {
                self.0 == 0
            }

            /// Sets or clears `other` according to `value`.
            pub fn set(&mut self, other: Self, value: bool) {
                if value {
                    self.0 |= other.0;
                } else {
                    self.0 &= !other.0;
                }
            }
        }

        impl std::ops::BitOr for $name {
            type Output = Self;
            fn bitor(self, rhs: Self) -> Self {
                Self(self.0 | rhs.0)
            }
        }

        impl std::ops::BitOrAssign for $name {
            fn bitor_assign(&mut self, rhs: Self) {
                self.0 |= rhs.0;
            }
        }

        impl std::ops::BitAnd for $name {
            type Output = Self;
            fn bitand(self, rhs: Self) -> Self {
                Self(self.0 & rhs.0)
            }
        }

        impl Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut names = vec![];
                $(if self.contains(Self::$flag) {
                    names.push(stringify!($flag));
                })*
                write!(f, "{}({})", stringify!($name), names.join(" | "))
            }
        }
    };
}

bitflags! {
    /// The method by which to open a path, mirroring `wasi:filesystem`'s
    /// `open-flags`.
    pub struct OpenFlags: u8 {
        /// Create the object if it does not exist (`O_CREAT`).
        const CREATE = 1 << 0;
        /// Fail if the object is not a directory (`O_DIRECTORY`).
        const DIRECTORY = 1 << 1;
        /// Fail if the object already exists (`O_EXCL`).
        const EXCLUSIVE = 1 << 2;
        /// Truncate the object to zero length (`O_TRUNC`).
        const TRUNCATE = 1 << 3;
    }
}

bitflags! {
    /// The access a descriptor confers, mirroring `wasi:filesystem`'s
    /// `descriptor-flags`.
    pub struct DescriptorFlags: u8 {
        /// Data may be read.
        const READ = 1 << 0;
        /// Data may be written.
        const WRITE = 1 << 1;
        /// Writes should be file-integrity synchronised (`O_SYNC`).
        const FILE_INTEGRITY_SYNC = 1 << 2;
        /// Writes should be data-integrity synchronised (`O_DSYNC`).
        const DATA_INTEGRITY_SYNC = 1 << 3;
        /// Reads should match the integrity requested for writes (`O_RSYNC`).
        const REQUESTED_WRITE_SYNC = 1 << 4;
        /// Directory contents may be mutated. Only meaningful on directories.
        const MUTATE_DIRECTORY = 1 << 5;
    }
}

/// The type of a filesystem object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DescriptorType {
    /// A block device inode.
    BlockDevice,
    /// A character device inode.
    CharacterDevice,
    /// A directory inode.
    Directory,
    /// A named pipe.
    Fifo,
    /// A symbolic link inode.
    SymbolicLink,
    /// A regular file inode.
    RegularFile,
    /// A socket.
    Socket,
    /// Anything else.
    #[default]
    Unknown,
}

impl DescriptorType {
    /// True if this is [`DescriptorType::Directory`].
    pub fn is_dir(self) -> bool {
        self == Self::Directory
    }
}

/// The attributes of a filesystem object, mirroring `wasi:filesystem`'s
/// `descriptor-stat`.
#[derive(Clone, Debug)]
pub struct Stat {
    /// The object's type.
    pub type_: DescriptorType,
    /// The number of hard links to the object.
    pub link_count: u64,
    /// The size in bytes; for a symlink, the length of its target.
    pub size: u64,
    /// Last data access time, if the backend tracks one.
    pub data_access_timestamp: Option<SystemTime>,
    /// Last data modification time, if the backend tracks one.
    pub data_modification_timestamp: Option<SystemTime>,
    /// Last status change time, if the backend tracks one.
    pub status_change_timestamp: Option<SystemTime>,
}

impl Stat {
    /// A `Stat` for an object of the given type and size, with no timestamps.
    pub fn new(type_: DescriptorType, size: u64) -> Self {
        Self {
            type_,
            link_count: 1,
            size,
            data_access_timestamp: None,
            data_modification_timestamp: None,
            status_change_timestamp: None,
        }
    }
}

/// An entry in a directory listing.
#[derive(Clone, Debug)]
pub struct DirEntry {
    /// The type of the object the entry refers to.
    pub type_: DescriptorType,
    /// The entry's name, with no path separators.
    pub name: String,
}

/// The new value for a timestamp.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NewTimestamp {
    /// Leave the timestamp alone.
    #[default]
    NoChange,
    /// Set the timestamp to the current time.
    Now,
    /// Set the timestamp to a specific time.
    Timestamp(SystemTime),
}

/// The timestamps to apply in a `set-times` operation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SetTimes {
    /// The new data access timestamp.
    pub access: NewTimestamp,
    /// The new data modification timestamp.
    pub modification: NewTimestamp,
}

impl SetTimes {
    /// True if neither timestamp would change.
    pub fn is_noop(&self) -> bool {
        self.access == NewTimestamp::NoChange && self.modification == NewTimestamp::NoChange
    }
}

/// An access-pattern hint, mirroring `wasi:filesystem`'s `advice`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Advice {
    /// No advice to give.
    Normal,
    /// Access will be sequential from lower to higher offsets.
    Sequential,
    /// Access will be in random order.
    Random,
    /// The data will be accessed soon.
    WillNeed,
    /// The data will not be accessed soon.
    DontNeed,
    /// The data will be accessed once and not reused.
    NoReuse,
}

/// A 128-bit hash of an object's metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct MetadataHash {
    /// The low 64 bits.
    pub lower: u64,
    /// The high 64 bits.
    pub upper: u64,
}

impl MetadataHash {
    /// Derives a hash from a size and modification time, which is what most
    /// backends have to work with.
    pub fn from_stat(stat: &Stat) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        stat.size.hash(&mut hasher);
        if let Some(mtime) = stat.data_modification_timestamp {
            mtime
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
                .hash(&mut hasher);
        }
        let lower = hasher.finish();
        // Cheap avalanche so `upper` is not simply `lower` again.
        let upper = lower.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(31);
        Self { lower, upper }
    }
}

/// A stable identity for a filesystem object within one [`Filesystem`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u128);

/// The error codes of `wasi:filesystem`, which are POSIX's `errno` by another
/// name.
///
/// Not every backend needs to produce every variant. The ones that matter for
/// almost all backends are [`Self::NoEntry`], [`Self::Exist`],
/// [`Self::NotDirectory`], [`Self::IsDirectory`], [`Self::NotEmpty`],
/// [`Self::NotPermitted`], and [`Self::Unsupported`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[allow(missing_docs, reason = "each variant documents its POSIX equivalent")]
pub enum ErrorCode {
    /// `EACCES`
    Access,
    /// `EWOULDBLOCK`
    WouldBlock,
    /// `EALREADY`
    Already,
    /// `EBADF`
    BadDescriptor,
    /// `EBUSY`
    Busy,
    /// `EDEADLK`
    Deadlock,
    /// `EDQUOT`
    Quota,
    /// `EEXIST`
    Exist,
    /// `EFBIG`
    FileTooLarge,
    /// `EILSEQ`
    IllegalByteSequence,
    /// `EINPROGRESS`
    InProgress,
    /// `EINTR`
    Interrupted,
    /// `EINVAL`
    Invalid,
    /// `EIO`
    Io,
    /// `EISDIR`
    IsDirectory,
    /// `ELOOP`
    Loop,
    /// `EMLINK`
    TooManyLinks,
    /// `EMSGSIZE`
    MessageSize,
    /// `ENAMETOOLONG`
    NameTooLong,
    /// `ENODEV`
    NoDevice,
    /// `ENOENT`
    NoEntry,
    /// `ENOLCK`
    NoLock,
    /// `ENOMEM`
    InsufficientMemory,
    /// `ENOSPC`
    InsufficientSpace,
    /// `ENOTDIR`
    NotDirectory,
    /// `ENOTEMPTY`
    NotEmpty,
    /// `ENOTRECOVERABLE`
    NotRecoverable,
    /// `ENOTSUP`/`ENOSYS`
    Unsupported,
    /// `ENOTTY`
    NoTty,
    /// `ENXIO`
    NoSuchDevice,
    /// `EOVERFLOW`
    Overflow,
    /// `EPERM`
    NotPermitted,
    /// `EPIPE`
    Pipe,
    /// `EROFS`
    ReadOnly,
    /// `ESPIPE`
    InvalidSeek,
    /// `ETXTBSY`
    TextFileBusy,
    /// `EXDEV`
    CrossDevice,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ErrorCode {}

impl From<&std::io::Error> for ErrorCode {
    fn from(err: &std::io::Error) -> Self {
        // Prefer the raw errno where there is one. `ErrorKind` is close to
        // complete these days but still collapses a few distinctions guests
        // care about (`ELOOP`, `EOVERFLOW`, `ENOLCK`).
        #[cfg(unix)]
        if let Some(code) = err.raw_os_error().and_then(from_errno) {
            return code;
        }

        use std::io::ErrorKind;
        match err.kind() {
            ErrorKind::NotFound => Self::NoEntry,
            ErrorKind::PermissionDenied => Self::NotPermitted,
            ErrorKind::AlreadyExists => Self::Exist,
            ErrorKind::InvalidInput => Self::Invalid,
            ErrorKind::WouldBlock => Self::WouldBlock,
            ErrorKind::Interrupted => Self::Interrupted,
            ErrorKind::Unsupported => Self::Unsupported,
            ErrorKind::OutOfMemory => Self::InsufficientMemory,
            ErrorKind::BrokenPipe => Self::Pipe,
            ErrorKind::NotADirectory => Self::NotDirectory,
            ErrorKind::IsADirectory => Self::IsDirectory,
            ErrorKind::DirectoryNotEmpty => Self::NotEmpty,
            ErrorKind::ReadOnlyFilesystem => Self::ReadOnly,
            ErrorKind::CrossesDevices => Self::CrossDevice,
            ErrorKind::TooManyLinks => Self::TooManyLinks,
            ErrorKind::FileTooLarge => Self::FileTooLarge,
            ErrorKind::StorageFull => Self::InsufficientSpace,
            ErrorKind::QuotaExceeded => Self::Quota,
            ErrorKind::NotSeekable => Self::InvalidSeek,
            ErrorKind::ResourceBusy => Self::Busy,
            ErrorKind::ExecutableFileBusy => Self::TextFileBusy,
            ErrorKind::Deadlock => Self::Deadlock,
            ErrorKind::InvalidFilename => Self::NameTooLong,
            ErrorKind::InvalidData => Self::IllegalByteSequence,
            _ => Self::Io,
        }
    }
}

#[cfg(unix)]
fn from_errno(raw: i32) -> Option<ErrorCode> {
    use rustix::io::Errno;
    Some(match Errno::from_raw_os_error(raw) {
        Errno::ACCESS => ErrorCode::Access,
        Errno::AGAIN => ErrorCode::WouldBlock,
        Errno::ALREADY => ErrorCode::Already,
        Errno::BADF => ErrorCode::BadDescriptor,
        Errno::BUSY => ErrorCode::Busy,
        Errno::DEADLK => ErrorCode::Deadlock,
        Errno::EXIST => ErrorCode::Exist,
        Errno::FBIG => ErrorCode::FileTooLarge,
        Errno::ILSEQ => ErrorCode::IllegalByteSequence,
        Errno::INPROGRESS => ErrorCode::InProgress,
        Errno::INTR => ErrorCode::Interrupted,
        Errno::INVAL => ErrorCode::Invalid,
        Errno::IO => ErrorCode::Io,
        Errno::ISDIR => ErrorCode::IsDirectory,
        Errno::LOOP => ErrorCode::Loop,
        Errno::MLINK => ErrorCode::TooManyLinks,
        Errno::MSGSIZE => ErrorCode::MessageSize,
        Errno::NAMETOOLONG => ErrorCode::NameTooLong,
        Errno::NODEV => ErrorCode::NoDevice,
        Errno::NOENT => ErrorCode::NoEntry,
        Errno::NOLCK => ErrorCode::NoLock,
        Errno::NOMEM => ErrorCode::InsufficientMemory,
        Errno::NOSPC => ErrorCode::InsufficientSpace,
        Errno::NOTDIR => ErrorCode::NotDirectory,
        Errno::NOTEMPTY => ErrorCode::NotEmpty,
        Errno::NOTRECOVERABLE => ErrorCode::NotRecoverable,
        Errno::NOTSUP => ErrorCode::Unsupported,
        Errno::NOTTY => ErrorCode::NoTty,
        Errno::NXIO => ErrorCode::NoSuchDevice,
        Errno::OVERFLOW => ErrorCode::Overflow,
        Errno::PERM => ErrorCode::NotPermitted,
        Errno::PIPE => ErrorCode::Pipe,
        Errno::ROFS => ErrorCode::ReadOnly,
        Errno::SPIPE => ErrorCode::InvalidSeek,
        Errno::TXTBSY => ErrorCode::TextFileBusy,
        Errno::XDEV => ErrorCode::CrossDevice,
        // On some platforms this shares a value with an arm above.
        #[allow(unreachable_patterns, reason = "aliases NOTSUP on some targets")]
        Errno::OPNOTSUPP => ErrorCode::Unsupported,
        _ => return None,
    })
}

impl From<std::io::Error> for ErrorCode {
    fn from(err: std::io::Error) -> Self {
        Self::from(&err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_paths_are_rejected() {
        assert_eq!(FsPath::new("/etc/passwd"), Err(ErrorCode::NotPermitted));
        assert_eq!(FsPath::new("/"), Err(ErrorCode::NotPermitted));
        assert!(FsPath::new("etc/passwd").is_ok());
        assert!(FsPath::new("").is_ok());
    }

    #[test]
    fn interior_nuls_are_rejected() {
        assert_eq!(FsPath::new("a\0b"), Err(ErrorCode::Invalid));
    }

    #[test]
    fn components_skip_empty_and_dot() {
        let path = FsPath::new("a//./b/../c/").unwrap();
        assert_eq!(path.components().collect::<Vec<_>>(), ["a", "b", "..", "c"]);
    }

    #[test]
    fn root_is_recognized() {
        assert!(FsPath::new("").unwrap().is_root());
        assert!(FsPath::new(".").unwrap().is_root());
        assert!(!FsPath::new("a").unwrap().is_root());
    }

    #[test]
    fn metadata_hash_tracks_size_and_mtime() {
        let mut a = Stat::new(DescriptorType::RegularFile, 10);
        a.data_modification_timestamp = Some(SystemTime::UNIX_EPOCH);
        let mut b = a.clone();
        assert_eq!(MetadataHash::from_stat(&a), MetadataHash::from_stat(&b));

        b.size = 11;
        assert_ne!(MetadataHash::from_stat(&a), MetadataHash::from_stat(&b));

        let mut c = a.clone();
        c.data_modification_timestamp =
            Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1));
        assert_ne!(MetadataHash::from_stat(&a), MetadataHash::from_stat(&c));
    }

    #[test]
    fn flags_behave_like_bitflags() {
        let mut flags = DescriptorFlags::READ | DescriptorFlags::WRITE;
        assert!(flags.contains(DescriptorFlags::READ));
        assert!(!flags.contains(DescriptorFlags::MUTATE_DIRECTORY));
        flags.set(DescriptorFlags::WRITE, false);
        assert!(!flags.contains(DescriptorFlags::WRITE));
        assert!(DescriptorFlags::empty().is_empty());
        assert!(DescriptorFlags::all().contains(DescriptorFlags::MUTATE_DIRECTORY));
    }
}
