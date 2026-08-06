//! The descriptor layer: `wasi:filesystem` semantics over the SPI.
//!
//! A [`Descriptor`] is what a guest's `wasi:filesystem` descriptor resolves
//! to on the host side: a [`Filesystem`] plus a path within it, and for
//! regular files an open [`File`] handle. Everything `wasi:filesystem`
//! specifies *about descriptors* - permission checks against the base,
//! flag inheritance, `cross-device` for operations that span mounts,
//! `is-same-object` - is enforced here, exactly once, so the p2 and p3
//! binding layers stay mechanical translations and backends never see an
//! operation they should not.
//!
//! # Permissions
//!
//! `wasi:filesystem` has no ambient permissions; everything flows from the
//! [`DescriptorFlags`] a descriptor carries. The rules applied here, chosen
//! to match `wasmtime-wasi` except where noted:
//!
//! - Reading through a directory descriptor (`open-at`, `stat-at`,
//!   `read-directory`, ...) requires [`DescriptorFlags::READ`]; refused with
//!   [`ErrorCode::NotPermitted`].
//! - Mutating the namespace through a directory descriptor
//!   (`create-directory-at`, `unlink-file-at`, `rename-at`, an `open-at`
//!   that creates, truncates, or requests write access, ...) requires
//!   [`DescriptorFlags::MUTATE_DIRECTORY`]; refused with
//!   [`ErrorCode::ReadOnly`]. This diverges from `wasmtime-wasi`'s
//!   `not-permitted` deliberately: a base without `mutate-directory` is a
//!   read-only mount, and `EROFS` is both what POSIX says a read-only
//!   filesystem reports and an error guests like `git` explain well.
//! - Byte I/O on a file descriptor requires the corresponding
//!   [`DescriptorFlags::READ`]/[`DescriptorFlags::WRITE`] on that
//!   descriptor; refused with [`ErrorCode::NotPermitted`] (or
//!   [`ErrorCode::BadDescriptor`] for the `*-via-stream` constructors,
//!   matching `wasmtime-wasi`).

use std::sync::Arc;

use crate::spi::{
    Advice, DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, Filesystem, FsPath,
    FsPathBuf, FsResult, MetadataHash, ObjectId, OpenFlags, OpenOptions, Opened, SetTimes, Stat,
};

/// The largest single positional read the descriptor layer will perform.
///
/// `wasi:filesystem`'s `read` allows short reads, so capping one call bounds
/// the allocation a guest can demand without changing observable semantics.
const MAX_READ_LEN: usize = 8 * 1024 * 1024;

/// An open `wasi:filesystem` descriptor.
///
/// Cheap to clone: clones share the filesystem and any open file handle.
#[derive(Clone)]
pub struct Descriptor {
    /// The filesystem this descriptor lives in. Also the unit of identity
    /// for `cross-device` checks: two descriptors belong to the same device
    /// exactly when they share this `Arc`.
    fs: Arc<dyn Filesystem>,
    /// This descriptor's path relative to `fs`'s root.
    path: FsPathBuf,
    /// What the descriptor refers to.
    kind: DescriptorKind,
    /// The access this descriptor confers.
    flags: DescriptorFlags,
}

#[derive(Clone)]
enum DescriptorKind {
    /// A directory: no state beyond the filesystem and path.
    Dir,
    /// A regular file, with its open handle.
    File(Arc<dyn File>),
}

impl std::fmt::Debug for Descriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Descriptor")
            .field("fs", &self.fs.summary())
            .field("path", &self.path)
            .field("type", &self.type_())
            .field("flags", &self.flags)
            .finish()
    }
}

impl Descriptor {
    /// A descriptor for the root of `fs` with the given access.
    pub fn root(fs: Arc<dyn Filesystem>, flags: DescriptorFlags) -> Self {
        Self {
            fs,
            path: FsPathBuf::root(),
            kind: DescriptorKind::Dir,
            flags,
        }
    }

    /// The type of the object this descriptor refers to.
    pub fn type_(&self) -> DescriptorType {
        match self.kind {
            DescriptorKind::Dir => DescriptorType::Directory,
            DescriptorKind::File(_) => DescriptorType::RegularFile,
        }
    }

    /// The access this descriptor confers.
    pub fn flags(&self) -> DescriptorFlags {
        self.flags
    }

    /// True if this descriptor refers to a directory.
    pub fn is_dir(&self) -> bool {
        matches!(self.kind, DescriptorKind::Dir)
    }

    /// A short description of the underlying filesystem, for diagnostics.
    pub fn fs_summary(&self) -> String {
        self.fs.summary()
    }

    /// The open file handle, or `bad-descriptor` for a directory.
    fn file(&self) -> FsResult<&Arc<dyn File>> {
        match &self.kind {
            DescriptorKind::File(file) => Ok(file),
            DescriptorKind::Dir => Err(ErrorCode::BadDescriptor),
        }
    }

    /// This descriptor's flags, or `not-directory` if it is not a directory.
    fn dir_flags(&self) -> FsResult<DescriptorFlags> {
        match self.kind {
            DescriptorKind::Dir => Ok(self.flags),
            DescriptorKind::File(_) => Err(ErrorCode::NotDirectory),
        }
    }

    /// Requires this to be a directory readable through this descriptor.
    fn readable_dir(&self) -> FsResult<()> {
        if !self.dir_flags()?.contains(DescriptorFlags::READ) {
            return Err(ErrorCode::NotPermitted);
        }
        Ok(())
    }

    /// Requires this to be a directory whose namespace this descriptor may
    /// mutate. `read-only` otherwise: see the module docs.
    fn mutable_dir(&self) -> FsResult<()> {
        if !self
            .dir_flags()?
            .contains(DescriptorFlags::MUTATE_DIRECTORY)
        {
            return Err(ErrorCode::ReadOnly);
        }
        Ok(())
    }

    /// Validates `rel` and joins it onto this descriptor's path.
    fn join(&self, rel: &str) -> FsResult<FsPathBuf> {
        let rel = FsPath::new(rel)?;
        if rel.as_str().is_empty() {
            // POSIX: an empty path in any `*at` call is `ENOENT`.
            return Err(ErrorCode::NoEntry);
        }
        if self.path.is_root() {
            Ok(rel.to_owned())
        } else {
            FsPathBuf::new(format!("{}/{}", self.path, rel))
        }
    }

    /// Opens `path` relative to this directory descriptor.
    ///
    /// This is `wasi:filesystem`'s `open-at`, including its permission
    /// model: the *base* descriptor decides what an open may do, and a
    /// mutating open - one that creates, truncates, or requests write or
    /// mutate-directory access - against a base without `mutate-directory`
    /// fails with `read-only`.
    pub async fn open_at(
        &self,
        path: &str,
        follow_symlinks: bool,
        open_flags: OpenFlags,
        flags: DescriptorFlags,
    ) -> FsResult<Descriptor> {
        self.readable_dir()?;
        let base_flags = self.flags;

        // Sync modes are not modelled by the SPI; refuse rather than lie.
        // (`wasmtime-wasi` does the same.)
        if flags.contains(DescriptorFlags::FILE_INTEGRITY_SYNC)
            || flags.contains(DescriptorFlags::DATA_INTEGRITY_SYNC)
            || flags.contains(DescriptorFlags::REQUESTED_WRITE_SYNC)
        {
            return Err(ErrorCode::Unsupported);
        }

        // `directory` cannot be combined with creation or truncation.
        // (`wasmtime-wasi` parity.)
        if open_flags.contains(OpenFlags::DIRECTORY)
            && (open_flags.contains(OpenFlags::CREATE)
                || open_flags.contains(OpenFlags::EXCLUSIVE)
                || open_flags.contains(OpenFlags::TRUNCATE))
        {
            return Err(ErrorCode::Invalid);
        }

        let mutating = flags.contains(DescriptorFlags::WRITE)
            || flags.contains(DescriptorFlags::MUTATE_DIRECTORY)
            || open_flags.contains(OpenFlags::CREATE)
            || open_flags.contains(OpenFlags::TRUNCATE);
        if mutating && !base_flags.contains(DescriptorFlags::MUTATE_DIRECTORY) {
            return Err(ErrorCode::ReadOnly);
        }

        let path = self.join(path)?;
        let opened = self
            .fs
            .open(
                &path,
                OpenOptions {
                    open_flags,
                    flags,
                    follow_symlinks,
                },
            )
            .await?;

        Ok(match opened {
            Opened::File(file) => Descriptor {
                fs: self.fs.clone(),
                path,
                kind: DescriptorKind::File(file),
                flags: flags & (DescriptorFlags::READ | DescriptorFlags::WRITE),
            },
            Opened::Dir => Descriptor {
                fs: self.fs.clone(),
                path,
                kind: DescriptorKind::Dir,
                // Directories inherit the base's rights: guests open
                // directories asking only for `read` and still expect to
                // create files through them on a writable mount.
                flags: base_flags,
            },
        })
    }

    /// Reads up to `len` bytes at `offset`, returning the data and an
    /// end-of-file flag.
    pub async fn read(&self, len: u64, offset: u64) -> FsResult<(Vec<u8>, bool)> {
        let file = self.file()?;
        if !self.flags.contains(DescriptorFlags::READ) {
            return Err(ErrorCode::NotPermitted);
        }
        let len = usize::try_from(len).unwrap_or(usize::MAX).min(MAX_READ_LEN);
        let mut buf = vec![0; len];
        let n = file.read_at(&mut buf, offset).await?;
        buf.truncate(n);
        Ok((buf, n == 0))
    }

    /// Writes `buf` at `offset`, returning the number of bytes written.
    pub async fn write(&self, buf: &[u8], offset: u64) -> FsResult<usize> {
        let file = self.file()?;
        if !self.flags.contains(DescriptorFlags::WRITE) {
            return Err(ErrorCode::NotPermitted);
        }
        file.write_at(buf, offset).await
    }

    /// Appends `buf`, returning the number of bytes written.
    pub async fn append(&self, buf: &[u8]) -> FsResult<usize> {
        let file = self.file()?;
        if !self.flags.contains(DescriptorFlags::WRITE) {
            return Err(ErrorCode::NotPermitted);
        }
        file.append(buf).await
    }

    /// Returns the open file handle for a stream, requiring `need` access.
    ///
    /// The `*-via-stream` constructors report a missing right as
    /// `bad-descriptor` rather than `not-permitted` (`wasmtime-wasi`
    /// parity), so this does too.
    pub fn file_for_stream(&self, need: DescriptorFlags) -> FsResult<Arc<dyn File>> {
        let file = self.file()?;
        if !self.flags.contains(need) {
            return Err(ErrorCode::BadDescriptor);
        }
        Ok(file.clone())
    }

    /// Truncates or extends the file to `size`.
    pub async fn set_size(&self, size: u64) -> FsResult<()> {
        let file = self.file()?;
        if !self.flags.contains(DescriptorFlags::WRITE) {
            return Err(ErrorCode::NotPermitted);
        }
        file.set_size(size).await
    }

    /// Passes an access-pattern hint to the backend.
    pub async fn advise(&self, offset: u64, len: u64, advice: Advice) -> FsResult<()> {
        self.file()?.advise(offset, len, advice).await
    }

    /// Returns this descriptor's attributes.
    pub async fn stat(&self) -> FsResult<Stat> {
        match &self.kind {
            DescriptorKind::File(file) => file.stat().await,
            DescriptorKind::Dir => self.fs.stat_at(&self.path, true).await,
        }
    }

    /// Sets this descriptor's timestamps.
    pub async fn set_times(&self, times: SetTimes) -> FsResult<()> {
        match &self.kind {
            DescriptorKind::File(file) => {
                if !self.flags.contains(DescriptorFlags::WRITE) {
                    return Err(ErrorCode::NotPermitted);
                }
                file.set_times(times).await
            }
            DescriptorKind::Dir => {
                if !self.flags.contains(DescriptorFlags::MUTATE_DIRECTORY) {
                    return Err(ErrorCode::ReadOnly);
                }
                self.fs.set_times_at(&self.path, true, times).await
            }
        }
    }

    /// Flushes data and metadata to durable storage.
    ///
    /// On a directory this is a no-op: the SPI has no directory sync, and
    /// backends that need one (none of the built-in ones do) sync on their
    /// own mutations.
    pub async fn sync(&self) -> FsResult<()> {
        match &self.kind {
            DescriptorKind::File(file) => file.sync().await,
            DescriptorKind::Dir => Ok(()),
        }
    }

    /// Flushes data to durable storage.
    pub async fn sync_data(&self) -> FsResult<()> {
        match &self.kind {
            DescriptorKind::File(file) => file.sync_data().await,
            DescriptorKind::Dir => Ok(()),
        }
    }

    /// Returns a hash of this descriptor's metadata.
    pub async fn metadata_hash(&self) -> FsResult<MetadataHash> {
        match &self.kind {
            DescriptorKind::File(file) => file.metadata_hash().await,
            DescriptorKind::Dir => self.fs.metadata_hash_at(&self.path, true).await,
        }
    }

    /// Lists this directory's entries. `.` and `..` are not included.
    pub async fn read_directory(&self) -> FsResult<Vec<DirEntry>> {
        self.readable_dir()?;
        self.fs.read_dir(&self.path).await
    }

    /// Creates a directory at `path` relative to this descriptor.
    pub async fn create_directory_at(&self, path: &str) -> FsResult<()> {
        self.mutable_dir()?;
        self.fs.create_dir(&self.join(path)?).await
    }

    /// Returns the attributes of the object at `path`.
    pub async fn stat_at(&self, path: &str, follow_symlinks: bool) -> FsResult<Stat> {
        self.readable_dir()?;
        self.fs.stat_at(&self.join(path)?, follow_symlinks).await
    }

    /// Sets the timestamps of the object at `path`.
    pub async fn set_times_at(
        &self,
        path: &str,
        follow_symlinks: bool,
        times: SetTimes,
    ) -> FsResult<()> {
        self.mutable_dir()?;
        self.fs
            .set_times_at(&self.join(path)?, follow_symlinks, times)
            .await
    }

    /// Reads the target of the symbolic link at `path`.
    pub async fn readlink_at(&self, path: &str) -> FsResult<String> {
        self.readable_dir()?;
        self.fs.readlink(&self.join(path)?).await
    }

    /// Removes the directory at `path`.
    pub async fn remove_directory_at(&self, path: &str) -> FsResult<()> {
        self.mutable_dir()?;
        self.fs.remove_dir(&self.join(path)?).await
    }

    /// Removes the non-directory object at `path`.
    pub async fn unlink_file_at(&self, path: &str) -> FsResult<()> {
        self.mutable_dir()?;
        self.fs.unlink(&self.join(path)?).await
    }

    /// Creates a symbolic link at `link_path` pointing at `target`.
    pub async fn symlink_at(&self, target: &str, link_path: &str) -> FsResult<()> {
        self.mutable_dir()?;
        self.fs.symlink(target, &self.join(link_path)?).await
    }

    /// Renames `old_path` (relative to this descriptor) to `new_path`
    /// (relative to `new_base`).
    ///
    /// The two bases must belong to the same mount; otherwise this fails
    /// with `cross-device` before any backend is consulted, which is why
    /// [`Filesystem::rename`] never has to think about other filesystems.
    pub async fn rename_at(
        &self,
        old_path: &str,
        new_base: &Descriptor,
        new_path: &str,
    ) -> FsResult<()> {
        self.mutable_dir()?;
        new_base.mutable_dir()?;
        if !Arc::ptr_eq(&self.fs, &new_base.fs) {
            return Err(ErrorCode::CrossDevice);
        }
        self.fs
            .rename(&self.join(old_path)?, &new_base.join(new_path)?)
            .await
    }

    /// Creates a hard link at `new_path` (relative to `new_base`) to the
    /// object at `old_path` (relative to this descriptor).
    ///
    /// Cross-mount links fail with `cross-device`, like renames.
    pub async fn link_at(
        &self,
        follow_symlinks: bool,
        old_path: &str,
        new_base: &Descriptor,
        new_path: &str,
    ) -> FsResult<()> {
        self.readable_dir()?;
        new_base.mutable_dir()?;
        if !Arc::ptr_eq(&self.fs, &new_base.fs) {
            return Err(ErrorCode::CrossDevice);
        }
        self.fs
            .hard_link(
                &self.join(old_path)?,
                follow_symlinks,
                &new_base.join(new_path)?,
            )
            .await
    }

    /// Returns a hash of the metadata of the object at `path`.
    pub async fn metadata_hash_at(
        &self,
        path: &str,
        follow_symlinks: bool,
    ) -> FsResult<MetadataHash> {
        self.readable_dir()?;
        self.fs
            .metadata_hash_at(&self.join(path)?, follow_symlinks)
            .await
    }

    /// True if `self` and `other` refer to the same object.
    ///
    /// Uses the backend's [`Filesystem::object_id_at`] where available;
    /// backends that report [`ErrorCode::Unsupported`] fall back to lexical
    /// path equality, which cannot see through hard links but is correct
    /// for everything else.
    pub async fn is_same_object(&self, other: &Descriptor) -> FsResult<bool> {
        if !Arc::ptr_eq(&self.fs, &other.fs) {
            return Ok(false);
        }
        match (self.object_id().await, other.object_id().await) {
            (Ok(a), Ok(b)) => Ok(a == b),
            (Err(ErrorCode::Unsupported), _) | (_, Err(ErrorCode::Unsupported)) => {
                Ok(normalized(&self.path) == normalized(&other.path))
            }
            (Err(err), _) | (_, Err(err)) => Err(err),
        }
    }

    async fn object_id(&self) -> FsResult<ObjectId> {
        match &self.kind {
            DescriptorKind::File(file) => file.object_id().await,
            DescriptorKind::Dir => self.fs.object_id_at(&self.path, true).await,
        }
    }
}

/// A path reduced to its meaningful components, for the `is-same-object`
/// fallback. `..` is preserved: without object identity it cannot be
/// resolved, and treating `a/..` as `.` would be wrong across symlinks.
fn normalized(path: &FsPath) -> Vec<&str> {
    path.components().collect()
}

/// The preopened directories handed to one component instance.
#[derive(Clone, Default)]
pub struct Preopens {
    entries: Vec<(Descriptor, String)>,
}

impl Preopens {
    /// Mounts `fs` at `guest_path`.
    ///
    /// A writable mount's root descriptor carries `mutate-directory`; a
    /// read-only mount's carries only `read`, so every mutation through it
    /// reports `read-only`.
    pub fn mount(
        &mut self,
        guest_path: impl Into<String>,
        fs: Arc<dyn Filesystem>,
        writable: bool,
    ) {
        let flags = if writable {
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY
        } else {
            DescriptorFlags::READ
        };
        self.entries
            .push((Descriptor::root(fs, flags), guest_path.into()));
    }

    /// The mounts, in the order they were added.
    pub fn entries(&self) -> impl Iterator<Item = &(Descriptor, String)> {
        self.entries.iter()
    }

    /// True if no mounts have been added.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryFilesystem;
    use async_trait::async_trait;

    fn mount(writable: bool) -> Descriptor {
        let mut preopens = Preopens::default();
        preopens.mount("/", Arc::new(MemoryFilesystem::new()), writable);
        preopens.entries().next().unwrap().0.clone()
    }

    async fn create(base: &Descriptor, path: &str, contents: &[u8]) -> Descriptor {
        let desc = base
            .open_at(
                path,
                true,
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();
        if !contents.is_empty() {
            desc.write(contents, 0).await.unwrap();
        }
        desc
    }

    #[tokio::test]
    async fn read_only_mount_reports_read_only_for_mutation() {
        let rw = mount(true);
        create(&rw, "f", b"x").await;

        let ro = mount(false);
        for err in [
            ro.open_at("f", true, OpenFlags::empty(), DescriptorFlags::WRITE)
                .await
                .err(),
            ro.open_at("g", true, OpenFlags::CREATE, DescriptorFlags::READ)
                .await
                .err(),
            ro.open_at("f", true, OpenFlags::TRUNCATE, DescriptorFlags::READ)
                .await
                .err(),
            ro.create_directory_at("d").await.err(),
            ro.unlink_file_at("f").await.err(),
            ro.remove_directory_at("d").await.err(),
            ro.symlink_at("t", "l").await.err(),
            ro.set_times_at("f", true, SetTimes::default()).await.err(),
            ro.rename_at("f", &ro.clone(), "g").await.err(),
        ] {
            assert_eq!(err, Some(ErrorCode::ReadOnly));
        }
        // `link-at` mutates through its *new* base.
        assert_eq!(
            ro.link_at(true, "f", &ro.clone(), "l").await.unwrap_err(),
            ErrorCode::ReadOnly
        );
    }

    #[tokio::test]
    async fn read_only_mount_still_reads() {
        let ro = mount(false);
        // (The mount is empty; what matters is that nothing below reports a
        // permission error.)
        assert_eq!(
            ro.stat_at("missing", true).await.unwrap_err(),
            ErrorCode::NoEntry
        );
        assert!(ro.read_directory().await.unwrap().is_empty());
        assert!(ro.stat().await.unwrap().type_.is_dir());
    }

    #[tokio::test]
    async fn writable_mount_full_cycle() {
        let root = mount(true);
        root.create_directory_at("sub").await.unwrap();

        // A subdirectory opened read-only still inherits the mount's
        // mutability, like an inherited `DirPerms` in wasmtime-wasi.
        let sub = root
            .open_at("sub", true, OpenFlags::DIRECTORY, DescriptorFlags::READ)
            .await
            .unwrap();
        assert!(sub.flags().contains(DescriptorFlags::MUTATE_DIRECTORY));

        let file = create(&sub, "f", b"hello").await;
        assert_eq!(file.read(5, 0).await.unwrap(), (b"hello".to_vec(), false));
        assert_eq!(file.read(5, 5).await.unwrap(), (vec![], true));

        let entries = sub.read_directory().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "f");

        sub.rename_at("f", &root, "g").await.unwrap();
        assert_eq!(root.stat_at("g", true).await.unwrap().size, 5);
    }

    #[tokio::test]
    async fn file_descriptor_rights_are_enforced() {
        let root = mount(true);
        create(&root, "f", b"data").await;

        let read_only = root
            .open_at("f", true, OpenFlags::empty(), DescriptorFlags::READ)
            .await
            .unwrap();
        assert_eq!(read_only.read(4, 0).await.unwrap().0, b"data");
        assert_eq!(
            read_only.write(b"x", 0).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
        assert_eq!(
            read_only.set_size(0).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
        assert_eq!(
            read_only
                .file_for_stream(DescriptorFlags::WRITE)
                .map(|_| ())
                .unwrap_err(),
            ErrorCode::BadDescriptor
        );

        let write_only = root
            .open_at("f", true, OpenFlags::empty(), DescriptorFlags::WRITE)
            .await
            .unwrap();
        assert_eq!(
            write_only.read(1, 0).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
        write_only.write(b"D", 0).await.unwrap();
        assert_eq!(read_only.read(4, 0).await.unwrap().0, b"Data");
    }

    #[tokio::test]
    async fn kind_mismatches() {
        let root = mount(true);
        let file = create(&root, "f", b"").await;

        // Directory operations on a file descriptor.
        assert_eq!(
            file.stat_at(".", true).await.unwrap_err(),
            ErrorCode::NotDirectory
        );
        assert_eq!(
            file.read_directory().await.unwrap_err(),
            ErrorCode::NotDirectory
        );
        // Byte I/O on a directory descriptor.
        assert_eq!(root.read(1, 0).await.unwrap_err(), ErrorCode::BadDescriptor);
        assert_eq!(
            root.write(b"x", 0).await.unwrap_err(),
            ErrorCode::BadDescriptor
        );
        // But stat and sync work on both.
        assert!(root.stat().await.unwrap().type_.is_dir());
        root.sync().await.unwrap();
        file.sync().await.unwrap();
    }

    #[tokio::test]
    async fn cross_mount_rename_and_link_are_cross_device() {
        let a = mount(true);
        let b = mount(true);
        create(&a, "f", b"").await;

        assert_eq!(
            a.rename_at("f", &b, "f").await.unwrap_err(),
            ErrorCode::CrossDevice
        );
        assert_eq!(
            a.link_at(true, "f", &b, "l").await.unwrap_err(),
            ErrorCode::CrossDevice
        );
    }

    #[tokio::test]
    async fn open_at_flag_validation() {
        let root = mount(true);
        assert_eq!(
            root.open_at(
                "d",
                true,
                OpenFlags::DIRECTORY | OpenFlags::CREATE,
                DescriptorFlags::READ
            )
            .await
            .unwrap_err(),
            ErrorCode::Invalid
        );
        assert_eq!(
            root.open_at(
                "f",
                true,
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::FILE_INTEGRITY_SYNC
            )
            .await
            .unwrap_err(),
            ErrorCode::Unsupported
        );
    }

    #[tokio::test]
    async fn path_validation() {
        let root = mount(true);
        assert_eq!(
            root.stat_at("/abs", true).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
        assert_eq!(
            root.stat_at("", true).await.unwrap_err(),
            ErrorCode::NoEntry
        );
        assert_eq!(
            root.stat_at("a\0b", true).await.unwrap_err(),
            ErrorCode::Invalid
        );
    }

    #[tokio::test]
    async fn is_same_object_by_identity() {
        let root = mount(true);
        create(&root, "a", b"").await;
        root.link_at(true, "a", &root.clone(), "b").await.unwrap();
        create(&root, "c", b"").await;

        let a = root
            .open_at("a", true, OpenFlags::empty(), DescriptorFlags::READ)
            .await
            .unwrap();
        let b = root
            .open_at("b", true, OpenFlags::empty(), DescriptorFlags::READ)
            .await
            .unwrap();
        let c = root
            .open_at("c", true, OpenFlags::empty(), DescriptorFlags::READ)
            .await
            .unwrap();
        assert!(a.is_same_object(&b).await.unwrap());
        assert!(!a.is_same_object(&c).await.unwrap());

        // Descriptors on different mounts are never the same object.
        let elsewhere = mount(true);
        assert!(!root.is_same_object(&elsewhere).await.unwrap());
    }

    /// A filesystem with no notion of object identity, to exercise the
    /// `is-same-object` path fallback.
    struct NoIdentity(MemoryFilesystem);

    #[async_trait]
    impl Filesystem for NoIdentity {
        fn summary(&self) -> String {
            "no-identity".into()
        }
        async fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Opened> {
            self.0.open(path, opts).await
        }
        async fn stat_at(&self, path: &FsPath, follow: bool) -> FsResult<Stat> {
            self.0.stat_at(path, follow).await
        }
        async fn set_times_at(&self, path: &FsPath, follow: bool, times: SetTimes) -> FsResult<()> {
            self.0.set_times_at(path, follow, times).await
        }
        async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.0.read_dir(path).await
        }
        async fn create_dir(&self, path: &FsPath) -> FsResult<()> {
            self.0.create_dir(path).await
        }
        async fn remove_dir(&self, path: &FsPath) -> FsResult<()> {
            self.0.remove_dir(path).await
        }
        async fn unlink(&self, path: &FsPath) -> FsResult<()> {
            self.0.unlink(path).await
        }
        async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.0.rename(from, to).await
        }
        async fn symlink(&self, target: &str, link: &FsPath) -> FsResult<()> {
            self.0.symlink(target, link).await
        }
        async fn readlink(&self, path: &FsPath) -> FsResult<String> {
            self.0.readlink(path).await
        }
        async fn hard_link(&self, from: &FsPath, follow: bool, to: &FsPath) -> FsResult<()> {
            self.0.hard_link(from, follow, to).await
        }
        async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash> {
            self.0.metadata_hash_at(path, follow).await
        }
        async fn object_id_at(&self, _path: &FsPath, _follow: bool) -> FsResult<ObjectId> {
            Err(ErrorCode::Unsupported)
        }
    }

    #[tokio::test]
    async fn is_same_object_falls_back_to_paths() {
        let mut preopens = Preopens::default();
        preopens.mount("/", Arc::new(NoIdentity(MemoryFilesystem::new())), true);
        let root = preopens.entries().next().unwrap().0.clone();
        root.create_directory_at("d").await.unwrap();

        let via_dot = root
            .open_at("./d", true, OpenFlags::DIRECTORY, DescriptorFlags::READ)
            .await
            .unwrap();
        let direct = root
            .open_at("d", true, OpenFlags::DIRECTORY, DescriptorFlags::READ)
            .await
            .unwrap();
        assert!(via_dot.is_same_object(&direct).await.unwrap());
        assert!(!via_dot.is_same_object(&root).await.unwrap());
    }
}
