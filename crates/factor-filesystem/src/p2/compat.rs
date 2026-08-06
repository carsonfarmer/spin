//! Host implementations of the pre-0.2.0 `wasi:filesystem` snapshots.
//!
//! Spin still runs components built against the `0.2.0-rc-2023-10-18` and
//! `0.2.0-rc-2023-11-10` WASI snapshots. Their `wasi:filesystem` shims used
//! to live in `spin-factor-wasi` and delegate to `wasmtime-wasi`; they live
//! here now and delegate to this crate's `wasi:filesystem@0.2.x` host, so
//! snapshot-era guests see exactly the same pluggable mounts as current
//! ones. (The non-filesystem halves of those snapshots remain in
//! `spin-factor-wasi`, which links them against `wasmtime-wasi` as before.)
//!
//! Stream resources interoperate across the versions because every bindgen
//! involved maps `wasi:io` streams to `wasmtime-wasi-io`'s types.

pub mod wasi_2023_10_18;
pub mod wasi_2023_11_10;

/// Generates `From` conversions between structurally identical generated
/// types. Adapted from the equivalent macro in `spin-factor-wasi`'s
/// snapshot shims.
macro_rules! convert {
    () => {};
    (struct $from:ty => $to:path { $($field:ident,)* } $($rest:tt)*) => {
        impl From<$from> for $to {
            fn from(e: $from) -> $to {
                $to {
                    $( $field: e.$field.into(), )*
                }
            }
        }

        crate::p2::compat::convert!($($rest)*);
    };
    (enum $from:path => $to:path { $($variant:ident $(($e:ident))?,)* } $($rest:tt)*) => {
        impl From<$from> for $to {
            fn from(e: $from) -> $to {
                use $from as A;
                use $to as B;
                match e {
                    $(
                        A::$variant $(($e))? => B::$variant $(($e.into()))?,
                    )*
                }
            }
        }

        crate::p2::compat::convert!($($rest)*);
    };
    (flags $from:path => $to:path { $($flag:ident,)* } $($rest:tt)*) => {
        impl From<$from> for $to {
            fn from(e: $from) -> $to {
                use $from as A;
                use $to as B;
                let mut out = B::empty();
                $(
                    if e.contains(A::$flag) {
                        out |= B::$flag;
                    }
                )*
                out
            }
        }

        crate::p2::compat::convert!($($rest)*);
    };
}

pub(crate) use convert;

/// Generates the snapshot-to-current filesystem conversions, which are the
/// same for every snapshot except for the module the snapshot types come
/// from.
macro_rules! filesystem_conversions {
    ($types:path, $wall_clock:path) => {
        use $types as snapshot_types;
        use $wall_clock as snapshot_wall_clock;

        crate::p2::compat::convert! {
            struct crate::p2::wall_clock::Datetime => snapshot_wall_clock::Datetime {
                seconds,
                nanoseconds,
            }

            struct snapshot_wall_clock::Datetime => crate::p2::wall_clock::Datetime {
                seconds,
                nanoseconds,
            }

            enum crate::p2::types::ErrorCode => snapshot_types::ErrorCode {
                Access,
                WouldBlock,
                Already,
                BadDescriptor,
                Busy,
                Deadlock,
                Quota,
                Exist,
                FileTooLarge,
                IllegalByteSequence,
                InProgress,
                Interrupted,
                Invalid,
                Io,
                IsDirectory,
                Loop,
                TooManyLinks,
                MessageSize,
                NameTooLong,
                NoDevice,
                NoEntry,
                NoLock,
                InsufficientMemory,
                InsufficientSpace,
                NotDirectory,
                NotEmpty,
                NotRecoverable,
                Unsupported,
                NoTty,
                NoSuchDevice,
                Overflow,
                NotPermitted,
                Pipe,
                ReadOnly,
                InvalidSeek,
                TextFileBusy,
                CrossDevice,
            }

            enum snapshot_types::Advice => crate::p2::types::Advice {
                Normal,
                Sequential,
                Random,
                WillNeed,
                DontNeed,
                NoReuse,
            }

            flags snapshot_types::DescriptorFlags => crate::p2::types::DescriptorFlags {
                READ,
                WRITE,
                FILE_INTEGRITY_SYNC,
                DATA_INTEGRITY_SYNC,
                REQUESTED_WRITE_SYNC,
                MUTATE_DIRECTORY,
            }

            flags crate::p2::types::DescriptorFlags => snapshot_types::DescriptorFlags {
                READ,
                WRITE,
                FILE_INTEGRITY_SYNC,
                DATA_INTEGRITY_SYNC,
                REQUESTED_WRITE_SYNC,
                MUTATE_DIRECTORY,
            }

            enum snapshot_types::DescriptorType => crate::p2::types::DescriptorType {
                Unknown,
                BlockDevice,
                CharacterDevice,
                Directory,
                Fifo,
                SymbolicLink,
                RegularFile,
                Socket,
            }

            enum crate::p2::types::DescriptorType => snapshot_types::DescriptorType {
                Unknown,
                BlockDevice,
                CharacterDevice,
                Directory,
                Fifo,
                SymbolicLink,
                RegularFile,
                Socket,
            }

            enum snapshot_types::NewTimestamp => crate::p2::types::NewTimestamp {
                NoChange,
                Now,
                Timestamp(e),
            }

            flags snapshot_types::PathFlags => crate::p2::types::PathFlags {
                SYMLINK_FOLLOW,
            }

            flags snapshot_types::OpenFlags => crate::p2::types::OpenFlags {
                CREATE,
                DIRECTORY,
                EXCLUSIVE,
                TRUNCATE,
            }

            struct crate::p2::types::MetadataHashValue => snapshot_types::MetadataHashValue {
                lower,
                upper,
            }

            struct crate::p2::types::DirectoryEntry => snapshot_types::DirectoryEntry {
                type_,
                name,
            }
        }

        impl From<crate::p2::types::DescriptorStat> for snapshot_types::DescriptorStat {
            fn from(e: crate::p2::types::DescriptorStat) -> snapshot_types::DescriptorStat {
                snapshot_types::DescriptorStat {
                    type_: e.type_.into(),
                    link_count: e.link_count,
                    size: e.size,
                    data_access_timestamp: e.data_access_timestamp.map(Into::into),
                    data_modification_timestamp: e.data_modification_timestamp.map(Into::into),
                    status_change_timestamp: e.status_change_timestamp.map(Into::into),
                }
            }
        }
    };
}

pub(crate) use filesystem_conversions;

/// Generates the `HostDescriptor` methods shared by every snapshot: the
/// ones whose shape matches `wasi:filesystem@0.2.x` exactly, delegating to
/// this crate's host with type conversions at the boundary.
macro_rules! shared_descriptor_methods {
    () => {
        async fn read_via_stream(
            &mut self,
            fd: Resource<Descriptor>,
            offset: types::Filesize,
        ) -> FsResult<Resource<DynInputStream>> {
            current::HostDescriptor::read_via_stream(self, fd, offset).await
        }

        async fn write_via_stream(
            &mut self,
            fd: Resource<Descriptor>,
            offset: types::Filesize,
        ) -> FsResult<Resource<DynOutputStream>> {
            current::HostDescriptor::write_via_stream(self, fd, offset).await
        }

        async fn append_via_stream(
            &mut self,
            fd: Resource<Descriptor>,
        ) -> FsResult<Resource<DynOutputStream>> {
            current::HostDescriptor::append_via_stream(self, fd).await
        }

        async fn advise(
            &mut self,
            fd: Resource<Descriptor>,
            offset: types::Filesize,
            length: types::Filesize,
            advice: types::Advice,
        ) -> FsResult<()> {
            current::HostDescriptor::advise(self, fd, offset, length, advice.into()).await
        }

        async fn sync_data(&mut self, fd: Resource<Descriptor>) -> FsResult<()> {
            current::HostDescriptor::sync_data(self, fd).await
        }

        async fn get_flags(
            &mut self,
            fd: Resource<Descriptor>,
        ) -> FsResult<types::DescriptorFlags> {
            Ok(current::HostDescriptor::get_flags(self, fd).await?.into())
        }

        async fn get_type(&mut self, fd: Resource<Descriptor>) -> FsResult<types::DescriptorType> {
            Ok(current::HostDescriptor::get_type(self, fd).await?.into())
        }

        async fn set_size(
            &mut self,
            fd: Resource<Descriptor>,
            size: types::Filesize,
        ) -> FsResult<()> {
            current::HostDescriptor::set_size(self, fd, size).await
        }

        async fn set_times(
            &mut self,
            fd: Resource<Descriptor>,
            data_access_timestamp: types::NewTimestamp,
            data_modification_timestamp: types::NewTimestamp,
        ) -> FsResult<()> {
            current::HostDescriptor::set_times(
                self,
                fd,
                data_access_timestamp.into(),
                data_modification_timestamp.into(),
            )
            .await
        }

        async fn read(
            &mut self,
            fd: Resource<Descriptor>,
            length: types::Filesize,
            offset: types::Filesize,
        ) -> FsResult<(Vec<u8>, bool)> {
            current::HostDescriptor::read(self, fd, length, offset).await
        }

        async fn write(
            &mut self,
            fd: Resource<Descriptor>,
            buffer: Vec<u8>,
            offset: types::Filesize,
        ) -> FsResult<types::Filesize> {
            current::HostDescriptor::write(self, fd, buffer, offset).await
        }

        async fn read_directory(
            &mut self,
            fd: Resource<Descriptor>,
        ) -> FsResult<Resource<DirectoryEntryStream>> {
            current::HostDescriptor::read_directory(self, fd).await
        }

        async fn sync(&mut self, fd: Resource<Descriptor>) -> FsResult<()> {
            current::HostDescriptor::sync(self, fd).await
        }

        async fn create_directory_at(
            &mut self,
            fd: Resource<Descriptor>,
            path: String,
        ) -> FsResult<()> {
            current::HostDescriptor::create_directory_at(self, fd, path).await
        }

        async fn stat(&mut self, fd: Resource<Descriptor>) -> FsResult<types::DescriptorStat> {
            Ok(current::HostDescriptor::stat(self, fd).await?.into())
        }

        async fn stat_at(
            &mut self,
            fd: Resource<Descriptor>,
            path_flags: types::PathFlags,
            path: String,
        ) -> FsResult<types::DescriptorStat> {
            Ok(
                current::HostDescriptor::stat_at(self, fd, path_flags.into(), path)
                    .await?
                    .into(),
            )
        }

        async fn set_times_at(
            &mut self,
            fd: Resource<Descriptor>,
            path_flags: types::PathFlags,
            path: String,
            data_access_timestamp: types::NewTimestamp,
            data_modification_timestamp: types::NewTimestamp,
        ) -> FsResult<()> {
            current::HostDescriptor::set_times_at(
                self,
                fd,
                path_flags.into(),
                path,
                data_access_timestamp.into(),
                data_modification_timestamp.into(),
            )
            .await
        }

        async fn link_at(
            &mut self,
            fd: Resource<Descriptor>,
            old_path_flags: types::PathFlags,
            old_path: String,
            new_descriptor: Resource<Descriptor>,
            new_path: String,
        ) -> FsResult<()> {
            current::HostDescriptor::link_at(
                self,
                fd,
                old_path_flags.into(),
                old_path,
                new_descriptor,
                new_path,
            )
            .await
        }

        async fn readlink_at(
            &mut self,
            fd: Resource<Descriptor>,
            path: String,
        ) -> FsResult<String> {
            current::HostDescriptor::readlink_at(self, fd, path).await
        }

        async fn remove_directory_at(
            &mut self,
            fd: Resource<Descriptor>,
            path: String,
        ) -> FsResult<()> {
            current::HostDescriptor::remove_directory_at(self, fd, path).await
        }

        async fn rename_at(
            &mut self,
            fd: Resource<Descriptor>,
            old_path: String,
            new_descriptor: Resource<Descriptor>,
            new_path: String,
        ) -> FsResult<()> {
            current::HostDescriptor::rename_at(self, fd, old_path, new_descriptor, new_path).await
        }

        async fn symlink_at(
            &mut self,
            fd: Resource<Descriptor>,
            old_path: String,
            new_path: String,
        ) -> FsResult<()> {
            current::HostDescriptor::symlink_at(self, fd, old_path, new_path).await
        }

        async fn unlink_file_at(&mut self, fd: Resource<Descriptor>, path: String) -> FsResult<()> {
            current::HostDescriptor::unlink_file_at(self, fd, path).await
        }

        async fn is_same_object(
            &mut self,
            fd: Resource<Descriptor>,
            other: Resource<Descriptor>,
        ) -> wasmtime::Result<bool> {
            current::HostDescriptor::is_same_object(self, fd, other).await
        }

        async fn metadata_hash(
            &mut self,
            fd: Resource<Descriptor>,
        ) -> FsResult<types::MetadataHashValue> {
            Ok(current::HostDescriptor::metadata_hash(self, fd)
                .await?
                .into())
        }

        async fn metadata_hash_at(
            &mut self,
            fd: Resource<Descriptor>,
            path_flags: types::PathFlags,
            path: String,
        ) -> FsResult<types::MetadataHashValue> {
            Ok(
                current::HostDescriptor::metadata_hash_at(self, fd, path_flags.into(), path)
                    .await?
                    .into(),
            )
        }

        async fn drop(&mut self, fd: Resource<Descriptor>) -> wasmtime::Result<()> {
            current::HostDescriptor::drop(self, fd).await
        }
    };
}

pub(crate) use shared_descriptor_methods;
