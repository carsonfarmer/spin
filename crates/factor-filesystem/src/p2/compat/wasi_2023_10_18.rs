//! `wasi:filesystem@0.2.0-rc-2023-10-18` over this crate's host.

use wasmtime::component::Resource;
use wasmtime_wasi_io::streams::{DynInputStream, DynOutputStream, Error as IoError};

use crate::FilesystemCtxView;
use crate::descriptor::Descriptor;
use crate::p2::{DirectoryEntryStream, FsError, FsResult};

use super::filesystem_conversions;

mod bindings {
    #[allow(missing_docs, reason = "bindgen-generated")]
    mod generated {
        wasmtime::component::bindgen!({
            inline: r#"
                package spin:filesystem-host-2023-10-18;

                world filesystem-host {
                    import wasi:filesystem/types@0.2.0-rc-2023-10-18;
                    import wasi:filesystem/preopens@0.2.0-rc-2023-10-18;
                }
            "#,
            path: "../../wit",
            imports: { default: async | trappable },
            trappable_error_type: {
                "wasi:filesystem/types@0.2.0-rc-2023-10-18.error-code" => crate::p2::FsError,
            },
            with: {
                "wasi:io/poll.pollable": wasmtime_wasi_io::poll::DynPollable,
                "wasi:io/streams.input-stream": wasmtime_wasi_io::streams::DynInputStream,
                "wasi:io/streams.output-stream": wasmtime_wasi_io::streams::DynOutputStream,
                "wasi:io/streams.error": wasmtime_wasi_io::streams::Error,
                "wasi:filesystem/types.descriptor": crate::descriptor::Descriptor,
                "wasi:filesystem/types.directory-entry-stream": crate::p2::DirectoryEntryStream,
            },
            require_store_data_send: true,
        });
    }
    pub use generated::wasi::clocks0_2_0_rc_2023_10_18::wall_clock;
    pub use generated::wasi::filesystem0_2_0_rc_2023_10_18::{preopens, types};
}

pub use bindings::{preopens, types};

filesystem_conversions!(bindings::types, bindings::wall_clock);

mod current {
    pub use crate::p2::types::{Host, HostDescriptor, HostDirectoryEntryStream};
}

impl types::Host for FilesystemCtxView<'_> {
    fn convert_error_code(&mut self, err: FsError) -> wasmtime::Result<types::ErrorCode> {
        Ok(err.downcast()?.into())
    }

    async fn filesystem_error_code(
        &mut self,
        err: Resource<IoError>,
    ) -> wasmtime::Result<Option<types::ErrorCode>> {
        Ok(current::Host::filesystem_error_code(self, err)
            .await?
            .map(Into::into))
    }
}

impl types::HostDescriptor for FilesystemCtxView<'_> {
    super::shared_descriptor_methods!();

    async fn open_at(
        &mut self,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
        open_flags: types::OpenFlags,
        flags: types::DescriptorFlags,
        _modes: types::Modes,
    ) -> FsResult<Resource<Descriptor>> {
        current::HostDescriptor::open_at(
            self,
            fd,
            path_flags.into(),
            path,
            open_flags.into(),
            flags.into(),
        )
        .await
    }

    async fn access_at(
        &mut self,
        _fd: Resource<Descriptor>,
        _path_flags: types::PathFlags,
        _path: String,
        _type_: types::AccessType,
    ) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "access-at API is no longer supported in the latest snapshot"
        )))
    }

    async fn change_file_permissions_at(
        &mut self,
        _fd: Resource<Descriptor>,
        _path_flags: types::PathFlags,
        _path: String,
        _modes: types::Modes,
    ) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "change-file-permissions-at API is no longer supported in the latest snapshot"
        )))
    }

    async fn change_directory_permissions_at(
        &mut self,
        _fd: Resource<Descriptor>,
        _path_flags: types::PathFlags,
        _path: String,
        _modes: types::Modes,
    ) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "change-directory-permissions-at API is no longer supported in the latest snapshot"
        )))
    }

    async fn lock_shared(&mut self, _fd: Resource<Descriptor>) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "lock-shared API is no longer supported in the latest snapshot"
        )))
    }

    async fn lock_exclusive(&mut self, _fd: Resource<Descriptor>) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "lock-exclusive API is no longer supported in the latest snapshot"
        )))
    }

    async fn try_lock_shared(&mut self, _fd: Resource<Descriptor>) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "try-lock-shared API is no longer supported in the latest snapshot"
        )))
    }

    async fn try_lock_exclusive(&mut self, _fd: Resource<Descriptor>) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "try-lock-exclusive API is no longer supported in the latest snapshot"
        )))
    }

    async fn unlock(&mut self, _fd: Resource<Descriptor>) -> FsResult<()> {
        Err(FsError::trap(wasmtime::format_err!(
            "unlock API is no longer supported in the latest snapshot"
        )))
    }
}

impl types::HostDirectoryEntryStream for FilesystemCtxView<'_> {
    async fn read_directory_entry(
        &mut self,
        stream: Resource<DirectoryEntryStream>,
    ) -> FsResult<Option<types::DirectoryEntry>> {
        Ok(
            current::HostDirectoryEntryStream::read_directory_entry(self, stream)
                .await?
                .map(Into::into),
        )
    }

    async fn drop(&mut self, stream: Resource<DirectoryEntryStream>) -> wasmtime::Result<()> {
        current::HostDirectoryEntryStream::drop(self, stream).await
    }
}

impl preopens::Host for FilesystemCtxView<'_> {
    async fn get_directories(&mut self) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
        crate::p2::preopens::Host::get_directories(self).await
    }
}
