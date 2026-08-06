//! Pluggable filesystems for Spin.
//!
//! See `docs/content/sips/024-virtual-filesystem-factor.md` for the design and
//! motivation. In short: guests import plain `wasi:filesystem`, and what sits
//! behind it is a Rust trait rather than a hardcoded host directory.

#![deny(missing_docs)]

pub mod backend;
pub mod descriptor;
mod factor;
pub mod p2;
pub mod p3;
pub mod runtime_config;
pub mod spi;
mod task;

use wasmtime::component::{HasData, ResourceTable};

pub use descriptor::{Descriptor, Preopens};
pub use factor::{
    AppState, FILESYSTEMS_KEY, FilesystemFactor, FilesystemMountMetadata, InstanceBuilder,
};
pub use runtime_config::{FilesystemDefinition, RuntimeConfig};
pub use spi::{
    Advice, DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, Filesystem, FsPath,
    FsPathBuf, FsResult, MetadataHash, NewTimestamp, ObjectId, OpenFlags, OpenOptions, Opened,
    SetTimes, Stat,
};

/// The per-instance filesystem state: the mounts one component instance can
/// see.
#[derive(Default)]
pub struct FilesystemCtx {
    /// The preopened directories.
    pub preopens: Preopens,
}

/// A mutable view of a [`FilesystemCtx`] together with the instance's
/// resource table - the shape the wasmtime linker's data closures produce,
/// and what the `wasi:filesystem` host implementations are written against.
pub struct FilesystemCtxView<'a> {
    /// The instance's filesystem state.
    pub ctx: &'a mut FilesystemCtx,
    /// The instance's resource table.
    pub table: &'a mut ResourceTable,
}

/// Marker satisfying wasmtime's `HasData` for the `add_to_linker` calls of
/// this crate's `wasi:filesystem` implementations.
pub struct HasFilesystem;

impl HasData for HasFilesystem {
    type Data<'a> = FilesystemCtxView<'a>;
}
