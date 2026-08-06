//! Pluggable filesystems for Spin.
//!
//! See `docs/content/sips/024-virtual-filesystem-factor.md` for the design and
//! motivation. In short: guests import plain `wasi:filesystem`, and what sits
//! behind it is a Rust trait rather than a hardcoded host directory.

#![deny(missing_docs)]

pub mod backend;
pub mod descriptor;
pub mod spi;

pub use descriptor::{Descriptor, Preopens};
pub use spi::{
    Advice, DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, Filesystem, FsPath,
    FsPathBuf, FsResult, MetadataHash, NewTimestamp, ObjectId, OpenFlags, OpenOptions, Opened,
    SetTimes, Stat,
};
