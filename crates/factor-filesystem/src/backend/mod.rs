//! Filesystem backends that ship with Spin.
//!
//! Both implement [`crate::spi::Filesystem`] and neither is privileged: an
//! out-of-tree backend sits alongside these on exactly the same footing.

pub mod host;
pub mod memory;

pub use host::HostFilesystem;
pub use memory::MemoryFilesystem;
