//! Runtime configuration for the filesystem factor.

pub mod spin;

use std::{collections::HashMap, sync::Arc};

use crate::spi::Filesystem;

/// A configured filesystem: the backend plus the access the mount grants.
#[derive(Clone)]
pub struct FilesystemDefinition {
    /// The backend serving the filesystem's contents.
    pub filesystem: Arc<dyn Filesystem>,
    /// Whether mounts of this filesystem may mutate it.
    ///
    /// Enforced by the descriptor layer: a non-writable mount's root
    /// descriptor lacks `mutate-directory`, so every mutation through it
    /// reports `read-only` regardless of what the backend would allow.
    pub writable: bool,
}

/// Runtime configuration for all filesystems: a map from the labels
/// components use in their `filesystems` manifest field to definitions.
#[derive(Default, Clone)]
pub struct RuntimeConfig {
    filesystems: HashMap<String, FilesystemDefinition>,
}

impl RuntimeConfig {
    /// Adds a filesystem definition for the given label.
    ///
    /// If a definition already exists for the label, it is replaced.
    pub fn add_filesystem(&mut self, label: String, definition: FilesystemDefinition) {
        self.filesystems.insert(label, definition);
    }

    /// Returns whether a filesystem is defined for the given label.
    pub fn has_filesystem(&self, label: &str) -> bool {
        self.filesystems.contains_key(label)
    }

    /// Returns the filesystem definition for the given label.
    pub fn get_filesystem(&self, label: &str) -> Option<&FilesystemDefinition> {
        self.filesystems.get(label)
    }
}
