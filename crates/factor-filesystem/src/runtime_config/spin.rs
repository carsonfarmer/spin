//! Runtime configuration implementation used by the Spin CLI.
//!
//! Filesystems are declared in runtime config as `[filesystem.<label>]`
//! tables whose `type` field selects a registered backend:
//!
//! ```toml
//! [filesystem.repos]
//! type = "host"
//! path = "/var/lib/spin/repos"
//! writable = true
//!
//! [filesystem.scratch]
//! type = "memory"
//! ```
//!
//! Embedders register additional types - an object store, an overlay -
//! with [`RuntimeConfigResolver::register_filesystem_type`], exactly like
//! `[key_value_store.<label>]` types.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use spin_factors::runtime_config::toml::GetTomlValue;

use super::{FilesystemDefinition, RuntimeConfig};
use crate::backend::{HostFilesystem, MemoryFilesystem};

/// Defines the construction of a filesystem from serialized runtime config.
pub trait MakeFilesystem: 'static + Send + Sync {
    /// Unique identifier for the backend, matched against the `type` field.
    const RUNTIME_CONFIG_TYPE: &'static str;
    /// The deserialized shape of the backend's `[filesystem.<label>]` table.
    type RuntimeConfig: DeserializeOwned;

    /// Creates a filesystem from its runtime configuration.
    ///
    /// Called once per label per application, so a stateful backend (like
    /// an in-memory tree) lives for the lifetime of the application and is
    /// shared by every component instance that mounts the label.
    fn make_filesystem(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<FilesystemDefinition>;
}

type FilesystemFromToml =
    Arc<dyn Fn(toml::Table) -> anyhow::Result<FilesystemDefinition> + Send + Sync>;

fn filesystem_from_toml_fn<T: MakeFilesystem>(maker: T) -> FilesystemFromToml {
    Arc::new(move |table| {
        let runtime_config: T::RuntimeConfig = table
            .try_into()
            .context("could not parse filesystem runtime config")?;
        maker
            .make_filesystem(runtime_config)
            .context("could not make filesystem from runtime config")
    })
}

/// Converts `[filesystem.<label>]` TOML tables into a [`RuntimeConfig`].
///
/// Backend types (the `type` field) are registered with
/// [`Self::register_filesystem_type`]; the `host` and `memory` backends
/// that ship with Spin are registered by [`Self::default_types`].
#[derive(Default, Clone)]
pub struct RuntimeConfigResolver {
    filesystem_types: HashMap<&'static str, FilesystemFromToml>,
}

impl RuntimeConfigResolver {
    /// Creates a resolver with no registered filesystem types.
    pub fn new() -> Self {
        <Self as Default>::default()
    }

    /// Creates a resolver with the built-in `host` and `memory` types.
    ///
    /// Relative `path`s in `host` definitions resolve against `base_path`,
    /// conventionally the directory containing the runtime config file.
    pub fn default_types(base_path: Option<PathBuf>) -> Self {
        let mut resolver = Self::new();
        resolver
            .register_filesystem_type(HostFilesystemMaker::new(base_path))
            .expect("no duplicate types in an empty resolver");
        resolver
            .register_filesystem_type(MemoryFilesystemMaker)
            .expect("no duplicate types in an empty resolver");
        resolver
    }

    /// Registers a filesystem type with the resolver.
    pub fn register_filesystem_type<T: MakeFilesystem>(
        &mut self,
        filesystem_type: T,
    ) -> anyhow::Result<()> {
        if self
            .filesystem_types
            .insert(
                T::RUNTIME_CONFIG_TYPE,
                filesystem_from_toml_fn(filesystem_type),
            )
            .is_some()
        {
            anyhow::bail!("duplicate filesystem type {:?}", T::RUNTIME_CONFIG_TYPE);
        }
        Ok(())
    }

    /// Resolves the `[filesystem.<label>]` tables of the given TOML into a
    /// runtime config.
    pub fn resolve(&self, table: Option<&impl GetTomlValue>) -> anyhow::Result<RuntimeConfig> {
        let mut runtime_config = RuntimeConfig::default();
        let Some(table) = table.and_then(|t| t.get("filesystem")) else {
            return Ok(runtime_config);
        };
        let table: HashMap<String, FilesystemConfig> = table.clone().try_into()?;
        for (label, config) in table {
            let definition = self
                .filesystem_from_config(config)
                .with_context(|| format!("could not configure filesystem with label '{label}'"))?;
            runtime_config.add_filesystem(label, definition);
        }
        Ok(runtime_config)
    }

    fn filesystem_from_config(
        &self,
        config: FilesystemConfig,
    ) -> anyhow::Result<FilesystemDefinition> {
        let config_type = config.type_.as_str();
        let maker = self.filesystem_types.get(config_type).with_context(|| {
            format!(
                "the filesystem type '{config_type}' was not registered with the config resolver"
            )
        })?;
        maker(config.config)
    }
}

/// A `[filesystem.<label>]` table: a `type` plus type-specific fields.
#[derive(Deserialize, Clone)]
pub struct FilesystemConfig {
    /// The registered backend type.
    #[serde(rename = "type")]
    pub type_: String,
    /// The type-specific configuration.
    #[serde(flatten)]
    pub config: toml::Table,
}

/// The built-in `host` filesystem type: a real directory on the host,
/// sandboxed exactly like a `files = [...]` mount.
pub struct HostFilesystemMaker {
    /// Base for resolving relative `path`s, conventionally the runtime
    /// config directory.
    base_path: Option<PathBuf>,
}

impl HostFilesystemMaker {
    /// Creates a maker resolving relative paths against `base_path`.
    pub fn new(base_path: Option<PathBuf>) -> Self {
        Self { base_path }
    }
}

/// Configuration for a `type = "host"` filesystem.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HostFilesystemRuntimeConfig {
    /// The host directory serving the filesystem's contents.
    pub path: PathBuf,
    /// Whether mounts may mutate the directory. Defaults to read-only.
    #[serde(default)]
    pub writable: bool,
}

impl MakeFilesystem for HostFilesystemMaker {
    const RUNTIME_CONFIG_TYPE: &'static str = "host";
    type RuntimeConfig = HostFilesystemRuntimeConfig;

    fn make_filesystem(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<FilesystemDefinition> {
        let path = match &self.base_path {
            Some(base) if runtime_config.path.is_relative() => base.join(&runtime_config.path),
            _ => runtime_config.path.clone(),
        };
        let filesystem = HostFilesystem::open(&path)
            .with_context(|| format!("failed to open host directory {}", path.display()))?;
        Ok(FilesystemDefinition {
            filesystem: Arc::new(filesystem),
            writable: runtime_config.writable,
        })
    }
}

/// The built-in `memory` filesystem type: an empty in-memory tree that
/// lives as long as the application and is shared by all its instances.
pub struct MemoryFilesystemMaker;

/// Configuration for a `type = "memory"` filesystem.
#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MemoryFilesystemRuntimeConfig {
    /// Whether mounts may mutate the tree. Defaults to writable, since a
    /// permanently empty read-only filesystem serves no purpose.
    #[serde(default = "default_true")]
    pub writable: bool,
}

fn default_true() -> bool {
    true
}

impl MakeFilesystem for MemoryFilesystemMaker {
    const RUNTIME_CONFIG_TYPE: &'static str = "memory";
    type RuntimeConfig = MemoryFilesystemRuntimeConfig;

    fn make_filesystem(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<FilesystemDefinition> {
        Ok(FilesystemDefinition {
            filesystem: Arc::new(MemoryFilesystem::new()),
            writable: runtime_config.writable,
        })
    }
}
