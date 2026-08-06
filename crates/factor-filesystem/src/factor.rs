//! The [`Factor`] implementation providing `wasi:filesystem`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, ensure};
use serde::{Deserialize, Serialize};
use spin_common::{ui::quoted_path, url::parse_file_url};
use spin_factors::{
    ConfigureAppContext, Factor, FactorInstanceBuilder, InitContext, PrepareContext, RuntimeFactors,
};
use spin_locked_app::MetadataKey;

use crate::backend::HostFilesystem;
use crate::runtime_config::RuntimeConfig;
use crate::spi::Filesystem;
use crate::{FilesystemCtx, FilesystemCtxView, HasFilesystem, p2};

/// Metadata key for a component's filesystem mounts.
pub const FILESYSTEMS_KEY: MetadataKey<Vec<FilesystemMountMetadata>> =
    MetadataKey::new("filesystems");

/// One entry of a component's `filesystems` manifest field, as recorded in
/// the locked application.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilesystemMountMetadata {
    /// The label of a filesystem defined in runtime config.
    pub label: String,
    /// The absolute guest path where the filesystem is mounted.
    pub path: String,
}

/// The factor providing `wasi:filesystem` to guests, with pluggable
/// storage behind every mount.
///
/// Two kinds of mount reach a component:
///
/// - `files = [...]` manifest mounts, lowered to [`HostFilesystem`] mounts
///   with the same paths and the same read-only-unless-transient-writes
///   semantics `spin-factor-wasi` gave them.
/// - `filesystems = [{ label, path }]` manifest mounts, resolved through
///   `[filesystem.<label>]` runtime config to whatever backend the label
///   is defined as.
///
/// This factor owns the `wasi:filesystem` interfaces in the linker, so the
/// WASI factor must be constructed without its own filesystem linking (see
/// `spin-factor-wasi`) when both are present.
pub struct FilesystemFactor {
    working_dir: PathBuf,
    allow_transient_writes: bool,
}

impl FilesystemFactor {
    /// Creates a filesystem factor.
    ///
    /// `working_dir` is what `files = [...]` content sources are resolved
    /// against; `allow_transient_writes` makes those mounts writable.
    pub fn new(working_dir: impl Into<PathBuf>, allow_transient_writes: bool) -> Self {
        Self {
            working_dir: working_dir.into(),
            allow_transient_writes,
        }
    }
}

impl Factor for FilesystemFactor {
    type RuntimeConfig = RuntimeConfig;
    type AppState = AppState;
    type InstanceBuilder = InstanceBuilder;

    fn init<T: InitContext<Self>>(&mut self, ctx: &mut T) -> anyhow::Result<()> {
        fn get_view<C: InitContext<FilesystemFactor>>(
            data: &mut C::StoreData,
        ) -> FilesystemCtxView<'_> {
            let (ctx, table) = C::get_data_with_table(data);
            FilesystemCtxView { ctx, table }
        }
        p2::types::add_to_linker::<_, HasFilesystem>(ctx.linker(), get_view::<T>)?;
        p2::preopens::add_to_linker::<_, HasFilesystem>(ctx.linker(), get_view::<T>)?;
        Ok(())
    }

    fn configure_app<T: RuntimeFactors>(
        &self,
        mut ctx: ConfigureAppContext<T, Self>,
    ) -> anyhow::Result<Self::AppState> {
        let runtime_config = ctx.take_runtime_config().unwrap_or_default();

        let mut component_mounts = HashMap::new();
        for component in ctx.app().components() {
            let mut mounts = Vec::new();

            // `files = [...]` mounts lower to host-backed mounts, preserving
            // the semantics they had under spin-factor-wasi.
            for content_dir in component.files() {
                let source_uri =
                    content_dir.content.source.as_deref().with_context(|| {
                        format!("Missing 'source' on files mount {content_dir:?}")
                    })?;
                let source_path = self.working_dir.join(parse_file_url(source_uri)?);
                ensure!(
                    source_path.is_dir(),
                    "file mounts must be directories; {} is not a directory",
                    quoted_path(&source_path),
                );
                let guest_path = content_dir
                    .path
                    .to_str()
                    .with_context(|| format!("guest path {:?} not valid UTF-8", content_dir.path))?
                    .to_string();
                let filesystem = HostFilesystem::open(&source_path).with_context(|| {
                    format!("failed to open file mount {}", quoted_path(&source_path))
                })?;
                mounts.push(Mount {
                    guest_path,
                    filesystem: Arc::new(filesystem),
                    writable: self.allow_transient_writes,
                });
            }

            // `filesystems = [...]` mounts resolve through runtime config.
            for mount in component.get_metadata(FILESYSTEMS_KEY)?.unwrap_or_default() {
                let definition =
                    runtime_config
                        .get_filesystem(&mount.label)
                        .with_context(|| {
                            format!(
                                "unknown filesystems label {:?} for component {:?}; \
                         define it as [filesystem.{}] in runtime config",
                                mount.label,
                                component.id(),
                                mount.label,
                            )
                        })?;
                ensure!(
                    mount.path.starts_with('/'),
                    "filesystem mount path {:?} for component {:?} must start with '/'",
                    mount.path,
                    component.id(),
                );
                mounts.push(Mount {
                    guest_path: mount.path,
                    filesystem: definition.filesystem.clone(),
                    writable: definition.writable,
                });
            }

            // Duplicate guest paths would silently shadow one another in the
            // guest libc's preopen table; refuse them up front.
            let mut seen = HashSet::new();
            for mount in &mounts {
                ensure!(
                    seen.insert(mount.guest_path.as_str()),
                    "component {:?} mounts {:?} more than once",
                    component.id(),
                    mount.guest_path,
                );
            }

            component_mounts.insert(component.id().to_string(), mounts);
        }

        Ok(AppState { component_mounts })
    }

    fn prepare<T: RuntimeFactors>(
        &self,
        ctx: PrepareContext<T, Self>,
    ) -> anyhow::Result<InstanceBuilder> {
        let mounts = ctx
            .app_state()
            .component_mounts
            .get(ctx.app_component().id())
            .cloned()
            .unwrap_or_default();
        Ok(InstanceBuilder { mounts })
    }
}

/// One mount for one component: where it appears, what serves it, and
/// whether writes are allowed through it.
#[derive(Clone)]
struct Mount {
    guest_path: String,
    filesystem: Arc<dyn Filesystem>,
    writable: bool,
}

/// The filesystem factor's application state: each component's resolved
/// mounts.
pub struct AppState {
    component_mounts: HashMap<String, Vec<Mount>>,
}

/// Builds the per-instance filesystem state.
pub struct InstanceBuilder {
    mounts: Vec<Mount>,
}

impl InstanceBuilder {
    /// Adds a mount for this instance, in addition to those from the
    /// application configuration. For host embedders that provision mounts
    /// programmatically.
    pub fn mount(
        &mut self,
        guest_path: impl Into<String>,
        filesystem: Arc<dyn Filesystem>,
        writable: bool,
    ) {
        self.mounts.push(Mount {
            guest_path: guest_path.into(),
            filesystem,
            writable,
        });
    }
}

impl FactorInstanceBuilder for InstanceBuilder {
    type InstanceState = FilesystemCtx;

    fn build(self) -> anyhow::Result<Self::InstanceState> {
        let mut ctx = FilesystemCtx::default();
        for mount in self.mounts {
            ctx.preopens
                .mount(mount.guest_path, mount.filesystem, mount.writable);
        }
        Ok(ctx)
    }
}
