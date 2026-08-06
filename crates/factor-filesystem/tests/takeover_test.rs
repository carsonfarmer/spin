//! Tests for the linker takeover of `wasi:filesystem`.
//!
//! The WASI factor links every `wasi:filesystem` version as part of its
//! all-in-one setup; the filesystem factor then redefines those interfaces
//! by shadowing. These tests pin both sides of that contract: registered
//! after the WASI factor it works, registered before it fails loudly at
//! init rather than mis-linking silently.

use std::sync::Arc;

use spin_factor_filesystem::backend::MemoryFilesystem;
use spin_factor_filesystem::{
    DescriptorFlags, FilesystemDefinition, FilesystemFactor, RuntimeConfig,
};
use spin_factor_wasi::{DummyFilesMounter, WasiFactor};
use spin_factors::RuntimeFactors;
use spin_factors_test::{TestEnvironment, toml};

#[derive(RuntimeFactors)]
struct TestFactors {
    wasi: WasiFactor,
    filesystem: FilesystemFactor,
}

impl From<RuntimeConfig> for TestFactorsRuntimeConfig {
    fn from(value: RuntimeConfig) -> Self {
        Self {
            filesystem: Some(value),
            ..Default::default()
        }
    }
}

#[tokio::test]
async fn filesystem_factor_shadows_wasi_factor_filesystem() -> anyhow::Result<()> {
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.add_filesystem(
        "data".into(),
        FilesystemDefinition {
            filesystem: Arc::new(MemoryFilesystem::new()),
            writable: true,
        },
    );

    // `TestEnvironment::new` runs `RuntimeFactors::init`, which is where a
    // duplicate `wasi:filesystem` definition would fail; getting past it
    // proves the shadow takeover linked cleanly over the WASI factor.
    let env = TestEnvironment::new(TestFactors {
        wasi: WasiFactor::new(DummyFilesMounter),
        filesystem: FilesystemFactor::new(".", false),
    })
    .extend_manifest(toml! {
        [component.test-component]
        source = "does-not-exist.wasm"
        filesystems = [{ label = "data", path = "/data" }]
    });

    let state = env
        .runtime_config(runtime_config)?
        .build_instance_state()
        .await?;
    let preopens: Vec<_> = state.filesystem.preopens.entries().collect();
    assert_eq!(preopens.len(), 1);
    assert_eq!(preopens[0].1, "/data");
    assert!(
        preopens[0]
            .0
            .flags()
            .contains(DescriptorFlags::MUTATE_DIRECTORY)
    );
    Ok(())
}

#[derive(RuntimeFactors)]
struct WrongOrderFactors {
    filesystem: FilesystemFactor,
    wasi: WasiFactor,
}

#[test]
fn registering_filesystem_before_wasi_fails_at_init() {
    let engine = spin_factors::wasmtime::Engine::default();
    let mut linker = spin_factors::wasmtime::component::Linker::<
        <WrongOrderFactors as RuntimeFactors>::InstanceState,
    >::new(&engine);
    let mut factors = WrongOrderFactors {
        filesystem: FilesystemFactor::new(".", false),
        wasi: WasiFactor::new(DummyFilesMounter),
    };

    // The WASI factor initializes second and tries to define
    // `wasi:filesystem` again with shadowing off: a loud duplicate
    // definition error, not a silently wrong linker.
    let err = factors
        .init(&mut linker)
        .expect_err("wrong factor order should fail init");
    assert!(
        format!("{err:#}").contains("filesystem"),
        "unexpected error: {err:#}"
    );
}
