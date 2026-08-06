use std::sync::Arc;

use anyhow::bail;
use spin_factor_filesystem::backend::MemoryFilesystem;
use spin_factor_filesystem::runtime_config::spin::RuntimeConfigResolver;
use spin_factor_filesystem::{
    DescriptorFlags, ErrorCode, FilesystemDefinition, FilesystemFactor, RuntimeConfig,
};
use spin_factors::RuntimeFactors;
use spin_factors_test::{TestEnvironment, toml};

#[derive(RuntimeFactors)]
struct TestFactors {
    filesystem: FilesystemFactor,
}

impl From<RuntimeConfig> for TestFactorsRuntimeConfig {
    fn from(value: RuntimeConfig) -> Self {
        Self {
            filesystem: Some(value),
        }
    }
}

fn test_factors() -> TestFactors {
    TestFactors {
        filesystem: FilesystemFactor::new(".", false),
    }
}

fn memory_definition(writable: bool) -> FilesystemDefinition {
    FilesystemDefinition {
        filesystem: Arc::new(
            MemoryFilesystem::with_files([("seed.txt", "hi")]).expect("valid seed"),
        ),
        writable,
    }
}

#[tokio::test]
async fn labeled_mount_is_preopened() -> anyhow::Result<()> {
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.add_filesystem("data".into(), memory_definition(true));
    let env = TestEnvironment::new(test_factors()).extend_manifest(toml! {
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
    let (root, guest_path) = &preopens[0];
    assert_eq!(guest_path, "/data");
    assert!(root.flags().contains(DescriptorFlags::MUTATE_DIRECTORY));

    // The mount serves the backend's content...
    assert_eq!(root.stat_at("seed.txt", true).await?.size, 2);
    // ...and, being writable, accepts mutations.
    root.create_directory_at("fresh").await?;
    Ok(())
}

#[tokio::test]
async fn read_only_definition_gives_read_only_mount() -> anyhow::Result<()> {
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.add_filesystem("data".into(), memory_definition(false));
    let env = TestEnvironment::new(test_factors()).extend_manifest(toml! {
        [component.test-component]
        source = "does-not-exist.wasm"
        filesystems = [{ label = "data", path = "/data" }]
    });
    let state = env
        .runtime_config(runtime_config)?
        .build_instance_state()
        .await?;

    let preopens: Vec<_> = state.filesystem.preopens.entries().collect();
    let (root, _) = &preopens[0];
    assert!(!root.flags().contains(DescriptorFlags::MUTATE_DIRECTORY));
    assert_eq!(root.stat_at("seed.txt", true).await?.size, 2);
    assert_eq!(
        root.create_directory_at("fresh").await.unwrap_err(),
        ErrorCode::ReadOnly
    );
    Ok(())
}

#[tokio::test]
async fn unknown_label_errors() -> anyhow::Result<()> {
    let env = TestEnvironment::new(test_factors()).extend_manifest(toml! {
        [component.test-component]
        source = "does-not-exist.wasm"
        filesystems = [{ label = "nope", path = "/data" }]
    });
    let Err(err) = env
        .runtime_config(RuntimeConfig::default())?
        .build_instance_state()
        .await
    else {
        bail!("expected instance build to fail but it didn't");
    };
    assert!(
        err.to_string()
            .contains(r#"unknown filesystems label "nope""#),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[tokio::test]
async fn relative_mount_path_errors() -> anyhow::Result<()> {
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.add_filesystem("data".into(), memory_definition(true));
    let env = TestEnvironment::new(test_factors()).extend_manifest(toml! {
        [component.test-component]
        source = "does-not-exist.wasm"
        filesystems = [{ label = "data", path = "data" }]
    });
    let Err(err) = env
        .runtime_config(runtime_config)?
        .build_instance_state()
        .await
    else {
        bail!("expected instance build to fail but it didn't");
    };
    assert!(
        err.to_string().contains("must start with '/'"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_mount_path_errors() -> anyhow::Result<()> {
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.add_filesystem("a".into(), memory_definition(true));
    runtime_config.add_filesystem("b".into(), memory_definition(true));
    let env = TestEnvironment::new(test_factors()).extend_manifest(toml! {
        [component.test-component]
        source = "does-not-exist.wasm"
        filesystems = [{ label = "a", path = "/data" }, { label = "b", path = "/data" }]
    });
    let Err(err) = env
        .runtime_config(runtime_config)?
        .build_instance_state()
        .await
    else {
        bail!("expected instance build to fail but it didn't");
    };
    assert!(
        err.to_string().contains("more than once"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[tokio::test]
async fn toml_definitions_resolve() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    std::fs::create_dir(tmp.path().join("content"))?;
    std::fs::write(tmp.path().join("content/hello.txt"), "hello")?;

    let table: toml::Table = toml::from_str(
        r#"
        [filesystem.host-data]
        type = "host"
        path = "content"
        writable = true

        [filesystem.host-frozen]
        type = "host"
        path = "content"

        [filesystem.scratch]
        type = "memory"
        "#,
    )?;
    let resolver = RuntimeConfigResolver::default_types(Some(tmp.path().to_owned()));
    let config = resolver.resolve(Some(&table))?;

    let host_data = config.get_filesystem("host-data").unwrap();
    assert!(host_data.writable);
    assert_eq!(
        host_data
            .filesystem
            .stat_at(spin_factor_filesystem::FsPath::new("hello.txt")?, true)
            .await?
            .size,
        5
    );

    // `writable` defaults off for host filesystems and on for memory.
    assert!(!config.get_filesystem("host-frozen").unwrap().writable);
    assert!(config.get_filesystem("scratch").unwrap().writable);

    // Unregistered types are an error, not a silent skip.
    let bad: toml::Table = toml::from_str("[filesystem.x]\ntype = \"martian\"")?;
    let Err(err) = resolver.resolve(Some(&bad)) else {
        bail!("expected unregistered type to fail resolution");
    };
    assert!(err.to_string().contains("could not configure filesystem"));
    Ok(())
}
