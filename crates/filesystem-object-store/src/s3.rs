//! The `s3` runtime-config filesystem type.

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use spin_factor_filesystem::FilesystemDefinition;
use spin_factor_filesystem::runtime_config::spin::MakeFilesystem;

use crate::ObjectStoreFilesystem;

/// The `s3` filesystem type: a bucket (Amazon S3 or any S3-compatible
/// endpoint such as MinIO or R2) under a key prefix.
pub struct S3FilesystemMaker;

/// Configuration for a `type = "s3"` filesystem.
///
/// Credentials come from the table when given, and from the standard `AWS_*`
/// environment variables otherwise. For multi-tenant deployments, give each
/// mount its own prefix *and* credentials scoped to that prefix, so the
/// service enforces the same boundary the runtime does.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3FilesystemRuntimeConfig {
    /// The bucket holding the filesystem.
    pub bucket: String,
    /// The bucket's region. Falls back to `AWS_REGION`/`AWS_DEFAULT_REGION`.
    #[serde(default)]
    pub region: Option<String>,
    /// A custom endpoint URL, for S3-compatible stores.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// The key prefix the mount lives under. Defaults to the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// Access key ID; paired with `secret_access_key`.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Secret access key; paired with `access_key_id`.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Session token for temporary credentials.
    #[serde(default)]
    pub session_token: Option<String>,
    /// Permit `http://` endpoints, for local development stores.
    #[serde(default)]
    pub allow_http: bool,
    /// Whether mounts may mutate the bucket contents. Defaults to read-only.
    #[serde(default)]
    pub writable: bool,
}

impl MakeFilesystem for S3FilesystemMaker {
    const RUNTIME_CONFIG_TYPE: &'static str = "s3";
    type RuntimeConfig = S3FilesystemRuntimeConfig;

    fn make_filesystem(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<FilesystemDefinition> {
        // `from_env` honours the standard AWS variables (credentials, region,
        // profile-provided keys resolved by the environment); table values
        // override them.
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&runtime_config.bucket);
        if let Some(region) = &runtime_config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &runtime_config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(access_key_id) = &runtime_config.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &runtime_config.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }
        if let Some(session_token) = &runtime_config.session_token {
            builder = builder.with_token(session_token);
        }
        if runtime_config.allow_http {
            builder = builder.with_allow_http(true);
        }
        let store = builder.build()?;

        let summary = if runtime_config.prefix.is_empty() {
            format!("s3 {}", runtime_config.bucket)
        } else {
            format!("s3 {}/{}", runtime_config.bucket, runtime_config.prefix)
        };
        let filesystem =
            ObjectStoreFilesystem::new(Arc::new(store), &runtime_config.prefix, summary)?;
        Ok(FilesystemDefinition {
            filesystem: Arc::new(filesystem),
            writable: runtime_config.writable,
        })
    }
}
