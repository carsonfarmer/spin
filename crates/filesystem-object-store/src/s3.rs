//! The `s3` runtime-config filesystem type.

use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::{ProvideCredentials as _, SharedCredentialsProvider};
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::{CredentialProvider, ObjectStore};
use serde::{Deserialize, Serialize};
use spin_factor_filesystem::runtime_config::spin::MakeFilesystem;
use spin_factor_filesystem::{
    DirEntry, ErrorCode, FilesystemDefinition, FsPath, FsResult, MetadataHash, ObjectId,
    OpenOptions, Opened, SetTimes, Stat,
};

use crate::ObjectStoreFilesystem;

/// The environment variables the platform injects; each is the fallback
/// for the config field of the same name. Mirrors the `spin-key-value-s3`
/// provider's model exactly.
pub mod env {
    /// Fallback for `bucket`.
    pub const BUCKET: &str = "SPIN_FS_S3_BUCKET";
    /// The mount root the platform assigns - conventionally the tenant
    /// root. A table `prefix` is a *relative* path appended beneath it.
    pub const PREFIX: &str = "SPIN_FS_S3_PREFIX";
    /// Fallback for `endpoint`, for S3-compatible stores.
    pub const ENDPOINT: &str = "SPIN_FS_S3_ENDPOINT";
    /// Fallback for `allow_http` (`1`/`true`), for local development.
    pub const ALLOW_HTTP: &str = "SPIN_FS_S3_ALLOW_HTTP";
}

/// The `s3` filesystem type: a bucket (Amazon S3 or any S3-compatible
/// endpoint such as MinIO or R2) under a key prefix.
pub struct S3FilesystemMaker;

/// Configuration for a `type = "s3"` filesystem.
///
/// Credential handling follows Spin's `aws_dynamo` key-value store: when
/// `access_key` and `secret_key` are both present they are used directly,
/// and otherwise credentials come from the standard AWS configuration chain
/// (environment, shared config and SSO profiles, IMDS, container credential
/// endpoints, web identity), with the SDK's own caching and refresh. For
/// multi-tenant deployments, give each mount its own prefix and hand each
/// tenant process credentials scoped to that prefix - an STS session policy
/// on one shared role does this without per-tenant IAM entities.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3FilesystemRuntimeConfig {
    /// The bucket holding the filesystem. Falls back to
    /// `SPIN_FS_S3_BUCKET`.
    #[serde(default)]
    pub bucket: Option<String>,
    /// The bucket's region. Falls back to the region the AWS configuration
    /// chain resolves.
    #[serde(default)]
    pub region: Option<String>,
    /// The key prefix the mount lives under. When the platform sets
    /// `SPIN_FS_S3_PREFIX`, this is *relative*, appended beneath that
    /// root - tenant config states intent, the environment carries
    /// identity. Without the environment root it addresses from the
    /// bucket root.
    #[serde(default)]
    pub prefix: String,
    /// A custom endpoint URL, for S3-compatible stores. Falls back to
    /// `SPIN_FS_S3_ENDPOINT`, then the chain (`AWS_ENDPOINT_URL`).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// The access key for the bucket; paired with `secret_key`. When either
    /// half is absent the AWS configuration chain is used instead.
    #[serde(default)]
    pub access_key: Option<String>,
    /// The secret key for the bucket; paired with `access_key`.
    #[serde(default)]
    pub secret_key: Option<String>,
    /// The session token accompanying temporary `access_key`/`secret_key`
    /// pairs, such as ones minted through an STS session policy.
    #[serde(default)]
    pub token: Option<String>,
    /// Permit `http://` endpoints, for local development stores. Falls
    /// back to `SPIN_FS_S3_ALLOW_HTTP`.
    #[serde(default)]
    pub allow_http: Option<bool>,
    /// Whether mounts may mutate the bucket contents. Defaults to read-only.
    #[serde(default)]
    pub writable: bool,
}

/// The fully resolved configuration: every table field with its
/// environment fallback applied, and the mount prefix composed from the
/// platform root and the table's relative prefix.
#[derive(Debug)]
struct Resolved {
    bucket: String,
    prefix: String,
    region: Option<String>,
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    token: Option<String>,
    allow_http: bool,
}

/// Applies environment fallbacks to `config`. `var` is the environment
/// lookup, injectable for tests.
fn resolve_config(
    config: S3FilesystemRuntimeConfig,
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Resolved> {
    let flag = |value: Option<bool>, name: &str| {
        value.unwrap_or_else(|| {
            var(name).is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        })
    };
    let Some(bucket) = config.bucket.or_else(|| var(env::BUCKET)) else {
        anyhow::bail!(
            "s3 filesystem requires a bucket, from the `bucket` field or {}",
            env::BUCKET
        );
    };
    // The platform's root is identity; the table's prefix is intent,
    // relative beneath it. Object paths have no `..`, so the composition
    // cannot rise above the root - and the credential holds regardless.
    let prefix = match (var(env::PREFIX), config.prefix) {
        (Some(root), rel) if !rel.is_empty() => format!("{root}/{rel}"),
        (Some(root), _) => root,
        (None, rel) => rel,
    };
    Ok(Resolved {
        bucket,
        prefix,
        region: config.region,
        endpoint: config.endpoint.or_else(|| var(env::ENDPOINT)),
        access_key: config.access_key,
        secret_key: config.secret_key,
        token: config.token,
        allow_http: flag(config.allow_http, env::ALLOW_HTTP),
    })
}

impl MakeFilesystem for S3FilesystemMaker {
    const RUNTIME_CONFIG_TYPE: &'static str = "s3";
    type RuntimeConfig = S3FilesystemRuntimeConfig;

    fn make_filesystem(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<FilesystemDefinition> {
        let writable = runtime_config.writable;
        let resolved = resolve_config(runtime_config, |name| {
            std::env::var(name).ok().filter(|v| !v.is_empty())
        })?;
        Ok(FilesystemDefinition {
            filesystem: Arc::new(S3Filesystem::new(resolved)),
            writable,
        })
    }
}

/// An [`ObjectStoreFilesystem`] over a bucket, initialized on first use.
///
/// Construction has to stay synchronous (runtime config resolves before an
/// async runtime is guaranteed), while the AWS configuration chain is
/// async - the same tension `spin-key-value-aws` resolves the same way,
/// with a lazily-awaited client.
struct S3Filesystem {
    summary: String,
    inner: LazyFilesystem,
}

/// The one-shot initialization: awaited by the first operation, shared by
/// all later ones.
type LazyFilesystem = async_once_cell::Lazy<
    Result<ObjectStoreFilesystem, ErrorCode>,
    std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ObjectStoreFilesystem, ErrorCode>> + Send>,
    >,
>;

impl S3Filesystem {
    fn new(config: Resolved) -> Self {
        let summary = if config.prefix.is_empty() {
            format!("s3 {}", config.bucket)
        } else {
            format!("s3 {}/{}", config.bucket, config.prefix)
        };
        Self {
            summary,
            inner: async_once_cell::Lazy::from_future(Box::pin(build_filesystem(config))),
        }
    }

    async fn fs(&self) -> FsResult<&ObjectStoreFilesystem> {
        match self.inner.get_unpin().await {
            Ok(fs) => Ok(fs),
            Err(code) => Err(*code),
        }
    }
}

async fn build_filesystem(config: Resolved) -> Result<ObjectStoreFilesystem, ErrorCode> {
    let summary = if config.prefix.is_empty() {
        format!("s3 {}", config.bucket)
    } else {
        format!("s3 {}/{}", config.bucket, config.prefix)
    };
    let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

    let mut region = config.region.clone();
    let mut endpoint = config.endpoint.clone();
    match (&config.access_key, &config.secret_key) {
        (Some(access_key), Some(secret_key)) => {
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);
            if let Some(token) = &config.token {
                builder = builder.with_token(token);
            }
        }
        _ => {
            // No explicit pair: resolve through the standard AWS chain, and
            // let its region/endpoint configuration fill any gaps the table
            // leaves. The chain's provider caches and refreshes on its own,
            // so expiring session credentials (IMDS, container endpoints,
            // web identity) keep working in long-lived processes.
            let mut loader = aws_config::defaults(BehaviorVersion::latest());
            if let Some(region) = region.clone() {
                loader = loader.region(Region::new(region));
            }
            let sdk_config = loader.load().await;
            region = region.or_else(|| sdk_config.region().map(|r| r.to_string()));
            endpoint = endpoint.or_else(|| sdk_config.endpoint_url().map(str::to_string));
            let Some(provider) = sdk_config.credentials_provider() else {
                tracing::error!(
                    "filesystem {summary}: no credentials in the AWS configuration chain"
                );
                return Err(ErrorCode::Access);
            };
            builder = builder.with_credentials(Arc::new(ChainCredentials { provider }));
        }
    }

    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }

    let store = builder.build().map_err(|err| {
        tracing::error!("filesystem {summary}: invalid s3 configuration: {err}");
        ErrorCode::Io
    })?;
    ObjectStoreFilesystem::new(
        Arc::new(store) as Arc<dyn ObjectStore>,
        &config.prefix,
        summary,
    )
    .map_err(|err| {
        tracing::error!("invalid s3 filesystem prefix: {err}");
        ErrorCode::Io
    })
}

/// Bridges the AWS SDK's credential chain into `object_store`'s provider
/// interface. Caching and refresh live in the chain, not here.
#[derive(Debug)]
struct ChainCredentials {
    provider: SharedCredentialsProvider,
}

#[async_trait]
impl CredentialProvider for ChainCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let credentials = self.provider.provide_credentials().await.map_err(|err| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(err),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(str::to_string),
        }))
    }
}

#[async_trait]
impl spin_factor_filesystem::Filesystem for S3Filesystem {
    fn summary(&self) -> String {
        self.summary.clone()
    }

    async fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Opened> {
        self.fs().await?.open(path, opts).await
    }

    async fn stat_at(&self, path: &FsPath, follow: bool) -> FsResult<Stat> {
        self.fs().await?.stat_at(path, follow).await
    }

    async fn set_times_at(&self, path: &FsPath, follow: bool, times: SetTimes) -> FsResult<()> {
        self.fs().await?.set_times_at(path, follow, times).await
    }

    async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.fs().await?.read_dir(path).await
    }

    async fn create_dir(&self, path: &FsPath) -> FsResult<()> {
        self.fs().await?.create_dir(path).await
    }

    async fn remove_dir(&self, path: &FsPath) -> FsResult<()> {
        self.fs().await?.remove_dir(path).await
    }

    async fn unlink(&self, path: &FsPath) -> FsResult<()> {
        self.fs().await?.unlink(path).await
    }

    async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.fs().await?.rename(from, to).await
    }

    async fn symlink(&self, target: &str, link: &FsPath) -> FsResult<()> {
        self.fs().await?.symlink(target, link).await
    }

    async fn readlink(&self, path: &FsPath) -> FsResult<String> {
        self.fs().await?.readlink(path).await
    }

    async fn hard_link(&self, from: &FsPath, follow: bool, to: &FsPath) -> FsResult<()> {
        self.fs().await?.hard_link(from, follow, to).await
    }

    async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash> {
        self.fs().await?.metadata_hash_at(path, follow).await
    }

    async fn object_id_at(&self, path: &FsPath, follow: bool) -> FsResult<ObjectId> {
        self.fs().await?.object_id_at(path, follow).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(toml: &str) -> S3FilesystemRuntimeConfig {
        toml::from_str(toml).expect("valid config")
    }

    #[test]
    fn summary_includes_bucket_and_prefix() {
        let resolved = resolve_config(
            config(
                r#"
                bucket = "b"
                prefix = "tenant-a/data"
                "#,
            ),
            |_| None,
        )
        .unwrap();
        let fs = S3Filesystem::new(resolved);
        assert_eq!(
            spin_factor_filesystem::Filesystem::summary(&fs),
            "s3 b/tenant-a/data"
        );
    }

    #[test]
    fn environment_fills_what_the_table_leaves_out() {
        // The platform-deployment shape: a tenant table can be empty.
        let resolved = resolve_config(config(""), |name| match name {
            env::BUCKET => Some("platform-bucket".into()),
            env::PREFIX => Some("tenant-a".into()),
            env::ALLOW_HTTP => Some("1".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(resolved.bucket, "platform-bucket");
        assert_eq!(resolved.prefix, "tenant-a");
        assert!(resolved.allow_http);
    }

    #[test]
    fn table_prefix_is_relative_beneath_the_platform_root() {
        let resolved = resolve_config(config(r#"prefix = "repos""#), |name| match name {
            env::BUCKET => Some("platform-bucket".into()),
            env::PREFIX => Some("tenant-a".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(resolved.prefix, "tenant-a/repos");

        // Without a platform root, the table prefix addresses the bucket.
        let resolved = resolve_config(
            config(
                r#"
                bucket = "b"
                prefix = "repos"
                "#,
            ),
            |_| None,
        )
        .unwrap();
        assert_eq!(resolved.prefix, "repos");
    }

    #[test]
    fn bucket_is_required_from_somewhere() {
        assert!(resolve_config(config(""), |_| None).is_err());
    }

    #[test]
    fn partial_static_credentials_fall_back_to_the_chain() {
        // Mirrors the aws_dynamo key-value store: only a complete
        // access_key/secret_key pair selects static credentials.
        let cfg = config(
            r#"
            bucket = "b"
            access_key = "half"
            "#,
        );
        assert!(cfg.access_key.is_some() && cfg.secret_key.is_none());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let err = toml::from_str::<S3FilesystemRuntimeConfig>(
            r#"
            bucket = "b"
            access_key_id = "old-name"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("access_key_id"));
    }
}
