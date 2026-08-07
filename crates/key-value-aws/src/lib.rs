mod store;

use serde::Deserialize;
use spin_factor_key_value::runtime_config::spin::MakeKeyValueStore;
use store::{
    KeyLayout, KeyValueAwsDynamo, KeyValueAwsDynamoAuthOptions,
    KeyValueAwsDynamoRuntimeConfigOptions,
};

/// A key-value store that uses AWS Dynamo as the backend.
#[derive(Default)]
pub struct AwsDynamoKeyValueStore {
    _priv: (),
}

impl AwsDynamoKeyValueStore {
    /// Creates a new `AwsKeyValueStore`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Runtime configuration for the AWS Dynamo key-value store.
#[derive(Deserialize)]
pub struct AwsDynamoKeyValueRuntimeConfig {
    /// The access key for the AWS Dynamo DB account role.
    access_key: Option<String>,
    /// The secret key for authorization on the AWS Dynamo DB account.
    secret_key: Option<String>,
    /// The token for authorization on the AWS Dynamo DB account.
    token: Option<String>,
    /// The AWS region where the database is located
    region: String,
    /// Boolean determining whether to use strongly consistent reads.
    /// Defaults to `false` but can be set to `true` to improve atomicity
    consistent_read: Option<bool>,
    /// The AWS Dynamo DB table.
    table: String,
}

impl MakeKeyValueStore for AwsDynamoKeyValueStore {
    const RUNTIME_CONFIG_TYPE: &'static str = "aws_dynamo";

    type RuntimeConfig = AwsDynamoKeyValueRuntimeConfig;

    type StoreManager = KeyValueAwsDynamo;

    fn make_store(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<Self::StoreManager> {
        let AwsDynamoKeyValueRuntimeConfig {
            access_key,
            secret_key,
            token,
            region,
            consistent_read,
            table,
        } = runtime_config;
        let auth_options = match (access_key, secret_key) {
            (Some(access_key), Some(secret_key)) => {
                KeyValueAwsDynamoAuthOptions::RuntimeConfigValues(
                    KeyValueAwsDynamoRuntimeConfigOptions::new(access_key, secret_key, token),
                )
            }
            _ => KeyValueAwsDynamoAuthOptions::Environmental,
        };
        KeyValueAwsDynamo::new(
            region,
            consistent_read.unwrap_or(false),
            table,
            auth_options,
            KeyLayout::Simple,
        )
    }
}

/// A key-value store on a shared AWS DynamoDB table, one namespace per
/// store.
///
/// Where the `aws_dynamo` type owns a whole table (partition key `PK` =
/// the guest key), `aws_dynamo_shared` shares a table whose key schema is
/// composite: partition key `PK` = the configured `namespace`, sort key
/// `SK` = the guest key. That layout is what makes tenant isolation
/// enforceable at the service:
///
/// - `get_keys` is a partition Query, never a table Scan, so listing reads
///   only the namespace's own items - and stays inside an IAM
///   `dynamodb:LeadingKeys` condition, which a Scan cannot.
/// - Credentials scoped with `"Condition": {"ForAllValues:StringEquals":
///   {"dynamodb:LeadingKeys": ["<namespace>"]}}` confine every operation
///   this store issues to the one partition.
///
/// In deployments where tenants author their own runtime config, treat
/// `namespace` as untrusted input: the scoped credential - minted per
/// tenant, e.g. one shared role plus an STS inline session policy - is the
/// isolation boundary, not the config value. A tenant naming another
/// tenant's namespace gets `AccessDenied` from DynamoDB itself.
///
/// The two types must point at tables with matching key schemas; DynamoDB
/// rejects a mismatch on the first operation.
#[derive(Default)]
pub struct AwsDynamoKeyValueStoreShared {
    _priv: (),
}

impl AwsDynamoKeyValueStoreShared {
    /// Creates a new `AwsDynamoKeyValueStoreShared`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Runtime configuration for the AWS Dynamo key-value store, shared-table layout.
#[derive(Deserialize)]
pub struct AwsDynamoKeyValueRuntimeConfigShared {
    /// The access key for the AWS Dynamo DB account role.
    access_key: Option<String>,
    /// The secret key for authorization on the AWS Dynamo DB account.
    secret_key: Option<String>,
    /// The token for authorization on the AWS Dynamo DB account.
    token: Option<String>,
    /// The AWS region where the database is located
    region: String,
    /// Boolean determining whether to use strongly consistent reads.
    /// Defaults to `false` but can be set to `true` to improve atomicity
    consistent_read: Option<bool>,
    /// The AWS Dynamo DB table.
    table: String,
    /// The partition this store lives under: the table's partition key
    /// value for every item the store reads or writes.
    namespace: String,
}

impl MakeKeyValueStore for AwsDynamoKeyValueStoreShared {
    const RUNTIME_CONFIG_TYPE: &'static str = "aws_dynamo_shared";

    type RuntimeConfig = AwsDynamoKeyValueRuntimeConfigShared;

    type StoreManager = KeyValueAwsDynamo;

    fn make_store(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<Self::StoreManager> {
        let AwsDynamoKeyValueRuntimeConfigShared {
            access_key,
            secret_key,
            token,
            region,
            consistent_read,
            table,
            namespace,
        } = runtime_config;
        if namespace.is_empty() {
            anyhow::bail!("aws_dynamo_shared requires a non-empty namespace");
        }
        let auth_options = match (access_key, secret_key) {
            (Some(access_key), Some(secret_key)) => {
                KeyValueAwsDynamoAuthOptions::RuntimeConfigValues(
                    KeyValueAwsDynamoRuntimeConfigOptions::new(access_key, secret_key, token),
                )
            }
            _ => KeyValueAwsDynamoAuthOptions::Environmental,
        };
        KeyValueAwsDynamo::new(
            region,
            consistent_read.unwrap_or(false),
            table,
            auth_options,
            KeyLayout::Namespaced(std::sync::Arc::new(namespace)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_requires_namespace() {
        let missing: Result<AwsDynamoKeyValueRuntimeConfigShared, _> = toml::from_str(
            r#"
            region = "us-east-1"
            table = "kv"
            "#,
        );
        assert!(missing.is_err(), "namespace must be required");

        let empty: AwsDynamoKeyValueRuntimeConfigShared = toml::from_str(
            r#"
            region = "us-east-1"
            table = "kv"
            namespace = ""
            "#,
        )
        .unwrap();
        assert!(
            AwsDynamoKeyValueStoreShared::new()
                .make_store(empty)
                .is_err(),
            "empty namespace must be refused"
        );
    }

    #[test]
    fn v1_config_is_unchanged() {
        let config: AwsDynamoKeyValueRuntimeConfig = toml::from_str(
            r#"
            region = "us-east-1"
            table = "kv"
            "#,
        )
        .unwrap();
        assert!(AwsDynamoKeyValueStore::new().make_store(config).is_ok());
    }
}
