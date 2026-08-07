# spin-filesystem-object-store

An object-store-backed filesystem for Spin's filesystem factor, exposed in
runtime config as `type = "s3"` — Amazon S3 or any S3-compatible endpoint
(MinIO, Cloudflare R2, localstack, …).

In a platform deployment the environment carries identity and the tenant's
table carries only intent — the same model as `spin-key-value-s3`:

```toml
# The whole tenant-side config; SPIN_FS_S3_BUCKET, SPIN_FS_S3_PREFIX, and
# scoped credentials are injected by the platform. `prefix` here is
# *relative*, appended beneath the platform root.
[filesystem.repos]
type = "s3"
prefix = "repos"
writable = true
```

Self-hosted, the table can carry everything explicitly:

```toml
# runtime-config.toml
[filesystem.repos]
type = "s3"
bucket = "acme-spin-apps"
region = "us-east-1"
prefix = "tenant-a/repos"
writable = true
# Optional; when absent, the standard AWS configuration chain is used.
access_key = "AKIA…"
secret_key = "…"
token = "…"    # for temporary (STS) credentials
# For S3-compatible stores:
# endpoint = "http://localhost:9000"
# allow_http = true
```

Environment fallbacks: `SPIN_FS_S3_BUCKET`, `SPIN_FS_S3_PREFIX` (the
platform-assigned root), `SPIN_FS_S3_ENDPOINT`, `SPIN_FS_S3_ALLOW_HTTP`.
Explicit table values override the environment, except `prefix`, which
composes: the environment root is identity, the table prefix is a
relative path beneath it. Object paths have no `..`, so the composition
cannot rise above the root — and the credential holds regardless.

(S3 Express One Zone is deliberately not supported: directory buckets
authorize at bucket granularity, so prefix-scoped credentials — the pool
model's isolation — cannot apply. Revisit if AWS adds prefix-scoped
authorization there.)

## Credentials

Credential handling follows Spin's `aws_dynamo` key-value store. A complete
`access_key`/`secret_key` pair in the table (plus `token` for temporary
credentials) is used as-is; otherwise credentials resolve through the
standard AWS configuration chain — environment variables, shared config and
SSO profiles, IMDS, ECS/EKS container credential endpoints, web identity —
**with the SDK's own caching and refresh**. `region` and `endpoint` fall
back to what the chain resolves (`AWS_REGION`, `AWS_ENDPOINT_URL`).

Credential lifetime binds to the Spin *process*, not to component
instances: the client is built once per label when runtime config resolves.
That gives two clean deployment shapes and no refresh machinery in Spin:

- **Short-lived processes** (scale-to-zero, process-per-deploy): inject
  static session credentials at spawn with TTL comfortably above the
  process's maximum lifetime. `AssumeRole` sessions run 1 h by default, up
  to the role's 12 h maximum — and are capped at 1 h when the parent is
  itself an assumed role.
- **Long-lived processes**: omit table credentials and let the chain use a
  refreshing source — a container credentials endpoint
  (`AWS_CONTAINER_CREDENTIALS_FULL_URI`, the protocol a local
  session-vending broker can speak), web identity, or IMDS. The chain
  re-fetches before expiry on its own.

## Semantics

The backend meets the factor's `core` conformance tier: files, directories,
rename (clobber and cycle refusal included), and sandbox confinement. A file
is one object, atomically replaced on every write; a directory is a
`.spin-dir` marker plus everything under its key prefix. Object stores have
no hard links, symlinks, or settable timestamps, so those report
`unsupported`; a file unlinked while open stops being readable (object
stores keep no inode alive for open handles); directory renames move each
object under the prefix, so they are not atomic and cost time proportional
to the subtree.

Best fit: content whose unit of change is a whole file — repositories,
assets, documents. A workload that rewrites byte ranges of large files wants
the `sqlite` or `host` type instead.

## Multi-tenant isolation

One mount's boundary is `(credentials, bucket, prefix)`. Two mechanisms
enforce it, independently:

1. **The runtime.** Every key is built beneath the configured prefix, and
   path resolution refuses `..` past the mount root before any request is
   issued. One label cannot name another label's keys.
2. **The store.** Scope each tenant's credentials to its prefix, so the
   service refuses cross-tenant requests no matter what issues them:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::acme-spin-apps/tenant-a/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::acme-spin-apps",
      "Condition": { "StringLike": { "s3:prefix": "tenant-a/*" } }
    }
  ]
}
```

### One role, per-tenant sessions (the fleet pattern)

At fleet scale, per-tenant IAM entities are unnecessary. Keep **one** IAM
role with bucket-wide access, and have the orchestrator mint each tenant's
credentials with `AssumeRole` plus an **inline session policy** restricting
to that tenant's prefix. STS intersects the two, so the vended session can
touch only the tenant's keys; the role is the only IAM object you ever
manage, for any number of tenants.

```sh
aws sts assume-role \
  --role-arn arn:aws:iam::123456789012:role/spin-tenant-fs \
  --role-session-name tenant-a \
  --duration-seconds 3600 \
  --policy '{
    "Version": "2012-10-17",
    "Statement": [
      { "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        "Resource": "arn:aws:s3:::acme-spin-apps/tenant-a/*" },
      { "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::acme-spin-apps",
        "Condition": { "StringLike": { "s3:prefix": "tenant-a/*" } } }
    ]
  }'
```

The orchestrator injects the returned key/secret/token into the tenant's
Spin process (`AWS_*` environment variables, or the `access_key` /
`secret_key` / `token` table fields) and never hands the parent role to a
tenant-exposed process — that placement is what turns a runtime compromise
from a bucket-wide event into a one-prefix event.

Process lifetime decides the rest, as described under *Credentials* above:
short-lived processes just use the vended session as-is; long-lived ones
point the credential chain at a refreshing source (the orchestrator can
serve the container-credentials protocol) instead of static values.

MinIO supports the same `AssumeRole`-with-policy flow, so the pattern works
self-hosted; S3 Access Grants is the AWS-managed variant of the same
broker. A deployment that shares one set of broad credentials across
tenants leans on mechanism 1 alone; both walls together mean a defect in
either still leaves the boundary standing.

Tenants share nothing else: no cache, no connection-level state that could
carry data across mounts.
