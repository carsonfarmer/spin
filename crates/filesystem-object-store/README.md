# spin-filesystem-object-store

An object-store-backed filesystem for Spin's filesystem factor, exposed in
runtime config as `type = "s3"` — Amazon S3 or any S3-compatible endpoint
(MinIO, Cloudflare R2, localstack, …).

```toml
# runtime-config.toml
[filesystem.repos]
type = "s3"
bucket = "acme-spin-apps"
region = "us-east-1"
prefix = "tenant-a/repos"
writable = true
# Optional; standard AWS_* environment variables are used when omitted.
access_key_id = "AKIA…"
secret_access_key = "…"
# For S3-compatible stores:
# endpoint = "http://localhost:9000"
# allow_http = true
```

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

Issue such per-tenant credentials (IAM users, STS-assumed roles, or R2 API
tokens scoped by prefix) and put them in each tenant's
`[filesystem.<label>]` table. A deployment that shares one set of broad
credentials across tenants leans on mechanism 1 alone; both walls together
mean a defect in either still leaves the boundary standing.

Tenants share nothing else: no cache, no connection-level state that could
carry data across mounts.
