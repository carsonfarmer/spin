title = "SIP 024 - Pluggable Filesystems (the filesystem factor)"
template = "main"
date = "2026-08-06T12:00:00Z"

---

Summary: Move `wasi:filesystem` out of the WASI factor and into a dedicated factor whose
backends are pluggable. Guests keep importing plain `wasi:filesystem`; the bytes behind it
can come from a host directory, an in-memory tree, or any other Rust implementation of a
small `Filesystem`/`File` service-provider interface.

Owner(s): [carson@textile.io](mailto:carson@textile.io)

Created: August 6, 2026

# Background

Spin's `files` mount is the only way a component gets a filesystem today, and it means
exactly one thing: a real directory on the host, preopened through `wasmtime-wasi` and
sandboxed by `cap-std`. `spin-factor-wasi` builds a `WasiCtx`, calls
`WasiCtxBuilder::preopened_dir` for each `ContentPath`, and links every version of
`wasi:filesystem` that Spin supports straight at `wasmtime_wasi::filesystem::WasiFilesystemCtxView`.

That is a good default and it should stay the default. But `wasmtime-wasi`'s descriptor
type is a closed enum:

```rust
// wasmtime-wasi 47, src/filesystem.rs
pub enum Descriptor {
    File(File),  // File { file: Arc<cap_std::fs::File>, .. }
    Dir(Dir),    // Dir  { dir:  Arc<cap_std::fs::Dir>,  .. }
}
```

and its operations (`open_at`, `stat_at`, `read_directory`, …) are `pub(crate)`. There is no
seam. If the bytes you want to serve do not live in a directory on local disk, `wasi:filesystem`
is closed to you.

## Why this matters

A large amount of extremely good, extremely well-tested software assumes a POSIX-ish
filesystem and nothing else. `gitoxide` and `libgit2` are the motivating example: both
implement the whole of Git — packfiles, deltas, refs, index, gc — against `open`/`read`/
`write`/`readdir`/`rename`. Give either one a `wasi:filesystem` that happens to be backed by
object storage and you have a Git server without writing a byte of Git.

The same shape recurs constantly: SQLite, DuckDB, Lucene-style indexes, LLM model caches,
build tools, and essentially every language runtime's stdlib. The filesystem is the single
highest-leverage capability to make pluggable, because "make it work over my storage" is
otherwise a per-library porting project, and with this it is one trait implementation shared
by all of them.

`wasi:filesystem` also just became a much better target: version `0.3.0` is async, so a
backend that is genuinely I/O-bound (a network object store) no longer has to lie about it
by blocking a thread.

# Proposal

Introduce `spin-factor-filesystem`, a factor that owns `wasi:filesystem` and whose storage
is supplied by a trait rather than hardcoded to `cap-std`.

Three pieces:

1. **A service-provider interface** — `Filesystem` and `File` traits, plus the value types
   (`Stat`, `DirEntry`, `OpenOptions`, `ErrorCode`, …) that go with them. This is the durable
   artifact: it is what a third party implements, and it deliberately does not mention
   `wasmtime`, `bindgen`, or any WIT version.

2. **Backends** — `host` (a real directory, via `cap-std`, semantically identical to what
   Spin does today) and `memory` (a full in-memory tree with symlinks and hard links).
   Object storage, overlays, and content-addressed stores are then out-of-tree exercises for
   the reader, or follow-on crates.

3. **The factor** — resolves mounts for each component, holds the descriptor table, and
   implements the `wasi:filesystem` host bindings over the SPI.

Guests are unaffected. A component compiled for `wasm32-wasip2` importing
`wasi:filesystem/types@0.2.x` does not know or care which backend is behind its preopens.

## The SPI

Directories are addressed by path, files are addressed by handle. This split is not an
accident: it is exactly the shape of `wasi:filesystem` itself, where every directory
operation is an `*-at(path)` call on a base descriptor and only files carry position-free
`read-at`/`write-at` state.

```rust
/// A pluggable filesystem backend.
///
/// # Sandboxing
///
/// An implementation is a *root*. It must never resolve a path to an object outside
/// itself, including through `..` components and symbolic links. The factor rejects
/// absolute paths before they reach the backend, but everything else — in particular
/// symlink resolution — is the backend's responsibility, because only the backend knows
/// what its links mean.
#[async_trait]
pub trait Filesystem: Send + Sync + 'static {
    /// A short description of this filesystem, used in errors and diagnostics.
    fn summary(&self) -> String;

    async fn open_file(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Arc<dyn File>>;
    async fn open_dir(&self, path: &FsPath, opts: OpenOptions) -> FsResult<()>;

    async fn stat_at(&self, path: &FsPath, follow: bool) -> FsResult<Stat>;
    async fn set_times_at(&self, path: &FsPath, follow: bool, times: SetTimes) -> FsResult<()>;
    async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>>;
    async fn create_dir(&self, path: &FsPath) -> FsResult<()>;
    async fn remove_dir(&self, path: &FsPath) -> FsResult<()>;
    async fn unlink(&self, path: &FsPath) -> FsResult<()>;
    async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()>;
    async fn symlink(&self, target: &str, link: &FsPath) -> FsResult<()>;
    async fn readlink(&self, path: &FsPath) -> FsResult<String>;
    async fn hard_link(&self, from: &FsPath, follow: bool, to: &FsPath) -> FsResult<()>;
    async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash>;
}

/// An open regular file.
#[async_trait]
pub trait File: Send + Sync + 'static {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> FsResult<usize>;
    async fn write_at(&self, buf: &[u8], offset: u64) -> FsResult<usize>;
    async fn append(&self, buf: &[u8]) -> FsResult<usize>;
    async fn stat(&self) -> FsResult<Stat>;
    async fn set_size(&self, size: u64) -> FsResult<()>;
    async fn set_times(&self, times: SetTimes) -> FsResult<()>;
    async fn sync(&self) -> FsResult<()>;
    async fn sync_data(&self) -> FsResult<()>;
    async fn metadata_hash(&self) -> FsResult<MetadataHash>;
    async fn advise(&self, offset: u64, len: u64, advice: Advice) -> FsResult<()> { Ok(()) }
}
```

Notes on the design:

- **`FsPath` is a validated relative path.** UTF-8, `/`-separated, never absolute. `..` and
  `.` components survive normalization and reach the backend, because `a/link/../b` cannot be
  resolved lexically without knowing where `link` points. The factor's only lexical rule is
  the one `wasi:filesystem` specifies: a leading `/` is `not-permitted`.

- **Errors are `wasi:filesystem`'s `error-code`**, not `std::io::Error`. Backends that have
  no meaningful `EDQUOT` simply never return it, and there is exactly one place — the
  binding layer — that has to know how to talk to a guest.

- **Cross-filesystem `rename`/`link` return `cross-device`.** POSIX says the same. It keeps
  the trait free of a "which other filesystem is this" parameter that only the host backend
  could ever honour.

- **Every method is `async`.** `wasi:filesystem@0.3.0` is async end to end; the host backend
  bridges to blocking syscalls with `spawn_blocking` exactly as `wasmtime-wasi` does.

## Configuration

Mounts are declared like every other Spin capability: a label in the manifest, a definition
in runtime config.

```toml
# spin.toml
[component.git-server]
source = "git-server.wasm"
filesystems = [{ label = "repos", path = "/srv/git" }]
```

```toml
# runtime-config.toml
[filesystem.repos]
type = "memory"

# or
[filesystem.repos]
type = "host"
path = "/var/lib/spin/repos"
writable = true
```

`type` dispatches to a registered backend factory, mirroring `[key_value_store.<label>]` and
`[sqlite_database.<label>]` exactly — including the `RuntimeConfigResolver` that lets
embedders (SpinKube, a custom host) register their own types without patching Spin.

Existing `files = [...]` mounts keep working unchanged: they are lowered to `host` backend
mounts at the same guest paths with the same read-only-unless-`--allow-transient-write`
semantics.

## Where the seam goes

`wasi:filesystem` can only have one implementation in a `Linker`, so this is a takeover, not
an addition. Spin registers four `wasi:filesystem` families today:

| Interface | Source |
|---|---|
| `wasi:filesystem@0.2.12` | `wasmtime-wasi` p2 |
| `wasi:filesystem@0.3.0` | `wasmtime-wasi` p3 |
| `wasi:filesystem@0.3.0-rc-2026-03-15` | `spin-factor-wasi::wasi_2026_03_15`, delegating to p3 |
| `@0.2.0-rc-2023-10-18`, `@0.2.0-rc-2023-11-10` | `spin-factor-wasi::wasi_2023_*`, delegating to p2 |

The first two move to the new factor. The last three are already thin shims that delegate to
"whatever the current implementation is"; the new factor carries its own copies of those
shims, retargeted at its descriptor, so snapshot-era guests keep working.

The takeover itself is done by *shadowing* rather than by modifying `WasiFactor`: the
filesystem factor's `init` enables `Linker::allow_shadowing` just long enough to redefine
the `wasi:filesystem` interfaces over the WASI factor's definitions, then switches it back
off. This keeps the change fully contained in the new factor - `spin-factor-wasi` is not
modified at all, and keeps its all-in-one behaviour for embedders that do not register the
filesystem factor. The one obligation this design places on embedders is ordering: the
filesystem factor must be registered *after* the WASI factor. Registering it first fails at
startup with a duplicate-definition error (shadowing is off while the WASI factor links),
which is loud rather than subtle.

To keep the blast radius honest, the `host` backend is an adaptation of `wasmtime-wasi`'s
own `cap-std` logic rather than a fresh implementation. Both projects are Apache-2.0 WITH
LLVM-exception, the provenance is recorded in the source, and it means the default path for
every existing Spin app is running the same sandboxing and the same
`spawn_blocking`/`as_blocking_file` strategy it runs today.

# Alternatives considered

**Upstream a `Descriptor` trait into `wasmtime-wasi`.** The right long-term answer, and this
SIP is partly an argument for it — the SPI here is deliberately shaped so it could become
that. But it is not something Spin can wait on, and having a working implementation is the
best possible input to that conversation.

**Materialize virtual content into a temp directory.** Works for read-mostly, small,
known-in-advance content, which is to say: not a Git server. Writes have nowhere to go and
the working set has to fit on local disk.

**A Spin-specific filesystem interface next to `wasi:filesystem`.** Defeats the entire
purpose. The value here is that unmodified `gitoxide` runs on it.

# Future work

- An object-storage backend (S3/R2/GCS) with the write-combining and prefix-listing that
  makes packfile access tolerable over a network.
- An overlay backend (read-only lower + writable upper) — with `host` and `memory` in place
  this is a small amount of code and makes copy-on-write app content trivial.
- Per-mount quotas and operation limits, reusing `spin-connection-semaphore` the way the
  key-value factor does.
- Feeding the SPI back to `wasmtime-wasi` as an upstream `Descriptor` abstraction.
