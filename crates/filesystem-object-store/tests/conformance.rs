//! The factor's `core` conformance tier over an in-memory object store - the
//! same code path an S3 bucket takes, minus the network - plus the behaviours
//! that make this backend fit multi-tenant deployments: prefix containment
//! and marker invisibility.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use spin_factor_filesystem::conformance::cases::p;
use spin_factor_filesystem::{ErrorCode, Filesystem, conformance_core_cases};
use spin_filesystem_object_store::ObjectStoreFilesystem;

fn fs_at(store: Arc<dyn ObjectStore>, prefix: &str) -> ObjectStoreFilesystem {
    ObjectStoreFilesystem::new(store, prefix, format!("test {prefix}")).unwrap()
}

fn fixture() -> (ObjectStoreFilesystem, ()) {
    (fs_at(Arc::new(InMemory::new()), "tenant-a/data"), ())
}

conformance_core_cases!(fixture);

async fn write(fs: &dyn Filesystem, path: &str, contents: &[u8]) {
    use spin_factor_filesystem::{DescriptorFlags, OpenFlags, OpenOptions, Opened};
    let Opened::File(file) = fs
        .open(
            p(path),
            OpenOptions {
                open_flags: OpenFlags::CREATE,
                flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
                follow_symlinks: true,
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected a file");
    };
    if !contents.is_empty() {
        file.write_at(contents, 0).await.unwrap();
    }
}

/// Two mounts with different prefixes on one bucket cannot see each other,
/// and `..` cannot cross between them.
#[tokio::test]
async fn prefixes_isolate_mounts() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = fs_at(store.clone(), "tenant-a/data");
    let b = fs_at(store.clone(), "tenant-b/data");

    write(&a, "secret.txt", b"tenant a's bytes").await;

    // B's listing is empty and B cannot resolve A's content...
    assert_eq!(b.read_dir(p("")).await.unwrap().len(), 0);
    assert_eq!(
        b.stat_at(p("secret.txt"), true).await.unwrap_err(),
        ErrorCode::NoEntry
    );
    // ...including by spelling out an escape: `..` stops at the mount root.
    for path in ["..", "../..", "../../tenant-a/data/secret.txt"] {
        assert_eq!(
            b.stat_at(p(path), true).await.unwrap_err(),
            ErrorCode::NotPermitted,
            "path {path}"
        );
    }

    // A sibling prefix that *contains* the other's name as a prefix string
    // ("tenant-a/data" vs "tenant-a/data2") must not blur together.
    let a2 = fs_at(store.clone(), "tenant-a/data2");
    assert_eq!(
        a2.stat_at(p("secret.txt"), true).await.unwrap_err(),
        ErrorCode::NoEntry
    );
}

/// The directory marker never shows in listings and cannot be addressed.
#[tokio::test]
async fn dir_marker_is_invisible() {
    let (fs, ()) = fixture();
    fs.create_dir(p("d")).await.unwrap();
    write(&fs, "d/real.txt", b"x").await;

    let names: Vec<String> = fs
        .read_dir(p("d"))
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["real.txt"]);

    assert_eq!(
        fs.stat_at(p("d/.spin-dir"), true).await.unwrap_err(),
        ErrorCode::NoEntry
    );
    assert_eq!(
        fs.unlink(p("d/.spin-dir")).await.unwrap_err(),
        ErrorCode::NoEntry
    );
}

/// Links and settable timestamps are absent by design, reported as
/// `unsupported` - not mis-reported as something else.
#[tokio::test]
async fn unsupported_operations_say_so() {
    let (fs, ()) = fixture();
    write(&fs, "f", b"x").await;
    assert_eq!(
        fs.symlink("f", p("l")).await.unwrap_err(),
        ErrorCode::Unsupported
    );
    assert_eq!(
        fs.hard_link(p("f"), false, p("g")).await.unwrap_err(),
        ErrorCode::Unsupported
    );
}

/// Directory renames carry the whole subtree, including empty directories.
#[tokio::test]
async fn dir_rename_moves_subtree() {
    let (fs, ()) = fixture();
    fs.create_dir(p("src")).await.unwrap();
    fs.create_dir(p("src/sub")).await.unwrap();
    write(&fs, "src/a.txt", b"a").await;
    write(&fs, "src/sub/b.txt", b"b").await;

    fs.rename(p("src"), p("dst")).await.unwrap();

    assert_eq!(
        fs.stat_at(p("src"), true).await.unwrap_err(),
        ErrorCode::NoEntry
    );
    assert_eq!(fs.stat_at(p("dst/a.txt"), true).await.unwrap().size, 1);
    assert_eq!(fs.stat_at(p("dst/sub/b.txt"), true).await.unwrap().size, 1);
    assert!(fs.stat_at(p("dst/sub"), true).await.unwrap().type_.is_dir());
}
