//! Conformance suite instantiations for the built-in backends.
//!
//! The cases themselves live in [`spin_factor_filesystem::conformance`] (built
//! with the `conformance` feature) so that out-of-tree backends can run them
//! too; this file instantiates them for `memory` and `host`, plus a few
//! backend-specific cases that assert behaviour outside the shared contract.

use spin_factor_filesystem::conformance::cases::p;
use spin_factor_filesystem::{
    ErrorCode, Filesystem, conformance_portable_cases, conformance_posix_cases,
};

mod memory {
    use spin_factor_filesystem::Filesystem as _;
    use spin_factor_filesystem::backend::MemoryFilesystem;

    use super::*;

    fn fixture() -> (MemoryFilesystem, ()) {
        (MemoryFilesystem::new(), ())
    }

    conformance_portable_cases!(fixture);
    conformance_posix_cases!(fixture);

    /// The resolver gives up after 32 symlink traversals, like Linux gives up
    /// after 40: enough for real trees, finite for hostile ones.
    #[tokio::test]
    async fn symlink_budget_is_32_hops() {
        let fs = MemoryFilesystem::new();
        super::create_empty(&fs, "hop0").await;
        for i in 1..=33 {
            fs.symlink(&format!("hop{}", i - 1), p(&format!("hop{i}")))
                .await
                .unwrap();
        }
        // 32 traversals resolve...
        assert!(fs.stat_at(p("hop32"), true).await.is_ok());
        // ...and the 33rd is a loop.
        assert_eq!(
            fs.stat_at(p("hop33"), true).await.unwrap_err(),
            ErrorCode::Loop
        );
    }

    /// `hard_link` with `follow: false` links the symlink object itself.
    /// (The host backend reports this as `Unsupported`; see its docs.)
    #[tokio::test]
    async fn hard_link_nofollow_links_the_symlink_itself() {
        let fs = MemoryFilesystem::new();
        super::create_empty(&fs, "t").await;
        fs.symlink("t", p("l")).await.unwrap();
        fs.hard_link(p("l"), false, p("l2")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("l2"), false).await.unwrap().type_,
            spin_factor_filesystem::DescriptorType::SymbolicLink
        );
        assert_eq!(fs.readlink(p("l2")).await.unwrap(), "t");
    }

    #[tokio::test]
    async fn with_files_seeds_nested_content() {
        let fs = MemoryFilesystem::with_files([("a/b/c.txt", "hi"), ("top.txt", "there")]).unwrap();
        assert!(fs.stat_at(p("a/b"), true).await.unwrap().type_.is_dir());
        assert_eq!(fs.stat_at(p("a/b/c.txt"), true).await.unwrap().size, 2);
        assert_eq!(fs.stat_at(p("top.txt"), true).await.unwrap().size, 5);
    }
}

mod host {
    use spin_factor_filesystem::backend::HostFilesystem;
    use spin_factor_filesystem::conformance_portable_cases;

    fn fixture() -> (HostFilesystem, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fs = HostFilesystem::open(dir.path()).expect("open temp dir");
        (fs, dir)
    }

    conformance_portable_cases!(fixture);

    #[cfg(unix)]
    mod unix {
        use spin_factor_filesystem::conformance_posix_cases;

        use super::fixture;

        conformance_posix_cases!(fixture);
    }
}

/// Creates an empty file outside the `cases` helpers, for backend-specific
/// tests.
async fn create_empty(fs: &dyn Filesystem, path: &str) {
    use spin_factor_filesystem::{DescriptorFlags, FsPath, OpenFlags, OpenOptions};
    fs.open(
        FsPath::new(path).unwrap(),
        OpenOptions {
            open_flags: OpenFlags::CREATE,
            flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
            follow_symlinks: true,
        },
    )
    .await
    .unwrap();
}
