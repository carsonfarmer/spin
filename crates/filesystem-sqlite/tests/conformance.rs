//! The factor's full conformance suite - portable and posix tiers - over a
//! SQLite database file, plus behaviours specific to this backend:
//! persistence across reopens and the configured size cap.

use spin_factor_filesystem::conformance::cases::p;
use spin_factor_filesystem::{
    DescriptorFlags, ErrorCode, Filesystem, FsPath, OpenFlags, OpenOptions, Opened,
    conformance_portable_cases, conformance_posix_cases,
};
use spin_filesystem_sqlite::SqliteFilesystem;

fn fixture() -> (SqliteFilesystem, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let fs = SqliteFilesystem::open_file(&dir.path().join("fs.db")).expect("open database");
    (fs, dir)
}

conformance_portable_cases!(fixture);
conformance_posix_cases!(fixture);

async fn create(
    fs: &dyn Filesystem,
    path: &str,
) -> std::sync::Arc<dyn spin_factor_filesystem::File> {
    match fs
        .open(
            FsPath::new(path).unwrap(),
            OpenOptions {
                open_flags: OpenFlags::CREATE,
                flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
                follow_symlinks: true,
            },
        )
        .await
        .unwrap()
    {
        Opened::File(file) => file,
        Opened::Dir => panic!("expected a file"),
    }
}

/// The point of this backend: contents survive closing and reopening the
/// database.
#[tokio::test]
async fn contents_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("fs.db");
    {
        let fs = SqliteFilesystem::open_file(&db).unwrap();
        let file = create(&fs, "me.txt").await;
        file.write_at(b"durable", 0).await.unwrap();
        fs.create_dir(p("d")).await.unwrap();
        fs.symlink("me.txt", p("d/link")).await.unwrap();
    }
    let fs = SqliteFilesystem::open_file(&db).unwrap();
    assert_eq!(fs.stat_at(p("me.txt"), true).await.unwrap().size, 7);
    assert!(fs.stat_at(p("d"), true).await.unwrap().type_.is_dir());
    assert_eq!(fs.readlink(p("d/link")).await.unwrap(), "me.txt");
}

/// A row whose names were all removed while a handle was open does not
/// outlive the last handle across a reopen.
#[tokio::test]
async fn unlinked_rows_are_reclaimed_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("fs.db");
    {
        let fs = SqliteFilesystem::open_file(&db).unwrap();
        let file = create(&fs, "gone.txt").await;
        file.write_at(b"bytes", 0).await.unwrap();
        fs.unlink(p("gone.txt")).await.unwrap();
        // Still readable through the handle, POSIX-style.
        let mut buf = [0u8; 5];
        assert_eq!(file.read_at(&mut buf, 0).await.unwrap(), 5);
    }
    let fs = SqliteFilesystem::open_file(&db).unwrap();
    assert_eq!(
        fs.stat_at(p("gone.txt"), true).await.unwrap_err(),
        ErrorCode::NoEntry
    );
}

/// Growth beyond `maximum_size` reports `insufficient-space`, and the
/// filesystem keeps working afterwards.
#[tokio::test]
async fn maximum_size_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let fs = SqliteFilesystem::open_file(&dir.path().join("fs.db")).unwrap();
    fs.set_maximum_size(64 * 1024).unwrap();

    let file = create(&fs, "big").await;
    let chunk = vec![0xAB; 64 * 1024];
    let mut wrote = 0u64;
    let err = loop {
        match file.write_at(&chunk, wrote).await {
            Ok(n) => wrote += n as u64,
            Err(err) => break err,
        }
        assert!(wrote < 64 * 1024 * 1024, "cap never engaged");
    };
    assert_eq!(err, ErrorCode::InsufficientSpace);

    // Shrinking frees space for further writes.
    file.set_size(0).await.unwrap();
    file.write_at(b"still works", 0).await.unwrap();
}
