//! Behavioural conformance tests for [`Filesystem`] implementations.
//!
//! Every case is a plain async function over `&dyn Filesystem`, and the
//! `suite!` macro instantiates each one as a test per backend. A backend that
//! cannot honour part of the contract fails here, with a name that says what
//! it got wrong, rather than under a guest with a `git` stack trace.
//!
//! Cases assert POSIX-shaped semantics as `wasi:filesystem` specifies them:
//! notably, symlink resolution is physical (`a/link/..` lands in the link
//! target's parent), unlinked-but-open files keep their bytes, and nothing
//! ever resolves above the filesystem root.
//!
//! What is deliberately *not* asserted here: descriptor-level access
//! enforcement (opening read-only and then writing). `wasi:filesystem` puts
//! those checks on the descriptor, so they belong to the factor's descriptor
//! layer and are tested there; a [`File`] handed out by a backend is allowed
//! to be as permissive as the memory backend's.

use spin_factor_filesystem::Filesystem;

mod cases {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use spin_factor_filesystem::{
        DescriptorFlags as DF, DescriptorType, ErrorCode, File, Filesystem, FsPath, NewTimestamp,
        OpenFlags as OF, OpenOptions, Opened, SetTimes,
    };

    pub(crate) fn p(path: &str) -> &FsPath {
        FsPath::new(path).expect("valid test path")
    }

    async fn open_with(
        fs: &dyn Filesystem,
        path: &str,
        open_flags: OF,
        flags: DF,
        follow_symlinks: bool,
    ) -> Result<Opened, ErrorCode> {
        fs.open(
            p(path),
            OpenOptions {
                open_flags,
                flags,
                follow_symlinks,
            },
        )
        .await
    }

    /// Opens `path` expecting a regular file.
    async fn open_file(
        fs: &dyn Filesystem,
        path: &str,
        open_flags: OF,
        flags: DF,
    ) -> Result<Arc<dyn File>, ErrorCode> {
        match open_with(fs, path, open_flags, flags, true).await? {
            Opened::File(file) => Ok(file),
            Opened::Dir => panic!("{path}: expected a file, opened a directory"),
        }
    }

    /// Opens `path` expecting the error `expected`.
    async fn open_err(fs: &dyn Filesystem, path: &str, open_flags: OF, flags: DF) -> ErrorCode {
        match open_with(fs, path, open_flags, flags, true).await {
            Ok(_) => panic!("{path}: open unexpectedly succeeded"),
            Err(err) => err,
        }
    }

    /// Creates `path` with the given contents, returning the open handle.
    async fn create(fs: &dyn Filesystem, path: &str, contents: &[u8]) -> Arc<dyn File> {
        let file = open_file(fs, path, OF::CREATE, DF::READ | DF::WRITE)
            .await
            .unwrap_or_else(|err| panic!("create {path}: {err}"));
        if !contents.is_empty() {
            let n = file.write_at(contents, 0).await.expect("write");
            assert_eq!(n, contents.len());
        }
        file
    }

    async fn mkdir(fs: &dyn Filesystem, path: &str) {
        fs.create_dir(p(path))
            .await
            .unwrap_or_else(|err| panic!("mkdir {path}: {err}"));
    }

    async fn read_all(file: &dyn File) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = file
                .read_at(&mut buf, out.len() as u64)
                .await
                .expect("read");
            if n == 0 {
                return out;
            }
            out.extend_from_slice(&buf[..n]);
        }
    }

    async fn read_file(fs: &dyn Filesystem, path: &str) -> Vec<u8> {
        let file = open_file(fs, path, OF::empty(), DF::READ)
            .await
            .unwrap_or_else(|err| panic!("open {path}: {err}"));
        read_all(&*file).await
    }

    pub(crate) async fn create_write_read(fs: &dyn Filesystem) {
        let file = create(fs, "f.txt", b"hello world").await;
        let stat = file.stat().await.unwrap();
        assert_eq!(stat.type_, DescriptorType::RegularFile);
        assert_eq!(stat.size, 11);
        drop(file);

        assert_eq!(read_file(fs, "f.txt").await, b"hello world");
        let stat = fs.stat_at(p("f.txt"), true).await.unwrap();
        assert_eq!(stat.type_, DescriptorType::RegularFile);
        assert_eq!(stat.size, 11);

        // Overwriting in place leaves the tail alone.
        let file = open_file(fs, "f.txt", OF::empty(), DF::READ | DF::WRITE)
            .await
            .unwrap();
        file.write_at(b"HELLO", 0).await.unwrap();
        assert_eq!(read_all(&*file).await, b"HELLO world");
    }

    pub(crate) async fn open_missing_fails(fs: &dyn Filesystem) {
        assert_eq!(
            open_err(fs, "nope", OF::empty(), DF::READ).await,
            ErrorCode::NoEntry
        );
        assert_eq!(
            open_err(fs, "nope", OF::DIRECTORY, DF::READ).await,
            ErrorCode::NoEntry
        );
        assert_eq!(
            fs.stat_at(p("nope/deeper"), true).await.unwrap_err(),
            ErrorCode::NoEntry
        );

        // A file used as an intermediate directory is `NotDirectory`, not
        // `NoEntry`.
        create(fs, "plain", b"").await;
        assert_eq!(
            fs.stat_at(p("plain/child"), true).await.unwrap_err(),
            ErrorCode::NotDirectory
        );
    }

    pub(crate) async fn exclusive_create(fs: &dyn Filesystem) {
        create(fs, "f", b"x").await;

        // CREATE|EXCLUSIVE refuses an existing file...
        assert_eq!(
            open_err(fs, "f", OF::CREATE | OF::EXCLUSIVE, DF::READ | DF::WRITE).await,
            ErrorCode::Exist
        );
        // ...but EXCLUSIVE without CREATE is a plain open.
        let file = open_file(fs, "f", OF::EXCLUSIVE, DF::READ).await.unwrap();
        assert_eq!(read_all(&*file).await, b"x");

        // On a fresh name, CREATE|EXCLUSIVE creates an empty file.
        open_file(fs, "g", OF::CREATE | OF::EXCLUSIVE, DF::READ | DF::WRITE)
            .await
            .unwrap();
        assert_eq!(fs.stat_at(p("g"), true).await.unwrap().size, 0);
    }

    pub(crate) async fn truncate_on_open(fs: &dyn Filesystem) {
        create(fs, "f", b"data").await;
        let file = open_file(fs, "f", OF::TRUNCATE, DF::READ | DF::WRITE)
            .await
            .unwrap();
        assert_eq!(file.stat().await.unwrap().size, 0);
        assert_eq!(read_all(&*file).await, b"");
        // The truncation is visible through the namespace, not just the handle.
        assert_eq!(fs.stat_at(p("f"), true).await.unwrap().size, 0);
    }

    pub(crate) async fn set_size_truncates_and_extends(fs: &dyn Filesystem) {
        let file = create(fs, "f", b"hello world").await;
        file.set_size(5).await.unwrap();
        assert_eq!(read_all(&*file).await, b"hello");
        // Extending zero-fills.
        file.set_size(8).await.unwrap();
        assert_eq!(read_all(&*file).await, b"hello\0\0\0");
        assert_eq!(file.stat().await.unwrap().size, 8);
    }

    pub(crate) async fn write_past_end_zero_fills(fs: &dyn Filesystem) {
        let file = create(fs, "f", b"").await;
        file.write_at(b"xy", 4).await.unwrap();
        assert_eq!(read_all(&*file).await, b"\0\0\0\0xy");
    }

    pub(crate) async fn append_lands_at_end(fs: &dyn Filesystem) {
        let file = create(fs, "f", b"abc").await;
        assert_eq!(file.append(b"def").await.unwrap(), 3);
        assert_eq!(file.append(b"!").await.unwrap(), 1);
        assert_eq!(read_all(&*file).await, b"abcdef!");

        // Appends through a second handle of the same file land at the end
        // too - there is no per-handle cursor to go stale.
        let other = open_file(fs, "f", OF::empty(), DF::READ | DF::WRITE)
            .await
            .unwrap();
        other.append(b"?").await.unwrap();
        assert_eq!(read_all(&*file).await, b"abcdef!?");
    }

    pub(crate) async fn unlink_while_open_still_readable(fs: &dyn Filesystem) {
        let file = create(fs, "f", b"survives").await;
        fs.unlink(p("f")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("f"), true).await.unwrap_err(),
            ErrorCode::NoEntry
        );

        // The bytes outlive the name, and the handle works both ways.
        assert_eq!(read_all(&*file).await, b"survives");
        file.write_at(b"S", 0).await.unwrap();
        assert_eq!(read_all(&*file).await, b"Survives");

        // POSIX reports zero links for an unlinked-but-open file.
        assert_eq!(file.stat().await.unwrap().link_count, 0);
    }

    pub(crate) async fn hard_links_share_content_and_link_count(fs: &dyn Filesystem) {
        create(fs, "a", b"hi").await;
        fs.hard_link(p("a"), true, p("b")).await.unwrap();
        assert_eq!(fs.stat_at(p("a"), true).await.unwrap().link_count, 2);
        assert_eq!(fs.stat_at(p("b"), true).await.unwrap().link_count, 2);
        assert_eq!(read_file(fs, "b").await, b"hi");

        // Writes through one name are visible through the other.
        let via_b = open_file(fs, "b", OF::empty(), DF::READ | DF::WRITE)
            .await
            .unwrap();
        via_b.write_at(b"ho", 0).await.unwrap();
        drop(via_b);
        assert_eq!(read_file(fs, "a").await, b"ho");

        // Linking on top of an existing name is refused.
        create(fs, "c", b"").await;
        assert_eq!(
            fs.hard_link(p("a"), true, p("c")).await.unwrap_err(),
            ErrorCode::Exist
        );

        // Dropping one name leaves the object reachable through the other.
        fs.unlink(p("a")).await.unwrap();
        assert_eq!(fs.stat_at(p("b"), true).await.unwrap().link_count, 1);
        assert_eq!(read_file(fs, "b").await, b"ho");
    }

    pub(crate) async fn rename_replaces_existing_file(fs: &dyn Filesystem) {
        create(fs, "src", b"new").await;
        create(fs, "dst", b"old").await;
        fs.rename(p("src"), p("dst")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("src"), true).await.unwrap_err(),
            ErrorCode::NoEntry
        );
        assert_eq!(read_file(fs, "dst").await, b"new");
    }

    pub(crate) async fn rename_into_subdirectory(fs: &dyn Filesystem) {
        mkdir(fs, "d").await;
        create(fs, "f", b"x").await;
        fs.rename(p("f"), p("d/f")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("f"), true).await.unwrap_err(),
            ErrorCode::NoEntry
        );
        assert_eq!(read_file(fs, "d/f").await, b"x");

        // And back up out of the subdirectory.
        fs.rename(p("d/f"), p("g")).await.unwrap();
        assert_eq!(read_file(fs, "g").await, b"x");
    }

    pub(crate) async fn rename_dir_into_itself_refused(fs: &dyn Filesystem) {
        mkdir(fs, "d").await;
        mkdir(fs, "d/e").await;

        // Straight down into itself...
        assert_eq!(
            fs.rename(p("d"), p("d/e/z")).await.unwrap_err(),
            ErrorCode::Invalid
        );
        // ...and onto one of its own children. The child must survive the
        // refusal: a failed rename may not destroy its destination.
        assert_eq!(
            fs.rename(p("d"), p("d/e")).await.unwrap_err(),
            ErrorCode::Invalid
        );
        assert!(fs.stat_at(p("d/e"), true).await.unwrap().type_.is_dir());
    }

    pub(crate) async fn rename_between_hard_links_is_noop(fs: &dyn Filesystem) {
        create(fs, "a", b"x").await;
        fs.hard_link(p("a"), true, p("b")).await.unwrap();

        // POSIX: renaming a name onto another name for the same object does
        // nothing - and in particular does not remove the source name.
        fs.rename(p("a"), p("b")).await.unwrap();
        assert_eq!(read_file(fs, "a").await, b"x");
        assert_eq!(read_file(fs, "b").await, b"x");
    }

    pub(crate) async fn read_dir_omits_dot_entries(fs: &dyn Filesystem) {
        create(fs, "a", b"").await;
        mkdir(fs, "sub").await;
        create(fs, "sub/inner", b"").await;

        let mut root = fs.read_dir(p("")).await.unwrap();
        root.sort_by(|x, y| x.name.cmp(&y.name));
        let names: Vec<_> = root.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["a", "sub"], "`.` and `..` must not be listed");
        assert_eq!(root[0].type_, DescriptorType::RegularFile);
        assert_eq!(root[1].type_, DescriptorType::Directory);

        let sub = fs.read_dir(p("sub")).await.unwrap();
        assert_eq!(sub.len(), 1);
        assert_eq!(sub[0].name, "inner");

        assert_eq!(
            fs.read_dir(p("a")).await.unwrap_err(),
            ErrorCode::NotDirectory
        );
    }

    pub(crate) async fn create_and_remove_dir(fs: &dyn Filesystem) {
        mkdir(fs, "d").await;
        assert!(fs.stat_at(p("d"), true).await.unwrap().type_.is_dir());
        assert_eq!(fs.create_dir(p("d")).await.unwrap_err(), ErrorCode::Exist);

        create(fs, "d/f", b"").await;
        assert_eq!(
            fs.remove_dir(p("d")).await.unwrap_err(),
            ErrorCode::NotEmpty
        );
        // `unlink` does not remove directories...
        assert_eq!(fs.unlink(p("d")).await.unwrap_err(), ErrorCode::IsDirectory);
        // ...and `remove_dir` does not remove files.
        assert_eq!(
            fs.remove_dir(p("d/f")).await.unwrap_err(),
            ErrorCode::NotDirectory
        );

        fs.unlink(p("d/f")).await.unwrap();
        fs.remove_dir(p("d")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("d"), true).await.unwrap_err(),
            ErrorCode::NoEntry
        );
    }

    pub(crate) async fn open_directory_semantics(fs: &dyn Filesystem) {
        mkdir(fs, "d").await;
        assert!(matches!(
            open_with(fs, "d", OF::DIRECTORY, DF::READ, true)
                .await
                .unwrap(),
            Opened::Dir
        ));
        // A plain open of a directory also yields a directory.
        assert!(matches!(
            open_with(fs, "d", OF::empty(), DF::READ, true)
                .await
                .unwrap(),
            Opened::Dir
        ));

        // DIRECTORY on a file is refused.
        create(fs, "f", b"").await;
        assert_eq!(
            open_err(fs, "f", OF::DIRECTORY, DF::READ).await,
            ErrorCode::NotDirectory
        );

        // Directories cannot be opened for byte I/O.
        assert_eq!(
            open_err(fs, "d", OF::empty(), DF::READ | DF::WRITE).await,
            ErrorCode::IsDirectory
        );
        assert_eq!(
            open_err(fs, "d", OF::TRUNCATE, DF::READ).await,
            ErrorCode::IsDirectory
        );
    }

    pub(crate) async fn symlink_stat_follow_vs_nofollow(fs: &dyn Filesystem) {
        create(fs, "target", b"t").await;
        fs.symlink("target", p("link")).await.unwrap();

        let followed = fs.stat_at(p("link"), true).await.unwrap();
        assert_eq!(followed.type_, DescriptorType::RegularFile);
        assert_eq!(followed.size, 1);

        let unfollowed = fs.stat_at(p("link"), false).await.unwrap();
        assert_eq!(unfollowed.type_, DescriptorType::SymbolicLink);

        // Reading through the link reads the target.
        assert_eq!(read_file(fs, "link").await, b"t");
    }

    pub(crate) async fn symlink_open_nofollow_is_loop(fs: &dyn Filesystem) {
        create(fs, "target", b"").await;
        fs.symlink("target", p("link")).await.unwrap();
        assert_eq!(
            open_with(fs, "link", OF::empty(), DF::READ, false)
                .await
                .map(|_| ())
                .unwrap_err(),
            ErrorCode::Loop
        );

        // `O_NOFOLLOW`'s `ELOOP` wins even when `O_DIRECTORY` is also set,
        // matching Linux and wasmtime-wasi.
        mkdir(fs, "dir").await;
        fs.symlink("dir", p("dirlink")).await.unwrap();
        assert_eq!(
            open_with(fs, "dirlink", OF::DIRECTORY, DF::READ, false)
                .await
                .map(|_| ())
                .unwrap_err(),
            ErrorCode::Loop
        );
        // With follow, the same open succeeds.
        assert!(matches!(
            open_with(fs, "dirlink", OF::DIRECTORY, DF::READ, true)
                .await
                .unwrap(),
            Opened::Dir
        ));
    }

    pub(crate) async fn symlink_readlink(fs: &dyn Filesystem) {
        // Dangling targets are fine; a symlink is uninterpreted text until
        // something resolves through it.
        fs.symlink("some/where", p("l")).await.unwrap();
        assert_eq!(fs.readlink(p("l")).await.unwrap(), "some/where");

        create(fs, "f", b"").await;
        assert_eq!(fs.readlink(p("f")).await.unwrap_err(), ErrorCode::Invalid);
    }

    pub(crate) async fn symlink_over_existing_refused(fs: &dyn Filesystem) {
        create(fs, "f", b"").await;
        assert_eq!(
            fs.symlink("anywhere", p("f")).await.unwrap_err(),
            ErrorCode::Exist
        );
    }

    pub(crate) async fn unlink_symlink_leaves_target(fs: &dyn Filesystem) {
        create(fs, "target", b"t").await;
        fs.symlink("target", p("link")).await.unwrap();
        fs.unlink(p("link")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("link"), true).await.unwrap_err(),
            ErrorCode::NoEntry
        );
        assert_eq!(read_file(fs, "target").await, b"t");
    }

    pub(crate) async fn symlink_cycle_is_loop(fs: &dyn Filesystem) {
        fs.symlink("b", p("a")).await.unwrap();
        fs.symlink("a", p("b")).await.unwrap();
        assert_eq!(fs.stat_at(p("a"), true).await.unwrap_err(), ErrorCode::Loop);
        assert_eq!(
            open_err(fs, "a", OF::empty(), DF::READ).await,
            ErrorCode::Loop
        );
        // A cycle in the middle of a path is just as fatal.
        assert_eq!(
            fs.stat_at(p("a/tail"), true).await.unwrap_err(),
            ErrorCode::Loop
        );
    }

    pub(crate) async fn dotdot_at_root_not_permitted(fs: &dyn Filesystem) {
        assert_eq!(
            fs.stat_at(p(".."), true).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
        assert_eq!(
            open_err(fs, "..", OF::empty(), DF::READ).await,
            ErrorCode::NotPermitted
        );
        mkdir(fs, "a").await;
        assert_eq!(
            fs.stat_at(p("a/../.."), true).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
        // `..` that stays inside the root is fine.
        assert!(fs.stat_at(p("a/.."), true).await.unwrap().type_.is_dir());
    }

    pub(crate) async fn absolute_symlink_target_refused(fs: &dyn Filesystem) {
        // WASI: creating a symlink to an absolute target fails with
        // `not-permitted`, and nothing is created.
        assert_eq!(
            fs.symlink("/etc/passwd", p("evil")).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
        assert_eq!(
            fs.stat_at(p("evil"), false).await.unwrap_err(),
            ErrorCode::NoEntry
        );
    }

    pub(crate) async fn escaping_symlink_refused(fs: &dyn Filesystem) {
        // A relative target may be *created*, but resolving through it must
        // not step above the filesystem root.
        fs.symlink("../outside", p("l")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("l"), true).await.unwrap_err(),
            ErrorCode::NotPermitted
        );
    }

    pub(crate) async fn dotdot_through_symlink_lands_in_target_parent(fs: &dyn Filesystem) {
        mkdir(fs, "dir1").await;
        mkdir(fs, "dir2").await;
        mkdir(fs, "dir2/inner").await;
        create(fs, "dir2/sentinel", b"real").await;
        create(fs, "dir1/sentinel", b"decoy").await;
        fs.symlink("../dir2/inner", p("dir1/link")).await.unwrap();

        // `dir1/link/..` must resolve to `dir2` - the parent of the link's
        // *target* - not to `dir1`, which is what a lexical resolver would
        // say.
        assert_eq!(read_file(fs, "dir1/link/../sentinel").await, b"real");
        let via_link = fs.object_id_at(p("dir1/link/.."), true).await.unwrap();
        let direct = fs.object_id_at(p("dir2"), true).await.unwrap();
        assert_eq!(via_link, direct);
    }

    pub(crate) async fn create_through_dangling_symlink(fs: &dyn Filesystem) {
        mkdir(fs, "d").await;
        fs.symlink("d/real", p("link")).await.unwrap();
        assert_eq!(
            fs.stat_at(p("link"), true).await.unwrap_err(),
            ErrorCode::NoEntry
        );

        // O_CREAT through a dangling link creates the *target*, POSIX-style;
        // the link itself stays a link.
        let file = open_file(fs, "link", OF::CREATE, DF::READ | DF::WRITE)
            .await
            .unwrap();
        file.write_at(b"made", 0).await.unwrap();
        drop(file);
        assert_eq!(read_file(fs, "d/real").await, b"made");
        assert_eq!(
            fs.stat_at(p("link"), false).await.unwrap().type_,
            DescriptorType::SymbolicLink
        );

        // O_CREAT|O_EXCL refuses a symlink even when it dangles.
        fs.symlink("nowhere", p("dangling")).await.unwrap();
        assert_eq!(
            open_err(
                fs,
                "dangling",
                OF::CREATE | OF::EXCLUSIVE,
                DF::READ | DF::WRITE
            )
            .await,
            ErrorCode::Exist
        );
    }

    pub(crate) async fn object_identity(fs: &dyn Filesystem) {
        create(fs, "a", b"one").await;
        create(fs, "other", b"two").await;
        fs.hard_link(p("a"), true, p("b")).await.unwrap();

        let a = fs.object_id_at(p("a"), true).await.unwrap();
        let b = fs.object_id_at(p("b"), true).await.unwrap();
        let other = fs.object_id_at(p("other"), true).await.unwrap();
        assert_eq!(a, b, "hard links are the same object");
        assert_ne!(a, other, "distinct files are distinct objects");

        // The handle agrees with the namespace.
        let file = open_file(fs, "a", OF::empty(), DF::READ).await.unwrap();
        assert_eq!(file.object_id().await.unwrap(), a);
    }

    pub(crate) async fn metadata_hash_tracks_changes(fs: &dyn Filesystem) {
        let file = create(fs, "f", b"one").await;
        let before = fs.metadata_hash_at(p("f"), true).await.unwrap();
        assert_eq!(
            before,
            fs.metadata_hash_at(p("f"), true).await.unwrap(),
            "hash is stable while the file is unchanged"
        );
        assert_eq!(before, file.metadata_hash().await.unwrap());

        file.set_size(100).await.unwrap();
        assert_ne!(before, fs.metadata_hash_at(p("f"), true).await.unwrap());
    }

    pub(crate) async fn set_times_explicit(fs: &dyn Filesystem) {
        create(fs, "f", b"").await;
        // Whole seconds, so filesystems with coarse timestamps round-trip.
        let when = SystemTime::UNIX_EPOCH + Duration::from_secs(1_234_567_890);
        fs.set_times_at(
            p("f"),
            true,
            SetTimes {
                access: NewTimestamp::Timestamp(when),
                modification: NewTimestamp::Timestamp(when),
            },
        )
        .await
        .unwrap();
        let stat = fs.stat_at(p("f"), true).await.unwrap();
        assert_eq!(stat.data_modification_timestamp, Some(when));
        assert_eq!(stat.data_access_timestamp, Some(when));

        // NoChange leaves the other timestamp alone.
        let later = when + Duration::from_secs(60);
        fs.set_times_at(
            p("f"),
            true,
            SetTimes {
                access: NewTimestamp::NoChange,
                modification: NewTimestamp::Timestamp(later),
            },
        )
        .await
        .unwrap();
        let stat = fs.stat_at(p("f"), true).await.unwrap();
        assert_eq!(stat.data_modification_timestamp, Some(later));
        assert_eq!(stat.data_access_timestamp, Some(when));

        // And the same through an open handle.
        let file = open_file(fs, "f", OF::empty(), DF::READ | DF::WRITE)
            .await
            .unwrap();
        let via_handle = when + Duration::from_secs(120);
        file.set_times(SetTimes {
            access: NewTimestamp::Timestamp(via_handle),
            modification: NewTimestamp::Timestamp(via_handle),
        })
        .await
        .unwrap();
        assert_eq!(
            file.stat().await.unwrap().data_modification_timestamp,
            Some(via_handle)
        );
    }
}

/// Instantiates one `#[tokio::test]` per case, against the backend built by
/// `$fixture` (which returns `(filesystem, guard)`; the guard keeps e.g. a
/// temp directory alive for the duration of the test).
macro_rules! suite {
    ($fixture:path => $($case:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $case() {
                let (fs, _guard) = $fixture();
                crate::cases::$case(&fs).await;
            }
        )*
    };
}

/// Cases that hold on any platform for any backend.
macro_rules! portable_cases {
    ($fixture:path) => {
        suite!($fixture =>
            create_write_read,
            open_missing_fails,
            exclusive_create,
            truncate_on_open,
            set_size_truncates_and_extends,
            write_past_end_zero_fills,
            append_lands_at_end,
            hard_links_share_content_and_link_count,
            rename_into_subdirectory,
            read_dir_omits_dot_entries,
            create_and_remove_dir,
            open_directory_semantics,
            dotdot_at_root_not_permitted,
            object_identity,
            metadata_hash_tracks_changes,
            set_times_explicit,
        );
    };
}

/// Cases that assume POSIX behaviour the host backend can only provide on
/// Unix: symbolic links (a privileged operation on Windows), unlink of open
/// files, and rename-over-existing. The memory backend provides them
/// everywhere.
macro_rules! posix_cases {
    ($fixture:path) => {
        suite!($fixture =>
            unlink_while_open_still_readable,
            rename_replaces_existing_file,
            rename_dir_into_itself_refused,
            rename_between_hard_links_is_noop,
            symlink_stat_follow_vs_nofollow,
            symlink_open_nofollow_is_loop,
            symlink_readlink,
            symlink_over_existing_refused,
            unlink_symlink_leaves_target,
            symlink_cycle_is_loop,
            absolute_symlink_target_refused,
            escaping_symlink_refused,
            dotdot_through_symlink_lands_in_target_parent,
            create_through_dangling_symlink,
        );
    };
}

mod memory {
    use spin_factor_filesystem::backend::MemoryFilesystem;
    use spin_factor_filesystem::{ErrorCode, Filesystem};

    use crate::cases::p;

    fn fixture() -> (MemoryFilesystem, ()) {
        (MemoryFilesystem::new(), ())
    }

    portable_cases!(fixture);
    posix_cases!(fixture);

    /// The resolver gives up after 32 symlink traversals, like Linux gives up
    /// after 40: enough for real trees, finite for hostile ones.
    #[tokio::test]
    async fn symlink_budget_is_32_hops() {
        let fs = MemoryFilesystem::new();
        super::cases_create_empty(&fs, "hop0").await;
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
        super::cases_create_empty(&fs, "t").await;
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

    fn fixture() -> (HostFilesystem, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fs = HostFilesystem::open(dir.path()).expect("open temp dir");
        (fs, dir)
    }

    portable_cases!(fixture);

    #[cfg(unix)]
    mod unix {
        use super::fixture;

        posix_cases!(fixture);
    }
}

/// Creates an empty file outside the `cases` helpers, for backend-specific
/// tests.
async fn cases_create_empty(fs: &dyn Filesystem, path: &str) {
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
