use anyhow::{Context, Result, bail, ensure};
use spin_sdk::http_component;

/// Exercises `wasi:filesystem` against Spin's pluggable mounts through
/// plain `std::fs`: `/data` is a writable label mount and `/frozen` a
/// read-only one, both defined in the test's runtime config.
#[http_component]
fn filesystem_mounts(_req: http::Request<()>) -> Result<http::Response<String>> {
    match run_all() {
        Ok(()) => Ok(http::Response::builder().status(200).body("ok".into())?),
        Err(err) => Ok(http::Response::builder()
            .status(500)
            .body(format!("{err:#}"))?),
    }
}

fn run_all() -> Result<()> {
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};

    // Create, write, read back.
    fs::create_dir("/data/dir").context("create_dir")?;
    fs::write("/data/dir/file.txt", "hello filesystem").context("write")?;
    let contents = fs::read_to_string("/data/dir/file.txt").context("read_to_string")?;
    ensure!(contents == "hello filesystem", "read back {contents:?}");

    // Seek and partial reads through an open handle.
    let mut file = fs::File::open("/data/dir/file.txt").context("open")?;
    file.seek(SeekFrom::Start(6)).context("seek")?;
    let mut tail = String::new();
    file.read_to_string(&mut tail).context("read tail")?;
    ensure!(tail == "filesystem", "seek+read got {tail:?}");
    drop(file);

    // Append.
    let mut appender = fs::OpenOptions::new()
        .append(true)
        .open("/data/dir/file.txt")
        .context("open append")?;
    appender.write_all(b"!").context("append")?;
    drop(appender);
    ensure!(
        fs::read_to_string("/data/dir/file.txt").context("re-read")? == "hello filesystem!",
        "append did not land at end"
    );

    // Rename within and across directories.
    fs::rename("/data/dir/file.txt", "/data/dir/renamed.txt").context("rename")?;
    ensure!(
        fs::metadata("/data/dir/file.txt").is_err(),
        "old name still exists after rename"
    );
    fs::rename("/data/dir/renamed.txt", "/data/top.txt").context("rename across dirs")?;

    // Hard links share content.
    fs::hard_link("/data/top.txt", "/data/link.txt").context("hard_link")?;
    fs::write("/data/link.txt", "via link").context("write via link")?;
    ensure!(
        fs::read_to_string("/data/top.txt").context("read via original")? == "via link",
        "hard link does not share content"
    );

    // Truncation via set_len.
    let file = fs::OpenOptions::new()
        .write(true)
        .open("/data/top.txt")
        .context("open for truncate")?;
    file.set_len(3).context("set_len")?;
    drop(file);
    ensure!(
        fs::read_to_string("/data/top.txt").context("read truncated")? == "via",
        "set_len did not truncate"
    );

    // Directory listing.
    let mut names: Vec<String> = fs::read_dir("/data")
        .context("read_dir")?
        .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_>>()?;
    names.sort();
    ensure!(
        names == ["dir", "link.txt", "top.txt"],
        "unexpected listing {names:?}"
    );

    // Removal, and errors after removal.
    fs::remove_file("/data/link.txt").context("remove_file")?;
    fs::remove_file("/data/top.txt").context("remove_file 2")?;
    fs::remove_dir("/data/dir").context("remove_dir")?;
    ensure!(
        fs::read_dir("/data").context("read_dir after cleanup")?.count() == 0,
        "mount not empty after cleanup"
    );
    ensure!(
        fs::read_to_string("/data/top.txt").is_err(),
        "removed file still readable by name"
    );

    // Missing intermediate directories fail.
    if fs::write("/data/nope/deep.txt", "x").is_ok() {
        bail!("write through a missing directory unexpectedly succeeded");
    }

    // The read-only mount reads but refuses writes.
    ensure!(
        fs::read_dir("/frozen").context("read_dir frozen")?.count() == 0,
        "frozen mount should be empty"
    );
    if fs::write("/frozen/nope.txt", "x").is_ok() {
        bail!("write to read-only mount unexpectedly succeeded");
    }

    Ok(())
}
