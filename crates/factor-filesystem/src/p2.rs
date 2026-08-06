//! Host implementation of `wasi:filesystem@0.2.x` over the [`Descriptor`]
//! layer.
//!
//! This module is a mechanical translation: WIT types in, [`crate::spi`]
//! types out, with every semantic decision already made by
//! [`crate::descriptor`]. The one structural piece it owns is streams:
//! `read-via-stream`/`write-via-stream`/`append-via-stream` hand out
//! `wasi:io` stream resources, and those are implemented here over
//! [`File`] with the same non-blocking state machines `wasmtime-wasi` uses
//! for its own file streams - a `read`/`write` call starts a background
//! task and reports "not ready" until it finishes, so a slow backend never
//! blocks the executor.
//!
//! The generated bindings map `wasi:io` to `wasmtime-wasi-io`'s types,
//! which is what `spin-factor-wasi` links `wasi:io` with; the streams
//! created here are ordinary `DynInputStream`/`DynOutputStream` resources
//! that the existing `wasi:io/streams` implementation drives unmodified.

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use wasmtime::component::Resource;
use wasmtime_wasi_io::poll::Pollable;
use wasmtime_wasi_io::streams::{
    DynInputStream, DynOutputStream, Error as IoError, InputStream, OutputStream, StreamError,
    StreamResult,
};

use crate::descriptor::Descriptor;
use crate::spi::{
    Advice, DescriptorFlags, DescriptorType, ErrorCode, File, MetadataHash, NewTimestamp,
    OpenFlags, SetTimes, Stat,
};
use crate::{FilesystemCtxView, p2};

pub mod compat;

mod bindings {
    #[allow(missing_docs, reason = "bindgen-generated")]
    mod generated {
        wasmtime::component::bindgen!({
            inline: r#"
                package spin:filesystem-host;

                world filesystem-host {
                    import wasi:filesystem/types@0.2.6;
                    import wasi:filesystem/preopens@0.2.6;
                }
            "#,
            path: "../../wit",
            imports: { default: async | trappable },
            trappable_error_type: {
                "wasi:filesystem/types@0.2.6.error-code" => crate::p2::FsError,
            },
            with: {
                "wasi:io/poll": wasmtime_wasi_io::bindings::wasi::io::poll,
                "wasi:io/streams": wasmtime_wasi_io::bindings::wasi::io::streams,
                "wasi:io/error": wasmtime_wasi_io::bindings::wasi::io::error,
                "wasi:filesystem/types.descriptor": crate::descriptor::Descriptor,
                "wasi:filesystem/types.directory-entry-stream": crate::p2::DirectoryEntryStream,
            },
            require_store_data_send: true,
        });
    }
    pub use generated::wasi::clocks0_2_6::wall_clock;
    pub use generated::wasi::filesystem0_2_6::{preopens, types};
}

pub use bindings::wall_clock;
pub use bindings::{preopens, types};

/// The result type of `wasi:filesystem@0.2.x` host methods.
pub type FsResult<T> = Result<T, FsError>;

/// The error type of `wasi:filesystem@0.2.x` host methods: either a
/// `wasi:filesystem` `error-code` delivered to the guest, or a trap.
///
/// Constructed via `From` for the error path (`?` on SPI and descriptor
/// results) and [`FsError::trap`] for traps; the bindgen glue calls
/// [`types::Host::convert_error_code`] to pull the `error-code` back out.
pub struct FsError {
    err: wasmtime::Error,
}

impl FsError {
    /// An error that traps the guest rather than returning an `error-code`.
    pub fn trap(err: impl Into<wasmtime::Error>) -> Self {
        Self { err: err.into() }
    }

    /// The `error-code` to deliver to the guest, or the trap to raise.
    pub fn downcast(self) -> wasmtime::Result<types::ErrorCode> {
        self.err.downcast()
    }
}

impl std::fmt::Debug for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.err, f)
    }
}

impl From<types::ErrorCode> for FsError {
    fn from(code: types::ErrorCode) -> Self {
        Self {
            err: wasmtime::Error::new(code),
        }
    }
}

impl From<ErrorCode> for FsError {
    fn from(code: ErrorCode) -> Self {
        types::ErrorCode::from(code).into()
    }
}

impl From<wasmtime::component::ResourceTableError> for FsError {
    fn from(err: wasmtime::component::ResourceTableError) -> Self {
        Self::trap(err)
    }
}

impl From<ErrorCode> for types::ErrorCode {
    fn from(code: ErrorCode) -> Self {
        match code {
            ErrorCode::Access => Self::Access,
            ErrorCode::WouldBlock => Self::WouldBlock,
            ErrorCode::Already => Self::Already,
            ErrorCode::BadDescriptor => Self::BadDescriptor,
            ErrorCode::Busy => Self::Busy,
            ErrorCode::Deadlock => Self::Deadlock,
            ErrorCode::Quota => Self::Quota,
            ErrorCode::Exist => Self::Exist,
            ErrorCode::FileTooLarge => Self::FileTooLarge,
            ErrorCode::IllegalByteSequence => Self::IllegalByteSequence,
            ErrorCode::InProgress => Self::InProgress,
            ErrorCode::Interrupted => Self::Interrupted,
            ErrorCode::Invalid => Self::Invalid,
            ErrorCode::Io => Self::Io,
            ErrorCode::IsDirectory => Self::IsDirectory,
            ErrorCode::Loop => Self::Loop,
            ErrorCode::TooManyLinks => Self::TooManyLinks,
            ErrorCode::MessageSize => Self::MessageSize,
            ErrorCode::NameTooLong => Self::NameTooLong,
            ErrorCode::NoDevice => Self::NoDevice,
            ErrorCode::NoEntry => Self::NoEntry,
            ErrorCode::NoLock => Self::NoLock,
            ErrorCode::InsufficientMemory => Self::InsufficientMemory,
            ErrorCode::InsufficientSpace => Self::InsufficientSpace,
            ErrorCode::NotDirectory => Self::NotDirectory,
            ErrorCode::NotEmpty => Self::NotEmpty,
            ErrorCode::NotRecoverable => Self::NotRecoverable,
            ErrorCode::Unsupported => Self::Unsupported,
            ErrorCode::NoTty => Self::NoTty,
            ErrorCode::NoSuchDevice => Self::NoSuchDevice,
            ErrorCode::Overflow => Self::Overflow,
            ErrorCode::NotPermitted => Self::NotPermitted,
            ErrorCode::Pipe => Self::Pipe,
            ErrorCode::ReadOnly => Self::ReadOnly,
            ErrorCode::InvalidSeek => Self::InvalidSeek,
            ErrorCode::TextFileBusy => Self::TextFileBusy,
            ErrorCode::CrossDevice => Self::CrossDevice,
        }
    }
}

impl From<DescriptorType> for types::DescriptorType {
    fn from(type_: DescriptorType) -> Self {
        match type_ {
            DescriptorType::BlockDevice => Self::BlockDevice,
            DescriptorType::CharacterDevice => Self::CharacterDevice,
            DescriptorType::Directory => Self::Directory,
            DescriptorType::Fifo => Self::Fifo,
            DescriptorType::SymbolicLink => Self::SymbolicLink,
            DescriptorType::RegularFile => Self::RegularFile,
            DescriptorType::Socket => Self::Socket,
            DescriptorType::Unknown => Self::Unknown,
        }
    }
}

/// Pairwise translation between two flags types that only differ in where
/// they were declared.
macro_rules! translate_flags {
    ($value:expr, $from:path => $to:path, [$($flag:ident),* $(,)?]) => {{
        use $from as from_type;
        use $to as to_type;
        let value = $value;
        let mut out = to_type::empty();
        $(if value.contains(from_type::$flag) {
            out |= to_type::$flag;
        })*
        out
    }};
}

fn descriptor_flags_from(flags: types::DescriptorFlags) -> DescriptorFlags {
    translate_flags!(flags, types::DescriptorFlags => DescriptorFlags, [
        READ, WRITE, FILE_INTEGRITY_SYNC, DATA_INTEGRITY_SYNC, REQUESTED_WRITE_SYNC,
        MUTATE_DIRECTORY,
    ])
}

fn descriptor_flags_to(flags: DescriptorFlags) -> types::DescriptorFlags {
    translate_flags!(flags, DescriptorFlags => types::DescriptorFlags, [
        READ, WRITE, FILE_INTEGRITY_SYNC, DATA_INTEGRITY_SYNC, REQUESTED_WRITE_SYNC,
        MUTATE_DIRECTORY,
    ])
}

fn open_flags_from(flags: types::OpenFlags) -> OpenFlags {
    translate_flags!(flags, types::OpenFlags => OpenFlags, [
        CREATE, DIRECTORY, EXCLUSIVE, TRUNCATE,
    ])
}

fn follow(path_flags: types::PathFlags) -> bool {
    path_flags.contains(types::PathFlags::SYMLINK_FOLLOW)
}

fn advice_from(advice: types::Advice) -> Advice {
    match advice {
        types::Advice::Normal => Advice::Normal,
        types::Advice::Sequential => Advice::Sequential,
        types::Advice::Random => Advice::Random,
        types::Advice::WillNeed => Advice::WillNeed,
        types::Advice::DontNeed => Advice::DontNeed,
        types::Advice::NoReuse => Advice::NoReuse,
    }
}

fn datetime_to(time: SystemTime) -> Option<wall_clock::Datetime> {
    // Pre-epoch timestamps have no `datetime` representation; report them
    // as absent rather than wrapping around.
    let since_epoch = time.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    Some(wall_clock::Datetime {
        seconds: since_epoch.as_secs(),
        nanoseconds: since_epoch.subsec_nanos(),
    })
}

fn systemtime_from(datetime: wall_clock::Datetime) -> FsResult<SystemTime> {
    if datetime.nanoseconds >= 1_000_000_000 {
        return Err(ErrorCode::Invalid.into());
    }
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::new(datetime.seconds, datetime.nanoseconds))
        .ok_or_else(|| ErrorCode::Overflow.into())
}

fn new_timestamp_from(timestamp: types::NewTimestamp) -> FsResult<NewTimestamp> {
    Ok(match timestamp {
        types::NewTimestamp::NoChange => NewTimestamp::NoChange,
        types::NewTimestamp::Now => NewTimestamp::Now,
        types::NewTimestamp::Timestamp(datetime) => {
            NewTimestamp::Timestamp(systemtime_from(datetime)?)
        }
    })
}

fn set_times_from(atim: types::NewTimestamp, mtim: types::NewTimestamp) -> FsResult<SetTimes> {
    Ok(SetTimes {
        access: new_timestamp_from(atim)?,
        modification: new_timestamp_from(mtim)?,
    })
}

fn stat_to(stat: Stat) -> types::DescriptorStat {
    types::DescriptorStat {
        type_: stat.type_.into(),
        link_count: stat.link_count,
        size: stat.size,
        data_access_timestamp: stat.data_access_timestamp.and_then(datetime_to),
        data_modification_timestamp: stat.data_modification_timestamp.and_then(datetime_to),
        status_change_timestamp: stat.status_change_timestamp.and_then(datetime_to),
    }
}

fn metadata_hash_to(hash: MetadataHash) -> types::MetadataHashValue {
    types::MetadataHashValue {
        lower: hash.lower,
        upper: hash.upper,
    }
}

/// The chunk size for stream reads, matching `wasmtime-wasi`'s allocation
/// bound for the same streams.
const STREAM_READ_CHUNK: usize = 64 * 1024;

/// How much a file output stream reports as writable when idle.
const STREAM_WRITE_CAPACITY: usize = 1024 * 1024;

/// A spawned task whose handle aborts the task on drop, so a guest that
/// drops a stream mid-operation cannot leak background work.
struct Task<T>(tokio::task::JoinHandle<T>);

impl<T: Send + 'static> Task<T> {
    fn spawn(future: impl Future<Output = T> + Send + 'static) -> Self {
        Self(tokio::spawn(future))
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for Task<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(value)) => Poll::Ready(value),
            Poll::Ready(Err(err)) => match err.try_into_panic() {
                Ok(payload) => std::panic::resume_unwind(payload),
                // We hold the only handle and only abort on drop, so the
                // task cannot have been cancelled while being polled.
                Err(err) => unreachable!("task cancelled while awaited: {err}"),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A `wasi:io` input stream reading a [`File`] sequentially from a starting
/// offset.
pub struct FileInputStream {
    file: Arc<dyn File>,
    position: u64,
    state: ReadState,
}

enum ReadState {
    Idle,
    Waiting(Task<ReadState>),
    DataAvailable(Bytes),
    Error(ErrorCode),
    Closed,
}

impl FileInputStream {
    fn new(file: Arc<dyn File>, position: u64) -> Self {
        Self {
            file,
            position,
            state: ReadState::Idle,
        }
    }

    /// Starts a background read of up to `size` bytes at the current
    /// position.
    fn start_read(&mut self, size: usize) {
        let file = self.file.clone();
        let offset = self.position;
        let size = size.min(STREAM_READ_CHUNK);
        self.state = ReadState::Waiting(Task::spawn(async move {
            let mut buf = vec![0u8; size];
            match file.read_at(&mut buf, offset).await {
                Ok(0) => ReadState::Closed,
                Ok(n) => {
                    buf.truncate(n);
                    ReadState::DataAvailable(buf.into())
                }
                Err(err) => ReadState::Error(err),
            }
        }));
    }

    /// Waits for an in-flight read, without starting a new one.
    async fn wait_ready(&mut self) {
        if let ReadState::Waiting(task) = &mut self.state {
            self.state = task.await;
        }
    }
}

#[wasmtime_wasi_io::async_trait]
impl InputStream for FileInputStream {
    fn read(&mut self, size: usize) -> StreamResult<Bytes> {
        match &mut self.state {
            ReadState::Idle => {
                if size == 0 {
                    return Ok(Bytes::new());
                }
                self.start_read(size);
                Ok(Bytes::new())
            }
            ReadState::DataAvailable(data) => {
                let n = data.len().min(size);
                let chunk = data.split_to(n);
                if data.is_empty() {
                    self.state = ReadState::Idle;
                }
                self.position += n as u64;
                Ok(chunk)
            }
            ReadState::Waiting(_) => Ok(Bytes::new()),
            ReadState::Error(_) => match mem::replace(&mut self.state, ReadState::Closed) {
                ReadState::Error(err) => {
                    Err(StreamError::LastOperationFailed(wasmtime::Error::new(err)))
                }
                _ => unreachable!(),
            },
            ReadState::Closed => Err(StreamError::Closed),
        }
    }

    /// Read without the spawn/join round trip, since blocking is allowed.
    async fn blocking_read(&mut self, size: usize) -> StreamResult<Bytes> {
        self.wait_ready().await;
        if let ReadState::Idle = self.state {
            if size == 0 {
                return Ok(Bytes::new());
            }
            let mut buf = vec![0u8; size.min(STREAM_READ_CHUNK)];
            self.state = match self.file.read_at(&mut buf, self.position).await {
                Ok(0) => ReadState::Closed,
                Ok(n) => {
                    buf.truncate(n);
                    ReadState::DataAvailable(buf.into())
                }
                Err(err) => ReadState::Error(err),
            };
        }
        self.read(size)
    }

    async fn cancel(&mut self) {
        // Dropping a `Waiting` state aborts its task.
        self.state = ReadState::Closed;
    }
}

#[wasmtime_wasi_io::async_trait]
impl Pollable for FileInputStream {
    async fn ready(&mut self) {
        if let ReadState::Idle = self.state {
            // The guest is waiting for data without having issued a read;
            // start one on its behalf.
            const DEFAULT_READ_SIZE: usize = 4096;
            self.start_read(DEFAULT_READ_SIZE);
        }
        self.wait_ready().await;
    }
}

enum OutputMode {
    Position(u64),
    Append,
}

/// A `wasi:io` output stream writing a [`File`] sequentially from a
/// starting offset, or appending.
pub struct FileOutputStream {
    file: Arc<dyn File>,
    mode: OutputMode,
    state: OutputState,
}

enum OutputState {
    Ready,
    Waiting(Task<Result<usize, ErrorCode>>),
    Error(ErrorCode),
    Closed,
}

impl FileOutputStream {
    fn write_at(file: Arc<dyn File>, position: u64) -> Self {
        Self {
            file,
            mode: OutputMode::Position(position),
            state: OutputState::Ready,
        }
    }

    fn append(file: Arc<dyn File>) -> Self {
        Self {
            file,
            mode: OutputMode::Append,
            state: OutputState::Ready,
        }
    }

    /// Records `n` bytes as successfully written.
    fn advance(&mut self, n: usize) {
        if let OutputMode::Position(position) = &mut self.mode {
            *position += n as u64;
        }
    }
}

/// Writes all of `buf`, at `position` or appending, returning how much was
/// written even though that is always `buf.len()` on success.
async fn write_all(file: &dyn File, position: Option<u64>, buf: &[u8]) -> Result<usize, ErrorCode> {
    let mut written = 0;
    while written < buf.len() {
        let n = match position {
            Some(p) => file.write_at(&buf[written..], p + written as u64).await?,
            None => file.append(&buf[written..]).await?,
        };
        if n == 0 {
            // A backend that accepts zero bytes forever would otherwise spin.
            return Err(ErrorCode::Io);
        }
        written += n;
    }
    Ok(written)
}

#[wasmtime_wasi_io::async_trait]
impl OutputStream for FileOutputStream {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        match self.state {
            OutputState::Ready => {}
            OutputState::Closed => return Err(StreamError::Closed),
            OutputState::Waiting(_) | OutputState::Error(_) => {
                return Err(StreamError::trap(
                    "write not permitted: check-write not called first",
                ));
            }
        }
        let file = self.file.clone();
        let position = match self.mode {
            OutputMode::Position(p) => Some(p),
            OutputMode::Append => None,
        };
        self.state = OutputState::Waiting(Task::spawn(async move {
            write_all(&*file, position, &bytes).await
        }));
        Ok(())
    }

    /// Write without the spawn/join round trip, since blocking is allowed.
    async fn blocking_write_and_flush(&mut self, bytes: Bytes) -> StreamResult<()> {
        self.ready().await;
        match &mut self.state {
            OutputState::Ready => {}
            OutputState::Closed => return Err(StreamError::Closed),
            OutputState::Error(_) => match mem::replace(&mut self.state, OutputState::Closed) {
                OutputState::Error(err) => {
                    return Err(StreamError::LastOperationFailed(wasmtime::Error::new(err)));
                }
                _ => unreachable!(),
            },
            OutputState::Waiting(_) => unreachable!("just waited for readiness"),
        }

        let position = match self.mode {
            OutputMode::Position(p) => Some(p),
            OutputMode::Append => None,
        };
        match write_all(&*self.file.clone(), position, &bytes).await {
            Ok(n) => {
                self.advance(n);
                Ok(())
            }
            Err(err) => {
                self.state = OutputState::Closed;
                Err(StreamError::LastOperationFailed(wasmtime::Error::new(err)))
            }
        }
    }

    fn flush(&mut self) -> StreamResult<()> {
        match self.state {
            // The only buffering is the in-flight background write, which
            // `check-write` already gates on.
            OutputState::Ready | OutputState::Waiting(_) => Ok(()),
            OutputState::Closed => Err(StreamError::Closed),
            OutputState::Error(_) => match mem::replace(&mut self.state, OutputState::Closed) {
                OutputState::Error(err) => {
                    Err(StreamError::LastOperationFailed(wasmtime::Error::new(err)))
                }
                _ => unreachable!(),
            },
        }
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        match self.state {
            OutputState::Ready => Ok(STREAM_WRITE_CAPACITY),
            OutputState::Waiting(_) => Ok(0),
            OutputState::Closed => Err(StreamError::Closed),
            OutputState::Error(_) => match mem::replace(&mut self.state, OutputState::Closed) {
                OutputState::Error(err) => {
                    Err(StreamError::LastOperationFailed(wasmtime::Error::new(err)))
                }
                _ => unreachable!(),
            },
        }
    }

    async fn cancel(&mut self) {
        // Dropping a `Waiting` state aborts its task.
        self.state = OutputState::Closed;
    }
}

#[wasmtime_wasi_io::async_trait]
impl Pollable for FileOutputStream {
    async fn ready(&mut self) {
        let OutputState::Waiting(task) = &mut self.state else {
            return;
        };
        let result = task.await;
        self.state = match result {
            Ok(n) => {
                self.advance(n);
                OutputState::Ready
            }
            Err(err) => OutputState::Error(err),
        };
    }
}

/// The `directory-entry-stream` resource: a snapshot of a directory
/// listing, handed out one entry per call.
pub struct DirectoryEntryStream(Mutex<std::vec::IntoIter<types::DirectoryEntry>>);

impl DirectoryEntryStream {
    fn new(entries: Vec<types::DirectoryEntry>) -> Self {
        Self(Mutex::new(entries.into_iter()))
    }

    fn next(&self) -> Option<types::DirectoryEntry> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).next()
    }
}

impl types::Host for FilesystemCtxView<'_> {
    fn convert_error_code(&mut self, err: FsError) -> wasmtime::Result<types::ErrorCode> {
        err.downcast()
    }

    async fn filesystem_error_code(
        &mut self,
        err: Resource<IoError>,
    ) -> wasmtime::Result<Option<types::ErrorCode>> {
        let err = self.table.get(&err)?;
        // Errors produced by the streams above carry the SPI error code;
        // io errors can come out of other stream impls on the same table.
        if let Some(code) = err.downcast_ref::<ErrorCode>() {
            return Ok(Some((*code).into()));
        }
        if let Some(io) = err.downcast_ref::<std::io::Error>() {
            return Ok(Some(ErrorCode::from(io).into()));
        }
        Ok(None)
    }
}

impl types::HostDescriptor for FilesystemCtxView<'_> {
    async fn read_via_stream(
        &mut self,
        fd: Resource<Descriptor>,
        offset: types::Filesize,
    ) -> FsResult<Resource<DynInputStream>> {
        let file = self
            .table
            .get(&fd)?
            .file_for_stream(DescriptorFlags::READ)?;
        let reader: DynInputStream = Box::new(FileInputStream::new(file, offset));
        Ok(self.table.push(reader)?)
    }

    async fn write_via_stream(
        &mut self,
        fd: Resource<Descriptor>,
        offset: types::Filesize,
    ) -> FsResult<Resource<DynOutputStream>> {
        let file = self
            .table
            .get(&fd)?
            .file_for_stream(DescriptorFlags::WRITE)?;
        let writer: DynOutputStream = Box::new(FileOutputStream::write_at(file, offset));
        Ok(self.table.push(writer)?)
    }

    async fn append_via_stream(
        &mut self,
        fd: Resource<Descriptor>,
    ) -> FsResult<Resource<DynOutputStream>> {
        let file = self
            .table
            .get(&fd)?
            .file_for_stream(DescriptorFlags::WRITE)?;
        let appender: DynOutputStream = Box::new(FileOutputStream::append(file));
        Ok(self.table.push(appender)?)
    }

    async fn advise(
        &mut self,
        fd: Resource<Descriptor>,
        offset: types::Filesize,
        length: types::Filesize,
        advice: types::Advice,
    ) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor
            .advise(offset, length, advice_from(advice))
            .await?)
    }

    async fn sync_data(&mut self, fd: Resource<Descriptor>) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.sync_data().await?)
    }

    async fn get_flags(&mut self, fd: Resource<Descriptor>) -> FsResult<types::DescriptorFlags> {
        Ok(descriptor_flags_to(self.table.get(&fd)?.flags()))
    }

    async fn get_type(&mut self, fd: Resource<Descriptor>) -> FsResult<types::DescriptorType> {
        Ok(self.table.get(&fd)?.type_().into())
    }

    async fn set_size(&mut self, fd: Resource<Descriptor>, size: types::Filesize) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.set_size(size).await?)
    }

    async fn set_times(
        &mut self,
        fd: Resource<Descriptor>,
        atim: types::NewTimestamp,
        mtim: types::NewTimestamp,
    ) -> FsResult<()> {
        let times = set_times_from(atim, mtim)?;
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.set_times(times).await?)
    }

    async fn read(
        &mut self,
        fd: Resource<Descriptor>,
        length: types::Filesize,
        offset: types::Filesize,
    ) -> FsResult<(Vec<u8>, bool)> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.read(length, offset).await?)
    }

    async fn write(
        &mut self,
        fd: Resource<Descriptor>,
        buffer: Vec<u8>,
        offset: types::Filesize,
    ) -> FsResult<types::Filesize> {
        let descriptor = self.table.get(&fd)?.clone();
        let n = descriptor.write(&buffer, offset).await?;
        Ok(n as types::Filesize)
    }

    async fn read_directory(
        &mut self,
        fd: Resource<Descriptor>,
    ) -> FsResult<Resource<DirectoryEntryStream>> {
        let descriptor = self.table.get(&fd)?.clone();
        let entries = descriptor
            .read_directory()
            .await?
            .into_iter()
            .map(|entry| types::DirectoryEntry {
                type_: entry.type_.into(),
                name: entry.name,
            })
            .collect();
        Ok(self.table.push(DirectoryEntryStream::new(entries))?)
    }

    async fn sync(&mut self, fd: Resource<Descriptor>) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.sync().await?)
    }

    async fn create_directory_at(
        &mut self,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.create_directory_at(&path).await?)
    }

    async fn stat(&mut self, fd: Resource<Descriptor>) -> FsResult<types::DescriptorStat> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(stat_to(descriptor.stat().await?))
    }

    async fn stat_at(
        &mut self,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
    ) -> FsResult<types::DescriptorStat> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(stat_to(
            descriptor.stat_at(&path, follow(path_flags)).await?,
        ))
    }

    async fn set_times_at(
        &mut self,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
        atim: types::NewTimestamp,
        mtim: types::NewTimestamp,
    ) -> FsResult<()> {
        let times = set_times_from(atim, mtim)?;
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor
            .set_times_at(&path, follow(path_flags), times)
            .await?)
    }

    async fn link_at(
        &mut self,
        fd: Resource<Descriptor>,
        old_path_flags: types::PathFlags,
        old_path: String,
        new_descriptor: Resource<Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let old_base = self.table.get(&fd)?.clone();
        let new_base = self.table.get(&new_descriptor)?.clone();
        Ok(old_base
            .link_at(follow(old_path_flags), &old_path, &new_base, &new_path)
            .await?)
    }

    async fn open_at(
        &mut self,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
        open_flags: types::OpenFlags,
        flags: types::DescriptorFlags,
    ) -> FsResult<Resource<Descriptor>> {
        let descriptor = self.table.get(&fd)?.clone();
        let opened = descriptor
            .open_at(
                &path,
                follow(path_flags),
                open_flags_from(open_flags),
                descriptor_flags_from(flags),
            )
            .await?;
        Ok(self.table.push(opened)?)
    }

    async fn readlink_at(&mut self, fd: Resource<Descriptor>, path: String) -> FsResult<String> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.readlink_at(&path).await?)
    }

    async fn remove_directory_at(
        &mut self,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.remove_directory_at(&path).await?)
    }

    async fn rename_at(
        &mut self,
        fd: Resource<Descriptor>,
        old_path: String,
        new_descriptor: Resource<Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let old_base = self.table.get(&fd)?.clone();
        let new_base = self.table.get(&new_descriptor)?.clone();
        Ok(old_base.rename_at(&old_path, &new_base, &new_path).await?)
    }

    async fn symlink_at(
        &mut self,
        fd: Resource<Descriptor>,
        old_path: String,
        new_path: String,
    ) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.symlink_at(&old_path, &new_path).await?)
    }

    async fn unlink_file_at(&mut self, fd: Resource<Descriptor>, path: String) -> FsResult<()> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(descriptor.unlink_file_at(&path).await?)
    }

    async fn is_same_object(
        &mut self,
        fd: Resource<Descriptor>,
        other: Resource<Descriptor>,
    ) -> wasmtime::Result<bool> {
        let a = self.table.get(&fd)?.clone();
        let b = self.table.get(&other)?.clone();
        // `is-same-object` has no error return, so backend failures trap.
        a.is_same_object(&b).await.map_err(wasmtime::Error::new)
    }

    async fn metadata_hash(
        &mut self,
        fd: Resource<Descriptor>,
    ) -> FsResult<types::MetadataHashValue> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(metadata_hash_to(descriptor.metadata_hash().await?))
    }

    async fn metadata_hash_at(
        &mut self,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
    ) -> FsResult<types::MetadataHashValue> {
        let descriptor = self.table.get(&fd)?.clone();
        Ok(metadata_hash_to(
            descriptor
                .metadata_hash_at(&path, follow(path_flags))
                .await?,
        ))
    }

    async fn drop(&mut self, fd: Resource<Descriptor>) -> wasmtime::Result<()> {
        self.table.delete(fd)?;
        Ok(())
    }
}

impl types::HostDirectoryEntryStream for FilesystemCtxView<'_> {
    async fn read_directory_entry(
        &mut self,
        stream: Resource<DirectoryEntryStream>,
    ) -> FsResult<Option<types::DirectoryEntry>> {
        Ok(self.table.get(&stream)?.next())
    }

    async fn drop(&mut self, stream: Resource<DirectoryEntryStream>) -> wasmtime::Result<()> {
        self.table.delete(stream)?;
        Ok(())
    }
}

impl preopens::Host for FilesystemCtxView<'_> {
    async fn get_directories(&mut self) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
        let entries: Vec<(Descriptor, String)> = self
            .ctx
            .preopens
            .entries()
            .map(|(descriptor, path)| (descriptor.clone(), path.clone()))
            .collect();
        let mut out = Vec::with_capacity(entries.len());
        for (descriptor, path) in entries {
            out.push((self.table.push(descriptor)?, path));
        }
        Ok(out)
    }
}

// Referenced from the bindgen `with` clauses by their `crate::p2::` paths.
const _: () = {
    // Ensure the re-exports the bindgen relies on exist.
    fn _assert(_: p2::DirectoryEntryStream) {}
};

#[cfg(test)]
mod tests {
    use super::preopens::Host as _;
    use super::types::{Host as _, HostDescriptor, HostDirectoryEntryStream};
    use super::*;
    use crate::FilesystemCtx;
    use crate::backend::MemoryFilesystem;
    use wasmtime::component::ResourceTable;

    fn borrow<T: 'static>(resource: &Resource<T>) -> Resource<T> {
        Resource::new_borrow(resource.rep())
    }

    #[tokio::test]
    async fn p2_host_end_to_end() {
        let mut ctx = FilesystemCtx::default();
        ctx.preopens
            .mount("/", Arc::new(MemoryFilesystem::new()), true);
        let mut table = ResourceTable::new();
        let mut view = FilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        let mut dirs = view.get_directories().await.unwrap();
        assert_eq!(dirs.len(), 1);
        let (root, guest_path) = dirs.remove(0);
        assert_eq!(guest_path, "/");

        // Create and write through the descriptor interface.
        let fd = view
            .open_at(
                borrow(&root),
                types::PathFlags::SYMLINK_FOLLOW,
                "hello.txt".into(),
                types::OpenFlags::CREATE,
                types::DescriptorFlags::READ | types::DescriptorFlags::WRITE,
            )
            .await
            .unwrap();
        assert_eq!(
            view.write(borrow(&fd), b"hello world".to_vec(), 0)
                .await
                .unwrap(),
            11
        );
        let (data, eof) = view.read(borrow(&fd), 5, 6).await.unwrap();
        assert_eq!(data, b"world");
        assert!(!eof);
        let (data, eof) = view.read(borrow(&fd), 5, 11).await.unwrap();
        assert!(data.is_empty());
        assert!(eof);

        // Overwrite the head through an output stream, driven the way the
        // wasi:io/streams host drives it.
        let out = view.write_via_stream(borrow(&fd), 0).await.unwrap();
        view.table
            .get_mut(&out)
            .unwrap()
            .blocking_write_and_flush(Bytes::from_static(b"HELLO"))
            .await
            .unwrap();

        // And read everything back through an input stream.
        let input = view.read_via_stream(borrow(&fd), 0).await.unwrap();
        let stream = view.table.get_mut(&input).unwrap();
        let mut contents = Vec::new();
        loop {
            match stream.blocking_read(64).await {
                Ok(chunk) => contents.extend_from_slice(&chunk),
                Err(StreamError::Closed) => break,
                Err(err) => panic!("stream read failed: {err}"),
            }
        }
        assert_eq!(contents, b"HELLO world");

        // Directory listing comes back as a stream of entries.
        let listing = view.read_directory(borrow(&root)).await.unwrap();
        let entry = view
            .read_directory_entry(borrow(&listing))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.name, "hello.txt");
        assert_eq!(entry.type_, types::DescriptorType::RegularFile);
        assert!(
            view.read_directory_entry(borrow(&listing))
                .await
                .unwrap()
                .is_none()
        );

        // Stat reports what we wrote.
        let stat = view.stat(borrow(&fd)).await.unwrap();
        assert_eq!(stat.type_, types::DescriptorType::RegularFile);
        assert_eq!(stat.size, 11);

        // filesystem-error-code recovers codes from stream errors.
        let io_err: Resource<IoError> = view
            .table
            .push(wasmtime::Error::new(ErrorCode::NoEntry))
            .unwrap();
        assert_eq!(
            view.filesystem_error_code(borrow(&io_err)).await.unwrap(),
            Some(types::ErrorCode::NoEntry)
        );

        HostDirectoryEntryStream::drop(&mut view, listing)
            .await
            .unwrap();
        HostDescriptor::drop(&mut view, fd).await.unwrap();
        HostDescriptor::drop(&mut view, root).await.unwrap();
    }

    #[tokio::test]
    async fn p2_read_only_mount_reports_read_only() {
        let mut ctx = FilesystemCtx::default();
        ctx.preopens
            .mount("/", Arc::new(MemoryFilesystem::new()), false);
        let mut table = ResourceTable::new();
        let mut view = FilesystemCtxView {
            ctx: &mut ctx,
            table: &mut table,
        };

        let (root, _) = view.get_directories().await.unwrap().remove(0);
        let err = view
            .open_at(
                borrow(&root),
                types::PathFlags::SYMLINK_FOLLOW,
                "f".into(),
                types::OpenFlags::CREATE,
                types::DescriptorFlags::READ | types::DescriptorFlags::WRITE,
            )
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.downcast().unwrap(), types::ErrorCode::ReadOnly);
    }
}
