//! Host implementation of `wasi:filesystem@0.3.0` over the [`Descriptor`]
//! layer.
//!
//! `0.3.0` is the async revision of `wasi:filesystem`: no `wasi:io`
//! resources, and byte and directory streaming use component-model-native
//! `stream`/`future` types. The shape of this module follows
//! `wasmtime-wasi`'s p3 filesystem host - the four streaming methods are
//! implemented "with store" via [`StreamProducer`]/[`StreamConsumer`]
//! state machines, everything else is an ordinary async method - except
//! that the state machines drive the SPI's async [`File`] operations with
//! abort-on-drop tasks instead of dispatching blocking syscalls, so a
//! network-backed filesystem pends instead of occupying a thread.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

use bytes::BytesMut;
use tokio::sync::oneshot;
use wasmtime::StoreContextMut;
use wasmtime::component::{
    Access, Accessor, Destination, FutureReader, Resource, Source, StreamConsumer, StreamProducer,
    StreamReader, StreamResult,
};

use crate::descriptor::Descriptor;
use crate::spi::{
    Advice, DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, MetadataHash, NewTimestamp,
    OpenFlags, SetTimes, Stat,
};
use crate::task::Task;
use crate::{FilesystemCtxView, HasFilesystem};

mod bindings {
    #[allow(missing_docs, reason = "bindgen-generated")]
    mod generated {
        wasmtime::component::bindgen!({
            inline: r#"
                package spin:filesystem-host-p3;

                world filesystem-host {
                    import wasi:filesystem/types@0.3.0;
                    import wasi:filesystem/preopens@0.3.0;
                }
            "#,
            path: "../../wit",
            imports: {
                "wasi:filesystem/types@0.3.0.[method]descriptor.read-via-stream": store | trappable,
                "wasi:filesystem/types@0.3.0.[method]descriptor.write-via-stream": store | trappable,
                "wasi:filesystem/types@0.3.0.[method]descriptor.append-via-stream": store | trappable,
                "wasi:filesystem/types@0.3.0.[method]descriptor.read-directory": store | trappable,
                default: trappable,
            },
            trappable_error_type: {
                "wasi:filesystem/types@0.3.0.error-code" => crate::p3::FsError,
            },
            with: {
                "wasi:filesystem/types.descriptor": crate::descriptor::Descriptor,
            },
            require_store_data_send: true,
        });
    }
    pub use generated::wasi::clocks0_3_0::system_clock;
    pub use generated::wasi::filesystem0_3_0::{preopens, types};
}

pub use bindings::system_clock;
pub use bindings::{preopens, types};

/// The result type of `wasi:filesystem@0.3.0` host methods.
pub type FsResult<T> = Result<T, FsError>;

/// The error type of `wasi:filesystem@0.3.0` host methods: either an
/// `error-code` delivered to the guest, or a trap.
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
            // `0.3.0` dropped `would-block`; a file operation that cannot
            // complete now is closest to a plain I/O error.
            ErrorCode::WouldBlock => Self::Io,
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
            // `0.3.0` replaced `unknown` with a self-describing variant.
            DescriptorType::Unknown => Self::Other(None),
        }
    }
}

fn descriptor_flags_from(flags: types::DescriptorFlags) -> DescriptorFlags {
    let mut out = DescriptorFlags::empty();
    if flags.contains(types::DescriptorFlags::READ) {
        out |= DescriptorFlags::READ;
    }
    if flags.contains(types::DescriptorFlags::WRITE) {
        out |= DescriptorFlags::WRITE;
    }
    if flags.contains(types::DescriptorFlags::FILE_INTEGRITY_SYNC) {
        out |= DescriptorFlags::FILE_INTEGRITY_SYNC;
    }
    if flags.contains(types::DescriptorFlags::DATA_INTEGRITY_SYNC) {
        out |= DescriptorFlags::DATA_INTEGRITY_SYNC;
    }
    if flags.contains(types::DescriptorFlags::REQUESTED_WRITE_SYNC) {
        out |= DescriptorFlags::REQUESTED_WRITE_SYNC;
    }
    if flags.contains(types::DescriptorFlags::MUTATE_DIRECTORY) {
        out |= DescriptorFlags::MUTATE_DIRECTORY;
    }
    out
}

fn descriptor_flags_to(flags: DescriptorFlags) -> types::DescriptorFlags {
    let mut out = types::DescriptorFlags::empty();
    if flags.contains(DescriptorFlags::READ) {
        out |= types::DescriptorFlags::READ;
    }
    if flags.contains(DescriptorFlags::WRITE) {
        out |= types::DescriptorFlags::WRITE;
    }
    if flags.contains(DescriptorFlags::FILE_INTEGRITY_SYNC) {
        out |= types::DescriptorFlags::FILE_INTEGRITY_SYNC;
    }
    if flags.contains(DescriptorFlags::DATA_INTEGRITY_SYNC) {
        out |= types::DescriptorFlags::DATA_INTEGRITY_SYNC;
    }
    if flags.contains(DescriptorFlags::REQUESTED_WRITE_SYNC) {
        out |= types::DescriptorFlags::REQUESTED_WRITE_SYNC;
    }
    if flags.contains(DescriptorFlags::MUTATE_DIRECTORY) {
        out |= types::DescriptorFlags::MUTATE_DIRECTORY;
    }
    out
}

fn open_flags_from(flags: types::OpenFlags) -> OpenFlags {
    let mut out = OpenFlags::empty();
    if flags.contains(types::OpenFlags::CREATE) {
        out |= OpenFlags::CREATE;
    }
    if flags.contains(types::OpenFlags::DIRECTORY) {
        out |= OpenFlags::DIRECTORY;
    }
    if flags.contains(types::OpenFlags::EXCLUSIVE) {
        out |= OpenFlags::EXCLUSIVE;
    }
    if flags.contains(types::OpenFlags::TRUNCATE) {
        out |= OpenFlags::TRUNCATE;
    }
    out
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

fn instant_to(time: SystemTime) -> Option<system_clock::Instant> {
    // `instant` has signed seconds, so pre-epoch timestamps are
    // representable, but backends hand us `SystemTime` which is easiest to
    // split by direction.
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(after) => Some(system_clock::Instant {
            seconds: after.as_secs().try_into().ok()?,
            nanoseconds: after.subsec_nanos(),
        }),
        Err(err) => {
            let before = err.duration();
            Some(system_clock::Instant {
                seconds: i64::try_from(before.as_secs()).ok().map(|s| -s)?,
                nanoseconds: before.subsec_nanos(),
            })
        }
    }
}

fn systemtime_from(instant: system_clock::Instant) -> FsResult<SystemTime> {
    if instant.nanoseconds >= 1_000_000_000 {
        return Err(ErrorCode::Invalid.into());
    }
    if let Ok(seconds) = u64::try_from(instant.seconds) {
        SystemTime::UNIX_EPOCH
            .checked_add(std::time::Duration::new(seconds, instant.nanoseconds))
            .ok_or_else(|| ErrorCode::Overflow.into())
    } else {
        SystemTime::UNIX_EPOCH
            .checked_sub(std::time::Duration::new(
                instant.seconds.unsigned_abs(),
                instant.nanoseconds,
            ))
            .ok_or_else(|| ErrorCode::Overflow.into())
    }
}

fn new_timestamp_from(timestamp: types::NewTimestamp) -> FsResult<NewTimestamp> {
    Ok(match timestamp {
        types::NewTimestamp::NoChange => NewTimestamp::NoChange,
        types::NewTimestamp::Now => NewTimestamp::Now,
        types::NewTimestamp::Timestamp(instant) => {
            NewTimestamp::Timestamp(systemtime_from(instant)?)
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
        data_access_timestamp: stat.data_access_timestamp.and_then(instant_to),
        data_modification_timestamp: stat.data_modification_timestamp.and_then(instant_to),
        status_change_timestamp: stat.status_change_timestamp.and_then(instant_to),
    }
}

fn metadata_hash_to(hash: MetadataHash) -> types::MetadataHashValue {
    types::MetadataHashValue {
        lower: hash.lower,
        upper: hash.upper,
    }
}

fn dir_entry_to(entry: DirEntry) -> types::DirectoryEntry {
    types::DirectoryEntry {
        type_: entry.type_.into(),
        name: entry.name,
    }
}

/// The buffer size for one streamed read, matching `wasmtime-wasi`'s p3
/// host.
const STREAM_READ_CHUNK: usize = 8192;

/// Produces the bytes of a [`File`] from a starting offset for
/// `read-via-stream`, reporting the final result on a oneshot future.
struct ReadStreamProducer {
    file: Arc<dyn File>,
    offset: u64,
    result: Option<oneshot::Sender<Result<(), types::ErrorCode>>>,
    task: Option<Task<Result<BytesMut, ErrorCode>>>,
}

impl ReadStreamProducer {
    fn close(&mut self, result: Result<(), types::ErrorCode>) {
        if let Some(tx) = self.result.take() {
            let _ = tx.send(result);
        }
    }
}

impl Drop for ReadStreamProducer {
    fn drop(&mut self) {
        self.close(Ok(()));
    }
}

impl<D> StreamProducer<D> for ReadStreamProducer {
    type Item = u8;
    type Buffer = BytesMut;

    fn poll_produce<'a>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _store: StoreContextMut<'a, D>,
        mut dst: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let me = &mut *self;
        let task = me.task.get_or_insert_with(|| {
            let mut buf = dst.take_buffer();
            buf.resize(STREAM_READ_CHUNK, 0);
            let file = me.file.clone();
            let offset = me.offset;
            Task::spawn(async move {
                match file.read_at(&mut buf, offset).await {
                    Ok(n) => {
                        buf.truncate(n);
                        Ok(buf)
                    }
                    Err(err) => Err(err),
                }
            })
        });

        let result = match Pin::new(&mut *task).poll(cx) {
            Poll::Pending if finish => {
                // Reads are side-effect free, so an abandoned read task can
                // simply be aborted (dropping it does that).
                me.task = None;
                return Poll::Ready(Ok(StreamResult::Cancelled));
            }
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        me.task = None;
        match result {
            Ok(buf) if buf.is_empty() => {
                me.close(Ok(()));
                Poll::Ready(Ok(StreamResult::Dropped))
            }
            Ok(buf) => {
                let n = buf.len() as u64;
                let Some(offset) = me.offset.checked_add(n) else {
                    me.close(Err(types::ErrorCode::Overflow));
                    return Poll::Ready(Ok(StreamResult::Dropped));
                };
                me.offset = offset;
                dst.set_buffer(buf);
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Err(err) => {
                me.close(Err(err.into()));
                Poll::Ready(Ok(StreamResult::Dropped))
            }
        }
    }
}

/// Consumes a guest byte stream into a [`File`] for `write-via-stream` and
/// `append-via-stream`, reporting the final result on a oneshot future.
struct WriteStreamConsumer {
    file: Arc<dyn File>,
    /// `Some(offset)` writes at an advancing offset; `None` appends.
    position: Option<u64>,
    result: Option<oneshot::Sender<Result<(), types::ErrorCode>>>,
    task: Option<Task<Result<usize, ErrorCode>>>,
}

impl WriteStreamConsumer {
    fn new_at(
        file: Arc<dyn File>,
        offset: u64,
        result: oneshot::Sender<Result<(), types::ErrorCode>>,
    ) -> Self {
        Self {
            file,
            position: Some(offset),
            result: Some(result),
            task: None,
        }
    }

    fn new_append(
        file: Arc<dyn File>,
        result: oneshot::Sender<Result<(), types::ErrorCode>>,
    ) -> Self {
        Self {
            file,
            position: None,
            result: Some(result),
            task: None,
        }
    }

    fn close(&mut self, result: Result<(), types::ErrorCode>) {
        if let Some(tx) = self.result.take() {
            let _ = tx.send(result);
        }
    }
}

impl Drop for WriteStreamConsumer {
    fn drop(&mut self) {
        self.close(Ok(()));
    }
}

/// Writes all of `buf` at `position` (or appends), returning the count.
async fn write_all(file: &dyn File, position: Option<u64>, buf: &[u8]) -> Result<usize, ErrorCode> {
    let mut written = 0;
    while written < buf.len() {
        let n = match position {
            Some(p) => file.write_at(&buf[written..], p + written as u64).await?,
            None => file.append(&buf[written..]).await?,
        };
        if n == 0 {
            return Err(ErrorCode::Io);
        }
        written += n;
    }
    Ok(written)
}

impl<D> StreamConsumer<D> for WriteStreamConsumer {
    type Item = u8;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        src: Source<Self::Item>,
        _finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let mut src = src.as_direct(store);
        let me = &mut *self;
        let task = me.task.get_or_insert_with(|| {
            let buf = src.remaining().to_vec();
            let file = me.file.clone();
            let position = me.position;
            Task::spawn(async move { write_all(&*file, position, &buf).await })
        });

        // Unlike reads, an in-flight write is not safely cancellable - the
        // backend may have applied part of it - so `finish` is ignored and
        // the (bounded, one-chunk) write is awaited.
        let result = match Pin::new(&mut *task).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        me.task = None;
        match result {
            Ok(n) => {
                src.mark_read(n);
                if let Some(position) = &mut me.position {
                    let Some(next) = position.checked_add(n as u64) else {
                        me.close(Err(types::ErrorCode::Overflow));
                        return Poll::Ready(Ok(StreamResult::Dropped));
                    };
                    *position = next;
                }
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Err(err) => {
                me.close(Err(err.into()));
                Poll::Ready(Ok(StreamResult::Dropped))
            }
        }
    }
}

/// Produces `directory-entry` items for `read-directory`: one async fetch
/// of the listing, then one item per poll.
struct DirEntryProducer {
    state: DirEntryState,
    result: Option<oneshot::Sender<Result<(), types::ErrorCode>>>,
}

enum DirEntryState {
    Pending(Task<Result<Vec<DirEntry>, crate::spi::ErrorCode>>),
    Yielding(std::vec::IntoIter<types::DirectoryEntry>),
}

impl DirEntryProducer {
    fn new(descriptor: Descriptor, result: oneshot::Sender<Result<(), types::ErrorCode>>) -> Self {
        Self {
            state: DirEntryState::Pending(Task::spawn(
                async move { descriptor.read_directory().await },
            )),
            result: Some(result),
        }
    }

    fn close(&mut self, result: Result<(), types::ErrorCode>) {
        if let Some(tx) = self.result.take() {
            let _ = tx.send(result);
        }
    }
}

impl Drop for DirEntryProducer {
    fn drop(&mut self) {
        self.close(Ok(()));
    }
}

impl<D> StreamProducer<D> for DirEntryProducer {
    type Item = types::DirectoryEntry;
    type Buffer = Option<types::DirectoryEntry>;

    fn poll_produce<'a>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _store: StoreContextMut<'a, D>,
        mut dst: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let me = &mut *self;
        loop {
            match &mut me.state {
                DirEntryState::Pending(task) => {
                    let result = match Pin::new(task).poll(cx) {
                        Poll::Pending if finish => {
                            return Poll::Ready(Ok(StreamResult::Cancelled));
                        }
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => result,
                    };
                    match result {
                        Ok(entries) => {
                            me.state = DirEntryState::Yielding(
                                entries
                                    .into_iter()
                                    .map(dir_entry_to)
                                    .collect::<Vec<_>>()
                                    .into_iter(),
                            );
                        }
                        Err(err) => {
                            me.close(Err(err.into()));
                            return Poll::Ready(Ok(StreamResult::Dropped));
                        }
                    }
                }
                DirEntryState::Yielding(entries) => {
                    return match entries.next() {
                        Some(entry) => {
                            dst.set_buffer(Some(entry));
                            Poll::Ready(Ok(StreamResult::Completed))
                        }
                        None => {
                            me.close(Ok(()));
                            Poll::Ready(Ok(StreamResult::Dropped))
                        }
                    };
                }
            }
        }
    }
}

impl types::Host for FilesystemCtxView<'_> {
    fn convert_error_code(&mut self, err: FsError) -> wasmtime::Result<types::ErrorCode> {
        err.downcast()
    }
}

/// Fetches a cloned [`Descriptor`] out of the store's resource table.
fn get_descriptor<U: 'static>(
    store: &Accessor<U, HasFilesystem>,
    fd: &Resource<Descriptor>,
) -> FsResult<Descriptor> {
    store.with(|mut access| Ok(access.get().table.get(fd)?.clone()))
}

impl<U: 'static> types::HostDescriptorWithStore<U> for HasFilesystem {
    fn read_via_stream(
        mut store: Access<'_, U, Self>,
        fd: Resource<Descriptor>,
        offset: types::Filesize,
    ) -> wasmtime::Result<(StreamReader<u8>, FutureReader<Result<(), types::ErrorCode>>)> {
        match store
            .get()
            .table
            .get(&fd)
            .map_err(wasmtime::Error::new)
            .and_then(|d| {
                d.file_for_stream(DescriptorFlags::READ)
                    .map_err(wasmtime::Error::new)
            }) {
            Ok(file) => {
                let (result_tx, result_rx) = oneshot::channel();
                Ok((
                    StreamReader::new(
                        &mut store,
                        ReadStreamProducer {
                            file,
                            offset,
                            result: Some(result_tx),
                            task: None,
                        },
                    )?,
                    FutureReader::new(&mut store, result_rx)?,
                ))
            }
            Err(err) => {
                let code = error_code_of(err)?;
                Ok((
                    StreamReader::new(&mut store, std::iter::empty())?,
                    FutureReader::new(
                        &mut store,
                        async move { Ok::<_, wasmtime::Error>(Err(code)) },
                    )?,
                ))
            }
        }
    }

    fn write_via_stream(
        mut store: Access<'_, U, Self>,
        fd: Resource<Descriptor>,
        mut data: StreamReader<u8>,
        offset: types::Filesize,
    ) -> wasmtime::Result<FutureReader<Result<(), types::ErrorCode>>> {
        let (result_tx, result_rx) = oneshot::channel();
        match store
            .get()
            .table
            .get(&fd)
            .map_err(wasmtime::Error::new)
            .and_then(|d| {
                d.file_for_stream(DescriptorFlags::WRITE)
                    .map_err(wasmtime::Error::new)
            }) {
            Ok(file) => {
                data.pipe(
                    &mut store,
                    WriteStreamConsumer::new_at(file, offset, result_tx),
                )?;
            }
            Err(err) => {
                let code = error_code_of(err)?;
                data.close(&mut store)?;
                let _ = result_tx.send(Err(code));
            }
        }
        FutureReader::new(&mut store, result_rx)
    }

    fn append_via_stream(
        mut store: Access<'_, U, Self>,
        fd: Resource<Descriptor>,
        mut data: StreamReader<u8>,
    ) -> wasmtime::Result<FutureReader<Result<(), types::ErrorCode>>> {
        let (result_tx, result_rx) = oneshot::channel();
        match store
            .get()
            .table
            .get(&fd)
            .map_err(wasmtime::Error::new)
            .and_then(|d| {
                d.file_for_stream(DescriptorFlags::WRITE)
                    .map_err(wasmtime::Error::new)
            }) {
            Ok(file) => {
                data.pipe(&mut store, WriteStreamConsumer::new_append(file, result_tx))?;
            }
            Err(err) => {
                let code = error_code_of(err)?;
                data.close(&mut store)?;
                let _ = result_tx.send(Err(code));
            }
        }
        FutureReader::new(&mut store, result_rx)
    }

    fn read_directory(
        mut store: Access<'_, U, Self>,
        fd: Resource<Descriptor>,
    ) -> wasmtime::Result<(
        StreamReader<types::DirectoryEntry>,
        FutureReader<Result<(), types::ErrorCode>>,
    )> {
        let (result_tx, result_rx) = oneshot::channel();
        let stream = match store.get().table.get(&fd).cloned() {
            Ok(descriptor) => {
                StreamReader::new(&mut store, DirEntryProducer::new(descriptor, result_tx))?
            }
            Err(err) => {
                let _ = result_tx.send(Err(types::ErrorCode::BadDescriptor));
                let _ = err;
                StreamReader::new(&mut store, std::iter::empty())?
            }
        };
        Ok((stream, FutureReader::new(&mut store, result_rx)?))
    }

    async fn advise(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        offset: types::Filesize,
        length: types::Filesize,
        advice: types::Advice,
    ) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor
            .advise(offset, length, advice_from(advice))
            .await?)
    }

    async fn sync_data(store: &Accessor<U, Self>, fd: Resource<Descriptor>) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.sync_data().await?)
    }

    async fn get_flags(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FsResult<types::DescriptorFlags> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor_flags_to(descriptor.flags()))
    }

    async fn get_type(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FsResult<types::DescriptorType> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.type_().into())
    }

    async fn set_size(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        size: types::Filesize,
    ) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.set_size(size).await?)
    }

    async fn set_times(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        data_access_timestamp: types::NewTimestamp,
        data_modification_timestamp: types::NewTimestamp,
    ) -> FsResult<()> {
        let times = set_times_from(data_access_timestamp, data_modification_timestamp)?;
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.set_times(times).await?)
    }

    async fn sync(store: &Accessor<U, Self>, fd: Resource<Descriptor>) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.sync().await?)
    }

    async fn create_directory_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.create_directory_at(&path).await?)
    }

    async fn stat(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FsResult<types::DescriptorStat> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(stat_to(descriptor.stat().await?))
    }

    async fn stat_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
    ) -> FsResult<types::DescriptorStat> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(stat_to(
            descriptor.stat_at(&path, follow(path_flags)).await?,
        ))
    }

    async fn set_times_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
        data_access_timestamp: types::NewTimestamp,
        data_modification_timestamp: types::NewTimestamp,
    ) -> FsResult<()> {
        let times = set_times_from(data_access_timestamp, data_modification_timestamp)?;
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor
            .set_times_at(&path, follow(path_flags), times)
            .await?)
    }

    async fn link_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        old_path_flags: types::PathFlags,
        old_path: String,
        new_descriptor: Resource<Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let (old_base, new_base) =
            store.with(|mut access| -> FsResult<(Descriptor, Descriptor)> {
                let table = &access.get().table;
                Ok((table.get(&fd)?.clone(), table.get(&new_descriptor)?.clone()))
            })?;
        Ok(old_base
            .link_at(follow(old_path_flags), &old_path, &new_base, &new_path)
            .await?)
    }

    async fn open_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
        open_flags: types::OpenFlags,
        flags: types::DescriptorFlags,
    ) -> FsResult<Resource<Descriptor>> {
        let descriptor = get_descriptor(store, &fd)?;
        let opened = descriptor
            .open_at(
                &path,
                follow(path_flags),
                open_flags_from(open_flags),
                descriptor_flags_from(flags),
            )
            .await?;
        Ok(store.with(|mut access| access.get().table.push(opened))?)
    }

    async fn readlink_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FsResult<String> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.readlink_at(&path).await?)
    }

    async fn remove_directory_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.remove_directory_at(&path).await?)
    }

    async fn rename_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        old_path: String,
        new_descriptor: Resource<Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let (old_base, new_base) =
            store.with(|mut access| -> FsResult<(Descriptor, Descriptor)> {
                let table = &access.get().table;
                Ok((table.get(&fd)?.clone(), table.get(&new_descriptor)?.clone()))
            })?;
        Ok(old_base.rename_at(&old_path, &new_base, &new_path).await?)
    }

    async fn symlink_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        old_path: String,
        new_path: String,
    ) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.symlink_at(&old_path, &new_path).await?)
    }

    async fn unlink_file_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(descriptor.unlink_file_at(&path).await?)
    }

    async fn is_same_object(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        other: Resource<Descriptor>,
    ) -> wasmtime::Result<bool> {
        let (a, b) = store.with(|mut access| -> wasmtime::Result<(Descriptor, Descriptor)> {
            let table = &access.get().table;
            Ok((
                table.get(&fd).map_err(wasmtime::Error::new)?.clone(),
                table.get(&other).map_err(wasmtime::Error::new)?.clone(),
            ))
        })?;
        a.is_same_object(&b).await.map_err(wasmtime::Error::new)
    }

    async fn metadata_hash(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
    ) -> FsResult<types::MetadataHashValue> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(metadata_hash_to(descriptor.metadata_hash().await?))
    }

    async fn metadata_hash_at(
        store: &Accessor<U, Self>,
        fd: Resource<Descriptor>,
        path_flags: types::PathFlags,
        path: String,
    ) -> FsResult<types::MetadataHashValue> {
        let descriptor = get_descriptor(store, &fd)?;
        Ok(metadata_hash_to(
            descriptor
                .metadata_hash_at(&path, follow(path_flags))
                .await?,
        ))
    }
}

impl types::HostDescriptor for FilesystemCtxView<'_> {
    fn drop(&mut self, fd: Resource<Descriptor>) -> wasmtime::Result<()> {
        self.table.delete(fd)?;
        Ok(())
    }
}

impl preopens::Host for FilesystemCtxView<'_> {
    fn get_directories(&mut self) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
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

/// Extracts the `error-code` from an error known to carry one, trapping
/// otherwise.
fn error_code_of(err: wasmtime::Error) -> wasmtime::Result<types::ErrorCode> {
    if let Some(code) = err.downcast_ref::<ErrorCode>() {
        return Ok((*code).into());
    }
    err.downcast()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn instants_round_trip_across_the_epoch() {
        for offset in [0i64, 1, -1, 1_234_567_890, -1_234_567_890] {
            let time = if offset >= 0 {
                SystemTime::UNIX_EPOCH + Duration::from_secs(offset as u64)
            } else {
                SystemTime::UNIX_EPOCH - Duration::from_secs(offset.unsigned_abs())
            };
            let instant = instant_to(time).expect("representable");
            assert_eq!(instant.seconds, offset, "offset {offset}");
            assert_eq!(systemtime_from(instant).expect("valid"), time);
        }
    }

    #[test]
    fn invalid_instants_are_refused() {
        let bogus = system_clock::Instant {
            seconds: 0,
            nanoseconds: 1_000_000_000,
        };
        assert!(systemtime_from(bogus).is_err());
    }

    #[test]
    fn unknown_type_maps_to_other() {
        assert!(matches!(
            types::DescriptorType::from(DescriptorType::Unknown),
            types::DescriptorType::Other(None)
        ));
        assert_eq!(
            types::DescriptorType::from(DescriptorType::Directory),
            types::DescriptorType::Directory
        );
    }

    #[test]
    fn would_block_degrades_to_io() {
        assert_eq!(
            types::ErrorCode::from(ErrorCode::WouldBlock),
            types::ErrorCode::Io
        );
        assert_eq!(
            types::ErrorCode::from(ErrorCode::ReadOnly),
            types::ErrorCode::ReadOnly
        );
    }
}
