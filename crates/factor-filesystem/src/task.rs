//! A small abort-on-drop wrapper for spawned tasks.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A spawned tokio task whose handle aborts the task when dropped, so a
/// guest that drops a stream mid-operation cannot leak background work.
pub(crate) struct Task<T>(tokio::task::JoinHandle<T>);

impl<T: Send + 'static> Task<T> {
    pub(crate) fn spawn(future: impl Future<Output = T> + Send + 'static) -> Self {
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
