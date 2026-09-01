use std::future::Future;

use tokio::sync::oneshot;
use tokio::task::AbortHandle;

/// RAII handle returned by `subscribe`/`subscribe_async`.
///
/// Dropping it requests cancellation via a `oneshot` signal and then aborts
/// the subscriber task. Because Rust has no `AsyncDrop`, the cancel signal
/// and the abort happen back-to-back with no guaranteed window for the task
/// to observe the signal first; this is best-effort cooperative
/// cancellation racing a hard abort, not a guaranteed graceful shutdown.
#[must_use = "dropping this Subscription immediately cancels it"]
pub struct Subscription {
    pub(crate) cancel: Option<oneshot::Sender<()>>,
    pub(crate) abort: Option<AbortHandle>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(abort) = self.abort.take() {
            abort.abort();
        }
    }
}

/// Spawns `future` as the subscriber task, and a small supervisor task next
/// to it that logs if it panics (`subscribe_async`'s handler can panic
/// mid-`.await`, where `catch_unwind` can't reach it the way
/// [`run_guarded`](crate::signal::run_guarded) reaches synchronous
/// closures). Returns the worker's [`AbortHandle`] for [`Subscription`] to
/// cancel it with; aborting it makes the supervisor observe a cancellation,
/// not a panic, so a normal `drop(subscription)` never logs an error.
pub(crate) fn spawn_supervised<F>(future: F) -> AbortHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = tokio::spawn(future);
    let abort = handle.abort_handle();
    tokio::spawn(async move {
        if let Err(err) = handle.await {
            if err.is_panic() {
                tracing::error!(?err, "ferx: subscriber task panicked");
            }
        }
    });
    abort
}
