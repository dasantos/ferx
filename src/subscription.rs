use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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
    pub(crate) handle: Option<JoinHandle<()>>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
