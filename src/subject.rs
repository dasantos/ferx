use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;

use crate::error::SendError;
use crate::signal::{Signal, Terminal, WireSignal, is_terminal, run_guarded};
use crate::subscription::Subscription;

/// Aborts background operator tasks once no `Subject` clone references them
/// anymore, so operator stages don't outlive their output handle.
struct TaskGuard(Vec<JoinHandle<()>>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        for handle in self.0.drain(..) {
            handle.abort();
        }
    }
}

/// Whether a `Subject` has already ended, guarded by a single lock so
/// "check terminal state" and "subscribe to the channel" (or "send") never
/// race each other. Plain `std::sync::Mutex` is fine: critical sections are
/// synchronous and tiny, never held across an `.await`.
struct SubjectState<E> {
    terminal: Option<Terminal<E>>,
}

/// Returned by `Subject::snapshot`: either a live receiver (subscribe before
/// any future termination is observed), or the terminal signal the subject
/// already ended with; a receiver created now would never see it, since a
/// `broadcast` channel doesn't replay history to new subscribers.
pub(crate) enum SourceSnapshot<T, E> {
    Live(broadcast::Receiver<WireSignal<T, E>>),
    Terminated(Terminal<E>),
}

/// A hot observable: values are emitted regardless of subscriber count, and
/// late subscribers miss earlier items. Cheap to clone; every clone shares
/// the same underlying channel and terminal state.
///
/// Once terminated (via [`error`](Subject::error) or
/// [`complete`](Subject::complete)), a `Subject` remembers it: further
/// emissions are ignored, and any subscriber that subscribes afterward
/// immediately receives the stored terminal signal.
///
/// # Examples
///
/// ```rust
/// use ferx::Subject;
///
/// # #[tokio::main]
/// # async fn main() {
/// let subject: Subject<i32, String> = Subject::new(16);
/// let sub = subject.subscribe(|signal| println!("{signal:?}"));
///
/// subject.next(1).ok();
/// subject.complete().ok();
/// # tokio::time::sleep(std::time::Duration::from_millis(20)).await;
/// drop(sub);
/// # }
/// ```
pub struct Subject<T, E> {
    sender: broadcast::Sender<WireSignal<T, E>>,
    state: Arc<Mutex<SubjectState<E>>>,
    // Shared so every clone keeps an operator-stage task alive; only the
    // last clone being dropped aborts it.
    task_guard: Option<Arc<TaskGuard>>,
}

impl<T, E> Clone for Subject<T, E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            state: self.state.clone(),
            task_guard: self.task_guard.clone(),
        }
    }
}

impl<T, E> Subject<T, E>
where
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    /// Creates a new hot `Subject` with the given channel capacity.
    ///
    /// # Panics
    /// Panics if `capacity` is 0; a broadcast channel needs room for at
    /// least one in-flight value.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Subject capacity must be greater than 0");
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            state: Arc::new(Mutex::new(SubjectState { terminal: None })),
            task_guard: None,
        }
    }

    /// Ties `handle`'s lifetime to this subject: once every clone of this
    /// subject is dropped, the task is aborted.
    pub(crate) fn attach_task(&mut self, handle: JoinHandle<()>) {
        self.attach_tasks(vec![handle]);
    }

    /// Same as `attach_task`, for operators (e.g. `merge`) backed by more
    /// than one background task.
    pub(crate) fn attach_tasks(&mut self, handles: Vec<JoinHandle<()>>) {
        self.task_guard = Some(Arc::new(TaskGuard(handles)));
    }

    /// Emits `OnNext(value)` to all current subscribers.
    ///
    /// Returns the number of subscribers the value was delivered to, or
    /// `Ok(0)` without sending anything if this subject already terminated.
    /// Errors only if there are no active subscribers at all.
    pub fn next(&self, value: T) -> Result<usize, SendError> {
        self.send(Some(Ok(value)))
    }

    /// Emits `OnError(err)`, terminating the sequence for all subscribers.
    ///
    /// A no-op (returns `Ok(0)`) if this subject already terminated.
    pub fn error(&self, err: E) -> Result<usize, SendError> {
        self.send(Some(Err(err)))
    }

    /// Emits `OnCompleted`, terminating the sequence for all subscribers.
    ///
    /// A no-op (returns `Ok(0)`) if this subject already terminated.
    pub fn complete(&self) -> Result<usize, SendError> {
        self.send(None)
    }

    fn send(&self, wire: WireSignal<T, E>) -> Result<usize, SendError> {
        let mut state = self.state.lock().unwrap();
        if state.terminal.is_some() {
            // Already terminated: ignore further next()/error()/complete()
            // calls, matching the Rx `Subject` contract.
            return Ok(0);
        }
        match &wire {
            Some(Err(e)) => state.terminal = Some(Terminal::Error(e.clone())),
            None => state.terminal = Some(Terminal::Complete),
            Some(Ok(_)) => {}
        }
        self.sender.send(wire).map_err(|_| SendError::NoReceivers)
    }

    /// Returns a clone of the underlying [`broadcast::Sender`], for direct
    /// use instead of [`next`](Subject::next)/[`error`](Subject::error)/
    /// [`complete`](Subject::complete). See [`WireSignal`] for the value
    /// type it carries. Sending through it bypasses this subject's
    /// terminal-state tracking.
    pub fn sender(&self) -> broadcast::Sender<WireSignal<T, E>> {
        self.sender.clone()
    }

    /// Atomically returns either a live receiver, or the terminal signal
    /// this subject already ended with. Guarded by the same lock as `send`,
    /// so nothing can terminate in the gap between the check and subscribing.
    pub(crate) fn snapshot(&self) -> SourceSnapshot<T, E> {
        let state = self.state.lock().unwrap();
        match &state.terminal {
            Some(terminal) => SourceSnapshot::Terminated(terminal.clone()),
            None => SourceSnapshot::Live(self.sender.subscribe()),
        }
    }

    /// Subscribes with a synchronous handler, called once per [`Signal`].
    /// Sugar for [`subscribe_async`](Subject::subscribe_async) with a
    /// handler that returns immediately.
    pub fn subscribe<F>(&self, handler: F) -> Subscription
    where
        F: Fn(Signal<T, E>) + Send + 'static,
    {
        self.subscribe_async(move |signal| {
            run_guarded("subscribe", || handler(signal));
            std::future::ready(())
        })
    }

    /// Subscribes with an async handler. Delivery stops after `OnError`,
    /// `OnCompleted`, or if the sender lags this subscriber (see module docs
    /// on `RecvError::Lagged` handling). If the subject already terminated,
    /// the handler is invoked once with that terminal signal directly,
    /// since a receiver created now would never see it on the channel.
    pub fn subscribe_async<F, Fut>(&self, handler: F) -> Subscription
    where
        F: Fn(Signal<T, E>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut rx = match self.snapshot() {
            SourceSnapshot::Terminated(terminal) => {
                let handle = tokio::spawn(async move {
                    handler(terminal.to_signal()).await;
                });
                return Subscription {
                    cancel: None,
                    handle: Some(handle),
                };
            }
            SourceSnapshot::Live(rx) => rx,
        };
        let (cancel_tx, mut cancel_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cancel_rx => break,
                    msg = rx.recv() => {
                        match msg {
                            Ok(wire) => {
                                let terminal = is_terminal(&wire);
                                handler(Signal::from(wire)).await;
                                if terminal {
                                    break;
                                }
                            }
                            // Lagged: terminate delivery for this subscriber rather
                            // than smuggle a transport error through the generic `E`.
                            Err(err @ broadcast::error::RecvError::Lagged(_)) => {
                                tracing::warn!(?err, "ferx: subscriber lagged; stopping delivery");
                                break;
                            }
                            // All senders (the Subject and its clones) were dropped
                            // without an explicit error()/complete() call, so this
                            // subscriber never gets a terminal Signal.
                            Err(broadcast::error::RecvError::Closed) => {
                                tracing::debug!(
                                    "ferx: subject closed without an explicit terminal signal; stopping delivery"
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        Subscription {
            cancel: Some(cancel_tx),
            handle: Some(handle),
        }
    }
}
