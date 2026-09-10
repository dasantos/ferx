use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::error::SendError;
use crate::signal::{
    ChannelSignal, NoReplay, Signal, SubjectPolicy, TerminalState, is_terminal, run_guarded,
};
use crate::subscription::{Subscription, spawn_supervised};

/// Distinguishes one `Subject`'s log/span output from another's; assigned
/// once per `Subject::new` call, shared by every clone.
static NEXT_SUBJECT_ID: AtomicU64 = AtomicU64::new(0);

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
/// synchronous and tiny, never held across an `.await`. `Buf` is the
/// `SubjectPolicy`'s replay buffer, guarded by the same lock so a replay
/// read is always consistent with the terminal state at that instant.
struct SubjectState<E, Buf> {
    terminal: Option<TerminalState<E>>,
    buffer: Buf,
}

/// Returned by `Subject::snapshot`: replayed items (empty under the default
/// [`NoReplay`] policy) plus either a live receiver (subscribe before any
/// future termination is observed), or the terminal signal the subject
/// already ended with; a receiver created now would never see it, since a
/// `broadcast` channel doesn't replay history to new subscribers.
pub(crate) enum SourceSnapshot<T, E> {
    Live {
        replay: Vec<T>,
        receiver: broadcast::Receiver<ChannelSignal<T, E>>,
    },
    Terminated {
        replay: Vec<T>,
        terminal: TerminalState<E>,
    },
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
/// use std::num::NonZeroUsize;
///
/// use ferx::Subject;
///
/// #[tokio::main]
/// async fn main() {
///     let subject: Subject<i32, String> = Subject::new(NonZeroUsize::new(16).unwrap());
///     let sub = subject.subscribe(|signal| println!("{signal:?}"));
///
///     subject.next(1).ok();
///     subject.complete().ok();
///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
///     drop(sub);
/// }
/// ```
pub struct Subject<T, E, P: SubjectPolicy<T> = NoReplay> {
    sender: broadcast::Sender<ChannelSignal<T, E>>,
    state: Arc<Mutex<SubjectState<E, P::Buffer>>>,
    // Shared so every clone keeps an operator-stage task alive; only the
    // last clone being dropped aborts it.
    task_guard: Option<Arc<TaskGuard>>,
    // Identifies this subject (shared by every clone) in tracing spans, so
    // logs from concurrently running subjects can be told apart.
    pub(crate) id: u64,
}

impl<T, E, P: SubjectPolicy<T>> Clone for Subject<T, E, P> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            state: self.state.clone(),
            task_guard: self.task_guard.clone(),
            id: self.id,
        }
    }
}

impl<T, E, P> Subject<T, E, P>
where
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
    P: SubjectPolicy<T>,
{
    /// Creates a new hot `Subject` with the given channel capacity.
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        let (sender, _) = broadcast::channel(capacity.get());
        Self {
            sender,
            state: Arc::new(Mutex::new(SubjectState {
                terminal: None,
                buffer: P::Buffer::default(),
            })),
            task_guard: None,
            id: NEXT_SUBJECT_ID.fetch_add(1, Ordering::Relaxed),
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
    ///
    /// # Errors
    /// Returns [`SendError::NoReceivers`] if there are no active subscribers.
    pub fn next(&self, value: T) -> Result<usize, SendError> {
        self.send(Some(Ok(value)))
    }

    /// Emits `OnError(err)`, terminating the sequence for all subscribers.
    ///
    /// A no-op (returns `Ok(0)`) if this subject already terminated.
    ///
    /// # Errors
    /// Returns [`SendError::NoReceivers`] if there are no active subscribers.
    pub fn error(&self, err: E) -> Result<usize, SendError> {
        self.send(Some(Err(err)))
    }

    /// Emits `OnCompleted`, terminating the sequence for all subscribers.
    ///
    /// A no-op (returns `Ok(0)`) if this subject already terminated.
    ///
    /// # Errors
    /// Returns [`SendError::NoReceivers`] if there are no active subscribers.
    pub fn complete(&self) -> Result<usize, SendError> {
        self.send(None)
    }

    /// Emits a raw `ChannelSignal` value, updating the terminal-state lock first.
    /// `pub(crate)` (not private) so operators can forward through it
    /// directly instead of through the `sender()` escape hatch, which would
    /// silently desync `state.terminal` from what's actually on the channel.
    pub(crate) fn send(&self, channel_signal: ChannelSignal<T, E>) -> Result<usize, SendError> {
        let mut state = self.state.lock().expect("ferx: state mutex poisoned");
        if state.terminal.is_some() {
            // Already terminated: ignore further next()/error()/complete()
            // calls, matching the Rx `Subject` contract.
            return Ok(0);
        }
        match &channel_signal {
            Some(Ok(t)) => P::record(&mut state.buffer, t),
            Some(Err(e)) => state.terminal = Some(TerminalState::Error(e.clone())),
            None => state.terminal = Some(TerminalState::Complete),
        }
        self.sender
            .send(channel_signal)
            .map_err(|_| SendError::NoReceivers)
    }

    /// Returns a clone of the underlying [`broadcast::Sender`], for direct
    /// use instead of [`next`](Subject::next)/[`error`](Subject::error)/
    /// [`complete`](Subject::complete). See [`ChannelSignal`] for the value
    /// type it carries.
    ///
    /// Sending through it bypasses this subject's terminal-state tracking:
    /// a value sent this way is never recorded, so if it's a terminal one
    /// (`None`/`Some(Err(_))`), a subscriber that subscribes *afterward*
    /// won't be told this subject already ended, and will wait on a
    /// receiver that will never get anything (`broadcast` doesn't replay to
    /// receivers created after the fact). Safe to use for `OnNext` values,
    /// or when no subscriber will ever subscribe after this subject ends.
    #[must_use]
    pub fn sender(&self) -> broadcast::Sender<ChannelSignal<T, E>> {
        self.sender.clone()
    }

    /// Atomically returns replayed items plus either a live receiver, or the
    /// terminal signal this subject already ended with. Guarded by the same
    /// lock as `send`, so nothing can terminate (or emit) in the gap between
    /// the check and subscribing.
    pub(crate) fn snapshot(&self) -> SourceSnapshot<T, E> {
        let state = self.state.lock().expect("ferx: state mutex poisoned");
        let replay = P::replay(&state.buffer);
        match &state.terminal {
            Some(terminal) => SourceSnapshot::Terminated {
                replay,
                terminal: terminal.clone(),
            },
            None => SourceSnapshot::Live {
                replay,
                receiver: self.sender.subscribe(),
            },
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
    /// Any items the policy replays are delivered first, in order.
    pub fn subscribe_async<F, Fut>(&self, handler: F) -> Subscription
    where
        F: Fn(Signal<T, E>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        match self.snapshot() {
            SourceSnapshot::Terminated { replay, terminal } => {
                let span = tracing::debug_span!("ferx_subscribe", subject = self.id);
                let fut = async move {
                    for value in replay {
                        handler(Signal::Next(value)).await;
                    }
                    handler(terminal.to_signal()).await;
                };
                let abort = spawn_supervised(fut.instrument(span));
                Subscription {
                    cancel: None,
                    abort: Some(abort),
                }
            }
            SourceSnapshot::Live { replay, receiver } => {
                let mut rx = receiver;
                let (cancel_tx, mut cancel_rx) = oneshot::channel();
                let span = tracing::debug_span!("ferx_subscribe", subject = self.id);

                let fut = async move {
                    for value in replay {
                        handler(Signal::Next(value)).await;
                    }
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
                };

                let abort = spawn_supervised(fut.instrument(span));

                Subscription {
                    cancel: Some(cancel_tx),
                    abort: Some(abort),
                }
            }
        }
    }
}
