/// The channel signal encoding used internally, and on the channel returned by
/// [`Subject::sender`](crate::Subject::sender).
///
/// - `Some(Ok(t))`: `OnNext`, deliver `t`.
/// - `Some(Err(e))`: `OnError`, terminal.
/// - `None`: `OnCompleted`, terminal.
pub type ChannelSignal<T, E> = Option<Result<T, E>>;

/// Returns whether a channel signal ends the sequence (`OnError` or `OnCompleted`).
pub(crate) fn is_terminal<T, E>(channel_signal: &ChannelSignal<T, E>) -> bool {
    matches!(channel_signal, None | Some(Err(_)))
}

/// A `Subject`'s recorded terminal state: it errored, or it completed.
/// Never represents `OnNext`; unlike `ChannelSignal`, this can't be "not yet
/// terminal", so a `Subject` can store "have I already ended, and how?"
/// without an extra `Option` layer around a value that might be a next item.
#[derive(Clone)]
pub(crate) enum TerminalState<E> {
    Error(E),
    Complete,
}

impl<E: Clone> TerminalState<E> {
    pub(crate) fn to_channel_signal<T>(&self) -> ChannelSignal<T, E> {
        match self {
            TerminalState::Error(e) => Some(Err(e.clone())),
            TerminalState::Complete => None,
        }
    }

    pub(crate) fn to_signal<T>(&self) -> Signal<T, E> {
        match self {
            TerminalState::Error(e) => Signal::Error(e.clone()),
            TerminalState::Complete => Signal::Complete,
        }
    }
}

/// Runs `f`, catching a panic so a bad user closure can't silently kill a
/// background task with no signal at all. Logs via `tracing` and returns
/// `None` on panic; callers should treat that as "stop this stage", since
/// there's no `E` value available to report the failure downstream.
pub(crate) fn run_guarded<R>(op: &'static str, f: impl FnOnce() -> R) -> Option<R> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(value) => Some(value),
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            tracing::error!(
                operator = op,
                panic = message,
                "ferx: user closure panicked; stopping this stage"
            );
            None
        }
    }
}

/// A single notification from a `Subject` or `Observable`.
///
/// A sequence emits zero or more [`Signal::Next`] values, then optionally
/// ends with exactly one of [`Signal::Error`] or [`Signal::Complete`];
/// never both, and never another `Next` afterward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Signal<T, E> {
    /// Delivers (emits) the next item in the sequence.
    Next(T),
    /// Terminates (notifies) the sequence with an error.
    Error(E),
    /// Terminates (notifies) the sequence successfully.
    Complete,
}

impl<T, E> From<ChannelSignal<T, E>> for Signal<T, E> {
    fn from(wire: ChannelSignal<T, E>) -> Self {
        match wire {
            Some(Ok(t)) => Signal::Next(t),
            Some(Err(e)) => Signal::Error(e),
            None => Signal::Complete,
        }
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Determines what a new subscriber sees before joining a `Subject`'s
/// live stream: nothing (`PublishSubject`), the last value (`BehaviorSubject`),
/// everything so far (`ReplaySubject`), and so on. Sealed: [`NoReplay`] is
/// the only implementation for now, matching today's `Subject` behavior.
/// Not yet a supported extension point for downstream crates; hidden from
/// docs because of that, even though it has to be `pub` for `Subject`'s
/// defaulted generic parameter to type-check.
#[doc(hidden)]
pub trait SubjectPolicy<T>: sealed::Sealed {
    /// Per-`Subject` state the policy keeps between emissions.
    type Buffer: Default + Send;

    /// Called once per accepted `OnNext`, before it's broadcast.
    fn record(buffer: &mut Self::Buffer, value: &T);

    /// What to replay to a subscriber joining right now.
    fn replay(buffer: &Self::Buffer) -> Vec<T>;
}

/// No replay: a new subscriber sees nothing until the next live emission.
/// This is `PublishSubject`'s behavior, and `Subject`'s default policy.
#[doc(hidden)]
pub struct NoReplay;

impl sealed::Sealed for NoReplay {}

impl<T: Clone> SubjectPolicy<T> for NoReplay {
    type Buffer = ();

    fn record(_buffer: &mut Self::Buffer, _value: &T) {}

    fn replay(_buffer: &Self::Buffer) -> Vec<T> {
        Vec::new()
    }
}
