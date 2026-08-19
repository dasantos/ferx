/// The wire encoding used internally, and on the channel returned by
/// [`Subject::sender`](crate::Subject::sender).
///
/// - `Some(Ok(t))`: `OnNext`, deliver `t`.
/// - `Some(Err(e))`: `OnError`, terminal.
/// - `None`: `OnCompleted`, terminal.
pub type WireSignal<T, E> = Option<Result<T, E>>;

/// Returns whether a wire signal ends the sequence (`OnError` or `OnCompleted`).
pub(crate) fn is_terminal<T, E>(wire: &WireSignal<T, E>) -> bool {
    matches!(wire, None | Some(Err(_)))
}

/// A `Subject`'s recorded terminal state: it errored, or it completed.
/// Never represents `OnNext`; unlike `WireSignal`, this can't be "not yet
/// terminal", so a `Subject` can store "have I already ended, and how?"
/// without an extra `Option` layer around a value that might be a next item.
#[derive(Clone)]
pub(crate) enum Terminal<E> {
    Error(E),
    Complete,
}

impl<E: Clone> Terminal<E> {
    pub(crate) fn to_wire<T>(&self) -> WireSignal<T, E> {
        match self {
            Terminal::Error(e) => Some(Err(e.clone())),
            Terminal::Complete => None,
        }
    }

    pub(crate) fn to_signal<T>(&self) -> Signal<T, E> {
        match self {
            Terminal::Error(e) => Signal::Error(e.clone()),
            Terminal::Complete => Signal::Complete,
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
    /// Delivers the next item in the sequence.
    Next(T),
    /// Terminates the sequence with an error.
    Error(E),
    /// Terminates the sequence successfully.
    Complete,
}

impl<T, E> From<WireSignal<T, E>> for Signal<T, E> {
    fn from(wire: WireSignal<T, E>) -> Self {
        match wire {
            Some(Ok(t)) => Signal::Next(t),
            Some(Err(e)) => Signal::Error(e),
            None => Signal::Complete,
        }
    }
}
