use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tracing::Instrument;

use crate::signal::{ChannelSignal, TerminalState, is_terminal, run_guarded};
use crate::subject::{SourceSnapshot, Subject};

/// Spawns a task that immediately forwards an already-recorded terminal
/// signal into `out`, for operator stages built on an already-ended
/// source. Goes through `Subject::send` (not the raw channel) so `out`'s
/// own terminal-state lock stays in sync with what's on the channel.
fn spawn_immediate_terminal<T, E>(
    op: &'static str,
    out: Subject<T, E>,
    terminal: TerminalState<E>,
) -> JoinHandle<()>
where
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    let span = tracing::debug_span!("ferx_operator", op, subject = out.id);
    tokio::spawn(
        async move {
            let _ = out.send(terminal.to_channel_signal());
        }
        .instrument(span),
    )
}

impl<T, E> Subject<T, E>
where
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    /// Transforms each `OnNext` value with `f`. `OnError`/`OnCompleted` pass
    /// through unchanged. `capacity` sizes the output subject's channel.
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
    ///     let capacity = NonZeroUsize::new(16).unwrap();
    ///     let source: Subject<i32, String> = Subject::new(capacity);
    ///     let doubled = source.map(capacity, |n| n * 2);
    ///
    ///     let sub = doubled.subscribe(|signal| println!("{signal:?}"));
    ///     source.next(21).ok(); // delivers Signal::Next(42)
    ///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    ///     drop(sub);
    /// }
    /// ```
    #[must_use]
    pub fn map<U, F>(&self, capacity: NonZeroUsize, f: F) -> Subject<U, E>
    where
        U: Clone + Send + 'static,
        F: Fn(T) -> U + Send + 'static,
    {
        let mut out = Subject::new(capacity);

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated { terminal, .. } => {
                let handle = spawn_immediate_terminal("map", out.clone(), terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live { receiver, .. } => receiver,
        };
        let mut stream = BroadcastStream::new(rx);

        let span = tracing::debug_span!(
            "ferx_operator",
            op = "map",
            subject = out.id,
            source = self.id
        );
        let out_task = out.clone();
        let fut = async move {
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: map source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let channel_state = match item {
                    Ok(channel_state) => channel_state,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: map source lagged; stopping this stage");
                        break;
                    }
                };
                let mapped: ChannelSignal<U, E> = match channel_state {
                    Some(Ok(t)) => match run_guarded("map", || f(t)) {
                        Some(u) => Some(Ok(u)),
                        None => break, // closure panicked, already logged
                    },
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                };
                let terminal = is_terminal(&mapped);
                // A send error here just means no one's listening right now
                // (a normal state for a hot multicast); only a genuine
                // terminal signal ends this stage.
                let _ = out_task.send(mapped);
                if terminal {
                    break;
                }
            }
        };
        out.attach_task(tokio::spawn(fut.instrument(span)));

        out
    }

    /// Keeps only `OnNext` values matching `predicate`. `OnError`/`OnCompleted`
    /// pass through unchanged. `capacity` sizes the output subject's channel.
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
    ///     let capacity = NonZeroUsize::new(16).unwrap();
    ///     let source: Subject<i32, String> = Subject::new(capacity);
    ///     let evens = source.filter(capacity, |n| n % 2 == 0);
    ///
    ///     let sub = evens.subscribe(|signal| println!("{signal:?}"));
    ///     source.next(1).ok(); // filtered out
    ///     source.next(2).ok(); // delivers Signal::Next(2)
    ///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    ///     drop(sub);
    /// }
    /// ```
    #[must_use]
    pub fn filter<F>(&self, capacity: NonZeroUsize, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + 'static,
    {
        let mut out = Self::new(capacity);

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated { terminal, .. } => {
                let handle = spawn_immediate_terminal("filter", out.clone(), terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live { receiver, .. } => receiver,
        };
        let mut stream = BroadcastStream::new(rx);

        let span = tracing::debug_span!(
            "ferx_operator",
            op = "filter",
            subject = out.id,
            source = self.id
        );
        let out_task = out.clone();
        let fut = async move {
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: filter source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let channel_state = match item {
                    Ok(channel_state) => channel_state,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: filter source lagged; stopping this stage");
                        break;
                    }
                };
                match channel_state {
                    Some(Ok(t)) => match run_guarded("filter", || predicate(&t)) {
                        Some(true) => {
                            let _ = out_task.send(Some(Ok(t)));
                        }
                        Some(false) => {} // filtered out, do not forward
                        None => break,    // predicate panicked, already logged
                    },
                    other => {
                        let _ = out_task.send(other);
                        break;
                    }
                }
            }
        };
        out.attach_task(tokio::spawn(fut.instrument(span)));

        out
    }

    /// Forwards the first `n` `OnNext` values, then synthesizes `OnCompleted`
    /// and stops, regardless of whether the source keeps emitting.
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
    ///     let capacity = NonZeroUsize::new(16).unwrap();
    ///     let source: Subject<i32, String> = Subject::new(capacity);
    ///     let first_two = source.take(capacity, 2);
    ///
    ///     let sub = first_two.subscribe(|signal| println!("{signal:?}"));
    ///     source.next(1).ok();
    ///     source.next(2).ok();
    ///     source.next(3).ok(); // never delivered; first_two already completed
    ///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    ///     drop(sub);
    /// }
    /// ```
    #[must_use]
    pub fn take(&self, capacity: NonZeroUsize, n: usize) -> Self {
        let mut out = Self::new(capacity);

        if n == 0 {
            let span = tracing::debug_span!(
                "ferx_operator",
                op = "take",
                subject = out.id,
                source = self.id
            );
            let out_task = out.clone();
            let handle = tokio::spawn(
                async move {
                    let _ = out_task.send(None);
                }
                .instrument(span),
            );
            out.attach_task(handle);
            return out;
        }

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated { terminal, .. } => {
                let handle = spawn_immediate_terminal("take", out.clone(), terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live { receiver, .. } => receiver,
        };
        let mut stream = BroadcastStream::new(rx);
        let span = tracing::debug_span!(
            "ferx_operator",
            op = "take",
            subject = out.id,
            source = self.id
        );
        let out_task = out.clone();
        let fut = async move {
            let mut remaining = n;
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: take source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let channel_state = match item {
                    Ok(channel_state) => channel_state,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: take source lagged; stopping this stage");
                        break;
                    }
                };
                match channel_state {
                    Some(Ok(t)) => {
                        remaining -= 1;
                        let done = remaining == 0;
                        let _ = out_task.send(Some(Ok(t)));
                        if done {
                            let _ = out_task.send(None);
                            break;
                        }
                    }
                    other => {
                        let _ = out_task.send(other);
                        break;
                    }
                }
            }
        };
        out.attach_task(tokio::spawn(fut.instrument(span)));

        out
    }

    /// Forwards `OnNext` values while `predicate` holds; on the first value
    /// that fails it, synthesizes `OnCompleted` (without forwarding that
    /// value) and stops. `OnError`/`OnCompleted` from the source pass
    /// through unchanged.
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
    ///     let capacity = NonZeroUsize::new(16).unwrap();
    ///     let source: Subject<i32, String> = Subject::new(capacity);
    ///     let until_ten = source.take_while(capacity, |n| *n < 10);
    ///
    ///     let sub = until_ten.subscribe(|signal| println!("{signal:?}"));
    ///     source.next(5).ok(); // delivers Signal::Next(5)
    ///     source.next(10).ok(); // fails predicate; completes instead
    ///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    ///     drop(sub);
    /// }
    /// ```
    #[must_use]
    pub fn take_while<F>(&self, capacity: NonZeroUsize, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + 'static,
    {
        let mut out = Self::new(capacity);

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated { terminal, .. } => {
                let handle = spawn_immediate_terminal("take_while", out.clone(), terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live { receiver, .. } => receiver,
        };
        let mut stream = BroadcastStream::new(rx);

        let span = tracing::debug_span!(
            "ferx_operator",
            op = "take_while",
            subject = out.id,
            source = self.id
        );
        let out_task = out.clone();
        let fut = async move {
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: take_while source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let channel_state = match item {
                    Ok(channel_state) => channel_state,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: take_while source lagged; stopping this stage");
                        break;
                    }
                };
                match channel_state {
                    Some(Ok(t)) => match run_guarded("take_while", || predicate(&t)) {
                        Some(true) => {
                            let _ = out_task.send(Some(Ok(t)));
                        }
                        Some(false) => {
                            let _ = out_task.send(None);
                            break;
                        }
                        None => break, // predicate panicked, already logged
                    },
                    other => {
                        let _ = out_task.send(other);
                        break;
                    }
                }
            }
        };
        out.attach_task(tokio::spawn(fut.instrument(span)));

        out
    }

    /// Drops the first `n` `OnNext` values, then forwards the rest.
    /// `OnError`/`OnCompleted` from the source pass through unchanged.
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
    ///     let capacity = NonZeroUsize::new(16).unwrap();
    ///     let source: Subject<i32, String> = Subject::new(capacity);
    ///     let after_first_two = source.skip(capacity, 2);
    ///
    ///     let sub = after_first_two.subscribe(|signal| println!("{signal:?}"));
    ///     source.next(1).ok(); // dropped
    ///     source.next(2).ok(); // dropped
    ///     source.next(3).ok(); // delivers Signal::Next(3)
    ///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    ///     drop(sub);
    /// }
    /// ```
    #[must_use]
    pub fn skip(&self, capacity: NonZeroUsize, n: usize) -> Self {
        let mut out = Self::new(capacity);

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated { terminal, .. } => {
                let handle = spawn_immediate_terminal("skip", out.clone(), terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live { receiver, .. } => receiver,
        };
        let mut stream = BroadcastStream::new(rx);

        let span = tracing::debug_span!(
            "ferx_operator",
            op = "skip",
            subject = out.id,
            source = self.id
        );
        let out_task = out.clone();
        let fut = async move {
            let mut remaining = n;
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: skip source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let channel_state = match item {
                    Ok(channel_state) => channel_state,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: skip source lagged; stopping this stage");
                        break;
                    }
                };
                match channel_state {
                    Some(Ok(t)) => {
                        if remaining > 0 {
                            remaining -= 1;
                            continue;
                        }
                        let _ = out_task.send(Some(Ok(t)));
                    }
                    other => {
                        let _ = out_task.send(other);
                        break;
                    }
                }
            }
        };
        out.attach_task(tokio::spawn(fut.instrument(span)));

        out
    }

    /// Combines this subject with `other`, forwarding `OnNext` from both.
    /// The output completes once *both* sources have completed; an `OnError`
    /// from either source is forwarded immediately and ends the merge.
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
    ///     let capacity = NonZeroUsize::new(16).unwrap();
    ///     let a: Subject<i32, String> = Subject::new(capacity);
    ///     let b: Subject<i32, String> = Subject::new(capacity);
    ///     let combined = a.merge(capacity, &b);
    ///
    ///     let sub = combined.subscribe(|signal| println!("{signal:?}"));
    ///     a.next(1).ok();
    ///     b.next(2).ok();
    ///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    ///     drop(sub);
    /// }
    /// ```
    #[must_use]
    pub fn merge(&self, capacity: NonZeroUsize, other: &Self) -> Self {
        let mut out = Self::new(capacity);
        // Counts how many of the two source arms have completed normally.
        let remaining = Arc::new(AtomicUsize::new(2));
        // Reliable cross-arm cancellation: unlike a bare `AtomicBool`, a
        // `watch` receiver observes a change even if it starts waiting on
        // it only after the change happened, so the "losing" arm can't get
        // stuck blocked on `recv()` forever after the other arm errors.
        let (terminated_tx, terminated_rx) = watch::channel(false);

        let h1 = spawn_merge_arm(
            tracing::debug_span!(
                "ferx_operator",
                op = "merge",
                subject = out.id,
                source = self.id
            ),
            self.snapshot(),
            out.clone(),
            remaining.clone(),
            terminated_rx.clone(),
            terminated_tx.clone(),
        );
        let h2 = spawn_merge_arm(
            tracing::debug_span!(
                "ferx_operator",
                op = "merge",
                subject = out.id,
                source = other.id
            ),
            other.snapshot(),
            out.clone(),
            remaining,
            terminated_rx,
            terminated_tx,
        );
        out.attach_tasks(vec![h1, h2]);

        out
    }
}

fn spawn_merge_arm<T, E>(
    span: tracing::Span,
    snapshot: SourceSnapshot<T, E>,
    out: Subject<T, E>,
    remaining: Arc<AtomicUsize>,
    mut terminated_rx: watch::Receiver<bool>,
    terminated_tx: watch::Sender<bool>,
) -> JoinHandle<()>
where
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    tokio::spawn(
        async move {
        let mut rx = match snapshot {
            SourceSnapshot::Live { receiver, .. } => receiver,
            // The source already ended before this merge arm started; settle
            // this arm's contribution immediately instead of listening forever.
            SourceSnapshot::Terminated {
                terminal: TerminalState::Error(e),
                ..
            } => {
                let is_first = terminated_tx.send_if_modified(|done| {
                    if *done {
                        false
                    } else {
                        *done = true;
                        true
                    }
                });
                if is_first {
                    let _ = out.send(Some(Err(e)));
                }
                return;
            }
            SourceSnapshot::Terminated {
                terminal: TerminalState::Complete,
                ..
            } => {
                // Release (not AcqRel): nothing from the sibling arm needs to
                // be observed here, this is just "give up my share of the count".
                if remaining.fetch_sub(1, Ordering::Release) == 1 {
                    let _ = out.send(None);
                }
                return;
            }
        };
        loop {
            tokio::select! {
                biased;
                // The sibling arm (or ourselves) ended the merge; stop
                // waiting on our own source, which may never emit again.
                _ = terminated_rx.changed() => break,
                msg = rx.recv() => {
                    match msg {
                        Ok(Some(Ok(t))) => {
                            let _ = out.send(Some(Ok(t)));
                        }
                        Ok(Some(Err(e))) => {
                            // Only the first arm to observe an error forwards it.
                            let is_first = terminated_tx.send_if_modified(|done| {
                                if *done {
                                    false
                                } else {
                                    *done = true;
                                    true
                                }
                            });
                            if is_first {
                                let _ = out.send(Some(Err(e)));
                            }
                            break;
                        }
                        Ok(None) => {
                            // Only the arm that observes the count drop to 0 signals completion.
                            // Release (not AcqRel): same reasoning as the Terminated{Complete} arm above.
                            if remaining.fetch_sub(1, Ordering::Release) == 1 {
                                let _ = out.send(None);
                            }
                            break;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(lagged = n, "ferx: merge source lagged; stopping this arm");
                            break;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!(
                                "ferx: merge source closed without an explicit terminal signal; stopping this arm"
                            );
                            break;
                        }
                    }
                }
            }
        }
        }
        .instrument(span),
    )
}
