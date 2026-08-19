use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::signal::{Terminal, WireSignal, run_guarded};
use crate::subject::{SourceSnapshot, Subject};

/// Spawns a task that immediately forwards an already-recorded terminal
/// signal into `out_sender`, for operator stages built on an already-ended
/// source.
fn spawn_immediate_terminal<T, E>(
    out_sender: broadcast::Sender<WireSignal<T, E>>,
    terminal: Terminal<E>,
) -> JoinHandle<()>
where
    T: Send + 'static,
    E: Clone + Send + 'static,
{
    tokio::spawn(async move {
        let _ = out_sender.send(terminal.to_wire());
    })
}

impl<T, E> Subject<T, E>
where
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    /// Transforms each `OnNext` value with `f`. `OnError`/`OnCompleted` pass
    /// through unchanged. `capacity` sizes the output subject's channel.
    pub fn map<U, F>(&self, capacity: usize, f: F) -> Subject<U, E>
    where
        U: Clone + Send + 'static,
        F: Fn(T) -> U + Send + 'static,
    {
        let mut out = Subject::new(capacity);
        let out_sender = out.sender();

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated(terminal) => {
                let handle = spawn_immediate_terminal(out_sender, terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live(rx) => rx,
        };
        let mut stream = BroadcastStream::new(rx);

        let handle = tokio::spawn(async move {
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: map source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let wire = match item {
                    Ok(wire) => wire,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: map source lagged; stopping this stage");
                        break;
                    }
                };
                let mapped: WireSignal<U, E> = match wire {
                    Some(Ok(t)) => match run_guarded("map", || f(t)) {
                        Some(u) => Some(Ok(u)),
                        None => break, // closure panicked, already logged
                    },
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                };
                let terminal = mapped.is_none() || matches!(mapped, Some(Err(_)));
                if out_sender.send(mapped).is_err() || terminal {
                    break;
                }
            }
        });
        out.attach_task(handle);

        out
    }

    /// Keeps only `OnNext` values matching `predicate`. `OnError`/`OnCompleted`
    /// pass through unchanged. `capacity` sizes the output subject's channel.
    pub fn filter<F>(&self, capacity: usize, predicate: F) -> Subject<T, E>
    where
        F: Fn(&T) -> bool + Send + 'static,
    {
        let mut out = Subject::new(capacity);
        let out_sender = out.sender();

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated(terminal) => {
                let handle = spawn_immediate_terminal(out_sender, terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live(rx) => rx,
        };
        let mut stream = BroadcastStream::new(rx);

        let handle = tokio::spawn(async move {
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: filter source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let wire = match item {
                    Ok(wire) => wire,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: filter source lagged; stopping this stage");
                        break;
                    }
                };
                match wire {
                    Some(Ok(t)) => match run_guarded("filter", || predicate(&t)) {
                        Some(true) => {
                            if out_sender.send(Some(Ok(t))).is_err() {
                                break;
                            }
                        }
                        Some(false) => continue, // filtered out, do not forward
                        None => break,           // predicate panicked, already logged
                    },
                    other => {
                        let _ = out_sender.send(other);
                        break;
                    }
                }
            }
        });
        out.attach_task(handle);

        out
    }

    /// Forwards the first `n` `OnNext` values, then synthesizes `OnCompleted`
    /// and stops, regardless of whether the source keeps emitting.
    pub fn take(&self, capacity: usize, n: usize) -> Subject<T, E> {
        let mut out = Subject::new(capacity);
        let out_sender = out.sender();

        if n == 0 {
            let handle = tokio::spawn(async move {
                let _ = out_sender.send(None);
            });
            out.attach_task(handle);
            return out;
        }

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated(terminal) => {
                let handle = spawn_immediate_terminal(out_sender, terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live(rx) => rx,
        };
        let mut stream = BroadcastStream::new(rx);
        let handle = tokio::spawn(async move {
            let mut remaining = n;
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: take source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let wire = match item {
                    Ok(wire) => wire,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: take source lagged; stopping this stage");
                        break;
                    }
                };
                match wire {
                    Some(Ok(t)) => {
                        remaining -= 1;
                        let done = remaining == 0;
                        if out_sender.send(Some(Ok(t))).is_err() {
                            break;
                        }
                        if done {
                            let _ = out_sender.send(None);
                            break;
                        }
                    }
                    other => {
                        let _ = out_sender.send(other);
                        break;
                    }
                }
            }
        });
        out.attach_task(handle);

        out
    }

    /// Forwards `OnNext` values while `predicate` holds; on the first value
    /// that fails it, synthesizes `OnCompleted` (without forwarding that
    /// value) and stops. `OnError`/`OnCompleted` from the source pass
    /// through unchanged.
    pub fn take_while<F>(&self, capacity: usize, predicate: F) -> Subject<T, E>
    where
        F: Fn(&T) -> bool + Send + 'static,
    {
        let mut out = Subject::new(capacity);
        let out_sender = out.sender();

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated(terminal) => {
                let handle = spawn_immediate_terminal(out_sender, terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live(rx) => rx,
        };
        let mut stream = BroadcastStream::new(rx);

        let handle = tokio::spawn(async move {
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: take_while source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let wire = match item {
                    Ok(wire) => wire,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: take_while source lagged; stopping this stage");
                        break;
                    }
                };
                match wire {
                    Some(Ok(t)) => match run_guarded("take_while", || predicate(&t)) {
                        Some(true) => {
                            if out_sender.send(Some(Ok(t))).is_err() {
                                break;
                            }
                        }
                        Some(false) => {
                            let _ = out_sender.send(None);
                            break;
                        }
                        None => break, // predicate panicked, already logged
                    },
                    other => {
                        let _ = out_sender.send(other);
                        break;
                    }
                }
            }
        });
        out.attach_task(handle);

        out
    }

    /// Drops the first `n` `OnNext` values, then forwards the rest.
    /// `OnError`/`OnCompleted` from the source pass through unchanged.
    pub fn skip(&self, capacity: usize, n: usize) -> Subject<T, E> {
        let mut out = Subject::new(capacity);
        let out_sender = out.sender();

        let rx = match self.snapshot() {
            SourceSnapshot::Terminated(terminal) => {
                let handle = spawn_immediate_terminal(out_sender, terminal);
                out.attach_task(handle);
                return out;
            }
            SourceSnapshot::Live(rx) => rx,
        };
        let mut stream = BroadcastStream::new(rx);

        let handle = tokio::spawn(async move {
            let mut remaining = n;
            loop {
                let Some(item) = stream.next().await else {
                    tracing::debug!(
                        "ferx: skip source closed without an explicit terminal signal; stopping this stage"
                    );
                    break;
                };
                let wire = match item {
                    Ok(wire) => wire,
                    Err(err) => {
                        tracing::warn!(?err, "ferx: skip source lagged; stopping this stage");
                        break;
                    }
                };
                match wire {
                    Some(Ok(t)) => {
                        if remaining > 0 {
                            remaining -= 1;
                            continue;
                        }
                        if out_sender.send(Some(Ok(t))).is_err() {
                            break;
                        }
                    }
                    other => {
                        let _ = out_sender.send(other);
                        break;
                    }
                }
            }
        });
        out.attach_task(handle);

        out
    }

    /// Combines this subject with `other`, forwarding `OnNext` from both.
    /// The output completes once *both* sources have completed; an `OnError`
    /// from either source is forwarded immediately and ends the merge.
    pub fn merge(&self, capacity: usize, other: &Subject<T, E>) -> Subject<T, E> {
        let mut out = Subject::new(capacity);
        let out_sender = out.sender();
        // Counts how many of the two source arms have completed normally.
        let remaining = Arc::new(AtomicUsize::new(2));
        // Reliable cross-arm cancellation: unlike a bare `AtomicBool`, a
        // `watch` receiver observes a change even if it starts waiting on
        // it only after the change happened, so the "losing" arm can't get
        // stuck blocked on `recv()` forever after the other arm errors.
        let (terminated_tx, terminated_rx) = watch::channel(false);

        let h1 = spawn_merge_arm(
            self.snapshot(),
            out_sender.clone(),
            remaining.clone(),
            terminated_rx.clone(),
            terminated_tx.clone(),
        );
        let h2 = spawn_merge_arm(
            other.snapshot(),
            out_sender,
            remaining,
            terminated_rx,
            terminated_tx,
        );
        out.attach_tasks(vec![h1, h2]);

        out
    }
}

fn spawn_merge_arm<T, E>(
    snapshot: SourceSnapshot<T, E>,
    out_sender: broadcast::Sender<WireSignal<T, E>>,
    remaining: Arc<AtomicUsize>,
    mut terminated_rx: watch::Receiver<bool>,
    terminated_tx: watch::Sender<bool>,
) -> JoinHandle<()>
where
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    tokio::spawn(async move {
        let mut rx = match snapshot {
            SourceSnapshot::Live(rx) => rx,
            // The source already ended before this merge arm started; settle
            // this arm's contribution immediately instead of listening forever.
            SourceSnapshot::Terminated(Terminal::Error(e)) => {
                let is_first = terminated_tx.send_if_modified(|done| {
                    if *done {
                        false
                    } else {
                        *done = true;
                        true
                    }
                });
                if is_first {
                    let _ = out_sender.send(Some(Err(e)));
                }
                return;
            }
            SourceSnapshot::Terminated(Terminal::Complete) => {
                if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                    let _ = out_sender.send(None);
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
                            if out_sender.send(Some(Ok(t))).is_err() {
                                break;
                            }
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
                                let _ = out_sender.send(Some(Err(e)));
                            }
                            break;
                        }
                        Ok(None) => {
                            // Only the arm that observes the count drop to 0 signals completion.
                            if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                                let _ = out_sender.send(None);
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
    })
}
