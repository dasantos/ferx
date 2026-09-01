use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::stream::unfold;
use tokio::sync::oneshot;
use tokio_stream::{Stream, StreamExt};

use crate::signal::{ChannelSignal, Signal, is_terminal, run_guarded};
use crate::subscription::{Subscription, spawn_supervised};

type BoxChannelStream<T, E> = Pin<Box<dyn Stream<Item = ChannelSignal<T, E>> + Send>>;

/// A cold observable: the factory runs once per subscriber, giving each its
/// own independent sequence from the start. Unlike [`Subject`](crate::Subject),
/// `T`/`E` don't need to be `Clone`. There's no fan-out to multiple
/// receivers.
///
/// # Examples
///
/// ```rust
/// use ferx::Observable;
///
/// #[tokio::main]
/// async fn main() {
///   let observable: Observable<i32, String> =
///     Observable::new(|| tokio_stream::iter(vec![Some(Ok(1)), Some(Ok(2)), None]));
///
///   let sub = observable.subscribe(|signal| println!("{signal:?}"));
///   tokio::time::sleep(std::time::Duration::from_millis(20)).await;
///   drop(sub);
/// }
/// ```
pub struct Observable<T, E> {
    factory: Arc<dyn Fn() -> BoxChannelStream<T, E> + Send + Sync>,
}

impl<T, E> Clone for Observable<T, E> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
        }
    }
}

impl<T, E> Observable<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    /// Wraps a factory that produces a fresh [`ChannelSignal`] stream for
    /// each subscriber.
    pub fn new<F, S>(factory: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: Stream<Item = ChannelSignal<T, E>> + Send + 'static,
    {
        Self {
            factory: Arc::new(move || Box::pin(factory())),
        }
    }

    /// Subscribes with a synchronous handler, running the factory once.
    pub fn subscribe<H>(&self, handler: H) -> Subscription
    where
        H: Fn(Signal<T, E>) + Send + 'static,
    {
        self.subscribe_async(move |signal| {
            run_guarded("subscribe", || handler(signal));
            std::future::ready(())
        })
    }

    /// Subscribes with an async handler, running the factory once.
    pub fn subscribe_async<H, Fut>(&self, handler: H) -> Subscription
    where
        H: Fn(Signal<T, E>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut stream = (self.factory)();
        let (cancel_tx, mut cancel_rx) = oneshot::channel();

        let abort = spawn_supervised(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cancel_rx => break,
                    item = stream.next() => {
                        match item {
                            Some(channel_state) => {
                                let terminal = is_terminal(&channel_state);
                                handler(Signal::from(channel_state)).await;
                                if terminal {
                                    break;
                                }
                            }
                            // Stream ended without an explicit OnCompleted.
                            None => break,
                        }
                    }
                }
            }
        });

        Subscription {
            cancel: Some(cancel_tx),
            abort: Some(abort),
        }
    }

    /// Runs the factory once and exposes the result as a plain `Signal`
    /// stream (ending after the first `OnError`/`OnCompleted`), for direct
    /// interop with `futures`/`tokio-stream` combinators.
    pub fn to_stream(&self) -> impl Stream<Item = Signal<T, E>> {
        let inner = (self.factory)();
        unfold(Some(inner), |state| async move {
            let mut inner = state?;
            let channel_state = inner.next().await?;
            let terminal = is_terminal(&channel_state);
            let next_state = if terminal { None } else { Some(inner) };
            Some((Signal::from(channel_state), next_state))
        })
    }
}
