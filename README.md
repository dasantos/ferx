# FeRx

[![codecov](https://codecov.io/gh/dasantos/ferx/graph/badge.svg)](https://codecov.io/gh/dasantos/ferx)
[![MSRV](https://img.shields.io/crates/msrv/ferx)](https://releases.rs/docs/1.85.0/)
[Benchmarks](https://dasantos.github.io/ferx/dev/bench/)

Reactive Extensions for async Rust: a (hot) observable (`Subject`) with operator composition, typed errors, and deterministic subscription cleanup, built on `tokio::sync::broadcast` and `futures`/`tokio-stream`.

## Why ferx?

The Rust ecosystem has excellent primitives for async data flow: `futures::Stream` for cold, pull-based sequences, and `tokio::sync::broadcast` for hot, multicast fan-out. Neither covers the full [ReactiveX](https://reactivex.io/) contract on its own: a hot observable with composable operators, typed errors, and deterministic subscription cleanup. Developers coming from Rx in .NET or RxJava either have to adopt a different mental model, or hand-roll operator composition and terminal signal handling on top of `broadcast` themselves. `ferx` is that missing layer: `Subject`/`Observable` implement the `OnNext`/`OnError`/`OnCompleted` contract directly, with `map`/`filter`/`take`/`merge` and friends built on top, the same way on both hot and cold sources.

> **Hot vs. cold**: a *cold* source produces nothing until a subscriber attaches, and each subscriber then gets its own independent run of the whole sequence from the start (like calling a function). A *hot* source produces values on its own schedule regardless of whether anyone is listening; subscribers only see what's emitted while they're subscribed, and a late subscriber misses whatever already happened. `Observable` is cold; `Subject` is hot.

| Characteristic | `futures::Stream` | `tokio::sync::broadcast` | `ferx` |
| -------------- | ----------------- | ------------------------ | ------ |
| Cold, per-subscriber sequence | yes | no | yes (`Observable`) |
| Hot, multicast fan-out | no | yes | yes (`Subject`) |
| Composable operators | yes, via `StreamExt` | no, raw channel | yes, same vocabulary on both |
| Typed terminal error (distinct from the item type) | no, `Item` only | no, `T` only; lag means silent data loss | yes, `Signal::Error(E)` |
| Deterministic unsubscribe | drop the stream, no cascade | drop the receiver, no cascade | RAII `Subscription`, cascades through operator chains |

Reach for plain `futures::Stream`/`tokio-stream` when a single pull-based pipeline is all you need: no multicast, no shared subscription lifecycle. Reach for plain `tokio::sync::broadcast` when you only need to fan a value out to several listeners, with no operators and no typed terminal signal. Reach for `ferx` when you want hot and cold sources to share one operator vocabulary and one subscription/error contract, instead of assembling that yourself on top of `broadcast` and `BroadcastStream` by hand.

**When not to use it:** this is an early, incomplete implementation. Only `map`, `filter`, `take`, `take_while`, `skip`, and `merge` exist so far; there's no `BehaviorSubject`/`ReplaySubject`, no backpressure policies, and no scheduler yet. If your use case is simple enough for a plain `Stream` or a bare `broadcast` channel, that's less to depend on and less to learn than adopting `ferx` at this stage.

## Overview

- **`Subject<T, E>`**: a hot, multicast observable. Values are pushed via `next`/`error`/`complete` and fanned out to every current subscriber.
- **`Observable<T, E>`**: a cold observable. Wraps a factory that produces an independent sequence for each subscriber.
- **`Signal<T, E>`**: the notification a subscriber receives: `Next(T)`, `Error(E)`, or `Complete`.
- **`Subscription`**: an RAII handle; dropping it cancels delivery.
- **`subscribe` / `subscribe_async`**: attach a sync or async handler to a `Subject` or `Observable`.
- **Operators**: `map`, `filter`, `take`, `take_while`, `skip`, `merge`, composable on `Subject` to build a processing pipeline.

## Quick start

```rust
use std::num::NonZeroUsize;

use ferx::{Signal, Subject};

#[tokio::main]
async fn main() {
    let capacity = NonZeroUsize::new(16).unwrap();
    let source: Subject<i32, String> = Subject::new(capacity);

    let evens = source.filter(capacity, |n| n % 2 == 0);
    let doubled = evens.map(capacity, |n| n * 2);

    let sub = doubled.subscribe(|signal| match signal {
        Signal::Next(n) => println!("next: {n}"),
        Signal::Error(e) => println!("error: {e}"),
        Signal::Complete => println!("complete"),
    });

    for n in 1..=5 {
        source.next(n).ok();
    }
    source.complete().ok();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(sub); // unsubscribes, deterministic, no GC needed
}
```

## API guide

### `Subject<T, E>`

A hot source: values are emitted regardless of subscriber count, and late subscribers miss earlier items. Clone it cheaply to share a handle; every clone shares the same underlying channel.

Once a `Subject` has emitted `OnError`/`OnCompleted`, it remembers that: further `next()`/`error()`/`complete()` calls are silently ignored, and any subscriber that subscribes afterward immediately receives that stored terminal signal instead of waiting on a channel that will never deliver it, matching the [Rx `Subject`/`PublishSubject` contract](https://reactivex.io/documentation/subject.html). This only applies to signals sent via `next`/`error`/`complete`; using the raw `sender()` bypasses it.

```rust
let capacity = NonZeroUsize::new(16).unwrap(); // channel capacity, can't be 0
let subject: Subject<i32, String> = Subject::new(capacity);

subject.next(42).ok();               // OnNext
subject.error("boom".to_string()).ok(); // OnError, terminates the sequence
subject.complete().ok();             // OnCompleted, terminates the sequence
```

`next`/`error`/`complete` return `Err(SendError::NoReceivers)` if no subscriber is currently listening.

### Subscribing

```rust
// Sync closure: for simple, non-blocking handlers
let sub = subject.subscribe(|signal| { /* ... */ });

// Async closure: for handlers that need to await
let sub = subject.subscribe_async(|signal| async move {
    // process(signal).await;
});
```

Both return a `#[must_use] Subscription`. Dropping it requests cancellation and aborts the subscriber task; see [Design notes](#design-notes) below.

### Operators

Each operator spawns a background task that reads from the source subject and re-publishes into a new one you get back. Except for `take`/`take_while` (which synthesize their own `OnCompleted`), `OnError`/`OnCompleted` always pass through untouched.

```rust
let doubled: Subject<i32, String> = subject.map(capacity, |n| n * 2);
// e.g. subject emits  1, 2, 3, 4, 5,  then completes
//   -> doubled emits  2, 4, 6, 8, 10, then completes

let evens: Subject<i32, String> = subject.filter(capacity, |n| n % 2 == 0);
// e.g. subject emits 1, 2, 3, 4, 5, then completes
//   -> evens emits      2,    4,    then completes

let first_three: Subject<i32, String> = subject.take(capacity, 3);
// e.g. subject emits     1, 2, 3, 4, 5, then completes
//   -> first_three emits 1, 2, 3,       then completes (synthesized right after the 3rd item)

let until_ten: Subject<i32, String> = subject.take_while(capacity, |n| *n < 10);
// e.g. subject emits   1, 5, 9, 10, 2, then completes
//   -> until_ten emits 1, 5, 9,        then completes (synthesized on the first failing value; 10 isn't forwarded)

let after_first_two: Subject<i32, String> = subject.skip(capacity, 2);
// e.g. subject emits         1, 2, 3, 4, 5, then completes
//   -> after_first_two emits       3, 4, 5, then completes

let combined: Subject<i32, String> = subject.merge(capacity, &other_subject);
// e.g.       subject emits 1, 3       and completes; 
//      other_subject emits 2, 4       and completes
//   -> combined emits      1, 2, 3, 4 (interleaved by arrival order), then completes once both sources have
```

The `capacity` argument sizes the new subject's internal channel; each operator stage allocates its own channel and task. `merge` completes once *both* sources have completed, and forwards an `OnError` from either side immediately.

Each operator also has its own runnable example in the [rustdoc](https://docs.rs/ferx).

### `Observable<T, E>` (cold)

A cold source: the factory you pass to `new` runs once per subscriber, so each one gets its own independent sequence. Unlike `Subject`, `T`/`E` don't need to be `Clone` since there's no fan-out.

```rust
use ferx::Observable;

let observable: Observable<i32, String> =
    Observable::new(|| tokio_stream::iter(vec![Some(Ok(1)), Some(Ok(2)), None]));

let sub = observable.subscribe(|signal| { /* ... */ });
```

`subscribe`/`subscribe_async` work the same as on `Subject`. For direct interop with `futures`/`tokio-stream` combinators, use `to_stream`:

```rust
use tokio_stream::StreamExt;

let signals: Vec<_> = observable.to_stream().collect().await;
```

### `Signal<T, E>`

```rust
pub enum Signal<T, E> {
    Next(T),
    Error(E),
    Complete,
}
```

`Error` and `Complete` are terminal: no further signals are delivered to that subscriber afterward.

## Design notes

- **Cancellation semantics**: `Subscription::drop` sends a cancel signal and then aborts the subscriber's task immediately after. Rust has no `AsyncDrop`, so this is best-effort cooperative cancellation racing a hard abort, not a guaranteed graceful shutdown.
- **Operator task lifetime**: `Subject` operators spawn one or more background tasks tied to the returned `Subject`'s lifetime; once every clone of it is dropped, the task(s) are aborted so they don't outlive their output or keep holding a receiver on the source(s).
- **Cold vs hot**: `Observable`'s factory reruns per subscriber with no shared channel, so there's no lag/backpressure policy to speak of on the cold side; each subscriber simply drives its own stream at its own pace.
- **Lagged subscribers**: if a subscriber falls behind the broadcast channel's capacity, delivery to that subscriber (or operator stage) simply stops. The wire format (`Option<Result<T, E>>`) has no slot for a transport error distinct from your own `E`, so this is not surfaced through `Signal::Error`.
- **Errors**: typed via `thiserror`, `#[non_exhaustive]` so new variants aren't breaking changes. `Subject::new`'s capacity is a `NonZeroUsize`, so a zero capacity is a compile error, not a runtime panic.

## Observability

`ferx` emits structured events via [`tracing`](https://docs.rs/tracing); it never installs a subscriber itself, so wire one up in your application to see them:

```toml
[dependencies]
tracing-subscriber = "0.3"
```

```rust
tracing_subscriber::fmt::init();
```

What gets logged, and at what level:

| Level | Event |
| ----- | ----- |
| `error` | A `map`/`filter`/`take_while`/`subscribe` closure panicked (includes the panic message); that stage stops. Also logged if a `subscribe_async` handler panics (via the spawned task's `JoinError`, message not included). |
| `warn` | A subscriber or operator stage fell behind the channel's capacity (lagged) and stopped receiving. |
| `debug` | A `Subject` (or all its clones) was dropped without an explicit `error()`/`complete()` call, so a stage or subscriber ends with no terminal `Signal`. |

All events are scoped under the `ferx` target, so you can filter on it, e.g. `RUST_LOG=ferx=debug`. Events emitted from a subscriber or operator stage's background task carry a span (`ferx_subscribe`/`ferx_operator`/`ferx_observable_subscribe`) with a `subject`/`subscription` id field, so logs from concurrently running subjects can be told apart.
