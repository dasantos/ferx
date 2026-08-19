use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ferx::{Observable, Signal};
use tokio_stream::StreamExt;

fn collector<T: Send + 'static>() -> (Arc<Mutex<Vec<T>>>, impl Fn(T) + Send + 'static) {
    let store = Arc::new(Mutex::new(Vec::new()));
    let store_clone = store.clone();
    let push = move |item: T| store_clone.lock().unwrap().push(item);
    (store, push)
}

#[tokio::test]
async fn factory_runs_once_per_subscriber() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = calls.clone();
    let observable: Observable<i32, String> = Observable::new(move || {
        calls_clone.fetch_add(1, Ordering::SeqCst);
        tokio_stream::iter(vec![Some(Ok(1)), Some(Ok(2)), None])
    });

    let (store_a, push_a) = collector();
    let (store_b, push_b) = collector();
    let _sub_a = observable.subscribe(push_a);
    let _sub_b = observable.subscribe(push_b);
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let expected = vec![Signal::Next(1), Signal::Next(2), Signal::Complete];
    assert_eq!(store_a.lock().unwrap().clone(), expected);
    assert_eq!(store_b.lock().unwrap().clone(), expected);
}

#[tokio::test]
async fn error_terminates_the_sequence() {
    let observable: Observable<i32, String> = Observable::new(|| {
        tokio_stream::iter(vec![
            Some(Ok(1)),
            Some(Err("boom".to_string())),
            Some(Ok(2)),
        ])
    });

    let (store, push) = collector();
    let _sub = observable.subscribe(push);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Error("boom".to_string())]
    );
}

#[tokio::test]
async fn to_stream_interops_with_stream_combinators() {
    let observable: Observable<i32, String> =
        Observable::new(|| tokio_stream::iter(vec![Some(Ok(1)), Some(Ok(2)), None]));

    let got: Vec<_> = observable.to_stream().collect().await;

    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Next(2), Signal::Complete]
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_subscription_stops_the_task() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = calls.clone();
    // Each tick genuinely awaits a (virtual) timer instead of spinning, so
    // the stream never starves the runtime and time only moves when we
    // explicitly advance it below; fully deterministic, no real hang risk.
    let observable: Observable<i32, String> = Observable::new(move || {
        let calls_clone = calls_clone.clone();
        futures_util::stream::unfold(calls_clone, |calls_clone| async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            calls_clone.fetch_add(1, Ordering::SeqCst);
            Some((Some(Ok(1)), calls_clone))
        })
    });

    let sub = observable.subscribe(|_: Signal<i32, String>| {});
    tokio::time::advance(Duration::from_millis(5)).await;
    drop(sub);
    let after_drop = calls.load(Ordering::SeqCst);

    tokio::time::advance(Duration::from_millis(50)).await;
    let after_more_time = calls.load(Ordering::SeqCst);

    assert_eq!(
        after_drop, after_more_time,
        "task kept running after Subscription was dropped"
    );
}
