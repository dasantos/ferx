use std::sync::{Arc, Mutex};
use std::time::Duration;

use ferx::{Signal, Subject};

fn collector<T: Send + 'static>() -> (Arc<Mutex<Vec<T>>>, impl Fn(T) + Send + 'static) {
    let store = Arc::new(Mutex::new(Vec::new()));
    let store_clone = store.clone();
    let push = move |item: T| store_clone.lock().unwrap().push(item);
    (store, push)
}

#[tokio::test]
async fn subject_delivers_next_and_complete() {
    let subject: Subject<i32, String> = Subject::new(16);
    let (store, push) = collector();
    let _sub = subject.subscribe(push);

    subject.next(1).unwrap();
    subject.next(2).unwrap();
    subject.complete().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Next(2), Signal::Complete,]
    );
}

#[tokio::test]
async fn late_subscriber_immediately_receives_stored_completion() {
    let subject: Subject<i32, String> = Subject::new(16);
    subject.next(1).ok();
    subject.complete().ok();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Subscribing only now, after the subject already completed, must
    // still deliver the terminal signal, per the Rx `Subject` contract.
    let (store, push) = collector();
    let _sub = subject.subscribe(push);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(got, vec![Signal::Complete]);
}

#[tokio::test]
async fn late_subscriber_immediately_receives_stored_error() {
    let subject: Subject<i32, String> = Subject::new(16);
    subject.error("boom".to_string()).ok();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let (store, push) = collector();
    let _sub = subject.subscribe(push);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(got, vec![Signal::Error("boom".to_string())]);
}

#[tokio::test]
async fn next_after_termination_is_ignored() {
    let subject: Subject<i32, String> = Subject::new(16);
    let (store, push) = collector();
    let _sub = subject.subscribe(push);

    subject.complete().unwrap();
    // Contract violation by the caller: must be a silent no-op, not
    // re-delivered to any subscriber.
    subject.next(1).ok();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(got, vec![Signal::Complete]);
}

#[tokio::test]
async fn map_of_an_already_terminated_source_forwards_immediately() {
    let subject: Subject<i32, String> = Subject::new(16);
    subject.error("boom".to_string()).ok();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mapped = subject.map(16, |n| n * 2);
    let (store, push) = collector();
    let _sub = mapped.subscribe(push);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(got, vec![Signal::Error("boom".to_string())]);
}

#[tokio::test]
async fn error_terminates_the_sequence() {
    let subject: Subject<i32, String> = Subject::new(16);
    let (store, push) = collector();
    let _sub = subject.subscribe(push);

    subject.next(1).unwrap();
    subject.error("boom".to_string()).unwrap();
    subject.next(2).unwrap(); // must not be delivered, sequence already ended
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Error("boom".to_string())]
    );
}

#[tokio::test]
async fn map_transforms_next_and_passes_terminal_through() {
    let subject: Subject<i32, String> = Subject::new(16);
    let mapped = subject.map(16, |n| n * 10);
    let (store, push) = collector();
    let _sub = mapped.subscribe(push);

    subject.next(1).unwrap();
    subject.next(2).unwrap();
    subject.complete().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(10), Signal::Next(20), Signal::Complete]
    );
}

#[tokio::test]
async fn filter_drops_non_matching_and_passes_terminal_through() {
    let subject: Subject<i32, String> = Subject::new(16);
    let evens = subject.filter(16, |n| n % 2 == 0);
    let (store, push) = collector();
    let _sub = evens.subscribe(push);

    for n in 1..=4 {
        subject.next(n).unwrap();
    }
    subject.complete().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(2), Signal::Next(4), Signal::Complete]
    );
}

#[tokio::test]
async fn dropping_subscription_stops_further_delivery() {
    let subject: Subject<i32, String> = Subject::new(16);
    let (store, push) = collector();
    let sub = subject.subscribe(push);

    subject.next(1).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(sub);
    tokio::time::sleep(Duration::from_millis(20)).await;

    // No receivers remain after the subscription was dropped, so this may
    // legitimately fail with NoReceivers; the point is nothing is delivered.
    subject.next(2).ok();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(got, vec![Signal::Next(1)]);
}

#[tokio::test]
async fn dropping_mapped_subject_aborts_its_background_task() {
    let subject: Subject<i32, String> = Subject::new(16);
    let mapped = subject.map(16, |n| n * 2);

    assert_eq!(subject.sender().receiver_count(), 1); // map's own receiver
    drop(mapped);
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(subject.sender().receiver_count(), 0);
}

#[test]
#[should_panic(expected = "capacity must be greater than 0")]
fn zero_capacity_panics() {
    let _: Subject<i32, String> = Subject::new(0);
}

#[tokio::test]
async fn take_stops_after_n_and_synthesizes_complete() {
    let subject: Subject<i32, String> = Subject::new(16);
    let taken = subject.take(16, 2);
    let (store, push) = collector();
    let _sub = taken.subscribe(push);

    for n in 1..=5 {
        subject.next(n).unwrap();
    }
    subject.complete().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Next(2), Signal::Complete]
    );
}

#[tokio::test]
async fn take_zero_completes_immediately() {
    let subject: Subject<i32, String> = Subject::new(16);
    let taken = subject.take(16, 0);
    let (store, push) = collector();
    let _sub = taken.subscribe(push);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(got, vec![Signal::Complete]);
}

#[tokio::test]
async fn take_while_stops_at_first_failing_value() {
    let subject: Subject<i32, String> = Subject::new(16);
    let taken = subject.take_while(16, |n| *n < 3);
    let (store, push) = collector();
    let _sub = taken.subscribe(push);

    for n in 1..=5 {
        subject.next(n).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Next(2), Signal::Complete]
    );
}

#[tokio::test]
async fn skip_drops_first_n_and_forwards_the_rest() {
    let subject: Subject<i32, String> = Subject::new(16);
    let skipped = subject.skip(16, 2);
    let (store, push) = collector();
    let _sub = skipped.subscribe(push);

    for n in 1..=4 {
        subject.next(n).unwrap();
    }
    subject.complete().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(3), Signal::Next(4), Signal::Complete]
    );
}

#[tokio::test]
async fn merge_forwards_both_sources_and_completes_after_both() {
    let a: Subject<i32, String> = Subject::new(16);
    let b: Subject<i32, String> = Subject::new(16);
    let merged = a.merge(16, &b);
    let (store, push) = collector();
    let _sub = merged.subscribe(push);

    a.next(1).unwrap();
    b.next(2).unwrap();
    a.complete().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // b hasn't completed yet; the merge must not complete early.
    assert_eq!(store.lock().unwrap().len(), 2);

    b.complete().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Next(2), Signal::Complete]
    );
}

#[tokio::test]
async fn merge_forwards_error_from_either_side_immediately() {
    let a: Subject<i32, String> = Subject::new(16);
    let b: Subject<i32, String> = Subject::new(16);
    let merged = a.merge(16, &b);
    let (store, push) = collector();
    let _sub = merged.subscribe(push);

    a.next(1).unwrap();
    b.error("boom".to_string()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // The `a` arm has already torn itself down in response to `b`'s error,
    // so it may legitimately have no receivers left by now.
    a.next(2).ok(); // must not be delivered, merge already ended
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![Signal::Next(1), Signal::Error("boom".to_string())]
    );
}

#[tokio::test]
async fn merge_losing_arm_stops_listening_after_error() {
    let a: Subject<i32, String> = Subject::new(16);
    let b: Subject<i32, String> = Subject::new(16);
    let merged = a.merge(16, &b);
    let _sub = merged.subscribe(|_: Signal<i32, String>| {});

    b.error("boom".to_string()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // The `a` arm must tear itself down promptly instead of blocking on
    // `a`'s receiver forever after `b` errors out the merge.
    assert_eq!(a.sender().receiver_count(), 0);
}

#[tokio::test]
async fn map_survives_a_panicking_closure() {
    let subject: Subject<i32, String> = Subject::new(16);
    let mapped = subject.map(16, |n| {
        if n == 2 {
            panic!("boom");
        }
        n * 10
    });
    let (store, push) = collector();
    let _sub = mapped.subscribe(push);

    subject.next(1).unwrap();
    subject.next(2).unwrap(); // panics inside the closure, must not crash the process
    subject.next(3).unwrap(); // must not be delivered, the stage stopped
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert_eq!(got, vec![Signal::Next(10)]);
}

#[tokio::test]
async fn lagged_subscriber_stops_without_hanging() {
    let subject: Subject<i32, String> = Subject::new(1);
    let (store, push) = collector();
    let _sub = subject.subscribe(push);

    // The subscriber task is spawned but never polled until we `.await`
    // below, so on this single-threaded test runtime these three sends all
    // land before it ever calls `recv()`. With capacity 1, that guarantees
    // it lags on its very first receive.
    subject.next(1).unwrap();
    subject.next(2).unwrap();
    subject.next(3).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Lagged is terminal: the subscriber gets nothing (not even a stale
    // value), and its task ends promptly instead of hanging.
    let got = store.lock().unwrap().clone();
    assert!(
        got.is_empty(),
        "lagged subscriber should receive nothing, got {got:?}"
    );
}

#[tokio::test]
async fn lagged_map_source_stops_that_stage_without_hanging() {
    let subject: Subject<i32, String> = Subject::new(1);
    let mapped = subject.map(1, |n| n * 2);
    let (store, push) = collector();
    let _sub = mapped.subscribe(push);

    // Same reasoning as `lagged_subscriber_stops_without_hanging`, but for
    // the `map` stage's own receiver on `subject`.
    subject.next(1).unwrap();
    subject.next(2).unwrap();
    subject.next(3).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let got = store.lock().unwrap().clone();
    assert!(
        got.is_empty(),
        "lagged map stage should forward nothing, got {got:?}"
    );
}

#[tokio::test]
async fn subscribe_async_panic_does_not_crash_the_process() {
    let subject: Subject<i32, String> = Subject::new(16);
    let sub = subject.subscribe_async(|signal| async move {
        if let Signal::Next(2) = signal {
            panic!("boom");
        }
    });

    subject.next(1).unwrap();
    subject.next(2).unwrap(); // panics inside the async handler
    tokio::time::sleep(Duration::from_millis(20)).await;

    // The panic is contained to that task (logged via the supervisor in
    // spawn_supervised); reaching this point at all is the assertion.
    drop(sub);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_emit_subscribe_and_terminate_is_race_free() {
    // Not loom: ferx's races live in tokio primitives (broadcast, watch,
    // select!), which loom doesn't model. This stress-tests the same
    // Mutex-guarded snapshot/send window with real OS-thread parallelism
    // instead, across many iterations, checking the Rx grammar
    // (Next* (Error | Complete)?) holds for every subscriber every time.
    for _ in 0..100 {
        let subject: Subject<i32, String> = Subject::new(64);
        let mut tasks = tokio::task::JoinSet::new();

        for i in 0..8 {
            let subject = subject.clone();
            tasks.spawn(async move {
                subject.next(i).ok();
            });
        }
        let terminator = subject.clone();
        tasks.spawn(async move {
            terminator.complete().ok();
        });

        let mut subscribers = Vec::new();
        for _ in 0..4 {
            let (store, push) = collector();
            let sub = subject.subscribe(push);
            subscribers.push((store, sub));
        }

        while tasks.join_next().await.is_some() {}
        tokio::time::sleep(Duration::from_millis(10)).await;

        for (store, _sub) in &subscribers {
            let got = store.lock().unwrap().clone();
            let terminal_count = got
                .iter()
                .filter(|s| matches!(s, Signal::Complete | Signal::Error(_)))
                .count();
            assert!(
                terminal_count <= 1,
                "saw multiple terminal signals: {got:?}"
            );
            if let Some(pos) = got
                .iter()
                .position(|s| matches!(s, Signal::Complete | Signal::Error(_)))
            {
                assert_eq!(pos, got.len() - 1, "terminal signal wasn't last: {got:?}");
            }
        }
    }
}
