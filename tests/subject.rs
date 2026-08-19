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
