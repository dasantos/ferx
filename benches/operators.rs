//! Benchmarks `Subject::next`'s own hot path (the `Mutex`-guarded
//! terminal-state check plus the send itself). Deliberately synchronous and
//! runtime-free: no `tokio::spawn`, no polling, no `subscribe()`, so the
//! measured time is ferx's own logic, not tokio's scheduler.

use std::num::NonZeroUsize;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ferx::Subject;

fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn next_no_receivers(c: &mut Criterion) {
    let subject: Subject<i32, String> = Subject::new(cap(1024));
    c.bench_function("next_no_receivers", |b| {
        b.iter(|| {
            subject.next(black_box(1)).ok();
        });
    });
}

fn next_with_a_receiver(c: &mut Criterion) {
    let subject: Subject<i32, String> = Subject::new(cap(1024));
    let _rx = subject.sender().subscribe();
    c.bench_function("next_with_a_receiver", |b| {
        b.iter(|| {
            subject.next(black_box(1)).ok();
        });
    });
}

fn next_after_termination(c: &mut Criterion) {
    let subject: Subject<i32, String> = Subject::new(cap(16));
    subject.complete().ok();
    c.bench_function("next_after_termination", |b| {
        b.iter(|| {
            subject.next(black_box(1)).ok();
        });
    });
}

criterion_group!(
    benches,
    next_no_receivers,
    next_with_a_receiver,
    next_after_termination
);
criterion_main!(benches);
