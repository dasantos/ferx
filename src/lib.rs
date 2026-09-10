//! Reactive Extensions for async Rust, built on `tokio`.
//!
//! `ferx` provides a hot [`Subject`], a cold [`Observable`], an RAII
//! [`Subscription`], and a set of composable operators
//! (`map`/`filter`/`take`/`take_while`/`skip`/`merge`).
//!
//! # Example
//!
//! ```rust
//! use std::num::NonZeroUsize;
//!
//! use ferx::{Signal, Subject};
//!
//! #[tokio::main]
//! async fn main() {
//!     let capacity = NonZeroUsize::new(16).unwrap();
//!     let source: Subject<i32, String> = Subject::new(capacity);
//!     let evens = source.filter(capacity, |n| n % 2 == 0);
//!
//!     let sub = evens.subscribe(|signal| match signal {
//!         Signal::Next(n) => println!("next: {n}"),
//!         Signal::Error(e) => println!("error: {e}"),
//!         Signal::Complete => println!("complete"),
//!     });
//!
//!     source.next(1).ok();
//!     source.next(2).ok();
//!     source.complete().ok();
//!
//!     tokio::time::sleep(std::time::Duration::from_millis(50)).await;
//!     drop(sub);
//! }
//! ```

#![warn(missing_docs)]

mod error;
mod observable;
mod operators;
mod signal;
mod subject;
mod subscription;

pub use error::SendError;
pub use observable::Observable;
pub use signal::{ChannelSignal, Signal};
// Hidden: required for `Subject`'s defaulted policy parameter to be
// nameable, not yet a supported extension for downstream crates.
#[doc(hidden)]
pub use signal::{NoReplay, SubjectPolicy};
pub use subject::Subject;
pub use subscription::Subscription;
