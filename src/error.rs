/// Errors returned by [`Subject`](crate::Subject)'s emitting methods.
///
/// Marked `#[non_exhaustive]`: new variants may be added without a breaking
/// change.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SendError {
    /// No subscriber is currently listening, so the value wasn't delivered.
    #[error("no active receivers for this subject")]
    NoReceivers,
}
