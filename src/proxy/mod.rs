//! Expose proxy models and HTTP/TLS clients.

/// HTTP/TLS plumbing shared by fetching and validation.
pub mod client;
/// Validated endpoint models passed through the pipeline.
pub mod models;
