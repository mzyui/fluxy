//! Negotiate plain HTTP connections.

use super::NegotiatorTrait;

/// Negotiates plain HTTP proxy connections.
pub struct HttpNegotiator;

impl NegotiatorTrait for HttpNegotiator {}
