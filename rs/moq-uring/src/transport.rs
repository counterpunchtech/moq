//! Adapt backend poll sessions to moq-net without changing thread affinity.
mod adapter;
mod held;
pub use adapter::{Error, HeldRecvStream, HeldSession, RecvStream, SendStream, Session};
