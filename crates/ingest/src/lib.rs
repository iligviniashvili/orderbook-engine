//! Market-data ingestion: exchange feed in, reconstructed level-2 books and
//! durable trade history out.
//!
//! The layering is deliberate. [`protocol`] is the only module that knows the
//! exchange's wire format, and [`book`] is a pure state machine with no I/O
//! and no clock.

pub mod book;
pub mod error;
pub mod protocol;

pub use book::{Applied, OrderBook, SequenceError};
pub use error::{Error, Result};
pub use protocol::{DepthDelta, DepthSnapshot, FeedEvent, TradeEvent};
