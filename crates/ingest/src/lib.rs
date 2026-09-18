//! Market-data ingestion: exchange feed in, reconstructed level-2 books and
//! durable trade history out.
//!
//! The layering is deliberate. [`protocol`] is the only module that knows the
//! exchange's wire format; [`book`] is a pure state machine with no I/O and no
//! clock; the transport, the snapshot source and the storage sink are all
//! traits, so what sits above them can be tested with nothing running.

pub mod backoff;
pub mod book;
pub mod error;
pub mod feed;
pub mod protocol;
pub mod sink;
pub mod source;

pub use backoff::Backoff;
pub use book::{Applied, OrderBook, SequenceError};
pub use error::{Error, Result};
pub use feed::{Feed, FeedItem, WebSocketFeed};
pub use protocol::{DepthDelta, DepthSnapshot, FeedEvent, TradeEvent};
pub use sink::{Sink, StorageSink};
pub use source::{HttpSnapshotSource, SnapshotSource};
