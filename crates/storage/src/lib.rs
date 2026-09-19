//! Persistence for the orderbook-engine: PostgreSQL for history, Redis for
//! the hot book.
//!
//! Both handles are built lazily, so a service can come up, serve its liveness
//! probe and report an unreachable dependency through readiness instead of
//! crash-looping before it can answer anything.
//!
//! Queries are plain `sqlx::query*` calls rather than the compile-time-checked
//! macros: the trade-off buys a build that needs no live database and no
//! checked-in query cache, and the schema is covered by integration tests that
//! run against a real PostgreSQL instead.

pub mod cache;
pub mod error;
pub mod model;
pub mod postgres;
pub mod repository;
pub mod stream;

pub use cache::{BookCache, Write};
pub use error::{Error, Result};
pub use model::{BookSnapshot, Level, NewSymbol, NewTrade, Side, Symbol, Trade};
pub use postgres::{Store, MIGRATOR};
pub use repository::{TradeQuery, MAX_TRADE_LIMIT};
pub use stream::{
    Channels, Kind, StreamEvent, StreamMessage, StreamPublisher, StreamSubscriber, StreamTrade,
    Subscription, Topic,
};
