//! Where reconstructed market data goes.
//!
//! A trait, so the pipeline can be driven end to end in a unit test with
//! neither PostgreSQL nor Redis running, and so the two very different write
//! paths — durable history and the hot book — stay behind one seam.

use std::future::Future;

use obe_storage::{BookCache, BookSnapshot, NewTrade, Store, Write};

use crate::error::Result;

/// The writes an ingest pipeline performs.
pub trait Sink: Send + Sync {
    /// Durable trade history. Returns how many rows were new; a replayed
    /// batch after a reconnect is expected to return fewer than it was given.
    fn write_trades(&self, trades: &[NewTrade]) -> impl Future<Output = Result<u64>> + Send;

    /// The hot book other services read. Returns [`Write::Stale`] when a newer
    /// sequence was already cached.
    fn publish_book(
        &self,
        symbol: &str,
        book: &BookSnapshot,
    ) -> impl Future<Output = Result<Write>> + Send;

    /// Durable book history. Returns `false` when that sequence was already
    /// stored.
    fn write_snapshot(&self, book: &BookSnapshot) -> impl Future<Output = Result<bool>> + Send;
}

/// The production sink: PostgreSQL for history, Redis for the hot book.
#[derive(Debug, Clone)]
pub struct StorageSink {
    store: Store,
    cache: BookCache,
    exchange: String,
}

impl StorageSink {
    pub fn new(store: Store, cache: BookCache, exchange: impl Into<String>) -> Self {
        Self {
            store,
            cache,
            exchange: exchange.into(),
        }
    }
}

impl Sink for StorageSink {
    async fn write_trades(&self, trades: &[NewTrade]) -> Result<u64> {
        Ok(self.store.insert_trades(trades).await?)
    }

    async fn publish_book(&self, symbol: &str, book: &BookSnapshot) -> Result<Write> {
        Ok(self.cache.put_book(&self.exchange, symbol, book).await?)
    }

    async fn write_snapshot(&self, book: &BookSnapshot) -> Result<bool> {
        Ok(self.store.insert_snapshot(book).await?)
    }
}
