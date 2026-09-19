//! The ingest loop: feed in, reconstructed books and durable trades out.
//!
//! The pipeline owns no I/O of its own. It is generic over [`Feed`],
//! [`SnapshotSource`] and [`Sink`], so the parts worth getting right —
//! resync, batching, throttling, back-pressure — are exercised in unit tests
//! with no exchange, no PostgreSQL, no Redis and no wall clock.

use std::collections::{HashMap, VecDeque};
use std::future::Future;

use obe_core::{Backoff, IngestConfig};
use obe_storage::{BookSnapshot, NewTrade, StreamEvent, StreamTrade, Write};
use tokio::time::{Duration, Instant};

use crate::book::{Applied, OrderBook};
use crate::error::Result;
use crate::feed::{Feed, FeedItem};
use crate::protocol::{DepthDelta, FeedEvent, TradeEvent};
use crate::sink::Sink;
use crate::source::SnapshotSource;

/// How many batches' worth of trades may wait for a database that is not
/// answering. Market data does not pause, so the buffer has to have an end:
/// past this, the oldest trades are dropped and counted. Dropping the oldest
/// keeps the most recent tape intact, which is the part anything downstream is
/// actually looking at.
const PENDING_BATCHES: usize = 8;

/// What one symbol's ingestion is doing.
#[derive(Debug)]
struct SymbolState {
    symbol_id: i32,
    /// `None` until a snapshot has been stitched onto the stream.
    book: Option<OrderBook>,
    /// When the next resync attempt is allowed; `None` once one has succeeded.
    resync_at: Option<Instant>,
    resync_backoff: Backoff,
    last_published: Option<Instant>,
    last_persisted: Option<Instant>,
}

/// Counters worth logging, and what the tests assert on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub events: u64,
    pub trades_buffered: u64,
    pub trades_written: u64,
    pub trades_dropped: u64,
    pub books_published: u64,
    pub snapshots_written: u64,
    pub resyncs: u64,
    pub sequence_breaks: u64,
    pub sink_failures: u64,
    /// Messages handed to the live stream. Counted separately from
    /// `sink_failures` because a stream that is down costs subscribers a feed,
    /// while a database that is down costs the service its record.
    pub stream_events: u64,
    pub stream_failures: u64,
}

/// Drives one feed into storage.
#[derive(Debug)]
pub struct Pipeline<F, S, K> {
    feed: F,
    source: S,
    sink: K,
    cfg: IngestConfig,
    symbols: HashMap<String, SymbolState>,
    /// `symbol_id` back to the exchange's ticker. The buffer holds rows keyed
    /// by id because that is what PostgreSQL wants; the stream is addressed by
    /// ticker because that is what a client subscribes to.
    tickers: HashMap<i32, String>,
    pending: VecDeque<NewTrade>,
    last_flush: Instant,
    stats: Stats,
}

impl<F, S, K> Pipeline<F, S, K>
where
    F: Feed,
    S: SnapshotSource,
    K: Sink,
{
    /// `instruments` maps the exchange's ticker to the `symbols` row id the
    /// storage layer keys on.
    pub fn new(
        feed: F,
        source: S,
        sink: K,
        cfg: IngestConfig,
        instruments: &[(String, i32)],
    ) -> Self {
        let now = Instant::now();
        let symbols = instruments
            .iter()
            .map(|(symbol, id)| {
                let state = SymbolState {
                    symbol_id: *id,
                    book: None,
                    // Every symbol starts out owing a snapshot.
                    resync_at: Some(now),
                    resync_backoff: Backoff::new(cfg.reconnect_base(), cfg.reconnect_max()),
                    last_published: None,
                    last_persisted: None,
                };
                (symbol.trim().to_ascii_uppercase(), state)
            })
            .collect();

        let tickers = instruments
            .iter()
            .map(|(symbol, id)| (*id, symbol.trim().to_ascii_uppercase()))
            .collect();

        Self {
            feed,
            source,
            sink,
            cfg,
            symbols,
            tickers,
            pending: VecDeque::new(),
            last_flush: now,
            stats: Stats::default(),
        }
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    pub fn sink(&self) -> &K {
        &self.sink
    }

    pub fn source(&self) -> &S {
        &self.source
    }

    /// Runs until `shutdown` resolves, then flushes what is buffered.
    pub async fn run(&mut self, shutdown: impl Future<Output = ()> + Send) -> Result<()> {
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => break,
                outcome = self.step() => outcome?,
            }
        }

        tracing::info!(stats = ?self.stats, "draining the ingest buffer");
        self.flush_trades().await;
        Ok(())
    }

    /// One turn of the loop: take whatever the feed has, or let the flush
    /// timer fire if it has nothing.
    ///
    /// The timeout is what makes a quiet market flush on schedule. Without it
    /// the loop would sit in `recv`, and the last trades before a lull would
    /// wait in memory for a trade that is not coming.
    pub async fn step(&mut self) -> Result<()> {
        let budget = self
            .cfg
            .trade_flush()
            .saturating_sub(self.last_flush.elapsed());

        match tokio::time::timeout(budget, self.feed.recv()).await {
            Ok(Ok(FeedItem::Event(event))) => self.on_event(event).await,
            Ok(Ok(FeedItem::Reconnected)) => self.on_reconnect(),
            Ok(Err(error)) => return Err(error),
            Err(_elapsed) => {}
        }

        if self.pending.len() >= self.cfg.trade_batch_size
            || self.last_flush.elapsed() >= self.cfg.trade_flush()
        {
            self.flush_trades().await;
        }
        Ok(())
    }

    /// A reconnect restarts the stream at an arbitrary update id, so every
    /// book is invalid even though none of them reported a gap.
    fn on_reconnect(&mut self) {
        let now = Instant::now();
        for state in self.symbols.values_mut() {
            state.book = None;
            state.resync_at = Some(now);
        }
        tracing::info!(symbols = self.symbols.len(), "feed reconnected; resyncing");
    }

    async fn on_event(&mut self, event: FeedEvent) {
        self.stats.events += 1;

        match event {
            FeedEvent::Depth(delta) => self.on_depth(delta).await,
            FeedEvent::Trade(trade) => self.on_trade(&trade),
        }
    }

    async fn on_depth(&mut self, delta: DepthDelta) {
        let key = delta.symbol.trim().to_ascii_uppercase();

        match self.symbols.get(&key) {
            // Not an instrument this service follows.
            None => return,
            Some(state) if state.book.is_none() => {
                if !self.resync(&key).await {
                    return;
                }
            }
            Some(_) => {}
        }

        let now = Instant::now();
        let Some(state) = self.symbols.get_mut(&key) else {
            return;
        };
        let Some(book) = state.book.as_mut() else {
            return;
        };

        match book.apply(&delta) {
            Ok(Applied::Accepted) => {}
            // Expected right after a resync: the socket still holds events the
            // snapshot already contains.
            Ok(Applied::Stale) => return,
            Err(error) => {
                state.book = None;
                state.resync_at = Some(now);
                self.stats.sequence_breaks += 1;
                tracing::warn!(symbol = %key, %error, "book diverged; resyncing");
                return;
            }
        }

        let publish = due(state.last_published, now, self.cfg.publish_interval());
        let persist = due(state.last_persisted, now, self.cfg.snapshot_interval());
        if !publish && !persist {
            // The common case on a busy symbol: the book moved, but neither
            // destination is due, so nothing is serialised at all.
            return;
        }

        let snapshot = book.snapshot(self.cfg.depth);
        self.write_book(&key, &snapshot, publish, persist, now)
            .await;
    }

    fn on_trade(&mut self, trade: &TradeEvent) {
        let Some(state) = self
            .symbols
            .get(trade.symbol.trim().to_ascii_uppercase().as_str())
        else {
            return;
        };

        if self.pending.len() >= self.cfg.trade_batch_size * PENDING_BATCHES {
            self.pending.pop_front();
            self.stats.trades_dropped += 1;
        }

        self.pending.push_back(NewTrade {
            symbol_id: state.symbol_id,
            exchange_trade_id: trade.trade_id,
            price: trade.price,
            quantity: trade.quantity,
            side: trade.side,
            traded_at: trade.traded_at,
        });
        self.stats.trades_buffered += 1;
    }

    /// Writes the buffered trades. The batch is only cleared once it is
    /// durable: a failed write keeps the rows for the next attempt, and the
    /// unique index on the exchange's trade id makes that retry idempotent.
    async fn flush_trades(&mut self) {
        self.last_flush = Instant::now();
        if self.pending.is_empty() {
            return;
        }

        // The tape goes out before the write and regardless of how it ends.
        // The stream is a live view, not a durability claim, and a PostgreSQL
        // outage should cost subscribers nothing.
        self.broadcast_trades().await;

        let batch = self.pending.make_contiguous();
        match self.sink.write_trades(batch).await {
            Ok(written) => {
                self.stats.trades_written += written;
                tracing::debug!(batch = self.pending.len(), written, "trade batch persisted");
                self.pending.clear();
            }
            Err(error) => {
                self.stats.sink_failures += 1;
                tracing::warn!(
                    %error,
                    buffered = self.pending.len(),
                    "trade batch failed; keeping it for the next flush"
                );
            }
        }
    }

    /// Fans the buffered trades out, one message per symbol in the batch.
    ///
    /// Sharing the flush boundary with the durable write is the whole point:
    /// the stream costs one publish per symbol per flush window rather than
    /// one per trade, at the price of the tape arriving in `trade_flush_ms`
    /// steps rather than continuously. The book, which is the latency-
    /// sensitive half, is on its own and much shorter `publish_interval_ms`.
    async fn broadcast_trades(&mut self) {
        let mut batches: HashMap<i32, Vec<StreamTrade>> = HashMap::new();
        for trade in &self.pending {
            batches
                .entry(trade.symbol_id)
                .or_default()
                .push(StreamTrade {
                    trade_id: trade.exchange_trade_id,
                    price: trade.price,
                    quantity: trade.quantity,
                    side: trade.side,
                    traded_at: trade.traded_at,
                });
        }

        for (symbol_id, trades) in batches {
            let Some(symbol) = self.tickers.get(&symbol_id).cloned() else {
                continue;
            };
            self.broadcast(StreamEvent::Trades {
                exchange: self.cfg.exchange.clone(),
                symbol,
                trades,
            })
            .await;
        }
    }

    /// Hands one event to the stream. Failures are counted and dropped: a
    /// subscriber missing an update is not a reason to stall the loop that
    /// keeps the books correct.
    async fn broadcast(&mut self, event: StreamEvent) {
        match self.sink.broadcast(&event).await {
            Ok(_) => self.stats.stream_events += 1,
            Err(error) => {
                self.stats.stream_failures += 1;
                tracing::warn!(
                    symbol = event.symbol(),
                    kind = event.kind().as_str(),
                    %error,
                    "publishing to the live stream failed"
                );
            }
        }
    }

    /// Pushes the book to the cache, and to durable history on the slower
    /// timer. Both are rate limited: a busy symbol produces a hundred book
    /// updates a second and neither destination needs to see all of them.
    async fn write_book(
        &mut self,
        symbol: &str,
        snapshot: &BookSnapshot,
        publish: bool,
        persist: bool,
        now: Instant,
    ) {
        if publish {
            match self.sink.publish_book(symbol, snapshot).await {
                Ok(Write::Stored) => {
                    self.stats.books_published += 1;
                    // Gated on the cache having accepted it, so the live
                    // stream and the cached book can never disagree about
                    // which snapshot is the newest.
                    self.broadcast(StreamEvent::Book {
                        exchange: self.cfg.exchange.clone(),
                        symbol: symbol.to_owned(),
                        sequence: snapshot.sequence,
                        captured_at: snapshot.captured_at,
                        bids: snapshot.bids.clone(),
                        asks: snapshot.asks.clone(),
                    })
                    .await;
                }
                // Another writer is ahead on this symbol; the cache kept the
                // newer book, which is the right outcome.
                Ok(Write::Stale) => {}
                Err(error) => {
                    self.stats.sink_failures += 1;
                    tracing::warn!(symbol, %error, "publishing the book failed");
                }
            }
        }

        if persist {
            match self.sink.write_snapshot(snapshot).await {
                Ok(true) => self.stats.snapshots_written += 1,
                Ok(false) => {}
                Err(error) => {
                    self.stats.sink_failures += 1;
                    tracing::warn!(symbol, %error, "persisting the snapshot failed");
                }
            }
        }

        if let Some(state) = self.symbols.get_mut(symbol) {
            if publish {
                state.last_published = Some(now);
            }
            if persist {
                state.last_persisted = Some(now);
            }
        }
    }

    /// Fetches a fresh snapshot and seeds the book. Returns whether the symbol
    /// is synced afterwards.
    ///
    /// Nothing buffers depth events across the fetch, and nothing needs to:
    /// the socket keeps delivering in order, so events that arrive while this
    /// is in flight are read afterwards and either dropped as already
    /// contained in the snapshot, or applied as the one that straddles it.
    async fn resync(&mut self, symbol: &str) -> bool {
        let now = Instant::now();
        match self.symbols.get(symbol).and_then(|state| state.resync_at) {
            Some(at) if at <= now => {}
            // Backing off after a failed attempt, or already synced.
            _ => return false,
        }

        let fetched = self
            .source
            .depth_snapshot(symbol, self.cfg.snapshot_depth)
            .await;

        let Some(state) = self.symbols.get_mut(symbol) else {
            return false;
        };

        match fetched {
            Ok(snapshot) => {
                state.book = Some(OrderBook::from_snapshot(
                    state.symbol_id,
                    &snapshot,
                    time::OffsetDateTime::now_utc(),
                ));
                state.resync_at = None;
                state.resync_backoff.reset();
                self.stats.resyncs += 1;
                tracing::info!(
                    symbol,
                    last_update_id = snapshot.last_update_id,
                    levels = snapshot.bids.len() + snapshot.asks.len(),
                    "book synced from a depth snapshot"
                );
                true
            }
            Err(error) => {
                // Backed off even when the failure is not retryable: a bad
                // symbol or limit would otherwise re-ask on every depth event,
                // which is how a client earns an IP ban.
                let delay = state.resync_backoff.next_delay();
                state.resync_at = Some(now + delay);
                tracing::warn!(
                    symbol,
                    %error,
                    retryable = error.is_retryable(),
                    delay_ms = delay.as_millis(),
                    "depth snapshot failed"
                );
                false
            }
        }
    }
}

fn due(last: Option<Instant>, now: Instant, interval: Duration) -> bool {
    last.is_none_or(|at| now.duration_since(at) >= interval)
}
