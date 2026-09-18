//! End-to-end tests for the ingest loop with the exchange, PostgreSQL and
//! Redis replaced by fakes, and the clock paused.
//!
//! The point is that the awkward paths — a sequence gap mid-stream, a
//! reconnect, a database that stops answering, a feed that floods the buffer —
//! are the ones that are hardest to reproduce against real infrastructure and
//! the ones most worth having covered.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use obe_core::{IngestConfig, InstrumentConfig};
use obe_ingest::error::{Error, Result};
use obe_ingest::protocol::{DepthDelta, DepthSnapshot, TradeEvent};
use obe_ingest::{Feed, FeedEvent, FeedItem, Pipeline, Sink, SnapshotSource};
use obe_storage::{BookSnapshot, Level, NewTrade, Side, Write};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use time::OffsetDateTime;

const SYMBOL: &str = "BTCUSDT";
const SYMBOL_ID: i32 = 7;

// ---------------------------------------------------------------- test doubles

/// Replays a fixed script, then blocks forever — which is what a real feed
/// does between events, and what lets the flush timer be the thing that fires.
#[derive(Debug)]
struct ScriptedFeed(VecDeque<FeedItem>);

impl ScriptedFeed {
    fn new(items: Vec<FeedItem>) -> Self {
        Self(items.into())
    }
}

impl Feed for ScriptedFeed {
    async fn recv(&mut self) -> Result<FeedItem> {
        match self.0.pop_front() {
            Some(item) => Ok(item),
            None => std::future::pending().await,
        }
    }
}

#[derive(Debug, Default)]
struct FakeSource {
    calls: AtomicUsize,
    /// Each resync gets the next one; the last repeats.
    snapshots: Mutex<VecDeque<DepthSnapshot>>,
    fail_until: AtomicUsize,
}

impl FakeSource {
    fn new(snapshots: Vec<DepthSnapshot>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            snapshots: Mutex::new(snapshots.into()),
            fail_until: AtomicUsize::new(0),
        }
    }

    fn failing_for(self, calls: usize) -> Self {
        self.fail_until.store(calls, Ordering::SeqCst);
        self
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl SnapshotSource for FakeSource {
    async fn depth_snapshot(&self, _symbol: &str, _limit: u16) -> Result<DepthSnapshot> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.fail_until.load(Ordering::SeqCst) {
            return Err(Error::SnapshotStatus { status: 503 });
        }

        let mut snapshots = self.snapshots.lock().unwrap();
        let next = snapshots.front().cloned().ok_or(Error::Closed)?;
        if snapshots.len() > 1 {
            snapshots.pop_front();
        }
        Ok(next)
    }
}

#[derive(Debug, Default)]
struct RecordingSink {
    trades: Mutex<Vec<NewTrade>>,
    books: Mutex<Vec<BookSnapshot>>,
    snapshots: Mutex<Vec<BookSnapshot>>,
    trades_fail: AtomicBool,
}

impl RecordingSink {
    fn set_trades_fail(&self, fail: bool) {
        self.trades_fail.store(fail, Ordering::SeqCst);
    }

    fn trades(&self) -> Vec<NewTrade> {
        self.trades.lock().unwrap().clone()
    }

    fn books(&self) -> Vec<BookSnapshot> {
        self.books.lock().unwrap().clone()
    }
}

impl Sink for RecordingSink {
    async fn write_trades(&self, trades: &[NewTrade]) -> Result<u64> {
        if self.trades_fail.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        self.trades.lock().unwrap().extend_from_slice(trades);
        Ok(trades.len() as u64)
    }

    async fn publish_book(&self, _symbol: &str, book: &BookSnapshot) -> Result<Write> {
        self.books.lock().unwrap().push(book.clone());
        Ok(Write::Stored)
    }

    async fn write_snapshot(&self, book: &BookSnapshot) -> Result<bool> {
        self.snapshots.lock().unwrap().push(book.clone());
        Ok(true)
    }
}

// --------------------------------------------------------------------- fixtures

fn config() -> IngestConfig {
    IngestConfig {
        exchange: "binance".into(),
        stream_url: "wss://example.invalid/stream".into(),
        snapshot_url: "https://example.invalid/depth".into(),
        instruments: vec![InstrumentConfig {
            symbol: SYMBOL.into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            price_precision: 2,
            quantity_precision: 5,
        }],
        depth: 5,
        snapshot_depth: 100,
        trade_batch_size: 3,
        trade_flush_ms: 1_000,
        snapshot_interval_ms: 5_000,
        publish_interval_ms: 250,
        reconnect_base_ms: 100,
        reconnect_max_ms: 1_000,
    }
}

fn levels(raw: &[(Decimal, Decimal)]) -> Vec<Level> {
    raw.iter().map(|(p, q)| Level::new(*p, *q)).collect()
}

fn snapshot(last_update_id: i64) -> DepthSnapshot {
    DepthSnapshot {
        last_update_id,
        bids: levels(&[(dec!(99), dec!(2)), (dec!(98), dec!(5))]),
        asks: levels(&[(dec!(101), dec!(1)), (dec!(102), dec!(4))]),
    }
}

fn at(secs: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(secs).unwrap()
}

fn depth(first: i64, last: i64, bid: Decimal) -> FeedItem {
    FeedItem::Event(FeedEvent::Depth(DepthDelta {
        symbol: SYMBOL.into(),
        first_update_id: first,
        final_update_id: last,
        event_time: at(last),
        bids: levels(&[(bid, dec!(1))]),
        asks: vec![],
    }))
}

fn trade(id: i64) -> FeedItem {
    FeedItem::Event(FeedEvent::Trade(TradeEvent {
        symbol: SYMBOL.into(),
        trade_id: id,
        price: dec!(100),
        quantity: dec!(0.5),
        side: Side::Buy,
        traded_at: at(id),
    }))
}

fn pipeline(
    items: Vec<FeedItem>,
    source: FakeSource,
    sink: RecordingSink,
    cfg: IngestConfig,
) -> Pipeline<ScriptedFeed, FakeSource, RecordingSink> {
    Pipeline::new(
        ScriptedFeed::new(items),
        source,
        sink,
        cfg,
        &[(SYMBOL.into(), SYMBOL_ID)],
    )
}

/// Runs `steps` turns of the loop. With the clock paused, a step that finds
/// nothing on the feed advances time to the flush deadline instead of waiting.
async fn drive(pipeline: &mut Pipeline<ScriptedFeed, FakeSource, RecordingSink>, steps: usize) {
    for _ in 0..steps {
        pipeline.step().await.expect("the pipeline should not fail");
    }
}

// ------------------------------------------------------------------------ tests

#[tokio::test(start_paused = true)]
async fn a_synced_book_is_published_and_trades_are_batched() {
    let mut pipeline = pipeline(
        vec![depth(101, 105, dec!(100)), trade(1), trade(2), trade(3)],
        FakeSource::new(vec![snapshot(100)]),
        RecordingSink::default(),
        config(),
    );

    drive(&mut pipeline, 4).await;

    let stats = pipeline.stats();
    assert_eq!(stats.resyncs, 1, "one snapshot fetch to seed the book");
    assert_eq!(stats.books_published, 1);
    // Three trades is the configured batch size, so the batch flushed without
    // waiting for the timer.
    assert_eq!(stats.trades_written, 3);
    assert_eq!(stats.sequence_breaks, 0);
}

#[tokio::test(start_paused = true)]
async fn the_published_book_reflects_the_applied_delta() {
    let sink = RecordingSink::default();
    let mut pipeline = pipeline(
        vec![depth(101, 105, dec!(100))],
        FakeSource::new(vec![snapshot(100)]),
        sink,
        config(),
    );

    drive(&mut pipeline, 1).await;
    let books = pipeline.sink().books();

    assert_eq!(books.len(), 1);
    assert_eq!(books[0].sequence, 105);
    assert_eq!(books[0].symbol_id, SYMBOL_ID);
    // The delta added a bid at 100, inside the snapshot's 99/101 spread.
    assert_eq!(books[0].bids[0], Level::new(dec!(100), dec!(1)));
    assert!(books[0].is_well_formed());
}

#[tokio::test(start_paused = true)]
async fn a_sequence_gap_forces_a_resync_rather_than_a_wrong_book() {
    let mut pipeline = pipeline(
        vec![
            depth(101, 105, dec!(100)),
            // 106 never arrived.
            depth(107, 110, dec!(100.5)),
            depth(111, 115, dec!(100.5)),
        ],
        FakeSource::new(vec![snapshot(100), snapshot(110)]),
        RecordingSink::default(),
        config(),
    );

    drive(&mut pipeline, 3).await;

    let stats = pipeline.stats();
    assert_eq!(stats.sequence_breaks, 1);
    assert_eq!(stats.resyncs, 2, "the gap forced a second snapshot");
    assert_eq!(pipeline.source().calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_reconnect_invalidates_every_book_before_a_gap_can_be_seen() {
    let mut pipeline = pipeline(
        vec![
            depth(101, 105, dec!(100)),
            FeedItem::Reconnected,
            // Contiguous with the pre-reconnect stream, so no gap would be
            // reported — but the stream restarted, so the book is not to be
            // trusted and is rebuilt anyway.
            depth(106, 110, dec!(100)),
        ],
        FakeSource::new(vec![snapshot(100), snapshot(105)]),
        RecordingSink::default(),
        config(),
    );

    drive(&mut pipeline, 3).await;

    assert_eq!(pipeline.stats().sequence_breaks, 0);
    assert_eq!(pipeline.stats().resyncs, 2);
}

#[tokio::test(start_paused = true)]
async fn a_failed_snapshot_backs_off_instead_of_hammering_the_exchange() {
    let mut pipeline = pipeline(
        vec![
            depth(101, 105, dec!(100)),
            depth(106, 110, dec!(100)),
            depth(111, 115, dec!(100)),
        ],
        FakeSource::new(vec![snapshot(100)]).failing_for(1),
        RecordingSink::default(),
        config(),
    );

    drive(&mut pipeline, 3).await;

    // Three depth events, one failed fetch, and the two that followed inside
    // the backoff window did not produce a fetch of their own.
    assert_eq!(pipeline.source().calls(), 1);
    assert_eq!(pipeline.stats().resyncs, 0);
}

#[tokio::test(start_paused = true)]
async fn a_quiet_market_still_flushes_on_the_timer() {
    let mut pipeline = pipeline(
        vec![trade(1)],
        FakeSource::new(vec![snapshot(100)]),
        RecordingSink::default(),
        config(),
    );

    // One step takes the trade; the next finds nothing and lets the flush
    // deadline arrive, which is the only thing that gets a partial batch out.
    drive(&mut pipeline, 1).await;
    assert_eq!(pipeline.stats().trades_written, 0);

    drive(&mut pipeline, 1).await;

    assert_eq!(pipeline.stats().trades_written, 1);
    assert_eq!(pipeline.sink().trades().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_failed_write_keeps_the_batch_for_the_next_attempt() {
    let mut pipeline = pipeline(
        vec![trade(1), trade(2), trade(3)],
        FakeSource::new(vec![snapshot(100)]),
        RecordingSink::default(),
        config(),
    );
    pipeline.sink().set_trades_fail(true);

    drive(&mut pipeline, 3).await;
    assert_eq!(pipeline.stats().trades_written, 0);
    assert_eq!(pipeline.stats().sink_failures, 1);
    assert!(pipeline.sink().trades().is_empty());

    pipeline.sink().set_trades_fail(false);
    drive(&mut pipeline, 1).await;

    // Nothing was lost: the same three trades land once the sink recovers.
    assert_eq!(pipeline.stats().trades_written, 3);
    assert_eq!(
        pipeline
            .sink()
            .trades()
            .iter()
            .map(|t| t.exchange_trade_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[tokio::test(start_paused = true)]
async fn the_buffer_sheds_the_oldest_trades_rather_than_growing_without_end() {
    let mut cfg = config();
    cfg.trade_batch_size = 2;
    // 2 * PENDING_BATCHES = 16 trades may wait.
    let capacity = cfg.trade_batch_size * 8;

    let trades: Vec<FeedItem> = (1..=20).map(trade).collect();
    let mut pipeline = pipeline(
        trades,
        FakeSource::new(vec![snapshot(100)]),
        RecordingSink::default(),
        cfg,
    );
    pipeline.sink().set_trades_fail(true);

    drive(&mut pipeline, 20).await;
    assert_eq!(pipeline.stats().trades_dropped, (20 - capacity) as u64);

    pipeline.sink().set_trades_fail(false);
    drive(&mut pipeline, 1).await;

    // The newest trades survived; the oldest four were shed.
    let kept: Vec<i64> = pipeline
        .sink()
        .trades()
        .iter()
        .map(|t| t.exchange_trade_id)
        .collect();
    assert_eq!(kept, (5..=20).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn book_publishing_is_rate_limited() {
    let updates: Vec<FeedItem> = (0..20)
        .map(|i| depth(106 + i * 5, 110 + i * 5, dec!(100)))
        .collect();
    let mut items = vec![depth(101, 105, dec!(100))];
    items.extend(updates);

    let mut pipeline = pipeline(
        items,
        FakeSource::new(vec![snapshot(100)]),
        RecordingSink::default(),
        config(),
    );

    drive(&mut pipeline, 21).await;

    // 21 accepted deltas, no time passing between them, one publish.
    assert_eq!(pipeline.stats().events, 21);
    assert_eq!(pipeline.stats().sequence_breaks, 0);
    assert_eq!(pipeline.stats().books_published, 1);
}

#[tokio::test(start_paused = true)]
async fn events_for_unfollowed_symbols_are_ignored() {
    let mut item = trade(1);
    if let FeedItem::Event(FeedEvent::Trade(ref mut trade)) = item {
        trade.symbol = "DOGEUSDT".into();
    }

    let mut pipeline = pipeline(
        vec![item],
        FakeSource::new(vec![snapshot(100)]),
        RecordingSink::default(),
        config(),
    );

    drive(&mut pipeline, 2).await;

    assert_eq!(pipeline.stats().events, 1);
    assert_eq!(pipeline.stats().trades_buffered, 0);
    assert_eq!(pipeline.stats().trades_written, 0);
}
