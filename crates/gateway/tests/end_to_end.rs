//! The whole service, end to end: exchange frames in, HTTP and WebSocket out.
//!
//! Everything between the feed and the client is the real thing — the real
//! book reconstructor, the real batching pipeline, a real PostgreSQL, a real
//! Redis, the real cache and pub/sub transport, the real router and the real
//! stream handler. Only the exchange is a double, because the point of the
//! test is that a scripted sequence of depth diffs and trades comes out the
//! other end as the book and the tape a client would see.
//!
//! Runs when `OBE_TEST_DATABASE_URL` and `OBE_TEST_REDIS_URL` are set and
//! skips with a note otherwise, like the other suites that need services.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use obe_core::{IngestConfig, InstrumentConfig, Settings};
use obe_gateway::{router, AppState};
use obe_ingest::error::Result as IngestResult;
use obe_ingest::protocol::{DepthDelta, DepthSnapshot, TradeEvent};
use obe_ingest::{Feed, FeedEvent, FeedItem, Pipeline, SnapshotSource, StorageSink};
use obe_storage::{BookCache, Level, NewSymbol, Side, Store, StreamEvent, StreamPublisher};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

const DATABASE_URL: &str = "OBE_TEST_DATABASE_URL";
const REDIS_URL: &str = "OBE_TEST_REDIS_URL";
const SYMBOL: &str = "BTCUSDT";

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique(kind: &str) -> String {
    let nth = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
    format!("{kind}-{nanos}-{nth}")
}

// ------------------------------------------------------------------ the exchange

/// Replays a script, then blocks, which is what a real feed does between
/// events.
struct ScriptedFeed(VecDeque<FeedItem>);

impl Feed for ScriptedFeed {
    async fn recv(&mut self) -> IngestResult<FeedItem> {
        match self.0.pop_front() {
            Some(item) => Ok(item),
            None => std::future::pending().await,
        }
    }
}

struct FixedSnapshot(DepthSnapshot);

impl SnapshotSource for FixedSnapshot {
    async fn depth_snapshot(&self, _symbol: &str, _limit: u16) -> IngestResult<DepthSnapshot> {
        Ok(self.0.clone())
    }
}

fn levels(raw: &[(Decimal, Decimal)]) -> Vec<Level> {
    raw.iter().map(|(p, q)| Level::new(*p, *q)).collect()
}

fn depth(first: i64, last: i64, bid: Decimal) -> FeedItem {
    FeedItem::Event(FeedEvent::Depth(DepthDelta {
        symbol: SYMBOL.into(),
        first_update_id: first,
        final_update_id: last,
        event_time: OffsetDateTime::now_utc(),
        bids: levels(&[(bid, dec!(1.5))]),
        asks: vec![],
    }))
}

fn trade(id: i64, price: Decimal) -> FeedItem {
    FeedItem::Event(FeedEvent::Trade(TradeEvent {
        symbol: SYMBOL.into(),
        trade_id: id,
        price,
        quantity: dec!(0.25),
        side: Side::Buy,
        traded_at: OffsetDateTime::now_utc(),
    }))
}

// ---------------------------------------------------------------- the deployment

/// One test's whole stack: an ingestor, a gateway, and the services they
/// share, all under a namespace no other test will touch.
struct Deployment {
    app: axum::Router,
    addr: SocketAddr,
    exchange: String,
    settings: Settings,
    /// The same handle the ingestor writes through; closing it is how a test
    /// takes PostgreSQL away mid-run.
    store: Store,
    pipeline: Pipeline<ScriptedFeed, FixedSnapshot, StorageSink>,
}

impl Deployment {
    /// `None` (with a printed note) when the environment has no services.
    async fn start(script: Vec<FeedItem>, snapshot_id: i64) -> Option<Self> {
        let (Ok(database_url), Ok(redis_url)) =
            (std::env::var(DATABASE_URL), std::env::var(REDIS_URL))
        else {
            eprintln!("skipping: {DATABASE_URL} and {REDIS_URL} must both be set");
            return None;
        };

        let exchange = unique("exchange");
        let mut settings =
            Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
                .expect("repository config should load");
        settings.database.url = database_url.as_str().into();
        settings.redis.url = redis_url.as_str().into();
        // One prefix for the cache and the stream, so the ingestor and the
        // gateway meet on the same keys and channels and no other test does.
        settings.redis.key_prefix = unique("obetest");
        settings.ingest = ingest_config(&exchange);

        let store = Store::connect_lazy(&settings.database).expect("pool should build");
        store.migrate().await.expect("migrations should apply");

        let symbol_id = store
            .upsert_symbol(&NewSymbol {
                exchange: exchange.clone(),
                symbol: SYMBOL.into(),
                base_asset: "BTC".into(),
                quote_asset: "USDT".into(),
                price_precision: 2,
                quantity_precision: 5,
                active: true,
            })
            .await
            .expect("symbol should upsert")
            .id;

        let sink = StorageSink::new(
            store.clone(),
            BookCache::connect_lazy(&settings.redis).expect("cache should build"),
            StreamPublisher::connect_lazy(&settings.redis).expect("publisher should build"),
            &exchange,
        );
        let pipeline = Pipeline::new(
            ScriptedFeed(script.into()),
            FixedSnapshot(DepthSnapshot {
                last_update_id: snapshot_id,
                bids: levels(&[(dec!(99), dec!(2)), (dec!(98), dec!(5))]),
                asks: levels(&[(dec!(101), dec!(1)), (dec!(102), dec!(4))]),
            }),
            sink,
            settings.ingest.clone(),
            &[(SYMBOL.into(), symbol_id)],
        );

        let state = AppState::new(&settings).expect("pools should build");
        state
            .follow_stream(&settings, std::future::pending())
            .expect("the stream reader should start");

        let app = router(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let served = app.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, served).await;
        });

        Some(Self {
            app,
            addr,
            exchange,
            settings,
            store,
            pipeline,
        })
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body should collect")
            .to_bytes();

        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// Runs `steps` turns of the ingest loop.
    ///
    /// The pause is what makes the publish tick deterministic: the config here
    /// sets `publish_interval_ms` to 1, and without it two steps could land
    /// inside the same millisecond and the second book would be throttled
    /// rather than published — a test that passes or fails on how fast CI's
    /// Redis answered.
    async fn ingest(&mut self, steps: usize) {
        for _ in 0..steps {
            tokio::time::sleep(Duration::from_millis(2)).await;
            self.pipeline
                .step()
                .await
                .expect("the pipeline should not fail");
        }
    }

    /// Waits until the gateway's `PSUBSCRIBE` is visible to a publisher.
    ///
    /// Pub/sub buffers nothing for a subscriber that has not arrived yet, so
    /// anything the pipeline published before this point would simply be lost
    /// — and the test would be timing-dependent rather than wrong-on-purpose.
    async fn await_subscriber(&self) {
        let probe =
            StreamPublisher::connect_lazy(&self.settings.redis).expect("probe should build");
        let event = StreamEvent::Trades {
            exchange: self.exchange.clone(),
            symbol: "PROBEUSDT".into(),
            trades: Vec::new(),
        };

        for _ in 0..200 {
            if probe.publish(&event).await.expect("probe publish") > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the gateway never subscribed to the stream");
    }
}

fn ingest_config(exchange: &str) -> IngestConfig {
    IngestConfig {
        exchange: exchange.to_owned(),
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
        trade_batch_size: 2,
        trade_flush_ms: 1_000,
        snapshot_interval_ms: 1,
        publish_interval_ms: 1,
        reconnect_base_ms: 50,
        reconnect_max_ms: 500,
    }
}

struct Client {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/v1/stream"))
            .await
            .expect("the upgrade should be accepted");
        Self { socket }
    }

    async fn send(&mut self, raw: &str) {
        self.socket
            .send(Message::Text(raw.into()))
            .await
            .expect("the command should go out");
    }

    async fn next(&mut self) -> Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(10), self.socket.next())
                .await
                .expect("a frame should arrive inside the timeout")
                .expect("the socket should stay open")
                .expect("the frame should be readable");

            match message {
                Message::Text(raw) => {
                    return serde_json::from_str(&raw).expect("frames should be JSON")
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
    }

    /// The next frame of a given `type`, skipping the rest.
    async fn next_of(&mut self, kind: &str) -> Value {
        for _ in 0..20 {
            let frame = self.next().await;
            if frame["type"] == kind {
                return frame;
            }
        }
        panic!("no `{kind}` frame arrived");
    }
}

macro_rules! deployment {
    ($script:expr, $snapshot:expr) => {
        match Deployment::start($script, $snapshot).await {
            Some(deployment) => deployment,
            None => return,
        }
    };
}

#[tokio::test]
async fn a_scripted_feed_becomes_a_book_a_tape_and_a_live_stream() {
    let mut deployment = deployment!(
        vec![
            depth(101, 105, dec!(100)),
            trade(1, dec!(100.5)),
            trade(2, dec!(100.75)),
        ],
        100
    );

    let mut client = Client::connect(deployment.addr).await;
    client
        .send(&format!(
            r#"{{"op":"subscribe","channels":["book:{0}:BTCUSDT","trades:{0}:BTCUSDT"]}}"#,
            deployment.exchange
        ))
        .await;
    assert_eq!(client.next().await["type"], "subscribed");
    deployment.await_subscriber().await;

    deployment.ingest(3).await;

    // 1. The book the reconstructor built reached the live stream.
    let streamed = client.next_of("book").await;
    assert_eq!(streamed["symbol"], SYMBOL);
    assert_eq!(streamed["sequence"], 105);
    // The delta put a bid at 100 inside the snapshot's 99/101 spread.
    assert_eq!(streamed["bids"][0]["price"], "100");
    assert_eq!(streamed["bids"][0]["quantity"], "1.5");

    // 2. The tape reached it too, batched at the flush boundary.
    let tape = client.next_of("trades").await;
    let ids: Vec<i64> = tape["trades"]
        .as_array()
        .expect("trades should be a list")
        .iter()
        .map(|t| t["trade_id"].as_i64().expect("an id"))
        .collect();
    assert_eq!(ids, vec![1, 2]);

    // 3. The same book is what the REST endpoint serves, out of the cache.
    let (status, body) = deployment
        .get(&format!("/v1/books/{}/BTCUSDT", deployment.exchange))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "cache");
    assert_eq!(body["sequence"], 105);
    assert_eq!(body["spread"], "1");

    // 4. And the trades are durable, out of PostgreSQL.
    let (status, body) = deployment
        .get(&format!("/v1/trades/{}/BTCUSDT", deployment.exchange))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 2);
    assert_eq!(body["trades"][0]["price"], "100.75");
}

#[tokio::test]
async fn a_sequence_gap_resyncs_without_ever_serving_a_wrong_book() {
    // The snapshot source always answers 100, so the resync after the gap
    // rebuilds from there and the events beyond it apply cleanly again.
    let mut deployment = deployment!(
        vec![
            depth(101, 105, dec!(100)),
            // 106 never arrived.
            depth(107, 110, dec!(100.25)),
            depth(101, 111, dec!(100.5)),
        ],
        100
    );

    deployment.ingest(3).await;

    let stats = deployment.pipeline.stats();
    assert_eq!(stats.sequence_breaks, 1);
    assert_eq!(stats.resyncs, 2, "the gap forced a second snapshot");

    // Every book that reached the cache is well formed, and the one being
    // served is the last one the reconstructor actually vouched for — the
    // broken intermediate state was never published.
    let (status, body) = deployment
        .get(&format!("/v1/books/{}/BTCUSDT", deployment.exchange))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["sequence"], 111);
    assert_eq!(body["bids"][0]["price"], "100.5");
}

#[tokio::test]
async fn a_restarted_gateway_picks_the_stream_back_up() {
    let mut deployment = deployment!(
        vec![depth(101, 105, dec!(100)), depth(106, 110, dec!(100.25))],
        100
    );

    // A first client sees the first update.
    let mut first = Client::connect(deployment.addr).await;
    first
        .send(&format!(
            r#"{{"op":"subscribe","channels":["book:{}:BTCUSDT"]}}"#,
            deployment.exchange
        ))
        .await;
    assert_eq!(first.next().await["type"], "subscribed");
    deployment.await_subscriber().await;

    deployment.ingest(1).await;
    assert_eq!(first.next_of("book").await["sequence"], 105);
    drop(first);

    // A second one connects later and is handed the cached book immediately,
    // rather than waiting for the next tick — the snapshot-then-stream
    // bootstrap is what makes a reconnect cheap.
    let mut second = Client::connect(deployment.addr).await;
    second
        .send(&format!(
            r#"{{"op":"subscribe","channels":["book:{}:BTCUSDT"]}}"#,
            deployment.exchange
        ))
        .await;
    assert_eq!(second.next().await["type"], "subscribed");

    let opening = second.next_of("book").await;
    assert_eq!(opening["sequence"], 105, "the opening book came from cache");

    deployment.ingest(1).await;
    assert_eq!(second.next_of("book").await["sequence"], 110);
}

#[tokio::test]
async fn a_dead_database_costs_the_record_but_not_the_live_feed() {
    let mut deployment = deployment!(vec![trade(1, dec!(100)), trade(2, dec!(101))], 100);

    let mut client = Client::connect(deployment.addr).await;
    client
        .send(&format!(
            r#"{{"op":"subscribe","channels":["trades:{}:BTCUSDT"]}}"#,
            deployment.exchange
        ))
        .await;
    assert_eq!(client.next().await["type"], "subscribed");
    deployment.await_subscriber().await;

    // Take PostgreSQL away mid-run by closing the pool the ingestor writes
    // through. Redis is untouched, which is the interesting half.
    deployment.store.close().await;

    deployment.ingest(2).await;

    let tape = client.next_of("trades").await;
    assert_eq!(
        tape["trades"].as_array().map(Vec::len),
        Some(2),
        "the tape still went out"
    );
    assert_eq!(deployment.pipeline.stats().trades_written, 0);
    assert_eq!(deployment.pipeline.stats().sink_failures, 1);
}
