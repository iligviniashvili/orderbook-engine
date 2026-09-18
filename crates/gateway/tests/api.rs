//! The read API against a real PostgreSQL and Redis, driven through the real
//! router.
//!
//! Like the storage suite, these run when `OBE_TEST_DATABASE_URL` and
//! `OBE_TEST_REDIS_URL` are set and skip with a note otherwise, so a plain
//! `cargo test` still works with nothing running. Each test namespaces its own
//! exchange and cache prefix so the suite is safe to run in parallel against
//! one shared database.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use obe_core::Settings;
use obe_gateway::{router, AppState};
use obe_storage::{BookCache, BookSnapshot, Level, NewSymbol, NewTrade, Side, Store};
use rust_decimal_macros::dec;
use serde_json::Value;
use time::{Duration, OffsetDateTime};
use tokio::sync::OnceCell;
use tower::ServiceExt;

const DATABASE_URL: &str = "OBE_TEST_DATABASE_URL";
const REDIS_URL: &str = "OBE_TEST_REDIS_URL";

static MIGRATED: OnceCell<()> = OnceCell::const_new();
static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique(kind: &str) -> String {
    let nth = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
    format!("{kind}-{nanos}-{nth}")
}

/// One test's world: a router wired to live services, plus the handles used to
/// seed them, all sharing one namespace.
struct Fixture {
    app: axum::Router,
    store: Store,
    cache: BookCache,
    exchange: String,
    symbol_id: i32,
}

impl Fixture {
    /// `None` (with a printed note) when the environment has no services.
    async fn new() -> Option<Self> {
        let (Ok(database_url), Ok(redis_url)) =
            (std::env::var(DATABASE_URL), std::env::var(REDIS_URL))
        else {
            eprintln!("skipping: {DATABASE_URL} and {REDIS_URL} must both be set");
            return None;
        };

        let mut settings =
            Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
                .expect("repository config should load");
        settings.database.url = database_url.as_str().into();
        settings.redis.url = redis_url.as_str().into();
        settings.redis.key_prefix = unique("obetest");

        let store = Store::connect_lazy(&settings.database).expect("pool should build");
        MIGRATED
            .get_or_init(|| async {
                store.migrate().await.expect("migrations should apply");
            })
            .await;

        let exchange = unique("exchange");
        let symbol_id = store
            .upsert_symbol(&NewSymbol {
                exchange: exchange.clone(),
                symbol: "BTCUSDT".into(),
                base_asset: "BTC".into(),
                quote_asset: "USDT".into(),
                price_precision: 2,
                quantity_precision: 5,
                active: true,
            })
            .await
            .expect("symbol should upsert")
            .id;

        let state = AppState::new(&settings).expect("pools should build");
        let cache = BookCache::connect_lazy(&settings.redis).expect("cache should build");

        Some(Self {
            app: router(state),
            store,
            cache,
            exchange,
            symbol_id,
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

        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("body should be JSON")
        };

        (status, body)
    }

    fn snapshot(&self, sequence: i64, best_bid: rust_decimal::Decimal) -> BookSnapshot {
        BookSnapshot {
            symbol_id: self.symbol_id,
            sequence,
            captured_at: OffsetDateTime::now_utc(),
            bids: vec![Level::new(best_bid, dec!(1)), Level::new(dec!(98), dec!(2))],
            asks: vec![
                Level::new(dec!(101), dec!(1)),
                Level::new(dec!(102), dec!(2)),
            ],
        }
    }
}

macro_rules! fixture {
    () => {
        match Fixture::new().await {
            Some(fixture) => fixture,
            None => return,
        }
    };
}

#[tokio::test]
async fn the_registry_lists_an_ingested_instrument() {
    let fixture = fixture!();

    let (status, body) = fixture.get("/v1/symbols").await;

    assert_eq!(status, StatusCode::OK);
    let listed = body["symbols"]
        .as_array()
        .expect("symbols should be a list")
        .iter()
        .any(|row| row["exchange"] == fixture.exchange && row["symbol"] == "BTCUSDT");
    assert!(listed, "{body}");
}

#[tokio::test]
async fn the_book_comes_from_the_cache_when_it_is_warm() {
    let fixture = fixture!();
    let snapshot = fixture.snapshot(42, dec!(100));
    fixture
        .cache
        .put_book(&fixture.exchange, "BTCUSDT", &snapshot)
        .await
        .expect("cache write should succeed");

    let (status, body) = fixture
        .get(&format!("/v1/books/{}/btcusdt", fixture.exchange))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "cache");
    assert_eq!(body["sequence"], 42);
    // Prices are strings end to end, so no JSON reader can turn one into a
    // float on the way out.
    assert_eq!(body["bids"][0]["price"], "100");
    assert_eq!(body["spread"], "1");
    assert!(body["age_ms"].as_i64().expect("age should be a number") >= 0);
}

#[tokio::test]
async fn the_book_falls_back_to_the_newest_persisted_snapshot() {
    let fixture = fixture!();
    fixture
        .store
        .insert_snapshot(&fixture.snapshot(7, dec!(99)))
        .await
        .expect("snapshot should insert");

    // Nothing was ever cached for this namespace, so the cold path runs.
    let (status, body) = fixture
        .get(&format!("/v1/books/{}/BTCUSDT", fixture.exchange))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "database");
    assert_eq!(body["sequence"], 7);
}

#[tokio::test]
async fn a_depth_limit_truncates_the_returned_book() {
    let fixture = fixture!();
    fixture
        .cache
        .put_book(
            &fixture.exchange,
            "BTCUSDT",
            &fixture.snapshot(9, dec!(100)),
        )
        .await
        .expect("cache write should succeed");

    let (status, body) = fixture
        .get(&format!("/v1/books/{}/BTCUSDT?depth=1", fixture.exchange))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["bids"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["asks"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn trade_history_is_windowed_and_newest_first() {
    let fixture = fixture!();
    let base = OffsetDateTime::now_utc();
    let trades: Vec<NewTrade> = (0..3)
        .map(|i| NewTrade {
            symbol_id: fixture.symbol_id,
            exchange_trade_id: i + 1,
            price: dec!(100),
            quantity: dec!(1),
            side: Side::Buy,
            traded_at: base + Duration::seconds(i),
        })
        .collect();
    fixture
        .store
        .insert_trades(&trades)
        .await
        .expect("trades should insert");

    let all = format!("/v1/trades/{}/BTCUSDT", fixture.exchange);
    let (status, body) = fixture.get(&all).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 3);
    assert_eq!(body["trades"][0]["exchange_trade_id"], 3);

    let windowed = format!(
        "/v1/trades/{}/BTCUSDT?start={}&end={}",
        fixture.exchange,
        urlencode(&rfc3339(base + Duration::seconds(1))),
        urlencode(&rfc3339(base + Duration::seconds(2))),
    );
    let (status, body) = fixture.get(&windowed).await;

    assert_eq!(status, StatusCode::OK);
    // Start is inclusive, end exclusive: only the middle trade.
    assert_eq!(body["count"], 1);
    assert_eq!(body["trades"][0]["exchange_trade_id"], 2);
}

#[tokio::test]
async fn vwap_is_computed_over_the_requested_window() {
    let fixture = fixture!();
    let base = OffsetDateTime::now_utc();
    fixture
        .store
        .insert_trades(&[
            NewTrade {
                symbol_id: fixture.symbol_id,
                exchange_trade_id: 1,
                price: dec!(100),
                quantity: dec!(1),
                side: Side::Buy,
                traded_at: base,
            },
            NewTrade {
                symbol_id: fixture.symbol_id,
                exchange_trade_id: 2,
                price: dec!(200),
                quantity: dec!(3),
                side: Side::Sell,
                traded_at: base + Duration::seconds(1),
            },
        ])
        .await
        .expect("trades should insert");

    let path = format!(
        "/v1/vwap/{}/BTCUSDT?start={}&end={}",
        fixture.exchange,
        urlencode(&rfc3339(base)),
        urlencode(&rfc3339(base + Duration::seconds(10))),
    );
    let (status, body) = fixture.get(&path).await;

    assert_eq!(status, StatusCode::OK);
    // (100*1 + 200*3) / 4 = 175, as an exact decimal string.
    assert_eq!(
        body["vwap"]
            .as_str()
            .expect("vwap should be a string")
            .parse::<f64>()
            .expect("vwap should parse"),
        175.0
    );
}

#[tokio::test]
async fn an_unknown_symbol_is_a_404_with_a_machine_readable_code() {
    let fixture = fixture!();

    let (status, body) = fixture
        .get(&format!("/v1/trades/{}/NOSUCHPAIR", fixture.exchange))
        .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn a_bad_query_parameter_is_rejected_before_the_database_is_touched() {
    let fixture = fixture!();

    let (status, body) = fixture
        .get(&format!("/v1/trades/{}/BTCUSDT?limit=0", fixture.exchange))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn a_symbol_with_no_book_anywhere_is_a_404() {
    let fixture = fixture!();

    let (status, body) = fixture
        .get(&format!("/v1/books/{}/BTCUSDT", fixture.exchange))
        .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .expect("timestamp should format")
}

/// Just enough escaping for an RFC 3339 timestamp in a query string.
fn urlencode(raw: &str) -> String {
    raw.replace('+', "%2B").replace(':', "%3A")
}
