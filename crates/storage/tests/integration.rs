//! End-to-end tests against a real PostgreSQL and Redis.
//!
//! They run when `OBE_TEST_DATABASE_URL` and `OBE_TEST_REDIS_URL` are set —
//! CI points them at service containers — and skip with a note otherwise, so
//! `cargo test` still works on a laptop with nothing running.
//!
//! Every test namespaces its own data (a unique exchange name, a unique cache
//! prefix), so the suite stays correct running in parallel against one shared
//! database.

use std::sync::atomic::{AtomicU64, Ordering};

use obe_core::Settings;
use obe_storage::{
    BookCache, BookSnapshot, Kind, Level, NewSymbol, NewTrade, Side, Store, StreamEvent,
    StreamPublisher, StreamSubscriber, StreamTrade, Topic, TradeQuery,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use time::{Duration, OffsetDateTime};
use tokio::sync::OnceCell;

const DATABASE_URL: &str = "OBE_TEST_DATABASE_URL";
const REDIS_URL: &str = "OBE_TEST_REDIS_URL";

/// Migrations are global state; run them once for the whole test binary.
static MIGRATED: OnceCell<()> = OnceCell::const_new();

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A name no other test in this run will pick.
fn unique(kind: &str) -> String {
    let nth = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
    format!("{kind}-{nanos}-{nth}")
}

fn settings() -> Settings {
    Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
        .expect("repository config should load")
}

/// `None` (with a printed note) when the environment has no services.
async fn store() -> Option<Store> {
    let Ok(url) = std::env::var(DATABASE_URL) else {
        eprintln!("skipping: {DATABASE_URL} is not set");
        return None;
    };

    let mut cfg = settings().database;
    cfg.url = url.as_str().into();
    let store = Store::connect_lazy(&cfg).expect("pool should build");

    MIGRATED
        .get_or_init(|| async {
            store.migrate().await.expect("migrations should apply");
        })
        .await;

    Some(store)
}

fn cache() -> Option<BookCache> {
    let Ok(url) = std::env::var(REDIS_URL) else {
        eprintln!("skipping: {REDIS_URL} is not set");
        return None;
    };

    let mut cfg = settings().redis;
    cfg.url = url.as_str().into();
    cfg.key_prefix = unique("obetest");

    Some(BookCache::connect_lazy(&cfg).expect("pool should build"))
}

async fn symbol(store: &Store) -> i32 {
    store
        .upsert_symbol(&NewSymbol {
            exchange: unique("exchange"),
            symbol: "BTCUSDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            price_precision: 2,
            quantity_precision: 8,
            active: true,
        })
        .await
        .expect("symbol should upsert")
        .id
}

fn trade(symbol_id: i32, id: i64, price: Decimal, qty: Decimal, at: OffsetDateTime) -> NewTrade {
    NewTrade::new(symbol_id, id, price, qty, Side::Buy, at)
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let Some(store) = store().await else { return };

    // The shared migration already ran; running it again must be a no-op
    // rather than an error, which is what a redeploy does.
    store.migrate().await.expect("second run should be a no-op");
    assert!(store.ping().await.is_ok());
}

#[tokio::test]
async fn upsert_symbol_is_idempotent_and_updates_in_place() {
    let Some(store) = store().await else { return };

    let exchange = unique("exchange");
    let mut new = NewSymbol {
        exchange: exchange.clone(),
        symbol: "ETHUSDT".into(),
        base_asset: "ETH".into(),
        quote_asset: "USDT".into(),
        price_precision: 2,
        quantity_precision: 6,
        active: true,
    };

    let first = store.upsert_symbol(&new).await.expect("first upsert");

    new.quantity_precision = 8;
    new.active = false;
    let second = store.upsert_symbol(&new).await.expect("second upsert");

    assert_eq!(first.id, second.id, "upsert must not create a second row");
    assert_eq!(second.quantity_precision, 8);
    assert!(!second.active);
    assert!(second.updated_at >= first.updated_at);

    let found = store
        .symbol_by_pair(&exchange, "ETHUSDT")
        .await
        .expect("lookup should succeed");
    assert_eq!(found.map(|s| s.id), Some(first.id));

    let active_only = store.list_symbols(true).await.expect("list");
    assert!(!active_only.iter().any(|s| s.id == first.id));
}

#[tokio::test]
async fn a_replayed_batch_inserts_nothing_twice() {
    let Some(store) = store().await else { return };
    let symbol_id = symbol(&store).await;
    let now = OffsetDateTime::now_utc();

    let batch = vec![
        trade(
            symbol_id,
            1,
            dec!(30000.5),
            dec!(0.25),
            now - Duration::seconds(2),
        ),
        trade(
            symbol_id,
            2,
            dec!(30001.0),
            dec!(0.50),
            now - Duration::seconds(1),
        ),
        trade(symbol_id, 3, dec!(30002.0), dec!(0.25), now),
    ];

    assert_eq!(store.insert_trades(&batch).await.expect("first insert"), 3);

    // What a reconnect does: the feed replays the tail, overlapping with what
    // is already stored. The unique (symbol_id, exchange_trade_id) key turns
    // the overlap into a no-op and only the genuinely new trade lands.
    let replay = vec![
        batch[1].clone(),
        batch[2].clone(),
        trade(
            symbol_id,
            4,
            dec!(30003.0),
            dec!(0.10),
            now + Duration::seconds(1),
        ),
    ];

    assert_eq!(store.insert_trades(&replay).await.expect("replay"), 1);
    assert_eq!(store.count_trades(symbol_id).await.expect("count"), 4);
}

#[tokio::test]
async fn trade_history_honours_the_window_and_limit() {
    let Some(store) = store().await else { return };
    let symbol_id = symbol(&store).await;
    let base = OffsetDateTime::now_utc() - Duration::hours(1);

    let batch: Vec<NewTrade> = (0..10)
        .map(|i| {
            trade(
                symbol_id,
                i,
                dec!(100) + Decimal::from(i),
                dec!(1),
                base + Duration::minutes(i),
            )
        })
        .collect();
    assert_eq!(store.insert_trades(&batch).await.expect("insert"), 10);

    let window = store
        .trades(
            TradeQuery::new(symbol_id)
                .between(base + Duration::minutes(2), base + Duration::minutes(5))
                .limit(100),
        )
        .await
        .expect("query");

    // Start inclusive, end exclusive: minutes 2, 3 and 4, newest first.
    let ids: Vec<i64> = window.iter().map(|t| t.exchange_trade_id).collect();
    assert_eq!(ids, vec![4, 3, 2]);

    let capped = store
        .trades(TradeQuery::new(symbol_id).limit(3))
        .await
        .expect("query");
    assert_eq!(capped.len(), 3);
    assert_eq!(capped[0].exchange_trade_id, 9);

    let latest = store.latest_trade(symbol_id).await.expect("latest");
    assert_eq!(latest.map(|t| t.exchange_trade_id), Some(9));
}

#[tokio::test]
async fn numeric_columns_survive_the_round_trip_exactly() {
    let Some(store) = store().await else { return };
    let symbol_id = symbol(&store).await;
    let at = OffsetDateTime::now_utc();

    // Twelve decimal places and a value a f64 cannot hold exactly.
    let price = dec!(30000.123456789012);
    let quantity = dec!(0.000000000001);
    store
        .insert_trades(&[trade(symbol_id, 1, price, quantity, at)])
        .await
        .expect("insert");

    let stored = store
        .latest_trade(symbol_id)
        .await
        .expect("query")
        .expect("one trade");

    assert_eq!(stored.price, price);
    assert_eq!(stored.quantity, quantity);
}

#[tokio::test]
async fn vwap_is_computed_in_the_database() {
    let Some(store) = store().await else { return };
    let symbol_id = symbol(&store).await;
    let base = OffsetDateTime::now_utc() - Duration::hours(2);

    store
        .insert_trades(&[
            trade(symbol_id, 1, dec!(100), dec!(1), base),
            trade(
                symbol_id,
                2,
                dec!(200),
                dec!(3),
                base + Duration::minutes(1),
            ),
        ])
        .await
        .expect("insert");

    let vwap = store
        .vwap(symbol_id, base, base + Duration::hours(1))
        .await
        .expect("vwap");

    // (100*1 + 200*3) / 4
    assert_eq!(vwap, Some(dec!(175)));

    let empty = store
        .vwap(
            symbol_id,
            base + Duration::hours(3),
            base + Duration::hours(4),
        )
        .await
        .expect("vwap");
    assert_eq!(empty, None);
}

#[tokio::test]
async fn retention_sweep_deletes_only_old_trades() {
    let Some(store) = store().await else { return };
    let symbol_id = symbol(&store).await;
    let now = OffsetDateTime::now_utc();

    store
        .insert_trades(&[
            trade(symbol_id, 1, dec!(1), dec!(1), now - Duration::days(30)),
            trade(symbol_id, 2, dec!(1), dec!(1), now),
        ])
        .await
        .expect("insert");

    let deleted = store
        .prune_trades_before(now - Duration::days(7))
        .await
        .expect("prune");

    assert_eq!(deleted, 1);
    assert_eq!(store.count_trades(symbol_id).await.expect("count"), 1);
}

fn snapshot(symbol_id: i32, sequence: i64, at: OffsetDateTime) -> BookSnapshot {
    BookSnapshot {
        symbol_id,
        sequence,
        captured_at: at,
        bids: vec![
            Level::new(dec!(30000.10), dec!(1.5)),
            Level::new(dec!(30000.00), dec!(2.25)),
        ],
        asks: vec![
            Level::new(dec!(30000.20), dec!(0.75)),
            Level::new(dec!(30000.30), dec!(3.0)),
        ],
    }
}

#[tokio::test]
async fn snapshots_are_stored_once_per_sequence() {
    let Some(store) = store().await else { return };
    let symbol_id = symbol(&store).await;
    let now = OffsetDateTime::now_utc();

    let first = snapshot(symbol_id, 100, now);
    assert!(store.insert_snapshot(&first).await.expect("insert"));
    assert!(
        !store.insert_snapshot(&first).await.expect("replay"),
        "the same sequence must not be stored twice"
    );

    let later = snapshot(symbol_id, 101, now + Duration::seconds(1));
    assert!(store.insert_snapshot(&later).await.expect("insert"));

    let latest = store
        .latest_snapshot(symbol_id)
        .await
        .expect("query")
        .expect("a snapshot");
    assert_eq!(latest.sequence, 101);
    assert_eq!(latest.bids, later.bids, "jsonb decimals must round-trip");
    assert_eq!(latest.spread(), Some(dec!(0.10)));

    let as_of = store
        .snapshot_as_of(symbol_id, now)
        .await
        .expect("query")
        .expect("a snapshot");
    assert_eq!(as_of.sequence, 100);
}

#[tokio::test]
async fn a_crossed_snapshot_is_refused_before_it_reaches_the_database() {
    let Some(store) = store().await else { return };
    let symbol_id = symbol(&store).await;

    let mut crossed = snapshot(symbol_id, 200, OffsetDateTime::now_utc());
    crossed.bids[0] = Level::new(dec!(99999), dec!(1));

    assert!(store.insert_snapshot(&crossed).await.is_err());
    assert!(store
        .latest_snapshot(symbol_id)
        .await
        .expect("query")
        .is_none());
}

#[tokio::test]
async fn cache_round_trips_a_book() {
    let Some(cache) = cache() else { return };
    let book = snapshot(1, 10, OffsetDateTime::now_utc());

    assert!(cache.ping().await.is_ok());
    assert!(cache
        .book("binance", "BTCUSDT")
        .await
        .expect("get")
        .is_none());

    assert!(cache
        .put_book("binance", "BTCUSDT", &book)
        .await
        .expect("put")
        .is_stored());

    let cached = cache
        .book("Binance", "btcusdt")
        .await
        .expect("get")
        .expect("a cached book");
    assert_eq!(cached, book, "normalised keys must hit the same entry");

    assert!(cache.invalidate("binance", "BTCUSDT").await.expect("del"));
    assert!(cache
        .book("binance", "BTCUSDT")
        .await
        .expect("get")
        .is_none());
}

#[tokio::test]
async fn an_older_sequence_never_overwrites_a_newer_book() {
    let Some(cache) = cache() else { return };
    let now = OffsetDateTime::now_utc();

    let newer = snapshot(1, 500, now);
    let older = snapshot(1, 499, now - Duration::seconds(1));

    assert!(cache
        .put_book("binance", "BTCUSDT", &newer)
        .await
        .expect("put")
        .is_stored());

    // What a reconnecting ingestor sends: a snapshot from before the gap.
    assert!(
        !cache
            .put_book("binance", "BTCUSDT", &older)
            .await
            .expect("put")
            .is_stored(),
        "a stale sequence must be rejected"
    );

    // Equal sequences are rejected too: there is nothing to gain from a write.
    assert!(!cache
        .put_book("binance", "BTCUSDT", &newer)
        .await
        .expect("put")
        .is_stored());

    let cached = cache
        .book("binance", "BTCUSDT")
        .await
        .expect("get")
        .expect("a cached book");
    assert_eq!(cached.sequence, 500);
}

#[tokio::test]
async fn cached_books_carry_an_expiry() {
    let Some(cache) = cache() else { return };
    let book = snapshot(1, 1, OffsetDateTime::now_utc());

    assert_eq!(
        cache
            .ttl_remaining("binance", "ETHUSDT")
            .await
            .expect("pttl"),
        None,
        "a missing key has no TTL"
    );

    cache
        .put_book("binance", "ETHUSDT", &book)
        .await
        .expect("put");

    // The TTL is (re)set on every write, so a symbol whose ingestor has died
    // goes cold instead of serving a frozen book for ever.
    let remaining = cache
        .ttl_remaining("binance", "ETHUSDT")
        .await
        .expect("pttl")
        .expect("a live TTL");

    assert!(
        remaining <= cache.ttl(),
        "{remaining:?} > {:?}",
        cache.ttl()
    );
    assert!(remaining > std::time::Duration::ZERO);
}

/// A publisher and a subscriber sharing one namespace, or `None` when Redis is
/// not configured.
fn stream() -> Option<(StreamPublisher, StreamSubscriber)> {
    let Ok(url) = std::env::var(REDIS_URL) else {
        eprintln!("skipping: {REDIS_URL} is not set");
        return None;
    };

    let mut cfg = settings().redis;
    cfg.url = url.as_str().into();
    cfg.key_prefix = unique("obetest");

    Some((
        StreamPublisher::connect_lazy(&cfg).expect("publisher should build"),
        StreamSubscriber::connect_lazy(&cfg).expect("subscriber should build"),
    ))
}

fn book_event(exchange: &str, sequence: i64) -> StreamEvent {
    StreamEvent::Book {
        exchange: exchange.to_owned(),
        symbol: "BTCUSDT".into(),
        sequence,
        captured_at: OffsetDateTime::now_utc(),
        bids: vec![Level::new(dec!(30000.10), dec!(1.5))],
        asks: vec![Level::new(dec!(30000.20), dec!(0.75))],
    }
}

/// `PUBLISH` reports how many subscribers it reached, but a `PSUBSCRIBE` that
/// has been acknowledged by the client is not always visible to a publisher on
/// another connection yet. Retry until it is, rather than sleeping a guess.
async fn publish_until_delivered(publisher: &StreamPublisher, event: &StreamEvent) {
    for _ in 0..100 {
        if publisher.publish(event).await.expect("publish") > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("the subscription never became visible to the publisher");
}

#[tokio::test]
async fn an_event_reaches_a_subscriber_with_its_payload_untouched() {
    let Some((publisher, subscriber)) = stream() else {
        return;
    };
    let exchange = unique("exchange");
    let mut subscription = subscriber.subscribe().await.expect("subscribe");

    let event = book_event(&exchange, 4_242);
    publish_until_delivered(&publisher, &event).await;

    let message = subscription
        .next()
        .await
        .expect("the subscription should stay open")
        .expect("the payload should route");

    assert_eq!(message.topic, Topic::new(Kind::Book, &exchange, "BTCUSDT"));
    assert_eq!(message.sequence, Some(4_242));
    // The gateway forwards these bytes verbatim, so they have to survive the
    // round trip exactly, decimals included.
    assert_eq!(
        serde_json::from_str::<StreamEvent>(&message.payload).expect("payload should decode"),
        event
    );
}

#[tokio::test]
async fn one_pattern_subscription_covers_every_instrument() {
    let Some((publisher, subscriber)) = stream() else {
        return;
    };
    let exchange = unique("exchange");
    let mut subscription = subscriber.subscribe().await.expect("subscribe");

    // Subscribed by glob, so an instrument the gateway never heard of at
    // startup still arrives.
    publish_until_delivered(&publisher, &book_event(&exchange, 1)).await;

    let tape = StreamEvent::Trades {
        exchange: exchange.clone(),
        symbol: "SOLUSDT".into(),
        trades: vec![StreamTrade {
            trade_id: 7,
            price: dec!(100.25),
            quantity: dec!(0.5),
            side: Side::Sell,
            traded_at: OffsetDateTime::now_utc(),
        }],
    };
    assert!(publisher.publish(&tape).await.expect("publish") > 0);

    let mut seen = Vec::new();
    for _ in 0..2 {
        let message = subscription
            .next()
            .await
            .expect("the subscription should stay open")
            .expect("the payload should route");
        seen.push(message.topic.name());
    }

    assert!(
        seen.contains(&format!("book:{exchange}:BTCUSDT")),
        "{seen:?}"
    );
    assert!(
        seen.contains(&format!("trades:{exchange}:SOLUSDT")),
        "{seen:?}"
    );
}

#[tokio::test]
async fn publishing_with_nobody_listening_is_not_an_error() {
    let Some((publisher, _)) = stream() else {
        return;
    };

    // The normal state of a service with no clients attached. Ingestion must
    // not treat it as a failure.
    assert_eq!(
        publisher
            .publish(&book_event(&unique("exchange"), 1))
            .await
            .expect("publish"),
        0
    );
}
