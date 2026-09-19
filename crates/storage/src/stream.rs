//! The live stream: Redis pub/sub between the ingestor and the gateways.
//!
//! The cache in [`crate::cache`] answers "what does the book look like now";
//! this answers "tell me the moment it changes". They are deliberately
//! separate channels through the same Redis: a gateway that has just accepted
//! a subscriber reads the cache once to get it a starting book, then follows
//! the stream, which is the same snapshot-then-deltas shape the ingestor uses
//! against the exchange.
//!
//! Three things are worth knowing about the transport.
//!
//! Pub/sub is fire-and-forget. Redis does not buffer for a subscriber that is
//! not connected and does not tell the publisher who missed what, so nothing
//! downstream may treat the stream as a source of truth — that is what
//! PostgreSQL is for. A gateway that reconnects re-reads the cache rather than
//! trying to replay.
//!
//! Delivery is not ordered end to end. Publishes leave through a pooled
//! connection, so two of them can take different sockets and arrive in the
//! other order; subscribers can also be fed by different Redis replicas.
//! Every book event therefore carries the exchange's own sequence number, and
//! a consumer is expected to drop anything that is not newer — the same
//! monotonic guard the cache applies in Lua, enforced one layer further out.
//!
//! The payload is JSON with decimals as strings, like everything else that
//! leaves this service, so no hop in the chain can turn `0.1` into a float.

use deadpool_redis::{Config as PoolSetup, Pool, Runtime};
use futures_util::StreamExt;
use obe_core::RedisConfig;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::model::{Level, Side};

/// Bumped with the payload shape, so a rolling deploy cannot have a new
/// ingestor's events parsed by an old gateway.
const SCHEMA_VERSION: &str = "v1";

/// What a stream channel carries. Separate kinds because they move at very
/// different rates: the book is republished on a fixed tick, the tape goes out
/// once per flush window, and a subscriber that only wants one should not pay
/// for the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Book,
    Trades,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Book => "book",
            Self::Trades => "trades",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "book" => Some(Self::Book),
            "trades" => Some(Self::Trades),
            _ => None,
        }
    }
}

/// One subscribable feed: a kind for one instrument on one exchange.
///
/// Normalised on construction the way cache keys are, so `binance/btcusdt` and
/// `Binance/BTCUSDT` are one topic rather than two that disagree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    pub kind: Kind,
    pub exchange: String,
    pub symbol: String,
}

impl Topic {
    pub fn new(kind: Kind, exchange: &str, symbol: &str) -> Self {
        Self {
            kind,
            exchange: exchange.trim().to_ascii_lowercase(),
            symbol: symbol.trim().to_ascii_uppercase(),
        }
    }

    /// The wire name clients subscribe by: `book:binance:BTCUSDT`.
    pub fn name(&self) -> String {
        format!("{}:{}:{}", self.kind.as_str(), self.exchange, self.symbol)
    }

    /// Parses a client-supplied topic name. `None` for anything malformed;
    /// the caller decides what to tell the client.
    pub fn parse(raw: &str) -> Option<Self> {
        let mut parts = raw.split(':');
        let kind = Kind::parse(parts.next()?.trim())?;
        let exchange = parts.next()?.trim();
        let symbol = parts.next()?.trim();

        if parts.next().is_some() || exchange.is_empty() || symbol.is_empty() {
            return None;
        }
        Some(Self::new(kind, exchange, symbol))
    }
}

/// A trade as it appears on the stream.
///
/// Not [`crate::Trade`]: that carries the database's own id and ingest
/// timestamp, neither of which a live subscriber has any use for, and the
/// stream must not imply the row has been committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrade {
    pub trade_id: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub side: Side,
    #[serde(with = "time::serde::rfc3339")]
    pub traded_at: OffsetDateTime,
}

/// What goes onto a channel. The `type` tag is the first field a consumer
/// reads, and it is also what the gateway routes on without deserialising the
/// levels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StreamEvent {
    Book {
        exchange: String,
        symbol: String,
        /// The exchange's update id. Monotonic per symbol, and the only thing
        /// that makes an unordered transport usable.
        sequence: i64,
        #[serde(with = "time::serde::rfc3339")]
        captured_at: OffsetDateTime,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Trades {
        exchange: String,
        symbol: String,
        trades: Vec<StreamTrade>,
    },
}

impl StreamEvent {
    pub fn kind(&self) -> Kind {
        match self {
            Self::Book { .. } => Kind::Book,
            Self::Trades { .. } => Kind::Trades,
        }
    }

    pub fn exchange(&self) -> &str {
        match self {
            Self::Book { exchange, .. } | Self::Trades { exchange, .. } => exchange,
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Book { symbol, .. } | Self::Trades { symbol, .. } => symbol,
        }
    }

    /// The sequence a book event carries; `None` for the tape, which has no
    /// ordering to enforce.
    pub fn sequence(&self) -> Option<i64> {
        match self {
            Self::Book { sequence, .. } => Some(*sequence),
            Self::Trades { .. } => None,
        }
    }

    pub fn topic(&self) -> Topic {
        Topic::new(self.kind(), self.exchange(), self.symbol())
    }
}

/// Just the routing header.
///
/// Serde ignores the fields this leaves out, so a gateway fanning a message
/// out to a hundred subscribers parses three short strings instead of a
/// twenty-level book, and forwards the publisher's bytes untouched.
#[derive(Debug, Clone, Deserialize)]
struct Header {
    #[serde(rename = "type")]
    kind: Kind,
    exchange: String,
    symbol: String,
    #[serde(default)]
    sequence: Option<i64>,
}

/// A message as it came off Redis: where it belongs, and the exact bytes the
/// publisher wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMessage {
    pub topic: Topic,
    /// `None` for the tape.
    pub sequence: Option<i64>,
    pub payload: String,
}

impl StreamMessage {
    /// Routes a raw payload without decoding the body.
    pub fn parse(payload: String) -> Result<Self> {
        let header: Header = serde_json::from_str(&payload)?;

        Ok(Self {
            topic: Topic::new(header.kind, &header.exchange, &header.symbol),
            sequence: header.sequence,
            payload,
        })
    }
}

/// Channel names, shared by the publisher and the subscriber so the two cannot
/// drift apart.
#[derive(Debug, Clone)]
pub struct Channels {
    prefix: String,
}

impl Channels {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// One channel per instrument, carrying both kinds. A channel per kind
    /// would double the subscriptions to save filtering three bytes of JSON.
    pub fn channel(&self, exchange: &str, symbol: &str) -> String {
        format!(
            "{}:{SCHEMA_VERSION}:stream:{}:{}",
            self.prefix,
            exchange.trim().to_ascii_lowercase(),
            symbol.trim().to_ascii_uppercase()
        )
    }

    /// Glob for every instrument, so one `PSUBSCRIBE` covers a gateway that
    /// does not know the instrument list at startup and does not have to
    /// resubscribe when the ingestor adds one.
    pub fn pattern(&self) -> String {
        format!("{}:{SCHEMA_VERSION}:stream:*", self.prefix)
    }
}

/// Writes events onto the stream.
#[derive(Debug, Clone)]
pub struct StreamPublisher {
    pool: Pool,
    channels: Channels,
}

impl StreamPublisher {
    /// Built without connecting, like every other handle here: Redis being
    /// down must not stop the ingestor from starting and reconstructing books.
    pub fn connect_lazy(cfg: &RedisConfig) -> Result<Self> {
        let mut setup = PoolSetup::from_url(cfg.url.expose());
        setup.pool = Some(deadpool_redis::PoolConfig::new(cfg.pool_size));

        Ok(Self {
            pool: setup.create_pool(Some(Runtime::Tokio1))?,
            channels: Channels::new(cfg.key_prefix.clone()),
        })
    }

    pub fn channels(&self) -> &Channels {
        &self.channels
    }

    /// Publishes one event. Returns how many subscribers Redis handed it to,
    /// which is zero when nobody is listening — not an error, just the usual
    /// state of a service with no clients attached.
    pub async fn publish(&self, event: &StreamEvent) -> Result<u32> {
        let channel = self.channels.channel(event.exchange(), event.symbol());
        let payload = serde_json::to_string(event)?;
        let mut conn = self.pool.get().await?;

        Ok(redis::cmd("PUBLISH")
            .arg(channel)
            .arg(payload)
            .query_async(&mut conn)
            .await?)
    }
}

/// Reads events off the stream.
///
/// Deliberately not pooled: `SUBSCRIBE` puts a Redis connection into a mode
/// where it no longer answers ordinary commands, so handing one back to a pool
/// shared with `GET` and `PUBLISH` would poison it.
#[derive(Debug, Clone)]
pub struct StreamSubscriber {
    client: redis::Client,
    channels: Channels,
}

impl StreamSubscriber {
    pub fn connect_lazy(cfg: &RedisConfig) -> Result<Self> {
        Ok(Self {
            client: redis::Client::open(cfg.url.expose())?,
            channels: Channels::new(cfg.key_prefix.clone()),
        })
    }

    pub fn channels(&self) -> &Channels {
        &self.channels
    }

    /// Opens a connection subscribed to every instrument's channel.
    pub async fn subscribe(&self) -> Result<Subscription> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.psubscribe(self.channels.pattern()).await?;

        Ok(Subscription { pubsub })
    }
}

/// A live subscription. Dropping it closes the connection.
pub struct Subscription {
    pubsub: redis::aio::PubSub,
}

/// Hand-written because `redis::aio::PubSub` is not `Debug`, and the workspace
/// warns on types that are not.
impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription").finish_non_exhaustive()
    }
}

impl Subscription {
    /// The next message, or `None` once the connection is gone.
    ///
    /// A payload that does not parse is reported rather than swallowed: it
    /// means a publisher is writing a shape this binary does not understand,
    /// which is a deploy problem, not a data problem.
    pub async fn next(&mut self) -> Option<Result<StreamMessage>> {
        let message = self.pubsub.on_message().next().await?;

        Some(
            message
                .get_payload::<String>()
                .map_err(Error::from)
                .and_then(StreamMessage::parse),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn book() -> StreamEvent {
        StreamEvent::Book {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            sequence: 71_283_044,
            captured_at: OffsetDateTime::UNIX_EPOCH,
            bids: vec![Level::new(dec!(0.1), dec!(2))],
            asks: vec![Level::new(dec!(100.5), dec!(1))],
        }
    }

    fn tape() -> StreamEvent {
        StreamEvent::Trades {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            trades: vec![StreamTrade {
                trade_id: 9,
                price: dec!(100),
                quantity: dec!(0.5),
                side: Side::Buy,
                traded_at: OffsetDateTime::UNIX_EPOCH,
            }],
        }
    }

    #[test]
    fn topics_are_normalised_and_round_trip_through_their_name() {
        let topic = Topic::new(Kind::Book, " Binance ", "btcusdt");

        assert_eq!(topic.name(), "book:binance:BTCUSDT");
        assert_eq!(Topic::parse("book:BINANCE:btcusdt"), Some(topic));
    }

    #[test]
    fn malformed_topic_names_are_rejected_rather_than_guessed() {
        for raw in [
            "",
            "book",
            "book:binance",
            "book:binance:BTCUSDT:extra",
            "quotes:binance:BTCUSDT",
            "book::BTCUSDT",
            "book:binance: ",
        ] {
            assert_eq!(Topic::parse(raw), None, "{raw}");
        }
    }

    #[test]
    fn channels_are_namespaced_and_covered_by_the_pattern() {
        let channels = Channels::new("obe");

        assert_eq!(
            channels.channel("Binance", " btcusdt "),
            "obe:v1:stream:binance:BTCUSDT"
        );
        let pattern = channels.pattern();
        let (head, _) = pattern
            .split_once('*')
            .expect("pattern should end in a glob");
        assert!(channels.channel("binance", "ETHUSDT").starts_with(head));
    }

    #[test]
    fn a_book_event_carries_its_sequence_and_prices_as_strings() {
        let payload = serde_json::to_string(&book()).unwrap();

        assert!(payload.contains(r#""type":"book""#), "{payload}");
        assert!(payload.contains(r#""price":"0.1""#), "{payload}");

        let parsed: StreamEvent = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed, book());
        assert_eq!(parsed.sequence(), Some(71_283_044));
        assert_eq!(parsed.topic().name(), "book:binance:BTCUSDT");
    }

    #[test]
    fn the_tape_has_no_sequence_to_enforce() {
        let parsed: StreamEvent = serde_json::from_str(&serde_json::to_string(&tape()).unwrap())
            .expect("tape should round trip");

        assert_eq!(parsed.sequence(), None);
        assert_eq!(parsed.topic().name(), "trades:binance:BTCUSDT");
    }

    #[test]
    fn routing_reads_the_header_and_leaves_the_payload_alone() {
        let payload = serde_json::to_string(&book()).unwrap();
        let message = StreamMessage::parse(payload.clone()).expect("header should parse");

        assert_eq!(message.topic, Topic::new(Kind::Book, "binance", "BTCUSDT"));
        assert_eq!(message.sequence, Some(71_283_044));
        // Forwarded byte for byte: the gateway never re-serialises a book.
        assert_eq!(message.payload, payload);
    }

    #[test]
    fn a_payload_from_an_unknown_publisher_is_an_error_not_a_silent_drop() {
        assert!(StreamMessage::parse(r#"{"type":"quotes"}"#.into()).is_err());
        assert!(StreamMessage::parse("not json".into()).is_err());
    }

    #[tokio::test]
    async fn the_handles_build_without_touching_the_network() {
        let settings =
            obe_core::Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
                .expect("repository config should load");
        let mut cfg = settings.redis;
        cfg.url = "redis://127.0.0.1:1".into();

        assert!(StreamPublisher::connect_lazy(&cfg).is_ok());
        assert!(StreamSubscriber::connect_lazy(&cfg).is_ok());
    }
}
