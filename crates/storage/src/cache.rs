//! Redis cache for the hot order book.
//!
//! The ingestor writes the newest snapshot per symbol here; the gateway reads
//! it instead of hitting PostgreSQL for "what does the book look like right
//! now". Entries carry a TTL, so a symbol whose ingestor has died goes cold
//! rather than serving a frozen book forever.

use std::time::{Duration, Instant};

use deadpool_redis::{Config as PoolSetup, Pool, Runtime};
use obe_core::RedisConfig;

use crate::error::{Error, Result};
use crate::model::BookSnapshot;

/// Bumped when the cached payload shape changes, so a rolling deploy cannot
/// read a new binary's snapshot with an old binary's parser.
const SCHEMA_VERSION: &str = "v1";

/// Width of the zero-padded sequence stored alongside the payload. A padded
/// decimal compares correctly with Lua's lexicographic `>=`, which keeps the
/// guard exact for every `i64` — `tonumber` would silently go through a double
/// and start tying above 2^53.
const SEQUENCE_WIDTH: usize = 20;

/// Compare-and-set on the sequence number.
///
/// Several ingestor tasks (and a reconnect replaying an older snapshot) can
/// race on the same symbol. Doing this as GET-then-SET in Rust would let a
/// stale writer win the interleaving; as a script it is one atomic step on the
/// Redis side, so an older sequence can never overwrite a newer one.
const SET_IF_NEWER: &str = r"
local stored = redis.call('HGET', KEYS[1], 'sequence')
if stored and stored >= ARGV[1] then
  return 0
end
redis.call('HSET', KEYS[1], 'sequence', ARGV[1], 'payload', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
";

/// Outcome of a cache write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    /// The snapshot became the cached book.
    Stored,
    /// A newer or equal sequence was already cached; nothing changed.
    Stale,
}

impl Write {
    pub fn is_stored(self) -> bool {
        matches!(self, Self::Stored)
    }
}

#[derive(Debug, Clone)]
pub struct BookCache {
    pool: Pool,
    prefix: String,
    ttl: Duration,
}

impl BookCache {
    /// Builds the pool without connecting, for the same reason [`crate::Store`]
    /// does: Redis being down must not stop the process from starting.
    pub fn connect_lazy(cfg: &RedisConfig) -> Result<Self> {
        let mut setup = PoolSetup::from_url(cfg.url.expose());
        setup.pool = Some(deadpool_redis::PoolConfig::new(cfg.pool_size));

        Ok(Self {
            pool: setup.create_pool(Some(Runtime::Tokio1))?,
            prefix: cfg.key_prefix.clone(),
            ttl: cfg.book_ttl(),
        })
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Key for one instrument's cached book. Exchange and ticker are
    /// normalised so `Binance/btcusdt` and `binance/BTCUSDT` cannot end up as
    /// two entries that disagree.
    pub fn book_key(&self, exchange: &str, symbol: &str) -> String {
        format!(
            "{}:{SCHEMA_VERSION}:book:{}:{}",
            self.prefix,
            exchange.trim().to_ascii_lowercase(),
            symbol.trim().to_ascii_uppercase()
        )
    }

    /// Caches the snapshot unless a newer one is already there.
    pub async fn put_book(
        &self,
        exchange: &str,
        symbol: &str,
        snapshot: &BookSnapshot,
    ) -> Result<Write> {
        if snapshot.sequence < 0 {
            return Err(Error::invalid("snapshot sequence must not be negative"));
        }

        let payload = serde_json::to_string(snapshot)?;
        let mut conn = self.pool.get().await?;

        let stored: i64 = redis::Script::new(SET_IF_NEWER)
            .key(self.book_key(exchange, symbol))
            .arg(pad_sequence(snapshot.sequence))
            .arg(payload)
            .arg(u64::try_from(self.ttl.as_millis()).unwrap_or(u64::MAX))
            .invoke_async(&mut conn)
            .await?;

        Ok(if stored == 1 {
            Write::Stored
        } else {
            Write::Stale
        })
    }

    /// The cached book, or `None` when it was never written or has expired.
    pub async fn book(&self, exchange: &str, symbol: &str) -> Result<Option<BookSnapshot>> {
        let mut conn = self.pool.get().await?;

        let payload: Option<String> = redis::cmd("HGET")
            .arg(self.book_key(exchange, symbol))
            .arg("payload")
            .query_async(&mut conn)
            .await?;

        payload
            .map(|raw| serde_json::from_str(&raw).map_err(Error::from))
            .transpose()
    }

    /// How much of the TTL a cached book has left, or `None` when there is no
    /// entry. Lets an operator see how close a symbol is to going cold.
    pub async fn ttl_remaining(&self, exchange: &str, symbol: &str) -> Result<Option<Duration>> {
        let mut conn = self.pool.get().await?;

        // PTTL answers -2 for "no such key" and -1 for "no expiry set".
        let millis: i64 = redis::cmd("PTTL")
            .arg(self.book_key(exchange, symbol))
            .query_async(&mut conn)
            .await?;

        Ok(u64::try_from(millis).ok().map(Duration::from_millis))
    }

    /// Drops the cached book. Returns `true` if there was one.
    pub async fn invalidate(&self, exchange: &str, symbol: &str) -> Result<bool> {
        let mut conn = self.pool.get().await?;

        let removed: i64 = redis::cmd("DEL")
            .arg(self.book_key(exchange, symbol))
            .query_async(&mut conn)
            .await?;

        Ok(removed > 0)
    }

    /// Round-trips a `PING` and reports how long it took.
    pub async fn ping(&self) -> Result<Duration> {
        let started = Instant::now();
        let mut conn = self.pool.get().await?;
        let reply: String = redis::cmd("PING").query_async(&mut conn).await?;

        if reply != "PONG" {
            return Err(Error::invalid(format!("unexpected PING reply `{reply}`")));
        }
        Ok(started.elapsed())
    }
}

fn pad_sequence(sequence: i64) -> String {
    format!("{sequence:0width$}", width = SEQUENCE_WIDTH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obe_core::Settings;

    fn cache() -> BookCache {
        let settings = Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
            .expect("repository config should load");
        BookCache::connect_lazy(&settings.redis).expect("pool should build without connecting")
    }

    #[test]
    fn keys_are_namespaced_and_normalised() {
        let cache = cache();

        assert_eq!(
            cache.book_key("Binance", " btcusdt "),
            "obe:v1:book:binance:BTCUSDT"
        );
        assert_eq!(
            cache.book_key("binance", "BTCUSDT"),
            cache.book_key("BINANCE", "btcusdt")
        );
    }

    #[test]
    fn padded_sequences_order_lexicographically() {
        // The property the Lua guard relies on, including past 2^53 where a
        // numeric comparison in Lua would start tying.
        let below = pad_sequence(9_007_199_254_740_992);
        let above = pad_sequence(9_007_199_254_740_993);

        assert_eq!(below.len(), SEQUENCE_WIDTH);
        assert!(above > below);
        assert!(pad_sequence(10) > pad_sequence(9));
        assert!(pad_sequence(i64::MAX) > pad_sequence(i64::MAX - 1));
    }

    #[tokio::test]
    async fn connect_lazy_does_not_touch_the_network() {
        let settings = Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
            .expect("repository config should load");
        let mut cfg = settings.redis;
        cfg.url = "redis://127.0.0.1:1".into();

        assert!(BookCache::connect_lazy(&cfg).is_ok());
    }
}
