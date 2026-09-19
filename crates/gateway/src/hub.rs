//! In-process fan-out from Redis pub/sub to connected WebSocket clients.
//!
//! The shape that matters: **one Redis subscription per gateway process, not
//! per client.** A hundred browsers watching `BTCUSDT` are a hundred
//! `broadcast` receivers behind a single `PSUBSCRIBE`, so the cost Redis sees
//! is flat in the number of clients. What each client gets is an `Arc` of the
//! publisher's own bytes: the hub parses a three-field routing header and
//! never re-serialises a book, so fanning one update out to a hundred sockets
//! is a hundred pointer clones and a hundred writes.
//!
//! Every subscriber sees every topic and filters its own, rather than the hub
//! keeping a topic-to-client index. For an instrument list of the size this
//! service follows that is the cheaper side of the trade; an exchange-wide
//! deployment would shard the channel by topic instead, and the seam for that
//! is [`Hub::subscribe`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use obe_core::Backoff;
use obe_storage::{StreamMessage, StreamSubscriber, Topic};
use tokio::sync::broadcast;
use tokio::time::Instant;

/// Messages a slow client may fall behind by before it is told it lagged.
/// Sized for a few seconds of a busy feed: long enough to ride out a stalled
/// write, short enough that the memory a wedged client pins is bounded.
pub const CAPACITY: usize = 1_024;

/// How long a reconnected subscription must stay up before the backoff
/// sequence resets, so a Redis that accepts and immediately drops connections
/// cannot turn the retry loop into a hot loop.
const HEALTHY_AFTER: Duration = Duration::from_secs(30);

/// Drops book events that are not newer than the last one forwarded for their
/// topic.
///
/// Publishes leave the ingestor through a pooled connection and can take
/// different sockets, so Redis pub/sub does not promise the order they were
/// written in. This is the same monotonic rule the cache enforces in Lua,
/// applied one layer further out — without it a reordered pair would leave
/// every client holding the older book until the next tick.
///
/// The watermark is forgotten after an idle window, matched to the cached
/// book's TTL: past that the cache itself calls the book dead, so a gateway
/// insisting on a sequence from before then would be holding a grudge on
/// behalf of data nobody can read any more.
#[derive(Debug)]
pub struct Monotonic {
    seen: HashMap<Topic, (i64, Instant)>,
    idle: Duration,
}

impl Monotonic {
    pub fn new(idle: Duration) -> Self {
        Self {
            seen: HashMap::new(),
            idle,
        }
    }

    /// Whether this message should be forwarded.
    pub fn admit(&mut self, message: &StreamMessage, now: Instant) -> bool {
        // The tape has no ordering to enforce: two trade batches are two
        // disjoint sets of trades, not two versions of one thing.
        let Some(sequence) = message.sequence else {
            return true;
        };

        match self.seen.get(&message.topic) {
            Some(&(last, at)) if sequence <= last && now.duration_since(at) < self.idle => false,
            _ => {
                self.seen.insert(message.topic.clone(), (sequence, now));
                true
            }
        }
    }

    pub fn tracked(&self) -> usize {
        self.seen.len()
    }
}

/// The fan-out point. Cloning it is cheap and shares one channel.
#[derive(Debug, Clone)]
pub struct Hub {
    tx: broadcast::Sender<Arc<StreamMessage>>,
}

impl Hub {
    pub fn new(capacity: usize) -> Self {
        Self {
            tx: broadcast::Sender::new(capacity),
        }
    }

    /// A receiver for one client. It starts empty: a subscriber is given its
    /// opening book from the cache, not from whatever happens to still be in
    /// the ring.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<StreamMessage>> {
        self.tx.subscribe()
    }

    pub fn receivers(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Hands one message to every receiver. Returns how many got it; zero
    /// means nobody is connected, which is not an error.
    pub fn dispatch(&self, message: StreamMessage) -> usize {
        self.tx.send(Arc::new(message)).unwrap_or(0)
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new(CAPACITY)
    }
}

/// Follows Redis and feeds `hub` until `shutdown` resolves.
///
/// Runs for the life of the process. A dropped subscription is expected — a
/// Redis failover, a `CLIENT KILL`, a network blip — so it is reconnected with
/// the same equal-jitter backoff the ingestor uses against the exchange.
/// Nothing is replayed across the gap: pub/sub has no history, and a client
/// that needs a starting book re-reads it from the cache.
pub async fn run(
    subscriber: StreamSubscriber,
    hub: Hub,
    mut backoff: Backoff,
    idle: Duration,
    shutdown: impl std::future::Future<Output = ()> + Send,
) {
    tokio::pin!(shutdown);
    let mut gate = Monotonic::new(idle);

    loop {
        let connected = tokio::select! {
            () = &mut shutdown => break,
            result = subscriber.subscribe() => result,
        };

        let mut subscription = match connected {
            Ok(subscription) => {
                tracing::info!(
                    pattern = %subscriber.channels().pattern(),
                    "subscribed to the market-data stream"
                );
                subscription
            }
            Err(error) => {
                let delay = backoff.next_delay();
                tracing::warn!(%error, delay_ms = delay.as_millis(), "stream subscribe failed");
                tokio::select! {
                    () = &mut shutdown => break,
                    () = tokio::time::sleep(delay) => continue,
                }
            }
        };

        let opened = Instant::now();
        loop {
            let next = tokio::select! {
                () = &mut shutdown => return,
                next = subscription.next() => next,
            };

            match next {
                Some(Ok(message)) => {
                    let now = Instant::now();
                    if gate.admit(&message, now) {
                        hub.dispatch(message);
                    } else {
                        tracing::debug!(
                            topic = %message.topic.name(),
                            sequence = message.sequence,
                            "dropped an out-of-order book event"
                        );
                    }
                }
                // A payload this binary cannot parse means a publisher is
                // writing a shape it does not know: a deploy problem, worth
                // saying out loud, but not worth dropping the connection over.
                Some(Err(error)) => tracing::warn!(%error, "undecodable stream payload"),
                None => break,
            }
        }

        if opened.elapsed() >= HEALTHY_AFTER {
            backoff.reset();
        }
        let delay = backoff.next_delay();
        tracing::warn!(
            delay_ms = delay.as_millis(),
            "the market-data stream closed; reconnecting"
        );
        tokio::select! {
            () = &mut shutdown => break,
            () = tokio::time::sleep(delay) => {}
        }
    }

    tracing::info!(topics = gate.tracked(), "stream reader stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use obe_storage::Kind;

    fn message(kind: Kind, symbol: &str, sequence: Option<i64>) -> StreamMessage {
        StreamMessage {
            topic: Topic::new(kind, "binance", symbol),
            sequence,
            payload: "{}".into(),
        }
    }

    #[test]
    fn a_reordered_book_event_is_dropped() {
        let mut gate = Monotonic::new(Duration::from_secs(30));
        let now = Instant::now();

        assert!(gate.admit(&message(Kind::Book, "BTCUSDT", Some(10)), now));
        assert!(!gate.admit(&message(Kind::Book, "BTCUSDT", Some(9)), now));
        // Equal is not newer either: it carries nothing a client does not have.
        assert!(!gate.admit(&message(Kind::Book, "BTCUSDT", Some(10)), now));
        assert!(gate.admit(&message(Kind::Book, "BTCUSDT", Some(11)), now));
    }

    #[test]
    fn each_topic_has_its_own_watermark() {
        let mut gate = Monotonic::new(Duration::from_secs(30));
        let now = Instant::now();

        assert!(gate.admit(&message(Kind::Book, "BTCUSDT", Some(500)), now));
        // A different instrument's ids have nothing to do with BTCUSDT's.
        assert!(gate.admit(&message(Kind::Book, "ETHUSDT", Some(4)), now));
        assert_eq!(gate.tracked(), 2);
    }

    #[test]
    fn the_tape_is_never_gated() {
        let mut gate = Monotonic::new(Duration::from_secs(30));
        let now = Instant::now();

        // Two trade batches are two disjoint sets of trades, not two versions
        // of one book.
        assert!(gate.admit(&message(Kind::Trades, "BTCUSDT", None), now));
        assert!(gate.admit(&message(Kind::Trades, "BTCUSDT", None), now));
        assert_eq!(gate.tracked(), 0);
    }

    #[test]
    fn a_watermark_is_forgotten_once_the_cached_book_would_be_dead() {
        let idle = Duration::from_secs(30);
        let mut gate = Monotonic::new(idle);
        let now = Instant::now();

        assert!(gate.admit(&message(Kind::Book, "BTCUSDT", Some(1_000)), now));
        assert!(!gate.admit(&message(Kind::Book, "BTCUSDT", Some(7)), now));

        // An exchange that restarts its update ids would otherwise wedge this
        // topic for the life of the process.
        let later = now + idle + Duration::from_secs(1);
        assert!(gate.admit(&message(Kind::Book, "BTCUSDT", Some(7)), later));
    }

    #[tokio::test]
    async fn every_subscriber_gets_the_same_message_without_it_being_copied() {
        let hub = Hub::new(8);
        let mut left = hub.subscribe();
        let mut right = hub.subscribe();

        assert_eq!(hub.receivers(), 2);
        assert_eq!(hub.dispatch(message(Kind::Book, "BTCUSDT", Some(1))), 2);

        let (left, right) = (left.recv().await.unwrap(), right.recv().await.unwrap());
        assert!(Arc::ptr_eq(&left, &right));
    }

    #[tokio::test]
    async fn dispatching_with_nobody_connected_is_not_an_error() {
        let hub = Hub::default();

        assert_eq!(hub.dispatch(message(Kind::Book, "BTCUSDT", Some(1))), 0);
    }

    #[tokio::test]
    async fn a_client_that_falls_behind_is_told_rather_than_silently_skipped() {
        let hub = Hub::new(2);
        let mut client = hub.subscribe();

        for sequence in 1..=5 {
            hub.dispatch(message(Kind::Book, "BTCUSDT", Some(sequence)));
        }

        // The oldest are gone, and the receiver reports how many rather than
        // handing over a feed with a hole in it.
        let error = client.recv().await.expect_err("the receiver should lag");
        assert!(
            matches!(error, broadcast::error::RecvError::Lagged(missed) if missed == 3),
            "{error:?}"
        );
        // And it resumes from the oldest message still held.
        assert_eq!(client.recv().await.unwrap().sequence, Some(4));
    }
}
