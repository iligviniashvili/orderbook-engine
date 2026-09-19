//! What a `/v1/stream` client says and hears.
//!
//! Split out from the socket handling so the whole conversation — every
//! command, every rejection, every ack — is a pure function of a string and is
//! covered by tests that open no socket and touch no Redis.

use obe_storage::Topic;
use serde::{Deserialize, Serialize};

/// Topics one connection may hold. A subscription is nearly free on the Redis
/// side — every gateway already sees every topic — but each one costs a JSON
/// encode and a socket write per update, so a single connection is not allowed
/// to ask for unbounded work.
pub const MAX_TOPICS: usize = 64;

/// What a client sends.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Command {
    Subscribe {
        channels: Vec<String>,
    },
    Unsubscribe {
        channels: Vec<String>,
    },
    /// Application-level, distinct from the protocol ping the server sends on
    /// its own timer: this one a client can use to check the socket without
    /// depending on its WebSocket library exposing control frames.
    Ping,
}

/// Why a command was refused. The `code` is what a client branches on; the
/// message is for whoever is reading the console.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub code: &'static str,
    pub message: String,
}

impl Rejection {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// What the server sends, other than the market-data payloads, which are
/// forwarded as the publisher wrote them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Frame {
    /// The full topic set after the command, not the delta, so a client never
    /// has to track what it thinks it asked for.
    Subscribed {
        channels: Vec<String>,
    },
    Pong,
    /// The connection fell behind and `missed` messages were dropped. Said out
    /// loud rather than swallowed: a gap a client does not know about is a
    /// chart that is quietly wrong. The right response is to re-read
    /// `/v1/books/...` and carry on.
    Lagged {
        missed: u64,
    },
    Error {
        code: &'static str,
        message: String,
    },
}

impl Frame {
    pub fn error(rejection: Rejection) -> Self {
        Self::Error {
            code: rejection.code,
            message: rejection.message,
        }
    }

    pub fn encode(&self) -> String {
        // The variants are plain data with no map keys that can fail, so this
        // cannot realistically fail; a fallback beats an unwrap on a socket.
        serde_json::to_string(self).unwrap_or_else(|_| {
            r#"{"type":"error","code":"internal","message":"frame encoding failed"}"#.to_owned()
        })
    }
}

impl Command {
    pub fn parse(raw: &str) -> Result<Self, Rejection> {
        serde_json::from_str(raw).map_err(|error| {
            Rejection::new(
                "invalid_command",
                format!("could not parse command: {error}"),
            )
        })
    }
}

/// One connection's topic set.
#[derive(Debug, Default)]
pub struct Subscriptions {
    topics: Vec<Topic>,
}

impl Subscriptions {
    pub fn contains(&self, topic: &Topic) -> bool {
        self.topics.contains(topic)
    }

    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }

    pub fn len(&self) -> usize {
        self.topics.len()
    }

    /// The current set, in the order it was built, for the ack.
    pub fn names(&self) -> Vec<String> {
        self.topics.iter().map(Topic::name).collect()
    }

    /// Adds topics, returning the ones that were not already held so the
    /// caller knows which need an opening snapshot.
    ///
    /// All-or-nothing: a request naming one bad topic changes nothing, so a
    /// client that mistyped one of ten does not end up half subscribed and
    /// having to work out which half.
    pub fn add(&mut self, channels: &[String]) -> Result<Vec<Topic>, Rejection> {
        let parsed = parse_all(channels)?;

        // Deduped against what is held *and* against the rest of this
        // command: `book:Binance:btcusdt` and `book:binance:BTCUSDT` normalise
        // to one topic, and sending both must not buy two opening snapshots.
        let mut added: Vec<Topic> = Vec::with_capacity(parsed.len());
        for topic in parsed {
            if !self.topics.contains(&topic) && !added.contains(&topic) {
                added.push(topic);
            }
        }

        if self.topics.len() + added.len() > MAX_TOPICS {
            return Err(Rejection::new(
                "too_many_topics",
                format!("a connection may hold at most {MAX_TOPICS} topics"),
            ));
        }

        self.topics.extend(added.iter().cloned());
        Ok(added)
    }

    /// Removes topics. Unsubscribing from something never held is not an
    /// error: the client wanted it gone, and it is gone.
    pub fn remove(&mut self, channels: &[String]) -> Result<(), Rejection> {
        let parsed = parse_all(channels)?;
        self.topics.retain(|topic| !parsed.contains(topic));
        Ok(())
    }
}

fn parse_all(channels: &[String]) -> Result<Vec<Topic>, Rejection> {
    if channels.is_empty() {
        return Err(Rejection::new(
            "invalid_command",
            "`channels` must not be empty",
        ));
    }
    if channels.len() > MAX_TOPICS {
        return Err(Rejection::new(
            "too_many_topics",
            format!("a command may name at most {MAX_TOPICS} topics"),
        ));
    }

    channels
        .iter()
        .map(|raw| {
            Topic::parse(raw).ok_or_else(|| {
                Rejection::new(
                    "unknown_channel",
                    format!("`{raw}` is not a channel; expected `book|trades:<exchange>:<symbol>`"),
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use obe_storage::Kind;

    fn channels(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn commands_parse_from_what_a_browser_would_send() {
        assert_eq!(
            Command::parse(r#"{"op":"subscribe","channels":["book:binance:BTCUSDT"]}"#).unwrap(),
            Command::Subscribe {
                channels: channels(&["book:binance:BTCUSDT"]),
            }
        );
        assert_eq!(Command::parse(r#"{"op":"ping"}"#).unwrap(), Command::Ping);
    }

    #[test]
    fn a_malformed_command_is_named_rather_than_ignored() {
        for raw in ["", "not json", r#"{"op":"trade"}"#, r#"{"op":"subscribe"}"#] {
            let rejection = Command::parse(raw).expect_err(raw);
            assert_eq!(rejection.code, "invalid_command");
        }
    }

    #[test]
    fn subscribing_normalises_and_dedupes() {
        let mut subscriptions = Subscriptions::default();

        let added = subscriptions
            .add(&channels(&["book:Binance:btcusdt", "book:binance:BTCUSDT"]))
            .unwrap();

        assert_eq!(added.len(), 1, "the same topic twice is one topic");
        assert_eq!(subscriptions.names(), vec!["book:binance:BTCUSDT"]);
        assert!(subscriptions.contains(&Topic::new(Kind::Book, "binance", "BTCUSDT")));
    }

    #[test]
    fn re_subscribing_reports_nothing_new_but_keeps_the_topic() {
        let mut subscriptions = Subscriptions::default();
        subscriptions
            .add(&channels(&["trades:binance:ETHUSDT"]))
            .unwrap();

        let added = subscriptions
            .add(&channels(&["trades:binance:ETHUSDT"]))
            .unwrap();

        assert!(added.is_empty(), "no second opening snapshot is owed");
        assert_eq!(subscriptions.len(), 1);
    }

    #[test]
    fn one_bad_channel_rejects_the_whole_command() {
        let mut subscriptions = Subscriptions::default();

        let rejection = subscriptions
            .add(&channels(&["book:binance:BTCUSDT", "book:binance"]))
            .expect_err("the command should be refused");

        assert_eq!(rejection.code, "unknown_channel");
        // Nothing was applied, so the client is not left half subscribed.
        assert!(subscriptions.is_empty());
    }

    #[test]
    fn an_empty_channel_list_is_a_mistake_worth_naming() {
        let mut subscriptions = Subscriptions::default();

        assert_eq!(
            subscriptions.add(&[]).expect_err("should be refused").code,
            "invalid_command"
        );
    }

    #[test]
    fn a_connection_cannot_hold_unbounded_topics() {
        let mut subscriptions = Subscriptions::default();
        let many: Vec<String> = (0..MAX_TOPICS)
            .map(|i| format!("book:binance:SYM{i}"))
            .collect();
        subscriptions.add(&many).unwrap();

        let rejection = subscriptions
            .add(&channels(&["book:binance:ONEMORE"]))
            .expect_err("the cap should hold");

        assert_eq!(rejection.code, "too_many_topics");
        assert_eq!(subscriptions.len(), MAX_TOPICS);
    }

    #[test]
    fn unsubscribing_from_something_never_held_is_fine() {
        let mut subscriptions = Subscriptions::default();
        subscriptions
            .add(&channels(&["book:binance:BTCUSDT"]))
            .unwrap();

        subscriptions
            .remove(&channels(&["trades:binance:SOLUSDT"]))
            .unwrap();
        subscriptions
            .remove(&channels(&["book:BINANCE:btcusdt"]))
            .unwrap();

        assert!(subscriptions.is_empty());
    }

    #[test]
    fn frames_carry_a_machine_readable_tag() {
        assert_eq!(Frame::Pong.encode(), r#"{"type":"pong"}"#);
        assert_eq!(
            Frame::Lagged { missed: 12 }.encode(),
            r#"{"type":"lagged","missed":12}"#
        );
        assert!(Frame::error(Rejection::new("unknown_channel", "nope"))
            .encode()
            .contains(r#""code":"unknown_channel""#));
    }
}
