//! The exchange wire format, kept in one place.
//!
//! Everything here is `serde` shapes and the conversion into this project's
//! own types. Nothing above this module knows that the feed spells "first
//! update id" `U`, or that a level arrives as a two-element array of strings.
//!
//! Prices and quantities are parsed straight from those strings into
//! [`Decimal`]. They are never routed through `f64`: the exchange sends
//! `"0.00000001"`, and a float would hand the book `1.0000000000000001e-8`.

use rust_decimal::Decimal;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;
use time::OffsetDateTime;

use obe_storage::{Level, Side};

/// A depth diff: the levels that changed between two update ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthDelta {
    pub symbol: String,
    /// `U` — the first update id covered by this event.
    pub first_update_id: i64,
    /// `u` — the last update id covered by this event.
    pub final_update_id: i64,
    pub event_time: OffsetDateTime,
    /// Absolute quantities, not deltas. Quantity zero removes the level.
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// One executed trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeEvent {
    pub symbol: String,
    pub trade_id: i64,
    pub price: Decimal,
    pub quantity: Decimal,
    /// Which side lifted the book, derived from the maker flag.
    pub side: Side,
    pub traded_at: OffsetDateTime,
}

/// A depth snapshot fetched over REST, the starting point of every resync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSnapshot {
    pub last_update_id: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// Anything the feed can hand the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedEvent {
    Depth(DepthDelta),
    Trade(TradeEvent),
}

impl FeedEvent {
    pub fn symbol(&self) -> &str {
        match self {
            Self::Depth(delta) => &delta.symbol,
            Self::Trade(trade) => &trade.symbol,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("malformed feed message")]
    Malformed(#[from] serde_json::Error),
    #[error("timestamp {0} is not a valid instant")]
    Timestamp(i64),
}

/// Just the two things routing needs: whether the payload is wrapped, and
/// whether it carries an event discriminator at all.
///
/// Both are [`RawValue`], which records where a value sits in the input
/// without decoding or allocating it. The discriminator is never read — the
/// tag on [`Payload`] does the matching — so borrowing its span is enough, and
/// it cannot fail on an escaped string the way a `&str` would.
#[derive(Debug, Deserialize)]
struct Envelope<'a> {
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
    #[serde(rename = "e", borrow, default)]
    kind: Option<&'a RawValue>,
}

/// Parses one text frame from the combined stream.
///
/// Returns `None` for frames that are valid but carry nothing to ingest: the
/// subscription acknowledgements the exchange sends on connect, and event
/// kinds this service does not consume. Those are not errors, and treating
/// them as errors would tear down a healthy socket.
///
/// This deliberately does not build a `serde_json::Value` first. Doing so used
/// to cost a whole allocated tree — a map and an owned `String` per key and
/// per value, a `Vec` per side — before a single field was read, and the
/// benchmark had decoding at several times the cost of applying the result to
/// the book. Routing on borrowed spans and then deserialising the body once,
/// straight into the wire types, takes those allocations out of the hot path.
pub fn parse_frame(raw: &str) -> Result<Option<FeedEvent>, ProtocolError> {
    let envelope: Envelope = serde_json::from_str(raw)?;

    // The combined stream wraps the payload in `data`; a single-stream socket
    // sends it bare.
    let (body, kind) = match envelope.data {
        Some(data) => {
            let inner: Envelope = serde_json::from_str(data.get())?;
            (data.get(), inner.kind)
        }
        None => (raw, envelope.kind),
    };

    // No discriminator means a control frame, not a market-data event. Parsing
    // is only attempted once one is present, so a genuinely malformed event
    // still fails loudly instead of being waved through as "unknown kind".
    if kind.is_none() {
        return Ok(None);
    }

    match serde_json::from_str::<Payload>(body)? {
        Payload::Depth(wire) => Ok(Some(FeedEvent::Depth(wire.try_into()?))),
        Payload::Trade(wire) => Ok(Some(FeedEvent::Trade(wire.try_into()?))),
        Payload::Other => Ok(None),
    }
}

/// Parses the REST depth snapshot body.
pub fn parse_snapshot(raw: &str) -> Result<DepthSnapshot, ProtocolError> {
    let wire: WireSnapshot = serde_json::from_str(raw)?;

    Ok(DepthSnapshot {
        last_update_id: wire.last_update_id,
        bids: into_levels(wire.bids),
        asks: into_levels(wire.asks),
    })
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
enum Payload {
    #[serde(rename = "depthUpdate")]
    Depth(WireDepth),
    #[serde(rename = "trade")]
    Trade(WireTrade),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct WireDepth {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: i64,
    #[serde(rename = "u")]
    final_update_id: i64,
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "b")]
    bids: Vec<WireLevel>,
    #[serde(rename = "a")]
    asks: Vec<WireLevel>,
}

#[derive(Debug, Deserialize)]
struct WireTrade {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: i64,
    #[serde(rename = "p")]
    price: Decimal,
    #[serde(rename = "q")]
    quantity: Decimal,
    #[serde(rename = "T")]
    traded_at_ms: i64,
    /// True when the *buyer* was the resting maker, i.e. a seller crossed the
    /// spread. The aggressor is what a tape shows, so the flag is inverted.
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Debug, Deserialize)]
struct WireSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: i64,
    bids: Vec<WireLevel>,
    asks: Vec<WireLevel>,
}

impl TryFrom<WireDepth> for DepthDelta {
    type Error = ProtocolError;

    fn try_from(wire: WireDepth) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: wire.symbol,
            first_update_id: wire.first_update_id,
            final_update_id: wire.final_update_id,
            event_time: millis_to_time(wire.event_time_ms)?,
            bids: into_levels(wire.bids),
            asks: into_levels(wire.asks),
        })
    }
}

impl TryFrom<WireTrade> for TradeEvent {
    type Error = ProtocolError;

    fn try_from(wire: WireTrade) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: wire.symbol,
            trade_id: wire.trade_id,
            price: wire.price,
            quantity: wire.quantity,
            side: if wire.buyer_is_maker {
                Side::Sell
            } else {
                Side::Buy
            },
            traded_at: millis_to_time(wire.traded_at_ms)?,
        })
    }
}

/// A price level on the wire: `["30000.10", "0.5"]`.
#[derive(Debug)]
struct WireLevel(Level);

impl<'de> Deserialize<'de> for WireLevel {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LevelVisitor;

        impl<'de> Visitor<'de> for LevelVisitor {
            type Value = WireLevel;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a [price, quantity] pair of decimal strings")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let price: Decimal = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let quantity: Decimal = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                // Some endpoints append an ignored third element. Drain it
                // rather than failing on a field nobody reads.
                while seq.next_element::<de::IgnoredAny>()?.is_some() {}

                Ok(WireLevel(Level::new(price, quantity)))
            }
        }

        deserializer.deserialize_seq(LevelVisitor)
    }
}

fn into_levels(wire: Vec<WireLevel>) -> Vec<Level> {
    wire.into_iter().map(|level| level.0).collect()
}

fn millis_to_time(millis: i64) -> Result<OffsetDateTime, ProtocolError> {
    let nanos = i128::from(millis) * 1_000_000;
    OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|_| ProtocolError::Timestamp(millis))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const DEPTH_FRAME: &str = r#"{
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate", "E": 1700000000000, "s": "BTCUSDT",
            "U": 157, "u": 160,
            "b": [["30000.10", "0.5"], ["29999.90", "0"]],
            "a": [["30000.50", "1.25"]]
        }
    }"#;

    const TRADE_FRAME: &str = r#"{
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade", "E": 1700000000000, "s": "BTCUSDT",
            "t": 88, "p": "30000.20", "q": "0.01",
            "T": 1700000000123, "m": true
        }
    }"#;

    #[test]
    fn a_depth_frame_becomes_a_delta() {
        let event = parse_frame(DEPTH_FRAME).unwrap().unwrap();
        let FeedEvent::Depth(delta) = event else {
            panic!("expected a depth delta");
        };

        assert_eq!(delta.symbol, "BTCUSDT");
        assert_eq!((delta.first_update_id, delta.final_update_id), (157, 160));
        assert_eq!(
            delta.bids,
            vec![
                Level::new(dec!(30000.10), dec!(0.5)),
                Level::new(dec!(29999.90), Decimal::ZERO),
            ]
        );
        assert_eq!(delta.asks, vec![Level::new(dec!(30000.50), dec!(1.25))]);
        assert_eq!(delta.event_time.unix_timestamp(), 1_700_000_000);
    }

    #[test]
    fn the_maker_flag_is_inverted_into_the_aggressor_side() {
        // `"m": true` means the buyer was resting, so a seller crossed.
        let FeedEvent::Trade(trade) = parse_frame(TRADE_FRAME).unwrap().unwrap() else {
            panic!("expected a trade");
        };

        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.trade_id, 88);
        assert_eq!(trade.price, dec!(30000.20));
        assert_eq!(
            trade.traded_at.unix_timestamp_nanos(),
            1_700_000_000_123_000_000
        );
    }

    #[test]
    fn prices_keep_every_digit_the_exchange_sent() {
        // The value a float would render as 1.0000000000000001e-8.
        let frame = r#"{"data":{"e":"depthUpdate","E":1,"s":"X","U":1,"u":1,
            "b":[["0.00000001","123456789.123456789"]],"a":[]}}"#;

        let FeedEvent::Depth(delta) = parse_frame(frame).unwrap().unwrap() else {
            panic!("expected a depth delta");
        };

        assert_eq!(delta.bids[0].price.to_string(), "0.00000001");
        assert_eq!(delta.bids[0].quantity.to_string(), "123456789.123456789");
    }

    #[test]
    fn a_subscription_ack_is_ignored_rather_than_failing() {
        // What the exchange answers a SUBSCRIBE with. Tearing the socket down
        // over it would mean never getting past the handshake.
        assert!(parse_frame(r#"{"result":null,"id":1}"#).unwrap().is_none());
    }

    #[test]
    fn an_unknown_event_kind_is_ignored() {
        let frame = r#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT"}}"#;

        assert!(parse_frame(frame).unwrap().is_none());
    }

    #[test]
    fn an_unwrapped_single_stream_frame_also_parses() {
        let frame = r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]}"#;

        let event = parse_frame(frame).unwrap().unwrap();
        assert_eq!(event.symbol(), "BTCUSDT");
    }

    #[test]
    fn malformed_json_is_an_error() {
        assert!(parse_frame("{not json").is_err());
    }

    #[test]
    fn a_rest_snapshot_parses_with_its_update_id() {
        let raw = r#"{"lastUpdateId":1027024,
            "bids":[["30000.00","1.0"]],"asks":[["30001.00","2.0"]]}"#;

        let snapshot = parse_snapshot(raw).unwrap();

        assert_eq!(snapshot.last_update_id, 1_027_024);
        assert_eq!(snapshot.bids, vec![Level::new(dec!(30000), dec!(1))]);
        assert_eq!(snapshot.asks, vec![Level::new(dec!(30001), dec!(2))]);
    }

    #[test]
    fn an_ignored_third_element_on_a_level_is_tolerated() {
        let raw = r#"{"lastUpdateId":1,"bids":[["30000.00","1.0",[]]],"asks":[]}"#;

        assert_eq!(
            parse_snapshot(raw).unwrap().bids,
            vec![Level::new(dec!(30000), dec!(1))]
        );
    }
}
