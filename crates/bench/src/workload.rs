//! Synthetic market data that behaves like the real thing.
//!
//! Generated rather than replayed so a run needs no fixture and no exchange,
//! and seeded so two runs are comparable. The shape matters more than the
//! prices: updates cluster near the top of book and occasionally delete a
//! level, which is what makes the `BTreeMap` work rather than just grow.

use rust_decimal::Decimal;

use obe_ingest::protocol::DepthSnapshot;
use obe_storage::Level;

use crate::harness::Rng;

pub const SYMBOL: &str = "BTCUSDT";
pub const SYMBOL_ID: i32 = 1;

/// Levels per side in the starting book, matching the ingestor's default
/// `snapshot_depth` order of magnitude.
pub const SNAPSHOT_LEVELS: usize = 1_000;

/// Levels per side in one diff event. Real depth diffs are small; the cost of
/// a busy feed is in how many of them arrive, not how fat each one is.
const LEVELS_PER_EVENT: usize = 4;

/// Mid price, in whole units, with two decimal places below it.
const MID_CENTS: i64 = 6_421_011;

pub fn snapshot(last_update_id: i64) -> DepthSnapshot {
    let bids = (0..SNAPSHOT_LEVELS)
        .map(|i| level(MID_CENTS - 1 - i as i64, 1 + (i as i64 % 9)))
        .collect();
    let asks = (0..SNAPSHOT_LEVELS)
        .map(|i| level(MID_CENTS + 1 + i as i64, 1 + (i as i64 % 7)))
        .collect();

    DepthSnapshot {
        last_update_id,
        bids,
        asks,
    }
}

/// One depth diff, contiguous with `previous_update_id`.
///
/// One update in eight carries quantity zero, which is a deletion: without
/// them the book only ever grows, and the measurement would miss the removal
/// path entirely.
pub fn depth_event(rng: &mut Rng, previous_update_id: i64) -> (i64, String) {
    let first = previous_update_id + 1;
    let last = first + rng.below(4) as i64;

    let mut bids = String::new();
    let mut asks = String::new();
    for n in 0..LEVELS_PER_EVENT {
        let offset = rng.below(SNAPSHOT_LEVELS as u64 / 4) as i64;
        let quantity = if rng.below(8) == 0 {
            0
        } else {
            1 + rng.below(50) as i64
        };

        let (side, price) = if n % 2 == 0 {
            (&mut bids, MID_CENTS - 1 - offset)
        } else {
            (&mut asks, MID_CENTS + 1 + offset)
        };
        if !side.is_empty() {
            side.push(',');
        }
        side.push_str(&format!(r#"["{}","{}.00000"]"#, cents(price), quantity));
    }

    let frame = format!(
        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1758200000000,"s":"{SYMBOL}","U":{first},"u":{last},"b":[{bids}],"a":[{asks}]}}}}"#
    );
    (last, frame)
}

/// One trade frame.
pub fn trade_event(rng: &mut Rng, id: i64) -> String {
    let price = cents(MID_CENTS + rng.below(3) as i64 - 1);
    let maker = rng.below(2) == 0;

    format!(
        r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","E":1758200000000,"s":"{SYMBOL}","t":{id},"p":"{price}","q":"0.0{}","T":1758200000000,"m":{maker}}}}}"#,
        1 + rng.below(9)
    )
}

fn level(price_cents: i64, quantity: i64) -> Level {
    Level::new(
        Decimal::new(price_cents, 2),
        Decimal::new(quantity * 1_000, 5),
    )
}

fn cents(value: i64) -> String {
    format!("{}.{:02}", value / 100, value % 100)
}
