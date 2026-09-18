//! In-memory level-2 book reconstruction.
//!
//! An exchange diff stream is not a sequence of self-contained messages: each
//! event only makes sense applied to the exact book state the one before it
//! produced. Miss an event and every later one is wrong, silently — the book
//! stays plausible while drifting away from the exchange's. So the sequence
//! rules are enforced here, and a violation is a hard error that forces a
//! resync rather than a warning nobody reads.
//!
//! The rules, in the order they apply:
//!
//! 1. An event whose final update id is at or below the snapshot's is already
//!    contained in the snapshot. Drop it.
//! 2. The first event applied after a snapshot must straddle it:
//!    `first_update_id <= snapshot + 1 <= final_update_id`. If the whole event
//!    is ahead of the snapshot, the gap between them was never delivered, and
//!    the snapshot is useless.
//! 3. Every later event must start exactly where the previous one ended:
//!    `first_update_id == previous final_update_id + 1`.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use time::OffsetDateTime;

use obe_storage::{BookSnapshot, Level};

use crate::protocol::{DepthDelta, DepthSnapshot};

/// What happened to an event handed to [`OrderBook::apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// The book moved forward.
    Accepted,
    /// The event predates the snapshot the book was built from, so it is
    /// already reflected. Expected while the buffer drains after a resync.
    Stale,
}

/// A break in the update sequence. Every variant means the same thing
/// operationally — throw the book away and resync — but they are separate
/// because they say different things about *why*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SequenceError {
    /// The snapshot is older than the oldest event still on the stream: the
    /// events bridging the two were dropped before the snapshot arrived.
    #[error("snapshot at {snapshot} predates the stream, which resumes at {first_update_id}")]
    SnapshotTooOld { snapshot: i64, first_update_id: i64 },
    /// Events went missing mid-stream.
    #[error("sequence gap: expected update {expected}, got {first_update_id}")]
    Gap { expected: i64, first_update_id: i64 },
    /// The book crossed after applying an event, which means the state
    /// diverged from the exchange's even though the ids lined up.
    #[error("book crossed at update {sequence}: best bid {bid} >= best ask {ask}")]
    Crossed {
        sequence: i64,
        bid: Decimal,
        ask: Decimal,
    },
}

/// A level-2 book for one instrument.
///
/// Both sides are `BTreeMap`s keyed by price, so levels stay sorted without a
/// sort per update and the top of book is an endpoint lookup. Bids are read
/// back to front; the map's ordering is the price ordering either way.
#[derive(Debug, Clone)]
pub struct OrderBook {
    symbol_id: i32,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_update_id: i64,
    /// Rule 2 applies to the first event after a snapshot, rule 3 to the rest.
    straddled: bool,
    updated_at: OffsetDateTime,
}

impl OrderBook {
    /// Seeds a book from a REST snapshot. Levels with quantity zero are
    /// dropped: they carry no liquidity and would only ever be removed.
    pub fn from_snapshot(symbol_id: i32, snapshot: &DepthSnapshot, at: OffsetDateTime) -> Self {
        Self {
            symbol_id,
            bids: collect_side(&snapshot.bids),
            asks: collect_side(&snapshot.asks),
            last_update_id: snapshot.last_update_id,
            straddled: false,
            updated_at: at,
        }
    }

    pub fn last_update_id(&self) -> i64 {
        self.last_update_id
    }

    pub fn updated_at(&self) -> OffsetDateTime {
        self.updated_at
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    /// Levels currently held, both sides. Useful as a drift signal: a book
    /// that only ever shrinks is one whose snapshot was too shallow.
    pub fn depth(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    /// Applies one diff event, enforcing the sequence rules above.
    pub fn apply(&mut self, delta: &DepthDelta) -> Result<Applied, SequenceError> {
        if delta.final_update_id <= self.last_update_id {
            return Ok(Applied::Stale);
        }

        if !self.straddled {
            // `final_update_id > last_update_id` is already established, so
            // the straddle reduces to this one bound.
            if delta.first_update_id > self.last_update_id + 1 {
                return Err(SequenceError::SnapshotTooOld {
                    snapshot: self.last_update_id,
                    first_update_id: delta.first_update_id,
                });
            }
        } else if delta.first_update_id != self.last_update_id + 1 {
            return Err(SequenceError::Gap {
                expected: self.last_update_id + 1,
                first_update_id: delta.first_update_id,
            });
        }

        apply_side(&mut self.bids, &delta.bids);
        apply_side(&mut self.asks, &delta.asks);
        self.last_update_id = delta.final_update_id;
        self.straddled = true;
        self.updated_at = delta.event_time;

        // Checked after the ids agreed, so a cross here is real divergence
        // rather than a missed event, and gets its own error for that reason.
        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) {
            if bid >= ask {
                return Err(SequenceError::Crossed {
                    sequence: self.last_update_id,
                    bid,
                    ask,
                });
            }
        }

        Ok(Applied::Accepted)
    }

    /// The top `depth` levels per side, in the shape storage and the API use.
    pub fn snapshot(&self, depth: usize) -> BookSnapshot {
        BookSnapshot {
            symbol_id: self.symbol_id,
            sequence: self.last_update_id,
            captured_at: self.updated_at,
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(price, qty)| Level::new(*price, *qty))
                .collect(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(price, qty)| Level::new(*price, *qty))
                .collect(),
        }
    }
}

fn collect_side(levels: &[Level]) -> BTreeMap<Decimal, Decimal> {
    levels
        .iter()
        .filter(|level| level.quantity > Decimal::ZERO)
        .map(|level| (level.price, level.quantity))
        .collect()
}

/// Diff levels are absolute quantities, not increments: the value replaces
/// whatever was there, and quantity zero deletes the level.
fn apply_side(side: &mut BTreeMap<Decimal, Decimal>, levels: &[Level]) {
    for level in levels {
        if level.quantity > Decimal::ZERO {
            side.insert(level.price, level.quantity);
        } else {
            side.remove(&level.price);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn at(secs: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(secs).unwrap()
    }

    fn levels(raw: &[(Decimal, Decimal)]) -> Vec<Level> {
        raw.iter().map(|(p, q)| Level::new(*p, *q)).collect()
    }

    fn seeded() -> OrderBook {
        let snapshot = DepthSnapshot {
            last_update_id: 100,
            bids: levels(&[(dec!(99), dec!(2)), (dec!(98), dec!(5))]),
            asks: levels(&[(dec!(101), dec!(1)), (dec!(102), dec!(4))]),
        };

        OrderBook::from_snapshot(7, &snapshot, at(0))
    }

    fn delta(
        first: i64,
        last: i64,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
    ) -> DepthDelta {
        DepthDelta {
            symbol: "BTCUSDT".into(),
            first_update_id: first,
            final_update_id: last,
            event_time: at(last),
            bids: levels(bids),
            asks: levels(asks),
        }
    }

    #[test]
    fn a_snapshot_seeds_the_top_of_book() {
        let book = seeded();

        assert_eq!(book.best_bid(), Some(dec!(99)));
        assert_eq!(book.best_ask(), Some(dec!(101)));
        assert_eq!(book.last_update_id(), 100);
    }

    #[test]
    fn events_already_in_the_snapshot_are_dropped() {
        let mut book = seeded();

        assert_eq!(book.apply(&delta(90, 100, &[], &[])), Ok(Applied::Stale));
        assert_eq!(book.last_update_id(), 100);
    }

    #[test]
    fn the_first_event_must_straddle_the_snapshot() {
        let mut book = seeded();

        // Starts at 102 when the snapshot ended at 100: update 101 was never
        // delivered, so the two cannot be joined.
        assert_eq!(
            book.apply(&delta(102, 105, &[], &[])),
            Err(SequenceError::SnapshotTooOld {
                snapshot: 100,
                first_update_id: 102,
            })
        );
    }

    #[test]
    fn an_event_overlapping_the_snapshot_is_accepted() {
        let mut book = seeded();

        // U=95 <= 101 <= u=105: the event covers the join.
        assert_eq!(book.apply(&delta(95, 105, &[], &[])), Ok(Applied::Accepted));
        assert_eq!(book.last_update_id(), 105);
    }

    #[test]
    fn later_events_must_be_contiguous() {
        let mut book = seeded();
        book.apply(&delta(101, 105, &[], &[])).unwrap();

        assert_eq!(
            book.apply(&delta(107, 110, &[], &[])),
            Err(SequenceError::Gap {
                expected: 106,
                first_update_id: 107,
            })
        );
    }

    #[test]
    fn a_zero_quantity_level_is_a_deletion() {
        let mut book = seeded();

        book.apply(&delta(101, 102, &[(dec!(99), Decimal::ZERO)], &[]))
            .unwrap();

        assert_eq!(book.best_bid(), Some(dec!(98)));
    }

    #[test]
    fn a_quantity_replaces_rather_than_increments() {
        let mut book = seeded();

        book.apply(&delta(101, 102, &[(dec!(99), dec!(7))], &[]))
            .unwrap();
        book.apply(&delta(103, 104, &[(dec!(99), dec!(3))], &[]))
            .unwrap();

        assert_eq!(book.snapshot(1).bids[0].quantity, dec!(3));
    }

    #[test]
    fn a_new_level_inside_the_spread_becomes_the_top() {
        let mut book = seeded();

        book.apply(&delta(101, 102, &[(dec!(100), dec!(1))], &[]))
            .unwrap();

        assert_eq!(book.best_bid(), Some(dec!(100)));
    }

    #[test]
    fn a_cross_is_reported_even_when_the_ids_line_up() {
        let mut book = seeded();

        let outcome = book.apply(&delta(101, 102, &[(dec!(103), dec!(1))], &[]));

        assert_eq!(
            outcome,
            Err(SequenceError::Crossed {
                sequence: 102,
                bid: dec!(103),
                ask: dec!(101),
            })
        );
    }

    #[test]
    fn snapshots_come_out_sorted_and_capped() {
        let mut book = seeded();
        book.apply(&delta(
            101,
            102,
            &[(dec!(97), dec!(1)), (dec!(100), dec!(1))],
            &[(dec!(103), dec!(1))],
        ))
        .unwrap();

        let snapshot = book.snapshot(2);

        assert_eq!(
            snapshot.bids,
            levels(&[(dec!(100), dec!(1)), (dec!(99), dec!(2))])
        );
        assert_eq!(
            snapshot.asks,
            levels(&[(dec!(101), dec!(1)), (dec!(102), dec!(4))])
        );
        assert_eq!(snapshot.sequence, 102);
        assert_eq!(snapshot.symbol_id, 7);
        assert!(snapshot.is_well_formed());
    }

    #[test]
    fn a_snapshot_carries_the_event_time_of_the_last_applied_event() {
        let mut book = seeded();
        book.apply(&delta(101, 150, &[], &[])).unwrap();

        assert_eq!(book.snapshot(5).captured_at, at(150));
    }

    #[test]
    fn zero_quantity_levels_never_enter_from_a_snapshot() {
        let snapshot = DepthSnapshot {
            last_update_id: 1,
            bids: levels(&[(dec!(99), Decimal::ZERO)]),
            asks: vec![],
        };

        let book = OrderBook::from_snapshot(1, &snapshot, at(0));

        assert_eq!(book.best_bid(), None);
        assert_eq!(book.depth(), 0);
    }
}
