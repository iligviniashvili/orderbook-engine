//! Row types shared by the repositories, the cache and the HTTP layer.
//!
//! Prices and quantities are [`Decimal`], never `f64`, and they serialise as
//! JSON strings so that neither PostgreSQL's `numeric` nor a JSON reader can
//! turn `0.1` into `0.09999999999999999`.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Which side of the book the aggressor was on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "trade_side", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Symbol {
    pub id: i32,
    pub exchange: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: i16,
    pub quantity_precision: i16,
    pub active: bool,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// The fields an upsert supplies; the database owns the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSymbol {
    pub exchange: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: i16,
    pub quantity_precision: i16,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Trade {
    pub id: i64,
    pub symbol_id: i32,
    pub exchange_trade_id: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub side: Side,
    #[serde(with = "time::serde::rfc3339")]
    pub traded_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub ingested_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTrade {
    pub symbol_id: i32,
    /// The exchange's own trade id. Doubles as the idempotency key.
    pub exchange_trade_id: i64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: Side,
    pub traded_at: OffsetDateTime,
}

/// One price level of a level-2 book.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

impl Level {
    pub fn new(price: Decimal, quantity: Decimal) -> Self {
        Self { price, quantity }
    }
}

/// A level-2 snapshot at a point in the exchange's update sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub symbol_id: i32,
    /// The exchange's update id at capture time. Monotonic per symbol.
    pub sequence: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub captured_at: OffsetDateTime,
    /// Descending by price: `bids[0]` is the best bid.
    pub bids: Vec<Level>,
    /// Ascending by price: `asks[0]` is the best ask.
    pub asks: Vec<Level>,
}

impl BookSnapshot {
    pub fn best_bid(&self) -> Option<&Level> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&Level> {
        self.asks.first()
    }

    /// Best ask minus best bid, or `None` if either side is empty.
    pub fn spread(&self) -> Option<Decimal> {
        Some(self.best_ask()?.price - self.best_bid()?.price)
    }

    /// True when the book is ordered as documented and not crossed. Cheap
    /// enough to assert on every snapshot before it is persisted or cached.
    pub fn is_well_formed(&self) -> bool {
        let bids_descend = self.bids.windows(2).all(|w| w[0].price > w[1].price);
        let asks_ascend = self.asks.windows(2).all(|w| w[0].price < w[1].price);
        let uncrossed = match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid.price < ask.price,
            _ => true,
        };
        let positive = self
            .bids
            .iter()
            .chain(&self.asks)
            .all(|level| level.price > Decimal::ZERO && level.quantity > Decimal::ZERO);

        bids_descend && asks_ascend && uncrossed && positive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn snapshot(bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> BookSnapshot {
        BookSnapshot {
            symbol_id: 1,
            sequence: 42,
            captured_at: OffsetDateTime::UNIX_EPOCH,
            bids: bids.into_iter().map(|(p, q)| Level::new(p, q)).collect(),
            asks: asks.into_iter().map(|(p, q)| Level::new(p, q)).collect(),
        }
    }

    #[test]
    fn spread_uses_the_top_of_book() {
        let book = snapshot(
            vec![(dec!(100.5), dec!(2)), (dec!(100.0), dec!(5))],
            vec![(dec!(100.75), dec!(1)), (dec!(101.0), dec!(3))],
        );

        assert_eq!(book.best_bid().unwrap().price, dec!(100.5));
        assert_eq!(book.spread(), Some(dec!(0.25)));
        assert!(book.is_well_formed());
    }

    #[test]
    fn an_empty_side_has_no_spread() {
        let book = snapshot(vec![], vec![(dec!(101), dec!(1))]);

        assert_eq!(book.spread(), None);
        assert!(book.is_well_formed());
    }

    #[test]
    fn a_crossed_book_is_rejected() {
        let book = snapshot(vec![(dec!(101), dec!(1))], vec![(dec!(100), dec!(1))]);

        assert!(!book.is_well_formed());
    }

    #[test]
    fn misordered_levels_are_rejected() {
        let book = snapshot(vec![(dec!(99), dec!(1)), (dec!(100), dec!(1))], vec![]);

        assert!(!book.is_well_formed());
    }

    #[test]
    fn zero_quantity_levels_are_rejected() {
        // A delta with quantity 0 means "remove this level"; it must be
        // applied, not stored.
        let book = snapshot(vec![(dec!(100), dec!(0))], vec![]);

        assert!(!book.is_well_formed());
    }

    #[test]
    fn decimals_round_trip_through_json_as_strings() {
        let book = snapshot(vec![(dec!(0.1), dec!(0.2))], vec![(dec!(1e-9), dec!(3))]);

        let json = serde_json::to_string(&book).unwrap();
        assert!(json.contains(r#""price":"0.1""#), "{json}");

        let parsed: BookSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, book);
    }

    #[test]
    fn side_serialises_as_the_postgres_enum_label() {
        assert_eq!(serde_json::to_string(&Side::Buy).unwrap(), r#""buy""#);
        assert_eq!(Side::Sell.as_str(), "sell");
    }
}
