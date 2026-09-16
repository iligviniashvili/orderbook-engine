//! Trade persistence and history queries.

use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::model::{NewTrade, Side, Trade};
use crate::postgres::Store;

const COLUMNS: &str =
    "id, symbol_id, exchange_trade_id, price, quantity, side, traded_at, ingested_at";

/// Largest page a caller can ask for, whatever it passes.
pub const MAX_TRADE_LIMIT: i64 = 1_000;

/// A window of a symbol's trade history, newest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeQuery {
    pub symbol_id: i32,
    /// Inclusive lower bound on `traded_at`.
    pub start: Option<OffsetDateTime>,
    /// Exclusive upper bound on `traded_at`.
    pub end: Option<OffsetDateTime>,
    pub limit: i64,
}

impl TradeQuery {
    pub fn new(symbol_id: i32) -> Self {
        Self {
            symbol_id,
            start: None,
            end: None,
            limit: 100,
        }
    }

    pub fn between(mut self, start: OffsetDateTime, end: OffsetDateTime) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = limit;
        self
    }

    fn validated(self) -> Result<Self> {
        if self.limit <= 0 {
            return Err(Error::invalid("trade query limit must be positive"));
        }
        if let (Some(start), Some(end)) = (self.start, self.end) {
            if start > end {
                return Err(Error::invalid("trade query start must not be after end"));
            }
        }
        Ok(Self {
            limit: self.limit.min(MAX_TRADE_LIMIT),
            ..self
        })
    }
}

impl Store {
    /// Inserts a batch of trades in a single round trip and returns how many
    /// were new.
    ///
    /// The rows are shipped as six parallel arrays and expanded server-side
    /// with `UNNEST`, which keeps the statement text constant no matter how
    /// large the batch is — no rebuilding `VALUES (...), (...)` per batch, no
    /// re-planning, and no bumping into the 65535-parameter wire limit.
    /// `ON CONFLICT DO NOTHING` on the exchange's trade id makes a replayed
    /// batch after a reconnect a no-op.
    pub async fn insert_trades(&self, trades: &[NewTrade]) -> Result<u64> {
        if trades.is_empty() {
            return Ok(0);
        }

        let mut symbol_ids = Vec::with_capacity(trades.len());
        let mut trade_ids = Vec::with_capacity(trades.len());
        let mut prices = Vec::with_capacity(trades.len());
        let mut quantities = Vec::with_capacity(trades.len());
        let mut sides = Vec::with_capacity(trades.len());
        let mut traded_ats = Vec::with_capacity(trades.len());

        for trade in trades {
            if trade.price <= Decimal::ZERO || trade.quantity <= Decimal::ZERO {
                return Err(Error::invalid(format!(
                    "trade {} has a non-positive price or quantity",
                    trade.exchange_trade_id
                )));
            }
            symbol_ids.push(trade.symbol_id);
            trade_ids.push(trade.exchange_trade_id);
            prices.push(trade.price);
            quantities.push(trade.quantity);
            sides.push(trade.side);
            traded_ats.push(trade.traded_at);
        }

        let inserted = sqlx::query(
            "INSERT INTO trades (symbol_id, exchange_trade_id, price, quantity, side, traded_at)
             SELECT * FROM UNNEST(
                 $1::integer[], $2::bigint[], $3::numeric[],
                 $4::numeric[], $5::trade_side[], $6::timestamptz[]
             )
             ON CONFLICT (symbol_id, exchange_trade_id) DO NOTHING",
        )
        .bind(&symbol_ids)
        .bind(&trade_ids)
        .bind(&prices)
        .bind(&quantities)
        .bind(&sides)
        .bind(&traded_ats)
        .execute(self.pool())
        .await?
        .rows_affected();

        Ok(inserted)
    }

    pub async fn trades(&self, query: TradeQuery) -> Result<Vec<Trade>> {
        let query = query.validated()?;

        let sql = format!(
            "SELECT {COLUMNS} FROM trades
             WHERE symbol_id = $1
               AND ($2::timestamptz IS NULL OR traded_at >= $2)
               AND ($3::timestamptz IS NULL OR traded_at < $3)
             ORDER BY traded_at DESC, id DESC
             LIMIT $4"
        );

        let rows = sqlx::query_as::<_, Trade>(&sql)
            .bind(query.symbol_id)
            .bind(query.start)
            .bind(query.end)
            .bind(query.limit)
            .fetch_all(self.pool())
            .await?;

        Ok(rows)
    }

    pub async fn latest_trade(&self, symbol_id: i32) -> Result<Option<Trade>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM trades
             WHERE symbol_id = $1
             ORDER BY traded_at DESC, id DESC
             LIMIT 1"
        );

        let row = sqlx::query_as::<_, Trade>(&sql)
            .bind(symbol_id)
            .fetch_optional(self.pool())
            .await?;

        Ok(row)
    }

    /// Volume-weighted average price over a window, computed in the database
    /// so the rows never cross the wire. `None` when the window is empty.
    pub async fn vwap(
        &self,
        symbol_id: i32,
        start: OffsetDateTime,
        end: OffsetDateTime,
    ) -> Result<Option<Decimal>> {
        if start > end {
            return Err(Error::invalid("vwap start must not be after end"));
        }

        let vwap = sqlx::query_scalar::<_, Option<Decimal>>(
            "SELECT SUM(price * quantity) / NULLIF(SUM(quantity), 0)
             FROM trades
             WHERE symbol_id = $1 AND traded_at >= $2 AND traded_at < $3",
        )
        .bind(symbol_id)
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        Ok(vwap)
    }

    /// Deletes trades older than `cutoff` and returns how many went. Used by
    /// the retention sweep; the BRIN index on `traded_at` keeps it cheap.
    pub async fn prune_trades_before(&self, cutoff: OffsetDateTime) -> Result<u64> {
        let deleted = sqlx::query("DELETE FROM trades WHERE traded_at < $1")
            .bind(cutoff)
            .execute(self.pool())
            .await?
            .rows_affected();

        Ok(deleted)
    }

    pub async fn count_trades(&self, symbol_id: i32) -> Result<i64> {
        let count =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM trades WHERE symbol_id = $1")
                .bind(symbol_id)
                .fetch_one(self.pool())
                .await?;

        Ok(count)
    }
}

/// Convenience constructor used by callers building batches.
impl NewTrade {
    pub fn new(
        symbol_id: i32,
        exchange_trade_id: i64,
        price: Decimal,
        quantity: Decimal,
        side: Side,
        traded_at: OffsetDateTime,
    ) -> Self {
        Self {
            symbol_id,
            exchange_trade_id,
            price,
            quantity,
            side,
            traded_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;

    #[test]
    fn limit_is_clamped_to_the_maximum() {
        let query = TradeQuery::new(1).limit(10_000).validated().unwrap();

        assert_eq!(query.limit, MAX_TRADE_LIMIT);
    }

    #[test]
    fn a_non_positive_limit_is_rejected() {
        assert!(TradeQuery::new(1).limit(0).validated().is_err());
    }

    #[test]
    fn an_inverted_window_is_rejected() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let query = TradeQuery::new(1).between(now, now - Duration::hours(1));

        assert!(query.validated().is_err());
    }

    #[test]
    fn an_open_ended_window_is_allowed() {
        let query = TradeQuery::new(1).validated().unwrap();

        assert_eq!((query.start, query.end), (None, None));
        assert_eq!(query.limit, 100);
    }
}
