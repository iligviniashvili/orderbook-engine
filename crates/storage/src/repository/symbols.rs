//! Instrument registry.

use crate::error::{Error, Result};
use crate::model::{NewSymbol, Symbol};
use crate::postgres::Store;

const COLUMNS: &str = "id, exchange, symbol, base_asset, quote_asset, \
     price_precision, quantity_precision, active, created_at, updated_at";

impl Store {
    /// Inserts the symbol, or updates the mutable fields if the pair is
    /// already known, and returns the stored row either way.
    ///
    /// The ingestor calls this on every startup for every configured symbol,
    /// so it has to be idempotent rather than fail the second time.
    pub async fn upsert_symbol(&self, new: &NewSymbol) -> Result<Symbol> {
        new.validate()?;

        let sql = format!(
            "INSERT INTO symbols (exchange, symbol, base_asset, quote_asset, \
                 price_precision, quantity_precision, active)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (exchange, symbol) DO UPDATE SET
                 base_asset = EXCLUDED.base_asset,
                 quote_asset = EXCLUDED.quote_asset,
                 price_precision = EXCLUDED.price_precision,
                 quantity_precision = EXCLUDED.quantity_precision,
                 active = EXCLUDED.active,
                 updated_at = now()
             RETURNING {COLUMNS}"
        );

        let row = sqlx::query_as::<_, Symbol>(&sql)
            .bind(&new.exchange)
            .bind(&new.symbol)
            .bind(&new.base_asset)
            .bind(&new.quote_asset)
            .bind(new.price_precision)
            .bind(new.quantity_precision)
            .bind(new.active)
            .fetch_one(self.pool())
            .await?;

        Ok(row)
    }

    pub async fn symbol_by_pair(&self, exchange: &str, symbol: &str) -> Result<Option<Symbol>> {
        let sql = format!("SELECT {COLUMNS} FROM symbols WHERE exchange = $1 AND symbol = $2");

        let row = sqlx::query_as::<_, Symbol>(&sql)
            .bind(exchange)
            .bind(symbol)
            .fetch_optional(self.pool())
            .await?;

        Ok(row)
    }

    /// Lists symbols ordered by exchange then ticker; `active_only` hides
    /// instruments that have been delisted.
    pub async fn list_symbols(&self, active_only: bool) -> Result<Vec<Symbol>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM symbols
             WHERE NOT $1 OR active
             ORDER BY exchange, symbol"
        );

        let rows = sqlx::query_as::<_, Symbol>(&sql)
            .bind(active_only)
            .fetch_all(self.pool())
            .await?;

        Ok(rows)
    }
}

impl NewSymbol {
    /// Mirrors the CHECK constraints, so an obvious mistake fails before it
    /// costs a round trip.
    pub(crate) fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("exchange", &self.exchange),
            ("symbol", &self.symbol),
            ("base_asset", &self.base_asset),
            ("quote_asset", &self.quote_asset),
        ] {
            if value.trim().is_empty() {
                return Err(Error::invalid(format!("symbol.{field} must not be blank")));
            }
        }
        if !(0..=12).contains(&self.price_precision) || !(0..=12).contains(&self.quantity_precision)
        {
            return Err(Error::invalid("symbol precision must be between 0 and 12"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> NewSymbol {
        NewSymbol {
            exchange: "binance".into(),
            symbol: "BTCUSDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            price_precision: 2,
            quantity_precision: 6,
            active: true,
        }
    }

    #[test]
    fn a_valid_symbol_passes() {
        assert!(valid().validate().is_ok());
    }

    #[test]
    fn blank_fields_are_rejected() {
        let mut symbol = valid();
        symbol.base_asset = "   ".into();

        assert!(symbol.validate().is_err());
    }

    #[test]
    fn out_of_range_precision_is_rejected() {
        let mut symbol = valid();
        symbol.price_precision = 13;

        assert!(symbol.validate().is_err());
    }
}
