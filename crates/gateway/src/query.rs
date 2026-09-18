//! Query-string parsing.
//!
//! Parameters arrive as raw strings and are validated here rather than by a
//! deserializer, for two reasons: the caller gets "`start` is not an RFC 3339
//! timestamp" instead of serde's rendering of the same fact, and the whole
//! module is a pure function of its input, so every rejection is covered by a
//! test that needs no router and no database.

use obe_storage::{TradeQuery, MAX_TRADE_LIMIT};
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::ApiError;

/// `GET /v1/symbols`
#[derive(Debug, Default, Deserialize)]
pub struct SymbolsParams {
    /// Hide delisted instruments. Defaults to true: a caller asking "what can
    /// I subscribe to" wants what is live.
    pub active_only: Option<bool>,
}

impl SymbolsParams {
    pub fn active_only(&self) -> bool {
        self.active_only.unwrap_or(true)
    }
}

/// `GET /v1/books/{exchange}/{symbol}`
#[derive(Debug, Default, Deserialize)]
pub struct BookParams {
    pub depth: Option<String>,
}

impl BookParams {
    /// Levels per side to return, or `None` for whatever is stored.
    pub fn depth(&self) -> Result<Option<usize>, ApiError> {
        let Some(raw) = self.depth.as_deref() else {
            return Ok(None);
        };

        let depth: usize = raw
            .parse()
            .map_err(|_| ApiError::invalid(format!("`depth` is not a number: `{raw}`")))?;
        if depth == 0 {
            return Err(ApiError::invalid("`depth` must be at least 1"));
        }
        Ok(Some(depth))
    }
}

/// `GET /v1/trades/{exchange}/{symbol}`
#[derive(Debug, Default, Deserialize)]
pub struct TradesParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<String>,
}

impl TradesParams {
    pub fn into_query(self, symbol_id: i32) -> Result<TradeQuery, ApiError> {
        let mut query = TradeQuery::new(symbol_id);
        query.start = parse_time("start", self.start.as_deref())?;
        query.end = parse_time("end", self.end.as_deref())?;

        if let Some(raw) = self.limit.as_deref() {
            let limit: i64 = raw
                .parse()
                .map_err(|_| ApiError::invalid(format!("`limit` is not a number: `{raw}`")))?;
            if limit <= 0 {
                return Err(ApiError::invalid("`limit` must be positive"));
            }
            // Clamped rather than rejected: a client asking for more than the
            // page cap gets the cap, which is what it would get by paging.
            query.limit = limit.min(MAX_TRADE_LIMIT);
        }

        if let (Some(start), Some(end)) = (query.start, query.end) {
            if start > end {
                return Err(ApiError::invalid("`start` must not be after `end`"));
            }
        }

        Ok(query)
    }
}

/// `GET /v1/vwap/{exchange}/{symbol}`
#[derive(Debug, Default, Deserialize)]
pub struct VwapParams {
    pub start: Option<String>,
    pub end: Option<String>,
}

impl VwapParams {
    /// Both bounds are required. A volume-weighted average over "everything
    /// ever recorded" is not a number anyone wants, and it is a full scan.
    pub fn window(self) -> Result<(OffsetDateTime, OffsetDateTime), ApiError> {
        let start = parse_time("start", self.start.as_deref())?
            .ok_or_else(|| ApiError::invalid("`start` is required"))?;
        let end = parse_time("end", self.end.as_deref())?
            .ok_or_else(|| ApiError::invalid("`end` is required"))?;

        if start > end {
            return Err(ApiError::invalid("`start` must not be after `end`"));
        }
        Ok((start, end))
    }
}

fn parse_time(field: &str, raw: Option<&str>) -> Result<Option<OffsetDateTime>, ApiError> {
    raw.map(|value| {
        OffsetDateTime::parse(value, &Rfc3339).map_err(|_| {
            ApiError::invalid(format!("`{field}` is not an RFC 3339 timestamp: `{value}`"))
        })
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    const EPOCH: &str = "1970-01-01T00:00:00Z";

    fn trades(start: Option<&str>, end: Option<&str>, limit: Option<&str>) -> TradesParams {
        TradesParams {
            start: start.map(str::to_owned),
            end: end.map(str::to_owned),
            limit: limit.map(str::to_owned),
        }
    }

    #[test]
    fn an_empty_trades_query_is_valid() {
        let query = trades(None, None, None).into_query(1).unwrap();

        assert_eq!((query.start, query.end), (None, None));
        assert_eq!(query.symbol_id, 1);
    }

    #[test]
    fn timestamps_are_rfc_3339() {
        let query = trades(Some(EPOCH), Some("2026-01-01T12:30:00+04:00"), None)
            .into_query(1)
            .unwrap();

        assert_eq!(query.start, Some(OffsetDateTime::UNIX_EPOCH));
        assert_eq!(query.end.unwrap().offset().whole_hours(), 4);
    }

    #[test]
    fn an_unparseable_timestamp_names_the_field_and_the_value() {
        let error = trades(Some("yesterday"), None, None)
            .into_query(1)
            .unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert!(error.to_string().contains("`start`"), "{error}");
        assert!(error.to_string().contains("yesterday"), "{error}");
    }

    #[test]
    fn an_inverted_window_is_rejected() {
        let error = trades(Some("2026-01-02T00:00:00Z"), Some(EPOCH), None)
            .into_query(1)
            .unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn an_oversized_limit_is_clamped_rather_than_rejected() {
        let query = trades(None, None, Some("100000")).into_query(1).unwrap();

        assert_eq!(query.limit, MAX_TRADE_LIMIT);
    }

    #[test]
    fn a_non_positive_limit_is_rejected() {
        assert!(trades(None, None, Some("0")).into_query(1).is_err());
        assert!(trades(None, None, Some("-5")).into_query(1).is_err());
        assert!(trades(None, None, Some("lots")).into_query(1).is_err());
    }

    #[test]
    fn vwap_requires_both_bounds() {
        let missing_end = VwapParams {
            start: Some(EPOCH.into()),
            end: None,
        };

        assert!(missing_end.window().is_err());
        assert!(VwapParams::default().window().is_err());
    }

    #[test]
    fn vwap_accepts_a_well_formed_window() {
        let params = VwapParams {
            start: Some(EPOCH.into()),
            end: Some("2026-01-01T00:00:00Z".into()),
        };

        let (start, end) = params.window().unwrap();
        assert!(start < end);
    }

    #[test]
    fn depth_defaults_to_the_stored_book() {
        assert_eq!(BookParams::default().depth().unwrap(), None);
    }

    #[test]
    fn a_zero_depth_is_rejected() {
        let params = BookParams {
            depth: Some("0".into()),
        };

        assert!(params.depth().is_err());
    }

    #[test]
    fn symbols_hide_delisted_instruments_by_default() {
        assert!(SymbolsParams::default().active_only());
        assert!(!SymbolsParams {
            active_only: Some(false),
        }
        .active_only());
    }
}
