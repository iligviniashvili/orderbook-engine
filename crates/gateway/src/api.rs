//! The public read API.
//!
//! Everything here is read-only: this service ingests from an exchange and
//! serves what it reconstructed, and there is nothing a client can write.

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use rust_decimal::Decimal;
use serde::Serialize;
use time::OffsetDateTime;

use obe_storage::{BookSnapshot, Level, Symbol, Trade};

use crate::error::ApiError;
use crate::query::{BookParams, SymbolsParams, TradesParams, VwapParams};
use crate::AppState;

type ApiResult<T> = Result<Json<T>, ApiError>;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/symbols", get(symbols))
        .route("/v1/books/{exchange}/{symbol}", get(book))
        .route("/v1/trades/{exchange}/{symbol}", get(trades))
        .route("/v1/vwap/{exchange}/{symbol}", get(vwap))
}

/// Where a book came from. Worth telling the caller: one is the live book, the
/// other is the last one that made it to disk, and they behave differently
/// when an ingestor has died.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Cache,
    Database,
}

#[derive(Debug, Serialize)]
pub struct SymbolsResponse {
    pub symbols: Vec<Symbol>,
}

#[derive(Debug, Serialize)]
pub struct BookResponse {
    pub exchange: String,
    pub symbol: String,
    pub source: Source,
    pub sequence: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub captured_at: OffsetDateTime,
    /// How stale the book is. A client watching this can tell a live feed from
    /// one whose ingestor stopped without having to diff sequence numbers.
    pub age_ms: i64,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub spread: Option<Decimal>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Serialize)]
pub struct TradesResponse {
    pub count: usize,
    pub trades: Vec<Trade>,
}

#[derive(Debug, Serialize)]
pub struct VwapResponse {
    pub symbol_id: i32,
    #[serde(with = "time::serde::rfc3339")]
    pub start: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub end: OffsetDateTime,
    /// `None` when no trade fell inside the window.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub vwap: Option<Decimal>,
}

async fn symbols(
    State(state): State<AppState>,
    Query(params): Query<SymbolsParams>,
) -> ApiResult<SymbolsResponse> {
    let symbols = state.store().list_symbols(params.active_only()).await?;

    Ok(Json(SymbolsResponse { symbols }))
}

/// The current book: from the cache when it is warm, from the newest persisted
/// snapshot when it is not.
///
/// The cache path never touches PostgreSQL — the key is built from the
/// exchange and ticker, so the hot request costs one Redis round trip and no
/// symbol lookup.
async fn book(
    State(state): State<AppState>,
    Path((exchange, symbol)): Path<(String, String)>,
    Query(params): Query<BookParams>,
) -> ApiResult<BookResponse> {
    let (exchange, symbol) = normalise(&exchange, &symbol);
    let depth = params.depth()?;

    // A cache failure is not a failed request while PostgreSQL can still
    // answer it, so it degrades to the slow path instead of a 503.
    let cached = match state.cache().book(&exchange, &symbol).await {
        Ok(cached) => cached,
        Err(error) => {
            tracing::warn!(%exchange, %symbol, %error, "book cache read failed");
            None
        }
    };

    let (source, snapshot) = match cached {
        Some(snapshot) => (Source::Cache, snapshot),
        None => {
            let id = resolve(&state, &exchange, &symbol).await?;
            let snapshot = state
                .store()
                .latest_snapshot(id)
                .await?
                .ok_or_else(|| ApiError::not_found("book", key(&exchange, &symbol)))?;
            (Source::Database, snapshot)
        }
    };

    Ok(Json(into_response(
        exchange,
        symbol,
        source,
        snapshot,
        depth,
        OffsetDateTime::now_utc(),
    )))
}

async fn trades(
    State(state): State<AppState>,
    Path((exchange, symbol)): Path<(String, String)>,
    Query(params): Query<TradesParams>,
) -> ApiResult<TradesResponse> {
    let (exchange, symbol) = normalise(&exchange, &symbol);
    let id = resolve(&state, &exchange, &symbol).await?;
    let trades = state.store().trades(params.into_query(id)?).await?;

    Ok(Json(TradesResponse {
        count: trades.len(),
        trades,
    }))
}

async fn vwap(
    State(state): State<AppState>,
    Path((exchange, symbol)): Path<(String, String)>,
    Query(params): Query<VwapParams>,
) -> ApiResult<VwapResponse> {
    let (exchange, symbol) = normalise(&exchange, &symbol);
    let id = resolve(&state, &exchange, &symbol).await?;
    let (start, end) = params.window()?;

    Ok(Json(VwapResponse {
        symbol_id: id,
        start,
        end,
        vwap: state.store().vwap(id, start, end).await?,
    }))
}

/// Path parameters are normalised the same way the cache keys are, so
/// `/v1/books/Binance/btcusdt` and `/v1/books/binance/BTCUSDT` are one
/// resource rather than two that disagree.
fn normalise(exchange: &str, symbol: &str) -> (String, String) {
    (
        exchange.trim().to_ascii_lowercase(),
        symbol.trim().to_ascii_uppercase(),
    )
}

fn key(exchange: &str, symbol: &str) -> String {
    format!("{exchange}/{symbol}")
}

async fn resolve(state: &AppState, exchange: &str, symbol: &str) -> Result<i32, ApiError> {
    state
        .store()
        .symbol_by_pair(exchange, symbol)
        .await?
        .map(|row| row.id)
        .ok_or_else(|| ApiError::not_found("symbol", key(exchange, symbol)))
}

/// Split out from the handler so the truncation and the age arithmetic are
/// testable without a request.
fn into_response(
    exchange: String,
    symbol: String,
    source: Source,
    mut snapshot: BookSnapshot,
    depth: Option<usize>,
    now: OffsetDateTime,
) -> BookResponse {
    if let Some(depth) = depth {
        snapshot.bids.truncate(depth);
        snapshot.asks.truncate(depth);
    }

    BookResponse {
        exchange,
        symbol,
        source,
        sequence: snapshot.sequence,
        captured_at: snapshot.captured_at,
        // Clamped: a snapshot captured by a host whose clock runs ahead should
        // read as fresh, not as a negative age.
        age_ms: ((now - snapshot.captured_at).whole_milliseconds() as i64).max(0),
        spread: snapshot.spread(),
        bids: snapshot.bids,
        asks: snapshot.asks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use time::Duration;

    fn snapshot() -> BookSnapshot {
        BookSnapshot {
            symbol_id: 1,
            sequence: 42,
            captured_at: OffsetDateTime::UNIX_EPOCH,
            bids: vec![
                Level::new(dec!(100), dec!(1)),
                Level::new(dec!(99), dec!(2)),
                Level::new(dec!(98), dec!(3)),
            ],
            asks: vec![
                Level::new(dec!(101), dec!(1)),
                Level::new(dec!(102), dec!(2)),
            ],
        }
    }

    fn respond(depth: Option<usize>, now: OffsetDateTime) -> BookResponse {
        into_response(
            "binance".into(),
            "BTCUSDT".into(),
            Source::Cache,
            snapshot(),
            depth,
            now,
        )
    }

    #[test]
    fn a_depth_limit_truncates_both_sides() {
        let response = respond(Some(2), OffsetDateTime::UNIX_EPOCH);

        assert_eq!(response.bids.len(), 2);
        assert_eq!(response.asks.len(), 2);
        assert_eq!(response.bids[0].price, dec!(100));
    }

    #[test]
    fn without_a_depth_the_whole_stored_book_is_returned() {
        let response = respond(None, OffsetDateTime::UNIX_EPOCH);

        assert_eq!(response.bids.len(), 3);
        assert_eq!(response.spread, Some(dec!(1)));
    }

    #[test]
    fn age_is_measured_against_the_capture_time() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(1_500);

        assert_eq!(respond(None, now).age_ms, 1_500);
    }

    #[test]
    fn a_capture_time_in_the_future_reads_as_fresh() {
        let now = OffsetDateTime::UNIX_EPOCH - Duration::seconds(5);

        assert_eq!(respond(None, now).age_ms, 0);
    }

    #[test]
    fn paths_are_normalised_the_way_cache_keys_are() {
        assert_eq!(
            normalise(" Binance ", "btcusdt"),
            ("binance".to_owned(), "BTCUSDT".to_owned())
        );
    }

    #[test]
    fn decimals_serialise_as_strings() {
        let json = serde_json::to_string(&respond(None, OffsetDateTime::UNIX_EPOCH)).unwrap();

        assert!(json.contains(r#""spread":"1""#), "{json}");
        assert!(json.contains(r#""source":"cache""#), "{json}");
    }
}
