//! Client-facing gateway.
//!
//! Health endpoints and the read API over what the ingestor reconstructed:
//! the instrument registry, the current book, trade history and a
//! volume-weighted average. The live WebSocket stream follows.

pub mod api;
pub mod error;
pub mod health;
pub mod query;
pub mod state;

use axum::routing::get;
use axum::Router;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;

pub use error::ApiError;
pub use state::AppState;

/// Builds the gateway router. Kept separate from `main` so tests can drive it
/// without binding a socket.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::summary))
        .route("/health/live", get(health::live))
        .route("/health/ready", get(health::ready))
        .merge(api::routes())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    tracing::info_span!(
                        "http",
                        method = %request.method(),
                        path = %request.uri().path(),
                    )
                })
                .on_response(DefaultOnResponse::new().level(Level::DEBUG)),
        )
        .with_state(state)
}
