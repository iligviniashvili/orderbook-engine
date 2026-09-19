//! Client-facing gateway.
//!
//! Health endpoints, the read API over what the ingestor reconstructed — the
//! instrument registry, the current book, trade history and a volume-weighted
//! average — and the live WebSocket stream that pushes the same data as it
//! changes.

pub mod api;
pub mod error;
pub mod health;
pub mod hub;
pub mod protocol;
pub mod query;
pub mod state;
pub mod stream;

use axum::routing::get;
use axum::Router;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;

pub use error::ApiError;
pub use hub::Hub;
pub use state::AppState;

/// Builds the gateway router. Kept separate from `main` so tests can drive it
/// without binding a socket.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::summary))
        .route("/health/live", get(health::live))
        .route("/health/ready", get(health::ready))
        .merge(api::routes())
        .merge(stream::routes())
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
