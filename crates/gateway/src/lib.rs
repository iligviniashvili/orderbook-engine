//! Client-facing gateway.
//!
//! Milestone 1 exposes health endpoints only; historical REST queries and the
//! live WebSocket stream are added in later milestones.

pub mod health;
pub mod state;

use axum::routing::get;
use axum::Router;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;

pub use state::AppState;

/// Builds the gateway router. Kept separate from `main` so tests can drive it
/// without binding a socket.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::summary))
        .route("/health/live", get(health::live))
        .route("/health/ready", get(health::ready))
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
