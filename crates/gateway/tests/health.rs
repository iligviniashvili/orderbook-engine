//! Drives the real router through `tower::ServiceExt::oneshot`, so routing,
//! extractors and serialisation are all exercised without binding a socket.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use obe_core::Settings;
use obe_gateway::{router, AppState};
use serde_json::Value;
use tower::ServiceExt;

async fn get(path: &str) -> (StatusCode, Value) {
    let settings = Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
        .expect("repository config should load");
    let app = router(AppState::new(&settings));

    let response = app
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body should collect")
        .to_bytes();

    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("body should be JSON")
    };

    (status, body)
}

#[tokio::test]
async fn liveness_reports_up() {
    let (status, body) = get("/health/live").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "up");
}

#[tokio::test]
async fn readiness_has_no_dependencies_yet() {
    let (status, body) = get("/health/ready").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "up");
    assert_eq!(body["checks"].as_array().map(Vec::len), Some(0));
}

#[tokio::test]
async fn summary_reports_service_and_version() {
    let (status, body) = get("/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["service"], "orderbook-engine");
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert!(body["uptime_secs"].is_u64());
}

#[tokio::test]
async fn unknown_route_is_404() {
    let (status, _) = get("/nope").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}
