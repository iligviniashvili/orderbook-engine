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
    let mut settings = Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
        .expect("repository config should load");
    // Point the dependencies at a closed port and keep the probe budget short:
    // these tests are about the router, and must not need live services or
    // wait two seconds for a connection refusal.
    settings.database.url = "postgres://nobody:nothing@127.0.0.1:1/absent".into();
    settings.redis.url = "redis://127.0.0.1:1".into();
    settings.server.readiness_timeout_ms = 500;

    let state = AppState::new(&settings).expect("pools should build without connecting");
    let app = router(state);

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
async fn readiness_reports_unreachable_dependencies() {
    let (status, body) = get("/health/ready").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "down");

    let checks = body["checks"].as_array().expect("checks should be a list");
    let names: Vec<&str> = checks
        .iter()
        .map(|check| check["name"].as_str().expect("name should be a string"))
        .collect();
    assert_eq!(names, ["postgres", "redis"]);

    for check in checks {
        assert_eq!(check["status"], "down", "{check}");
        assert!(check["latency_ms"].is_u64(), "{check}");
        assert!(check["detail"].is_string(), "{check}");
    }
}

#[tokio::test]
async fn liveness_ignores_the_dependencies() {
    // The whole point of splitting the probes: Postgres and Redis are down in
    // this test, and liveness still says up, so nothing restarts the process.
    let (live_status, live_body) = get("/health/live").await;
    let (ready_status, _) = get("/health/ready").await;

    assert_eq!(live_status, StatusCode::OK);
    assert_eq!(live_body["status"], "up");
    assert_eq!(ready_status, StatusCode::SERVICE_UNAVAILABLE);
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
