//! Health endpoints.
//!
//! `/health/live` answers "is the process up" and deliberately depends on
//! nothing: an orchestrator restarting the pod because PostgreSQL is slow
//! turns a degraded service into an outage. `/health/ready` answers "can it
//! serve traffic" and does check the dependencies, under a timeout, so a
//! wedged one is reported as down instead of hanging the probe.

use std::future::Future;
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

use crate::AppState;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Up,
    Down,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub status: Status,
    pub service: String,
    pub version: &'static str,
    pub uptime_secs: u64,
}

#[derive(Debug, Serialize)]
pub struct Liveness {
    pub status: Status,
}

#[derive(Debug, Serialize)]
pub struct Readiness {
    pub status: Status,
    pub checks: Vec<Check>,
}

#[derive(Debug, Serialize)]
pub struct Check {
    pub name: &'static str,
    pub status: Status,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

pub async fn summary(State(state): State<AppState>) -> Json<Summary> {
    Json(Summary {
        status: Status::Up,
        service: state.service_name().to_owned(),
        version: VERSION,
        uptime_secs: state.uptime_secs(),
    })
}

pub async fn live() -> Json<Liveness> {
    Json(Liveness { status: Status::Up })
}

pub async fn ready(State(state): State<AppState>) -> (StatusCode, Json<Readiness>) {
    // Probed concurrently: the endpoint costs one timeout, not two.
    let (postgres, redis) = tokio::join!(
        probe("postgres", &state, state.store().ping()),
        probe("redis", &state, state.cache().ping()),
    );

    let checks = vec![postgres, redis];
    let body = Readiness {
        status: overall(&checks),
        checks,
    };
    let code = match body.status {
        Status::Up => StatusCode::OK,
        Status::Down => StatusCode::SERVICE_UNAVAILABLE,
    };

    (code, Json(body))
}

/// Runs one dependency check under the configured budget and records how long
/// it took, whichever way it ended.
async fn probe<F, T, E>(name: &'static str, state: &AppState, check: F) -> Check
where
    F: Future<Output = Result<T, E>>,
    E: std::error::Error,
{
    let started = Instant::now();
    let outcome = tokio::time::timeout(state.readiness_timeout(), check).await;
    let latency_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);

    let (status, detail) = match outcome {
        Ok(Ok(_)) => (Status::Up, None),
        Ok(Err(error)) => (Status::Down, Some(describe(&error))),
        Err(_) => (
            Status::Down,
            Some(format!(
                "timed out after {}ms",
                state.readiness_timeout().as_millis()
            )),
        ),
    };

    if status == Status::Down {
        tracing::warn!(check = name, latency_ms, detail, "readiness check failed");
    }

    Check {
        name,
        status,
        latency_ms,
        detail,
    }
}

/// Flattens the error chain, because the outermost message on its own
/// ("database error") never says which host refused the connection.
fn describe(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();

    while let Some(cause) = source {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        source = cause.source();
    }

    rendered
}

fn overall(checks: &[Check]) -> Status {
    if checks.iter().all(|check| check.status == Status::Up) {
        Status::Up
    } else {
        Status::Down
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(name: &'static str, status: Status) -> Check {
        Check {
            name,
            status,
            latency_ms: 1,
            detail: None,
        }
    }

    #[test]
    fn no_checks_means_ready() {
        assert_eq!(overall(&[]), Status::Up);
    }

    #[test]
    fn all_checks_up_means_ready() {
        let checks = vec![check("postgres", Status::Up), check("redis", Status::Up)];

        assert_eq!(overall(&checks), Status::Up);
    }

    #[test]
    fn one_failing_check_fails_the_whole_probe() {
        let checks = vec![check("postgres", Status::Up), check("redis", Status::Down)];

        assert_eq!(overall(&checks), Status::Down);
    }

    #[test]
    fn describe_flattens_the_error_chain() {
        #[derive(Debug, thiserror::Error)]
        #[error("connection refused")]
        struct Inner;

        #[derive(Debug, thiserror::Error)]
        #[error("database error")]
        struct Outer(#[from] Inner);

        assert_eq!(
            describe(&Outer(Inner)),
            "database error: connection refused"
        );
    }
}
