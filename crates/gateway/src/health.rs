//! Health endpoints.
//!
//! `/health/live` answers "is the process up", `/health/ready` answers "can it
//! serve traffic". Readiness has no dependencies to check yet — Postgres and
//! Redis probes are wired in with the storage layer.

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

pub async fn ready(State(_state): State<AppState>) -> (StatusCode, Json<Readiness>) {
    let checks: Vec<Check> = Vec::new();
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

    #[test]
    fn no_checks_means_ready() {
        assert_eq!(overall(&[]), Status::Up);
    }

    #[test]
    fn one_failing_check_fails_the_whole_probe() {
        let checks = vec![
            Check {
                name: "postgres",
                status: Status::Up,
                detail: None,
            },
            Check {
                name: "redis",
                status: Status::Down,
                detail: Some("connection refused".into()),
            },
        ];

        assert_eq!(overall(&checks), Status::Down);
    }
}
