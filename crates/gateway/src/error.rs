//! What the API returns when it cannot return data.
//!
//! Two rules shape this module. A dependency being down is a `503`, not a
//! `500`: the gateway is fine, and the distinction is what tells a caller to
//! retry rather than to open a ticket. And the body of a `503` never carries
//! the underlying error text — a connection failure renders the DSN's host,
//! port and user, which is not something to hand an anonymous client. The
//! detail goes to the log, where it belongs.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The request itself is wrong: an unparseable timestamp, an inverted
    /// window, a limit of zero.
    #[error("{0}")]
    Invalid(String),
    #[error("no {resource} matching {key}")]
    NotFound { resource: &'static str, key: String },
    /// Storage did not answer.
    #[error("storage unavailable")]
    Unavailable(#[source] obe_storage::Error),
}

impl ApiError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    pub fn not_found(resource: &'static str, key: impl Into<String>) -> Self {
        Self::NotFound {
            resource,
            key: key.into(),
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// The machine-readable discriminator clients should branch on, rather
    /// than on the prose.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "invalid_request",
            Self::NotFound { .. } => "not_found",
            Self::Unavailable(_) => "unavailable",
        }
    }
}

/// A storage error is a rejected argument or a dependency failure, and the two
/// are not the same answer: `Invalid` means the caller asked for something
/// impossible, everything else means the database or cache let us down.
impl From<obe_storage::Error> for ApiError {
    fn from(error: obe_storage::Error) -> Self {
        match error {
            obe_storage::Error::Invalid(message) => Self::Invalid(message),
            other => Self::Unavailable(other),
        }
    }
}

#[derive(Debug, Serialize)]
struct Body {
    error: Detail,
}

#[derive(Debug, Serialize)]
struct Detail {
    code: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();

        // The only branch whose text is withheld; it is logged instead.
        let message = match &self {
            Self::Unavailable(source) => {
                tracing::error!(error = %source, "storage unavailable");
                "a storage dependency is unavailable".to_owned()
            }
            other => other.to_string(),
        };

        (
            status,
            Json(Body {
                error: Detail {
                    code: self.code(),
                    message,
                },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rejected_argument_from_storage_is_the_callers_fault() {
        let error: ApiError = obe_storage::Error::Invalid("limit must be positive".into()).into();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.to_string(), "limit must be positive");
    }

    #[test]
    fn a_dependency_failure_is_not_the_callers_fault() {
        let error: ApiError = obe_storage::Error::Database(sqlx::Error::PoolClosed).into();

        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code(), "unavailable");
    }

    #[test]
    fn a_missing_symbol_is_a_404_naming_what_was_looked_up() {
        let error = ApiError::not_found("symbol", "binance/DOGEUSDT");

        assert_eq!(error.status(), StatusCode::NOT_FOUND);
        assert_eq!(error.to_string(), "no symbol matching binance/DOGEUSDT");
    }

    #[tokio::test]
    async fn the_body_of_a_503_does_not_leak_the_connection_string() {
        use axum::body::to_bytes;

        let error = ApiError::Unavailable(obe_storage::Error::Invalid(
            "postgres://user:hunter2@db.internal:5432".into(),
        ));

        let body = to_bytes(error.into_response().into_body(), 4096)
            .await
            .expect("body should collect");
        let rendered = String::from_utf8(body.to_vec()).expect("body should be UTF-8");

        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("unavailable"), "{rendered}");
    }
}
