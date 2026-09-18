//! Ingestion errors.

use crate::protocol::ProtocolError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("websocket transport failed")]
    Transport(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("could not fetch the depth snapshot")]
    Http(#[from] reqwest::Error),
    #[error("the exchange answered the snapshot request with {status}")]
    SnapshotStatus { status: u16 },
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("storage failed")]
    Storage(#[from] obe_storage::Error),
    #[error("the feed closed the connection")]
    Closed,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether reconnecting stands a chance of fixing this.
    ///
    /// A dropped socket or a rate-limited snapshot is worth retrying; a frame
    /// this service cannot parse will parse no better the second time, and
    /// retrying it would hide a protocol change behind a reconnect loop.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::Http(_) | Self::Closed => true,
            Self::SnapshotStatus { status } => *status == 429 || *status >= 500,
            Self::Protocol(_) | Self::Storage(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_failures_are_retryable() {
        assert!(Error::Closed.is_retryable());
    }

    #[test]
    fn rate_limits_and_server_errors_are_retryable() {
        assert!(Error::SnapshotStatus { status: 429 }.is_retryable());
        assert!(Error::SnapshotStatus { status: 503 }.is_retryable());
    }

    #[test]
    fn a_rejected_request_is_not_retryable() {
        // 400 means the request itself is wrong: a bad symbol or limit.
        // Retrying it just rate-limits the client for nothing.
        assert!(!Error::SnapshotStatus { status: 400 }.is_retryable());
    }

    #[test]
    fn an_unparseable_frame_is_not_retryable() {
        let protocol = ProtocolError::Timestamp(i64::MAX);

        assert!(!Error::Protocol(protocol).is_retryable());
    }
}
