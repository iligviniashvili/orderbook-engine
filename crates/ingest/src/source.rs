//! Where a resync gets its starting book from.
//!
//! The diff stream alone can never bootstrap a book: it says what changed, not
//! what is there. Every sync therefore begins with a REST snapshot, and the
//! sequence rules in [`crate::book`] stitch the stream onto it.

use std::future::Future;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::protocol::{parse_snapshot, DepthSnapshot};

/// Fetches a starting book for one symbol.
pub trait SnapshotSource: Send + Sync {
    fn depth_snapshot(
        &self,
        symbol: &str,
        limit: u16,
    ) -> impl Future<Output = Result<DepthSnapshot>> + Send;
}

/// The exchange's REST depth endpoint.
#[derive(Debug, Clone)]
pub struct HttpSnapshotSource {
    client: reqwest::Client,
    url: String,
}

impl HttpSnapshotSource {
    pub fn new(url: impl Into<String>, timeout: Duration) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            url: url.into(),
        })
    }
}

impl SnapshotSource for HttpSnapshotSource {
    async fn depth_snapshot(&self, symbol: &str, limit: u16) -> Result<DepthSnapshot> {
        let limit = limit.to_string();
        let response = self
            .client
            .get(&self.url)
            .query(&[("symbol", symbol), ("limit", limit.as_str())])
            .send()
            .await?;

        // The status is checked before the body is read: an error body is not
        // a snapshot, and parsing it would report a protocol failure — which
        // is not retryable — for what is really a rate limit, which is.
        let status = response.status();
        if !status.is_success() {
            return Err(Error::SnapshotStatus {
                status: status.as_u16(),
            });
        }

        Ok(parse_snapshot(&response.text().await?)?)
    }
}
