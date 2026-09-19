//! The market-data transport.
//!
//! [`Feed`] is a trait rather than a concrete socket so that everything above
//! it — sequencing, batching, resync — is testable without an exchange, a
//! network or a clock that ticks in real time. [`WebSocketFeed`] is the one
//! implementation that talks to a real venue.

use std::future::Future;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::error::Result;
use crate::protocol::{parse_frame, FeedEvent};
use obe_core::Backoff;

/// How long a connection has to survive before it is treated as healthy and
/// the backoff sequence is allowed to start over. Without it, a socket that
/// accepts and immediately drops would reset the delay every time and turn the
/// backoff into a hot loop.
const HEALTHY_AFTER: Duration = Duration::from_secs(30);

/// What a feed hands back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedItem {
    Event(FeedEvent),
    /// The transport reconnected. Whatever the stream sent in between is gone,
    /// so every book built from it has to be rebuilt from a fresh snapshot.
    Reconnected,
}

/// A source of market-data events.
pub trait Feed: Send {
    /// The next item, waiting as long as it takes. Implementations that own a
    /// connection reconnect internally and report it as
    /// [`FeedItem::Reconnected`] instead of surfacing the error.
    fn recv(&mut self) -> impl Future<Output = Result<FeedItem>> + Send;
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A combined-stream WebSocket connection that reconnects on its own.
pub struct WebSocketFeed {
    url: String,
    socket: Option<Socket>,
    backoff: Backoff,
    connected_at: Option<Instant>,
}

/// Hand-written because the socket itself is not `Debug`, and because a
/// connection's buffers are not what anyone wants in a log line anyway.
impl std::fmt::Debug for WebSocketFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketFeed")
            .field("url", &self.url)
            .field("connected", &self.socket.is_some())
            .field("attempt", &self.backoff.attempt())
            .finish()
    }
}

impl WebSocketFeed {
    /// Builds a feed for `symbols`, subscribing each to its diff-depth and
    /// trade streams. Nothing connects until the first [`Feed::recv`].
    pub fn new(base_url: &str, symbols: &[String], backoff: Backoff) -> Self {
        Self {
            url: combined_url(base_url, &stream_names(symbols)),
            socket: None,
            backoff,
            connected_at: None,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    async fn connect(&mut self) -> Result<()> {
        if self.backoff.attempt() > 0 {
            let delay = self.backoff.next_delay();
            tracing::warn!(
                attempt = self.backoff.attempt(),
                delay_ms = delay.as_millis(),
                "reconnecting to the feed"
            );
            tokio::time::sleep(delay).await;
        } else {
            // Charge the first attempt so a failure right after it backs off
            // rather than retrying instantly.
            self.backoff.next_delay();
        }

        let (socket, _) = tokio_tungstenite::connect_async(&self.url).await?;
        self.socket = Some(socket);
        self.connected_at = Some(Instant::now());
        tracing::info!(url = %self.url, "feed connected");
        Ok(())
    }

    /// Drops the socket and decides whether the connection lasted long enough
    /// to count as healthy.
    fn disconnect(&mut self) {
        self.socket = None;
        if self
            .connected_at
            .take()
            .is_some_and(|since| since.elapsed() >= HEALTHY_AFTER)
        {
            self.backoff.reset();
        }
    }
}

impl Feed for WebSocketFeed {
    async fn recv(&mut self) -> Result<FeedItem> {
        loop {
            let Some(socket) = self.socket.as_mut() else {
                match self.connect().await {
                    Ok(()) => return Ok(FeedItem::Reconnected),
                    Err(error) if error.is_retryable() => {
                        tracing::warn!(%error, "feed connection failed");
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            };

            match socket.next().await {
                Some(Ok(Message::Text(raw))) => {
                    if let Some(event) = parse_frame(raw.as_str())? {
                        return Ok(FeedItem::Event(event));
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    // Exchanges drop clients that stop answering pings, and
                    // tungstenite only queues the pong — it needs a flush.
                    socket.send(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Close(frame))) => {
                    tracing::warn!(?frame, "feed closed the connection");
                    self.disconnect();
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    tracing::warn!(%error, "feed transport error");
                    self.disconnect();
                }
                None => self.disconnect(),
            }
        }
    }
}

/// The stream names one symbol needs: 100 ms diff depth, and the trade tape.
fn stream_names(symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .flat_map(|symbol| {
            let symbol = symbol.trim().to_ascii_lowercase();
            [format!("{symbol}@depth@100ms"), format!("{symbol}@trade")]
        })
        .collect()
}

/// Joins the stream names into one combined-stream URL. One socket for every
/// symbol, rather than one per symbol: exchanges cap connections per IP, and
/// a single socket also gives the events a single arrival order.
fn combined_url(base: &str, streams: &[String]) -> String {
    format!(
        "{}?streams={}",
        base.trim_end_matches('/'),
        streams.join("/")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols() -> Vec<String> {
        vec!["BTCUSDT".into(), " ethusdt ".into()]
    }

    #[test]
    fn each_symbol_subscribes_to_depth_and_trades() {
        assert_eq!(
            stream_names(&symbols()),
            vec![
                "btcusdt@depth@100ms",
                "btcusdt@trade",
                "ethusdt@depth@100ms",
                "ethusdt@trade",
            ]
        );
    }

    #[test]
    fn every_symbol_shares_one_socket() {
        let feed = WebSocketFeed::new(
            "wss://stream.example.com/stream/",
            &symbols(),
            Backoff::with_seed(Duration::from_millis(1), Duration::from_millis(2), 1),
        );

        assert_eq!(
            feed.url(),
            "wss://stream.example.com/stream?streams=\
             btcusdt@depth@100ms/btcusdt@trade/ethusdt@depth@100ms/ethusdt@trade"
        );
    }

    #[test]
    fn a_feed_with_no_symbols_still_builds_a_url() {
        assert_eq!(
            combined_url("wss://x/stream", &[]),
            "wss://x/stream?streams="
        );
    }
}
