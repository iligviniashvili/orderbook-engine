//! `GET /v1/stream` — the live WebSocket feed.
//!
//! A connection is one task holding one `broadcast` receiver. It does three
//! things and nothing else: read commands, forward the topics it was asked
//! for, and prove to itself that the peer is still there.
//!
//! Subscribing to a book replies with the cached book first and then follows
//! the stream — the same snapshot-then-deltas bootstrap the ingestor performs
//! against the exchange, for the same reason: a stream of updates alone cannot
//! tell you what is already there.

use std::time::Duration;

use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use obe_storage::{Kind, StreamEvent, Topic};
use tokio::sync::broadcast::error::RecvError;

use crate::protocol::{Command, Frame, Rejection, Subscriptions};
use crate::AppState;

/// How often the server pings an idle peer.
const HEARTBEAT: Duration = Duration::from_secs(15);

/// A peer that has said nothing at all — no pong, no frame — for this long is
/// gone. TCP will happily hold a socket open to a laptop that closed its lid,
/// and each one of those pins a receiver and a task.
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// Upper bound on one inbound frame. Commands are tens of bytes; nothing a
/// client sends has any business being larger, and the default lets an
/// anonymous peer buffer megabytes.
const MAX_COMMAND_BYTES: usize = 16 * 1024;

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/stream", get(upgrade))
}

async fn upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.max_message_size(MAX_COMMAND_BYTES)
        .on_upgrade(move |socket| serve(socket, state))
}

async fn serve(socket: WebSocket, state: AppState) {
    let (mut sink, mut source) = socket.split();
    let mut updates = state.hub().subscribe();
    let mut subscriptions = Subscriptions::default();

    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_seen = tokio::time::Instant::now();

    tracing::debug!(clients = state.hub().receivers(), "stream client connected");

    loop {
        tokio::select! {
            incoming = source.next() => {
                let Some(Ok(message)) = incoming else { break };
                last_seen = tokio::time::Instant::now();

                match message {
                    Message::Text(raw) => {
                        if !on_command(&mut sink, &state, &mut subscriptions, raw.as_str()).await {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    // Pongs and axum's automatic ping replies need no handling
                    // beyond the liveness stamp taken above.
                    Message::Ping(_) | Message::Pong(_) => {}
                    // The protocol here is JSON. A binary frame is a client
                    // bug, and saying so beats decoding it as text by accident.
                    Message::Binary(_) => {
                        let frame = Frame::error(binary_unsupported());
                        if send(&mut sink, frame.encode()).await.is_err() {
                            break;
                        }
                    }
                }
            }

            update = updates.recv() => {
                match update {
                    Ok(message) => {
                        if !subscriptions.contains(&message.topic) {
                            continue;
                        }
                        // The publisher's bytes, unmodified: the gateway has
                        // not parsed the book and will not re-encode it.
                        if send(&mut sink, message.payload.clone()).await.is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(missed)) => {
                        tracing::warn!(missed, "stream client fell behind");
                        if send(&mut sink, Frame::Lagged { missed }.encode()).await.is_err() {
                            break;
                        }
                    }
                    // The reader task is gone, which means the process is
                    // shutting down.
                    Err(RecvError::Closed) => break,
                }
            }

            _ = heartbeat.tick() => {
                if last_seen.elapsed() >= IDLE_TIMEOUT {
                    tracing::debug!("stream client went silent; closing");
                    break;
                }
                if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
        }
    }

    let _ = sink.close().await;
    tracing::debug!(topics = subscriptions.len(), "stream client disconnected");
}

/// Handles one command. Returns whether the connection should stay open — it
/// closes only when the socket itself fails, never because a client sent
/// something wrong.
async fn on_command(
    sink: &mut SplitSink<WebSocket, Message>,
    state: &AppState,
    subscriptions: &mut Subscriptions,
    raw: &str,
) -> bool {
    let command = match Command::parse(raw) {
        Ok(command) => command,
        Err(rejection) => return send(sink, Frame::error(rejection).encode()).await.is_ok(),
    };

    match command {
        Command::Subscribe { channels } => {
            let added = match subscriptions.add(&channels) {
                Ok(added) => added,
                Err(rejection) => {
                    return send(sink, Frame::error(rejection).encode()).await.is_ok()
                }
            };

            let ack = Frame::Subscribed {
                channels: subscriptions.names(),
            };
            if send(sink, ack.encode()).await.is_err() {
                return false;
            }

            // Then the opening book for each new topic, so a client is not
            // blind until the next tick.
            for topic in &added {
                if let Some(payload) = opening_book(state, topic).await {
                    if send(sink, payload).await.is_err() {
                        return false;
                    }
                }
            }
            true
        }
        Command::Unsubscribe { channels } => {
            if let Err(rejection) = subscriptions.remove(&channels) {
                return send(sink, Frame::error(rejection).encode()).await.is_ok();
            }
            let ack = Frame::Subscribed {
                channels: subscriptions.names(),
            };
            send(sink, ack.encode()).await.is_ok()
        }
        Command::Ping => send(sink, Frame::Pong.encode()).await.is_ok(),
    }
}

/// The cached book for a topic, rendered as the stream event a client would
/// have received had it been connected a moment earlier.
///
/// `None` when the topic is the tape (there is no "current" tape), when the
/// cache is cold, or when the cache is unreachable — in the last case the
/// subscription still stands and the next update will arrive, so a dead Redis
/// read costs the client a head start, not its connection.
async fn opening_book(state: &AppState, topic: &Topic) -> Option<String> {
    if topic.kind != Kind::Book {
        return None;
    }

    let snapshot = match state.cache().book(&topic.exchange, &topic.symbol).await {
        Ok(snapshot) => snapshot?,
        Err(error) => {
            tracing::warn!(topic = %topic.name(), %error, "opening book read failed");
            return None;
        }
    };

    let event = StreamEvent::Book {
        exchange: topic.exchange.clone(),
        symbol: topic.symbol.clone(),
        sequence: snapshot.sequence,
        captured_at: snapshot.captured_at,
        bids: snapshot.bids,
        asks: snapshot.asks,
    };

    serde_json::to_string(&event).ok()
}

async fn send(sink: &mut SplitSink<WebSocket, Message>, payload: String) -> Result<(), ()> {
    sink.send(Message::Text(Utf8Bytes::from(payload)))
        .await
        .map_err(|_| ())
}

fn binary_unsupported() -> Rejection {
    Rejection {
        code: "invalid_command",
        message: "commands must be text frames of JSON".to_owned(),
    }
}
