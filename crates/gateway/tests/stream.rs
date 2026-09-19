//! `/v1/stream` driven over a real WebSocket, with no Redis running.
//!
//! The hub's input is a channel, so the test publishes into it directly and
//! the whole path a client exercises — upgrade, subscribe, topic filtering,
//! unsubscribe, rejection handling — is covered without pub/sub, a container,
//! or a live feed. Redis's own half is covered in the storage integration
//! suite against a real server, and the lag and ordering rules are unit tests
//! on the hub.
//!
//! The server is bound on port 0 and torn down with the test, so the suite
//! runs in parallel without picking ports.

use std::net::SocketAddr;

use futures_util::{SinkExt, StreamExt};
use obe_core::Settings;
use obe_gateway::{router, AppState};
use obe_storage::{Kind, StreamMessage, Topic};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// A gateway listening on a loopback port, and the hub feeding it.
struct Server {
    addr: SocketAddr,
    state: AppState,
}

impl Server {
    async fn start() -> Self {
        let mut settings =
            Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
                .expect("repository config should load");
        // Closed ports: nothing here should reach a dependency, and a cold
        // cache is what an opening-book read has to cope with anyway.
        settings.database.url = "postgres://nobody:nothing@127.0.0.1:1/absent".into();
        settings.redis.url = "redis://127.0.0.1:1".into();

        let state = AppState::new(&settings).expect("pools should build without connecting");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let app = router(state.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self { addr, state }
    }

    async fn connect(&self) -> Client {
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{}/v1/stream", self.addr))
            .await
            .expect("the upgrade should be accepted");

        Client { socket }
    }

    fn publish(&self, symbol: &str, sequence: i64) -> usize {
        self.state.hub().dispatch(book(symbol, sequence))
    }

    fn publish_tape(&self, symbol: &str) -> usize {
        self.state.hub().dispatch(StreamMessage {
            topic: Topic::new(Kind::Trades, "binance", symbol),
            sequence: None,
            payload: format!(
                r#"{{"type":"trades","exchange":"binance","symbol":"{symbol}","trades":[]}}"#
            ),
        })
    }
}

struct Client {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl Client {
    async fn send(&mut self, raw: &str) {
        self.socket
            .send(Message::Text(raw.into()))
            .await
            .expect("the command should go out");
    }

    /// The next JSON frame, skipping the protocol pings axum may interleave.
    async fn next(&mut self) -> Value {
        loop {
            let message =
                tokio::time::timeout(std::time::Duration::from_secs(5), self.socket.next())
                    .await
                    .expect("a frame should arrive inside the timeout")
                    .expect("the socket should stay open")
                    .expect("the frame should be readable");

            match message {
                Message::Text(raw) => {
                    return serde_json::from_str(&raw).expect("frames should be JSON")
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
    }
}

fn book(symbol: &str, sequence: i64) -> StreamMessage {
    StreamMessage {
        topic: Topic::new(Kind::Book, "binance", symbol),
        sequence: Some(sequence),
        payload: format!(
            r#"{{"type":"book","exchange":"binance","symbol":"{symbol}","sequence":{sequence},"bids":[{{"price":"0.1","quantity":"2"}}],"asks":[]}}"#
        ),
    }
}

/// A published message only reaches a client that has already subscribed, so
/// wait for the receiver to exist rather than sleeping a guess.
async fn wait_for_clients(server: &Server, count: usize) {
    for _ in 0..200 {
        if server.state.hub().receivers() >= count {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the client never registered with the hub");
}

#[tokio::test]
async fn a_subscriber_receives_its_topic_and_nothing_else() {
    let server = Server::start().await;
    let mut client = server.connect().await;
    wait_for_clients(&server, 1).await;

    client
        .send(r#"{"op":"subscribe","channels":["book:binance:BTCUSDT"]}"#)
        .await;
    let ack = client.next().await;
    assert_eq!(ack["type"], "subscribed");
    assert_eq!(ack["channels"][0], "book:binance:BTCUSDT");

    // Not subscribed: this one must not arrive, and the assertion below would
    // see it first if the filter were wrong.
    server.publish("ETHUSDT", 1);
    server.publish("BTCUSDT", 99);

    let update = client.next().await;
    assert_eq!(update["symbol"], "BTCUSDT");
    assert_eq!(update["sequence"], 99);
    // Forwarded as the publisher wrote it: prices are still strings.
    assert_eq!(update["bids"][0]["price"], "0.1");
}

#[tokio::test]
async fn one_redis_subscription_feeds_every_connected_client() {
    let server = Server::start().await;
    let mut left = server.connect().await;
    let mut right = server.connect().await;
    wait_for_clients(&server, 2).await;

    for client in [&mut left, &mut right] {
        client
            .send(r#"{"op":"subscribe","channels":["book:binance:BTCUSDT"]}"#)
            .await;
        assert_eq!(client.next().await["type"], "subscribed");
    }

    // One dispatch, two sockets: what Redis sees does not grow with clients.
    assert_eq!(server.publish("BTCUSDT", 7), 2);

    assert_eq!(left.next().await["sequence"], 7);
    assert_eq!(right.next().await["sequence"], 7);
}

#[tokio::test]
async fn unsubscribing_stops_the_updates() {
    let server = Server::start().await;
    let mut client = server.connect().await;
    wait_for_clients(&server, 1).await;

    client
        .send(r#"{"op":"subscribe","channels":["book:binance:BTCUSDT","trades:binance:BTCUSDT"]}"#)
        .await;
    assert_eq!(
        client.next().await["channels"].as_array().map(Vec::len),
        Some(2)
    );

    client
        .send(r#"{"op":"unsubscribe","channels":["book:binance:BTCUSDT"]}"#)
        .await;
    let ack = client.next().await;
    assert_eq!(ack["channels"].as_array().map(Vec::len), Some(1));
    assert_eq!(ack["channels"][0], "trades:binance:BTCUSDT");

    // The book is gone; the tape still arrives.
    server.publish("BTCUSDT", 1);
    server.publish_tape("BTCUSDT");

    assert_eq!(client.next().await["type"], "trades");
}

#[tokio::test]
async fn a_bad_command_is_answered_and_the_connection_survives() {
    let server = Server::start().await;
    let mut client = server.connect().await;
    wait_for_clients(&server, 1).await;

    client.send("not json").await;
    let malformed = client.next().await;
    assert_eq!(malformed["type"], "error");
    assert_eq!(malformed["code"], "invalid_command");

    client
        .send(r#"{"op":"subscribe","channels":["book:binance"]}"#)
        .await;
    let rejected = client.next().await;
    assert_eq!(rejected["type"], "error");
    assert_eq!(rejected["code"], "unknown_channel");

    // Still usable: a client that fat-fingers one command is not disconnected.
    client.send(r#"{"op":"ping"}"#).await;
    assert_eq!(client.next().await["type"], "pong");
}

#[tokio::test]
async fn subscribing_to_a_cold_cache_still_succeeds() {
    let server = Server::start().await;
    let mut client = server.connect().await;
    wait_for_clients(&server, 1).await;

    // Redis is on a closed port here, so the opening-book read fails. That
    // costs the client its head start, not its subscription.
    client
        .send(r#"{"op":"subscribe","channels":["book:binance:BTCUSDT"]}"#)
        .await;
    assert_eq!(client.next().await["type"], "subscribed");

    server.publish("BTCUSDT", 5);
    assert_eq!(client.next().await["sequence"], 5);
}
