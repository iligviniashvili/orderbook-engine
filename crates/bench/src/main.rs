//! Throughput measurements for the orderbook-engine hot paths.
//!
//! ```text
//! cargo run --release -p obe-bench                 # default workload
//! cargo run --release -p obe-bench -- --events 200000
//! cargo run --release -p obe-bench -- --json       # machine readable
//! ```
//!
//! What is measured is CPU work only: decoding exchange frames, rebuilding the
//! book, running the ingest loop, and fanning one update out to many WebSocket
//! subscribers. PostgreSQL and Redis are deliberately absent — with them in,
//! the number is a measurement of the database, and the shape of the code
//! above it disappears into the round trip. The figures are therefore an upper
//! bound on a single core, not a service-level throughput claim, and the
//! README says so where it quotes them.
//!
//! Every scenario runs on one thread. Nothing here is a statement about what a
//! deployment sustains; it is a statement about where the CPU goes.

mod harness;
mod workload;

use std::hint::black_box;

use obe_core::{IngestConfig, InstrumentConfig};
use obe_gateway::hub::Hub;
use obe_ingest::protocol::{parse_frame, DepthDelta, FeedEvent};
use obe_ingest::{Feed, FeedItem, OrderBook, Pipeline, Sink, SnapshotSource};
use obe_storage::{BookSnapshot, Kind, NewTrade, StreamEvent, StreamMessage, Topic, Write};
use time::OffsetDateTime;

use harness::{measure, measure_async, Measurement, Rng};
use workload::{SYMBOL, SYMBOL_ID};

/// Events per timed pass. Big enough to swamp the setup, small enough that the
/// whole run stays under a few seconds in CI.
const DEFAULT_EVENTS: usize = 200_000;

/// Subscribers in the fan-out scenario — the question being "what does one
/// update cost when a crowd is watching".
const DEFAULT_SUBSCRIBERS: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let options = Options::parse();
    let mut results = Vec::new();

    results.push(bench_decode(options.events));
    results.push(bench_apply(options.events));
    results.push(bench_decode_and_apply(options.events));
    results.push(bench_pipeline(options.events).await);
    results.push(bench_fanout(options.events, options.subscribers).await);

    report(&results, &options);
}

#[derive(Debug)]
struct Options {
    events: usize,
    subscribers: usize,
    json: bool,
}

impl Options {
    fn parse() -> Self {
        let mut options = Self {
            events: DEFAULT_EVENTS,
            subscribers: DEFAULT_SUBSCRIBERS,
            json: false,
        };

        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--json" => options.json = true,
                "--events" => {
                    i += 1;
                    options.events =
                        args.get(i).and_then(|v| v.parse().ok()).unwrap_or_else(|| {
                            eprintln!("--events needs a number");
                            std::process::exit(2);
                        });
                }
                "--subscribers" => {
                    i += 1;
                    options.subscribers =
                        args.get(i).and_then(|v| v.parse().ok()).unwrap_or_else(|| {
                            eprintln!("--subscribers needs a number");
                            std::process::exit(2);
                        });
                }
                other => {
                    eprintln!("unknown argument `{other}`");
                    std::process::exit(2);
                }
            }
            i += 1;
        }
        options
    }
}

fn report(results: &[Measurement], options: &Options) {
    if options.json {
        let body: Vec<String> = results.iter().map(Measurement::as_json).collect();
        println!("[{}]", body.join(","));
        return;
    }

    println!(
        "orderbook-engine: {} events per pass, {} stream subscribers, single thread, no I/O\n",
        options.events, options.subscribers
    );
    for result in results {
        println!("{result}");
    }
    println!(
        "\nThroughput is amortised over the pass, not a latency distribution. \
         PostgreSQL and Redis are absent by design."
    );
}

// ------------------------------------------------------------------- scenarios

/// Exchange JSON to this project's own types: the per-message cost every
/// ingested event pays before anything else happens to it.
fn bench_decode(events: usize) -> Measurement {
    let frames = depth_frames(events);

    measure("decode wire frames", "events", |_| {
        for frame in &frames {
            let parsed = parse_frame(black_box(frame)).expect("the frame should parse");
            black_box(parsed);
        }
        frames.len() as u64
    })
}

/// The book state machine on its own: sequence checks, `BTreeMap` updates and
/// deletions, with the decode already paid for.
fn bench_apply(events: usize) -> Measurement {
    let deltas = depth_deltas(events);

    measure("apply deltas to the book", "events", |_| {
        let mut book = OrderBook::from_snapshot(
            SYMBOL_ID,
            &workload::snapshot(deltas[0].first_update_id - 1),
            OffsetDateTime::UNIX_EPOCH,
        );
        for delta in &deltas {
            black_box(
                book.apply(black_box(delta))
                    .expect("the delta should apply"),
            );
        }
        deltas.len() as u64
    })
}

/// Both halves together, which is what one socket read actually costs.
fn bench_decode_and_apply(events: usize) -> Measurement {
    let frames = depth_frames(events);
    let first = first_update_id(&frames[0]);

    measure("decode and apply", "events", |_| {
        let mut book = OrderBook::from_snapshot(
            SYMBOL_ID,
            &workload::snapshot(first - 1),
            OffsetDateTime::UNIX_EPOCH,
        );
        for frame in &frames {
            let Some(FeedEvent::Depth(delta)) =
                parse_frame(black_box(frame)).expect("the frame should parse")
            else {
                continue;
            };
            black_box(book.apply(&delta).expect("the delta should apply"));
        }
        frames.len() as u64
    })
}

/// The whole ingest loop — resync, sequence checks, trade batching,
/// rate-limited publishing — with the sinks in memory so what is being timed
/// is the loop and not a database.
async fn bench_pipeline(events: usize) -> Measurement {
    let script = mixed_script(events);

    measure_async("ingest pipeline (sinks in memory)", "events", |_| {
        let script = script.clone();
        async move {
            let count = script.len();
            let mut pipeline = Pipeline::new(
                Replay::new(script),
                Seeded,
                Discard,
                pipeline_config(),
                &[(SYMBOL.to_owned(), SYMBOL_ID)],
            );

            for _ in 0..count {
                pipeline
                    .step()
                    .await
                    .expect("the pipeline should not stall");
            }
            black_box(pipeline.stats());
            count as u64
        }
    })
    .await
}

/// One published update reaching every connected subscriber. The unit is
/// deliveries, not messages: that is the number the gateway's fan-out has to
/// keep up with, and the whole point of the `Arc` is that it grows cheaply.
async fn bench_fanout(events: usize, subscribers: usize) -> Measurement {
    // A tenth of the event count: this scenario does `subscribers` times the
    // work per iteration, and the comparable figure is deliveries per second.
    let messages = (events / 10).max(1);
    let payload = workload::depth_event(&mut Rng::new(99), 1_000).1;

    measure_async("stream fan-out", "deliveries", |_| {
        let payload = payload.clone();
        async move {
            let hub = Hub::new(messages.next_power_of_two());
            let mut receivers: Vec<_> = (0..subscribers).map(|_| hub.subscribe()).collect();

            for sequence in 0..messages {
                hub.dispatch(StreamMessage {
                    topic: Topic::new(Kind::Book, "binance", SYMBOL),
                    sequence: Some(sequence as i64),
                    payload: payload.clone(),
                });
            }

            // Drain every receiver: dispatching into a channel nobody reads
            // would measure the send and none of the delivery.
            let mut delivered = 0u64;
            for receiver in &mut receivers {
                while let Ok(message) = receiver.try_recv() {
                    black_box(&message.payload);
                    delivered += 1;
                }
            }
            delivered
        }
    })
    .await
}

// ------------------------------------------------------------------- workloads

fn depth_frames(events: usize) -> Vec<String> {
    let mut rng = Rng::new(0x0BE1);
    let mut last = 1_000_000i64;

    (0..events)
        .map(|_| {
            let (next, frame) = workload::depth_event(&mut rng, last);
            last = next;
            frame
        })
        .collect()
}

fn depth_deltas(events: usize) -> Vec<DepthDelta> {
    depth_frames(events)
        .iter()
        .filter_map(|frame| match parse_frame(frame) {
            Ok(Some(FeedEvent::Depth(delta))) => Some(delta),
            _ => None,
        })
        .collect()
}

/// Depth and trades in the proportion a real socket carries them: the tape is
/// busier than the book, and batching it is most of what the loop does.
fn mixed_script(events: usize) -> Vec<FeedItem> {
    let mut rng = Rng::new(0x0BE2);
    let mut last = 1_000_000i64;
    let mut items = Vec::with_capacity(events);

    for id in 0..events {
        let frame = if id % 3 == 0 {
            let (next, frame) = workload::depth_event(&mut rng, last);
            last = next;
            frame
        } else {
            workload::trade_event(&mut rng, id as i64)
        };

        if let Ok(Some(event)) = parse_frame(&frame) {
            items.push(FeedItem::Event(event));
        }
    }
    items
}

fn first_update_id(frame: &str) -> i64 {
    match parse_frame(frame) {
        Ok(Some(FeedEvent::Depth(delta))) => delta.first_update_id,
        _ => panic!("the first frame should be a depth event"),
    }
}

fn pipeline_config() -> IngestConfig {
    IngestConfig {
        exchange: "binance".into(),
        stream_url: "wss://example.invalid/stream".into(),
        snapshot_url: "https://example.invalid/depth".into(),
        instruments: vec![InstrumentConfig {
            symbol: SYMBOL.into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            price_precision: 2,
            quantity_precision: 5,
        }],
        depth: 20,
        snapshot_depth: 1_000,
        trade_batch_size: 500,
        trade_flush_ms: 1_000,
        snapshot_interval_ms: 5_000,
        publish_interval_ms: 250,
        reconnect_base_ms: 250,
        reconnect_max_ms: 30_000,
    }
}

// ---------------------------------------------------------------- test doubles

/// Replays a script and then blocks, so a run does exactly as many steps as it
/// has events.
#[derive(Debug)]
struct Replay {
    items: std::vec::IntoIter<FeedItem>,
}

impl Replay {
    fn new(items: Vec<FeedItem>) -> Self {
        Self {
            items: items.into_iter(),
        }
    }
}

impl Feed for Replay {
    async fn recv(&mut self) -> obe_ingest::Result<FeedItem> {
        match self.items.next() {
            Some(item) => Ok(item),
            None => std::future::pending().await,
        }
    }
}

#[derive(Debug)]
struct Seeded;

impl SnapshotSource for Seeded {
    async fn depth_snapshot(
        &self,
        _symbol: &str,
        _limit: u16,
    ) -> obe_ingest::Result<obe_ingest::protocol::DepthSnapshot> {
        Ok(workload::snapshot(1_000_000))
    }
}

/// Accepts everything and keeps nothing, so the scenario measures the loop and
/// not an allocator filling up.
#[derive(Debug)]
struct Discard;

impl Sink for Discard {
    async fn write_trades(&self, trades: &[NewTrade]) -> obe_ingest::Result<u64> {
        Ok(black_box(trades).len() as u64)
    }

    async fn publish_book(&self, _symbol: &str, book: &BookSnapshot) -> obe_ingest::Result<Write> {
        black_box(book);
        Ok(Write::Stored)
    }

    async fn write_snapshot(&self, book: &BookSnapshot) -> obe_ingest::Result<bool> {
        black_box(book);
        Ok(true)
    }

    async fn broadcast(&self, event: &StreamEvent) -> obe_ingest::Result<u32> {
        black_box(event);
        Ok(0)
    }
}
