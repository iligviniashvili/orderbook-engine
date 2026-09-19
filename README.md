# orderbook-engine

A market-data ingestion and order-book reconstruction service in Rust. It
connects to a public exchange WebSocket feed, rebuilds a level-2 order book per
symbol in memory, persists trades and periodic snapshots to PostgreSQL, fans
state out through Redis, and serves both historical REST queries and a live
WebSocket stream.

Status: **milestone 3 of 5, done** — workspace, configuration, telemetry,
health endpoints, the persistence layer (PostgreSQL schema with migrations,
the query layer over it, the Redis cache for the hot book), ingestion (an
exchange WebSocket client, level-2 book reconstruction with sequence-gap
detection, batched writes into storage), the read API over what it produces,
and the live WebSocket fan-out at `/v1/stream`. No performance numbers are
published yet; they will be added once there is a benchmark to measure.

## Architecture

```
exchange WS feed
       │
       ▼
┌──────────────┐   book deltas   ┌─────────┐   pub/sub    ┌──────────────┐
│  ingestor    │────────────────▶│  Redis  │─────────────▶│   gateway    │
│ (tokio task  │  hot snapshots  └─────────┘   live push  │ REST + WS    │
│  per symbol) │                                          └──────────────┘
└──────┬───────┘                                                 │
       │ trades + snapshots                                      │ historical
       ▼                                                         ▼ queries
  ┌────────────┐                                            ┌────────────┐
  │ PostgreSQL │◀───────────────────────────────────────────│ PostgreSQL │
  └────────────┘                                            └────────────┘
```

Crates:

| crate | what it does |
| --- | --- |
| `crates/core` (`obe-core`) | layered configuration, tracing setup, shutdown signalling |
| `crates/storage` (`obe-storage`) | PostgreSQL schema, migrations and queries; Redis book cache |
| `crates/ingest` (`obe-ingest`) | exchange WebSocket client, book reconstruction, batched writes |
| `crates/gateway` (`obe-gateway`) | HTTP service: health endpoints, the `/v1` read API and the `/v1/stream` WebSocket fan-out |

## Running it

```bash
docker compose up -d                        # PostgreSQL + Redis
cargo run -p obe-storage --bin obe-migrate  # apply the schema
cargo run -p obe-ingest                     # connect to the feed
cargo run -p obe-gateway                    # http://0.0.0.0:8080
curl localhost:8080/health/ready
```

`obe-ingest` talks to Binance's public market-data endpoints by default and
needs no credentials: it only reads. The instruments it follows are
`ingest.instruments` in the configuration.

Endpoints:

| method | path | purpose |
| --- | --- | --- |
| GET | `/health` | service name, version, uptime |
| GET | `/health/live` | liveness: the process is running |
| GET | `/health/ready` | readiness: PostgreSQL and Redis, probed concurrently |
| GET | `/v1/symbols` | the instrument registry (`?active_only=false` to include delisted) |
| GET | `/v1/books/{exchange}/{symbol}` | the current book (`?depth=N`) |
| GET | `/v1/trades/{exchange}/{symbol}` | trade history (`?start=&end=&limit=`) |
| GET | `/v1/vwap/{exchange}/{symbol}` | volume-weighted average price over a window |
| GET | `/v1/stream` | WebSocket: live books and trades |

Liveness deliberately checks nothing: restarting the process because PostgreSQL
is slow turns a degraded service into an outage. Readiness does check, under
`server.readiness_timeout_ms`, and returns `503` with a per-dependency reason
and latency:

```json
{
  "status": "down",
  "checks": [
    { "name": "postgres", "status": "up", "latency_ms": 2 },
    { "name": "redis", "status": "down", "latency_ms": 500,
      "detail": "cache error: Connection refused (os error 111)" }
  ]
}
```

## The read API

Everything under `/v1` is read-only — the service ingests from an exchange and
serves what it reconstructed, and there is nothing a client can write.

```bash
curl 'localhost:8080/v1/books/binance/BTCUSDT?depth=2'
```

```json
{
  "exchange": "binance", "symbol": "BTCUSDT",
  "source": "cache", "sequence": 71283044,
  "captured_at": "2026-09-18T09:14:22.418Z", "age_ms": 137,
  "spread": "0.01",
  "bids": [{ "price": "64210.11", "quantity": "0.532" }],
  "asks": [{ "price": "64210.12", "quantity": "1.204" }]
}
```

Decisions worth knowing about:

- **The book endpoint answers from Redis, and falls back to the newest
  persisted snapshot.** The cache key is built from the exchange and the
  ticker, so the warm path costs one Redis round trip and never touches
  PostgreSQL — not even to resolve a symbol id. The response says which path
  it took, and `age_ms` lets a client tell a live book from one whose ingestor
  has stopped without diffing sequence numbers itself.
- **A cache failure degrades instead of failing.** If Redis is unreachable but
  PostgreSQL can still answer, the request is served from disk and the failure
  is logged, because a 503 would be a worse answer than a slower one.
- **A dependency being down is a `503`, not a `500`,** and its body never
  carries the underlying error text: a connection failure renders the DSN's
  host, port and user, which is not something to hand an anonymous client. The
  detail goes to the log. A storage error that means "you asked for something
  impossible" becomes a `400` instead.
- **Query parameters are parsed by hand, not by a deserializer.** The caller
  gets ``` `start` is not an RFC 3339 timestamp: `yesterday` ``` rather than
  serde's rendering of the same fact, and the parsing is a pure function, so
  every rejection is covered without a router or a database.
- **Paths are normalised the way cache keys are**, so `/v1/books/Binance/btcusdt`
  and `/v1/books/binance/BTCUSDT` are one resource rather than two that
  disagree.
- **Prices stay strings end to end**, through `numeric`, `jsonb`, Redis and the
  HTTP response, so nothing in the chain can turn `0.1` into a float.

## The live stream

`/v1/stream` is a WebSocket. A client subscribes to channels named
`book:<exchange>:<symbol>` or `trades:<exchange>:<symbol>` and gets the
matching updates pushed as they happen.

```json
{ "op": "subscribe", "channels": ["book:binance:BTCUSDT", "trades:binance:BTCUSDT"] }
```

The server acknowledges with the full topic set, then sends the cached book
straight away and follows it with updates:

```json
{ "type": "subscribed", "channels": ["book:binance:BTCUSDT", "trades:binance:BTCUSDT"] }
{ "type": "book", "exchange": "binance", "symbol": "BTCUSDT", "sequence": 71283044,
  "captured_at": "2026-09-19T09:14:22.418Z",
  "bids": [{ "price": "64210.11", "quantity": "0.532" }], "asks": [] }
{ "type": "trades", "exchange": "binance", "symbol": "BTCUSDT",
  "trades": [{ "trade_id": 4211, "price": "64210.12", "quantity": "0.004",
               "side": "buy", "traded_at": "2026-09-19T09:14:22.502Z" }] }
```

`{"op":"unsubscribe","channels":[...]}` and `{"op":"ping"}` are the other two
commands. Both acks name the whole topic set, so a client never has to track
what it thinks it asked for.

Decisions worth knowing about:

- **One Redis subscription per gateway process, not per client.** Every
  connection is a `broadcast` receiver behind a single `PSUBSCRIBE`, so the
  load Redis sees does not grow with the number of clients. What each client
  receives is an `Arc` of the publisher's own bytes: the gateway parses a
  three-field routing header and never re-serialises a book, so fanning one
  update out to a hundred sockets is a hundred pointer clones and a hundred
  writes.
- **Subscribing sends the cached book first, then the stream.** The same
  snapshot-then-deltas bootstrap the ingestor performs against the exchange,
  for the same reason: updates alone cannot tell you what is already there. A
  cold or unreachable cache costs a client its head start, not its
  subscription.
- **The stream is not ordered, so every book event carries its sequence.**
  Publishes leave the ingestor through a pooled connection and can take
  different sockets. The gateway keeps a per-topic high-water mark and drops
  anything that is not newer — the monotonic rule the cache enforces in Lua,
  one layer further out — and clients should do the same. The watermark is
  forgotten after the cached book's TTL, so an exchange that restarts its
  update ids cannot wedge a topic for the life of the process.
- **A client that falls behind is told.** Rather than skipping messages
  silently, a lagging connection gets `{"type":"lagged","missed":N}`; a gap a
  client does not know about is a chart that is quietly wrong. The right
  response is to re-read `/v1/books/...` and carry on.
- **The stream is a live view, not a durability claim.** The tape goes out
  before the batch is written and regardless of how that write ends, so a
  PostgreSQL outage costs the service its record without also costing
  subscribers their feed. Nothing is replayed after a disconnect — pub/sub has
  no history, and the REST endpoints are there for what happened.
- **A book only streams once the cache accepted it.** The fan-out is gated on
  the compare-and-set, so the stream and `/v1/books/...` can never disagree
  about which snapshot is newest.
- **Connections are bounded**: 64 topics each, 16 KiB inbound frames, and a
  peer that has said nothing for 45 seconds is closed. TCP will happily hold a
  socket open to a laptop that shut its lid, and each one pins a task.

## Storage

Three tables — `symbols`, `trades`, `book_snapshots` — created by the sqlx
migrations in `crates/storage/migrations/`, which are embedded in the binary at
compile time. `obe-migrate` applies them; it takes a PostgreSQL advisory lock,
so running it from several replicas at once is safe.

Decisions worth knowing about:

- **Money is never a float.** Prices and quantities are `numeric(24, 12)` in
  PostgreSQL and `rust_decimal::Decimal` in Rust. 24 significant digits fit
  inside `Decimal`'s 96-bit mantissa, so the round trip cannot lose precision.
  Snapshot levels live in `jsonb` with the decimals as *strings*, so the JSON
  layer cannot reintroduce a float either.
- **Ingestion is idempotent.** A unique index on `(symbol_id,
  exchange_trade_id)` makes the exchange's own trade id the idempotency key, so
  the batch a feed replays after a reconnect is an `ON CONFLICT DO NOTHING`
  no-op. Snapshots dedupe the same way on `(symbol_id, sequence)`.
- **Batches are one round trip.** `insert_trades` ships the batch as six
  parallel arrays and expands them server-side with `UNNEST`, so the statement
  text is constant regardless of batch size — nothing to re-plan, and no
  bumping into PostgreSQL's 65535-parameter limit.
- **Two indexes on time, on purpose.** A composite btree on `(symbol_id,
  traded_at DESC)` serves the history query; a BRIN index on `traded_at` costs
  a few pages and is what the retention sweep scans.
- **Cache writes are monotonic.** The Redis book cache sets a snapshot through
  a Lua script that compares sequence numbers server-side, so a reconnecting
  ingestor replaying an older book can never overwrite a newer one. The
  sequence is stored zero-padded and compared as a string: Lua's `tonumber`
  goes through a double and would start tying above 2^53.
- **Connections are lazy.** Both pools are built without connecting, so the
  gateway starts and serves `/health/live` even when its dependencies are down.

Queries use `sqlx::query*` rather than the compile-time-checked macros. The
trade-off is deliberate: the build needs no live database and no checked-in
`.sqlx` cache, and the schema is covered by integration tests against a real
PostgreSQL instead.

## Ingestion

One WebSocket connection carries the diff-depth and trade streams for every
configured instrument. The diff stream alone cannot bootstrap a book — it says
what changed, not what is there — so each symbol starts from a REST depth
snapshot and the stream is stitched onto it.

The stitching is where a book quietly goes wrong, so the rules are enforced in
`OrderBook::apply` and a violation is an error, not a log line:

- An event whose final update id is at or below the snapshot's is already
  contained in it and is dropped. Expected right after a resync.
- The first event applied has to straddle the snapshot:
  `first_update_id <= snapshot + 1 <= final_update_id`. If the whole event is
  ahead of it, the updates bridging the two were never delivered and the
  snapshot is useless.
- Every later event has to start exactly where the last one ended.
- A book that crosses after the ids lined up gets its own error, because that
  is real divergence rather than a missed message.

Anything in that list drops the book and refetches a snapshot. So does a
reconnect, which restarts the stream at an arbitrary update id and would
otherwise leave a stale book looking perfectly contiguous.

Nothing buffers depth events across a resync, and nothing needs to: the socket
is the buffer. Events that arrive while the snapshot is in flight are read
after it and are either dropped as already contained or applied as the one that
straddles it.

The rest of the loop is about not letting a firehose become an outage:

- **Trades go out in batches**, on size or on a timer. The feed read is wrapped
  in a timeout set to what is left of the flush window, so a quiet market still
  flushes its partial batch instead of holding the last trades indefinitely.
- **A failed write keeps its batch.** The unique index on the exchange's trade
  id makes the retry a no-op for whatever already landed.
- **The buffer has an end.** Market data does not pause for a database, so past
  eight batches the oldest trades are shed and counted. Shedding the oldest
  keeps the most recent tape, which is the part anything downstream reads.
- **Cache and history writes run on separate timers**, and the book is only
  serialised once one of them is due — a busy symbol produces a hundred updates
  a second and neither destination needs all of them.
- **Reconnect delays are jittered.** Each is drawn from the lower half of a
  doubling ceiling upwards; plain exponential backoff has every client come
  back in the same instant after a venue-wide drop. A connection has to survive
  thirty seconds before the sequence resets, otherwise a socket that accepts
  and immediately drops turns the backoff into a hot loop.

`Feed`, `SnapshotSource` and `Sink` are traits, and the pipeline is generic
over all three, so the gap, reconnect, back-pressure and recovery paths are
covered by tests that need no exchange, no PostgreSQL and no Redis, with the
clock paused so the flush timer costs nothing to exercise.

The live stream is fed from the same loop. The book goes out on the publish
tick, gated on the cache having accepted it; the tape shares the flush
boundary with the durable write, which costs one publish per symbol per flush
window instead of one per trade. That is the trade: the tape arrives in
`trade_flush_ms` steps, while the book — the latency-sensitive half — moves on
the much shorter `publish_interval_ms`.

## Configuration

`config/default.toml` holds the baseline and is compiled into the binary, so the
service starts without a config directory. Sources are applied in order, each
overriding the previous one:

1. the embedded `config/default.toml`
2. `$OBE_CONFIG_DIR/default.toml` (defaults to `./config`)
3. `$OBE_CONFIG_DIR/$RUN_ENV.toml` — e.g. `production.toml`
4. `$OBE_CONFIG_DIR/local.toml` — git-ignored developer overrides
5. environment variables: `OBE__SERVER__PORT=9000`, `OBE__TELEMETRY__FORMAT=json`

The `[[ingest.instruments]]` entries spell out base and quote assets rather
than splitting the ticker: `BTCUSDT` only parses if you already know the quote
assets, and a wrong guess corrupts the symbol registry.

Unknown keys are rejected, so a typo in an override fails at startup instead of
being silently ignored. `RUST_LOG` overrides `telemetry.level` when set.

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The storage and API integration tests need live services. They skip with a note
when the two variables below are unset, so the command above works on a laptop
with nothing running:

```bash
docker compose up -d
export OBE_TEST_DATABASE_URL=postgres://orderbook:orderbook@localhost:5432/orderbook
export OBE_TEST_REDIS_URL=redis://localhost:6379
cargo test -p obe-storage --test integration -- --nocapture
cargo test -p obe-gateway --test api -- --nocapture
```

TLS for PostgreSQL is behind the optional `obe-storage/tls` feature; it is off
by default because `ring` needs a C toolchain that the tests do without. The
ingestor's `wss://` and `https://` clients use `native-tls` for the same
reason: it hands TLS to the platform — schannel on Windows, Secure Transport on
macOS, the system OpenSSL on Linux — instead of building a crypto provider.

CI runs fmt, clippy, build and test on every push and pull request, plus the
integration and API suites against PostgreSQL and Redis service containers and
a `docker compose config` validation.

## Roadmap

1. **Core infrastructure** — workspace, config, tracing, health endpoints, local
   Postgres/Redis via compose. *(done)*
2. **Storage** — Postgres schema for symbols, trades and order-book snapshots,
   sqlx migrations, Redis caching layer. *(done)*
3. **Ingestion and API** — exchange WebSocket client, order-book
   reconstruction, REST history and live WebSocket fan-out. *(done)*
4. **Integration testing** — end-to-end tests from feed to API, plus a
   throughput benchmark.
5. **Packaging** — multi-stage Docker images, full compose stack, release CI.

## License

MIT
