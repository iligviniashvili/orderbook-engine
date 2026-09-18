# orderbook-engine

A market-data ingestion and order-book reconstruction service in Rust. It
connects to a public exchange WebSocket feed, rebuilds a level-2 order book per
symbol in memory, persists trades and periodic snapshots to PostgreSQL, fans
state out through Redis, and serves both historical REST queries and a live
WebSocket stream.

Status: **milestone 3 of 5, in progress** — workspace, configuration,
telemetry, health endpoints, the persistence layer (PostgreSQL schema with
migrations, the query layer over it, the Redis cache for the hot book) and now
ingestion: an exchange WebSocket client, level-2 book reconstruction with
sequence-gap detection, and batched writes into storage. The public REST and
WebSocket API is the rest of this milestone. No performance numbers are
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
| `crates/gateway` (`obe-gateway`) | HTTP service; health endpoints today, REST + WebSocket API later |

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

The storage integration tests need live services. They skip with a note when
the two variables below are unset, so the command above works on a laptop with
nothing running:

```bash
docker compose up -d
export OBE_TEST_DATABASE_URL=postgres://orderbook:orderbook@localhost:5432/orderbook
export OBE_TEST_REDIS_URL=redis://localhost:6379
cargo test -p obe-storage --test integration -- --nocapture
```

TLS for PostgreSQL is behind the optional `obe-storage/tls` feature; it is off
by default because `ring` needs a C toolchain that the tests do without. The
ingestor's `wss://` and `https://` clients use `native-tls` for the same
reason: it hands TLS to the platform — schannel on Windows, Secure Transport on
macOS, the system OpenSSL on Linux — instead of building a crypto provider.

CI runs fmt, clippy, build and test on every push and pull request, plus the
integration suite against PostgreSQL and Redis service containers and a
`docker compose config` validation.

## Roadmap

1. **Core infrastructure** — workspace, config, tracing, health endpoints, local
   Postgres/Redis via compose. *(done)*
2. **Storage** — Postgres schema for symbols, trades and order-book snapshots,
   sqlx migrations, Redis caching layer. *(done)*
3. **Ingestion and API** — exchange WebSocket client, order-book
   reconstruction, REST history and live WebSocket fan-out. *(ingestion done;
   the API is next)*
4. **Integration testing** — end-to-end tests from feed to API, plus a
   throughput benchmark.
5. **Packaging** — multi-stage Docker images, full compose stack, release CI.

## License

MIT
