# orderbook-engine

A market-data ingestion and order-book reconstruction service in Rust. It
connects to a public exchange WebSocket feed, rebuilds a level-2 order book per
symbol in memory, persists trades and periodic snapshots to PostgreSQL, fans
state out through Redis, and serves both historical REST queries and a live
WebSocket stream.

Status: **milestone 2 of 5** — workspace, configuration, telemetry, health
endpoints, and the persistence layer: PostgreSQL schema with migrations, the
query layer over it, and the Redis cache for the hot book. Ingestion and the
public API land in the milestones below. No performance numbers are published
yet; they will be added once there is a benchmark to measure.

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
| `crates/gateway` (`obe-gateway`) | HTTP service; health endpoints today, REST + WebSocket API later |

The ingestor crate arrives with milestone 3.

## Running it

```bash
docker compose up -d                      # PostgreSQL + Redis
cargo run -p obe-storage --bin obe-migrate  # apply the schema
cargo run -p obe-gateway                  # http://0.0.0.0:8080
curl localhost:8080/health/ready
```

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

## Configuration

`config/default.toml` holds the baseline and is compiled into the binary, so the
service starts without a config directory. Sources are applied in order, each
overriding the previous one:

1. the embedded `config/default.toml`
2. `$OBE_CONFIG_DIR/default.toml` (defaults to `./config`)
3. `$OBE_CONFIG_DIR/$RUN_ENV.toml` — e.g. `production.toml`
4. `$OBE_CONFIG_DIR/local.toml` — git-ignored developer overrides
5. environment variables: `OBE__SERVER__PORT=9000`, `OBE__TELEMETRY__FORMAT=json`

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
by default because `ring` needs a C toolchain that the tests do without.

CI runs fmt, clippy, build and test on every push and pull request, plus the
integration suite against PostgreSQL and Redis service containers and a
`docker compose config` validation.

## Roadmap

1. **Core infrastructure** — workspace, config, tracing, health endpoints, local
   Postgres/Redis via compose. *(done)*
2. **Storage** — Postgres schema for symbols, trades and order-book snapshots,
   sqlx migrations, Redis caching layer. *(done)*
3. **Ingestion and API** — exchange WebSocket client, order-book
   reconstruction, REST history and live WebSocket fan-out.
4. **Integration testing** — end-to-end tests from feed to API, plus a
   throughput benchmark.
5. **Packaging** — multi-stage Docker images, full compose stack, release CI.

## License

MIT
