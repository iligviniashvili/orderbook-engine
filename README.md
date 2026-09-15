# orderbook-engine

A market-data ingestion and order-book reconstruction service in Rust. It
connects to a public exchange WebSocket feed, rebuilds a level-2 order book per
symbol in memory, persists trades and periodic snapshots to PostgreSQL, fans
state out through Redis, and serves both historical REST queries and a live
WebSocket stream.

Status: **milestone 1 of 5** — workspace, configuration, telemetry, health
endpoints and the local service dependencies. Ingestion, storage and the public
API land in the milestones below. No performance numbers are published yet;
they will be added once there is a benchmark to measure.

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
| `crates/gateway` (`obe-gateway`) | HTTP service; health endpoints today, REST + WebSocket API later |

The ingestor and storage crates arrive with milestones 2 and 3.

## Running it

```bash
docker compose up -d          # PostgreSQL + Redis (not used by the gateway yet)
cargo run -p obe-gateway      # http://0.0.0.0:8080
curl localhost:8080/health
```

Endpoints:

| method | path | purpose |
| --- | --- | --- |
| GET | `/health` | service name, version, uptime |
| GET | `/health/live` | liveness: the process is running |
| GET | `/health/ready` | readiness: dependency checks (none registered yet) |

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

CI runs the same three commands on every push and pull request, plus a
`docker compose config` validation.

## Roadmap

1. **Core infrastructure** — workspace, config, tracing, health endpoints, local
   Postgres/Redis via compose. *(done)*
2. **Storage** — Postgres schema for symbols, trades and order-book snapshots,
   sqlx migrations, Redis caching layer.
3. **Ingestion and API** — exchange WebSocket client, order-book
   reconstruction, REST history and live WebSocket fan-out.
4. **Integration testing** — end-to-end tests from feed to API, plus a
   throughput benchmark.
5. **Packaging** — multi-stage Docker images, full compose stack, release CI.

## License

MIT
