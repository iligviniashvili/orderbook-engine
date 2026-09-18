use std::time::Duration;

use anyhow::Context;
use obe_core::{shutdown_signal, telemetry, InstrumentConfig, Settings};
use obe_ingest::{Backoff, HttpSnapshotSource, Pipeline, StorageSink, WebSocketFeed};
use obe_storage::{BookCache, NewSymbol, Store};

/// Budget for one REST depth snapshot. Generous — it is a kilobyte or two of
/// JSON — but bounded, because a resync that never returns is a symbol that
/// never comes back.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::load().context("loading configuration")?;
    telemetry::init(&settings.telemetry).context("initialising telemetry")?;

    let cfg = settings.ingest.clone();
    if cfg.instruments.is_empty() {
        anyhow::bail!("ingest.instruments is empty; there is nothing to follow");
    }

    let store = Store::connect_lazy(&settings.database).context("building the database pool")?;
    let cache = BookCache::connect_lazy(&settings.redis).context("building the cache pool")?;

    // The registry is the one thing that must be reachable before ingestion
    // starts: without a symbol id there is nothing to key a trade on. The
    // upsert doubles as the startup connectivity check.
    let mut instruments = Vec::with_capacity(cfg.instruments.len());
    for instrument in &cfg.instruments {
        let symbol = store
            .upsert_symbol(&new_symbol(&cfg.exchange, instrument))
            .await
            .with_context(|| format!("registering {}", instrument.symbol))?;
        instruments.push((symbol.symbol.clone(), symbol.id));
    }

    tracing::info!(
        exchange = %cfg.exchange,
        instruments = instruments.len(),
        depth = cfg.depth,
        "starting ingestion"
    );

    let symbols: Vec<String> = instruments.iter().map(|(name, _)| name.clone()).collect();
    let feed = WebSocketFeed::new(
        &cfg.stream_url,
        &symbols,
        Backoff::new(cfg.reconnect_base(), cfg.reconnect_max()),
    );
    let source = HttpSnapshotSource::new(&cfg.snapshot_url, SNAPSHOT_TIMEOUT)
        .context("building the snapshot client")?;
    let sink = StorageSink::new(store, cache, &cfg.exchange);

    let mut pipeline = Pipeline::new(feed, source, sink, cfg, &instruments);
    pipeline.run(shutdown_signal()).await?;

    tracing::info!(stats = ?pipeline.stats(), "ingestion stopped");
    Ok(())
}

fn new_symbol(exchange: &str, instrument: &InstrumentConfig) -> NewSymbol {
    NewSymbol {
        exchange: exchange.to_owned(),
        symbol: instrument.symbol.trim().to_ascii_uppercase(),
        base_asset: instrument.base_asset.trim().to_ascii_uppercase(),
        quote_asset: instrument.quote_asset.trim().to_ascii_uppercase(),
        price_precision: instrument.price_precision,
        quantity_precision: instrument.quantity_precision,
        active: true,
    }
}
