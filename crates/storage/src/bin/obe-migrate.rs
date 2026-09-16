//! Applies pending database migrations, then exits.
//!
//! Deployments run this as a separate step before rolling the services, so a
//! schema change is one deliberate action rather than a side effect of
//! whichever replica happens to boot first.

use anyhow::Context;
use obe_core::{telemetry, Settings};
use obe_storage::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::load().context("loading configuration")?;
    telemetry::init(&settings.telemetry).context("initialising telemetry")?;

    tracing::info!(database = %settings.database.url, "applying migrations");

    let store = Store::connect_lazy(&settings.database).context("building the database pool")?;
    store.migrate().await.context("running migrations")?;
    store.close().await;

    Ok(())
}
