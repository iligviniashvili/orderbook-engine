use std::future::IntoFuture;

use anyhow::Context;
use obe_core::{shutdown_signal, telemetry, Settings};
use obe_gateway::{router, AppState};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::load().context("loading configuration")?;
    telemetry::init(&settings.telemetry).context("initialising telemetry")?;

    let addr = settings.server.socket_addr()?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;

    let state = AppState::new(&settings).context("building the storage handles")?;
    if settings.database.migrate_on_start {
        state
            .store()
            .migrate()
            .await
            .context("applying migrations at startup")?;
    }

    // The stream reader and the server stop on the same signal. The reader
    // owns the process's single Redis subscription; dropping it closes every
    // client's `broadcast` receiver, which is how open sockets learn to go.
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut reader_shutdown = shutdown_tx.subscribe();
    let reader = state
        .follow_stream(&settings, async move {
            let _ = reader_shutdown.recv().await;
        })
        .context("starting the market-data stream reader")?;

    let mut server_shutdown = shutdown_tx.subscribe();
    let app = router(state);
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = server_shutdown.recv().await;
    });

    tracing::info!(%addr, version = obe_gateway::health::VERSION, "gateway listening");
    let serving = tokio::spawn(server.into_future());

    shutdown_signal().await;
    let _ = shutdown_tx.send(());

    // Drain in-flight requests, but do not hang forever on a stuck connection.
    // Streaming clients are long-lived by design, so this grace period is the
    // thing that keeps them from holding a deploy open indefinitely.
    match tokio::time::timeout(settings.server.shutdown_grace(), serving).await {
        Ok(joined) => joined
            .context("server task panicked")?
            .context("server error")?,
        Err(_) => tracing::warn!(
            grace_secs = settings.server.shutdown_grace_secs,
            "grace period elapsed with connections still open; exiting anyway"
        ),
    }
    reader.abort();

    tracing::info!("gateway stopped");
    Ok(())
}
