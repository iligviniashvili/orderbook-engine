use std::future::IntoFuture;

use anyhow::Context;
use obe_core::{shutdown_signal, telemetry, Settings};
use obe_gateway::{router, AppState};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::load().context("loading configuration")?;
    telemetry::init(&settings.telemetry).context("initialising telemetry")?;

    let addr = settings.server.socket_addr()?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let app = router(AppState::new(&settings));
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = shutdown_rx.await;
    });

    tracing::info!(%addr, version = obe_gateway::health::VERSION, "gateway listening");
    let serving = tokio::spawn(server.into_future());

    shutdown_signal().await;
    let _ = shutdown_tx.send(());

    // Drain in-flight requests, but do not hang forever on a stuck connection.
    match tokio::time::timeout(settings.server.shutdown_grace(), serving).await {
        Ok(joined) => joined
            .context("server task panicked")?
            .context("server error")?,
        Err(_) => tracing::warn!(
            grace_secs = settings.server.shutdown_grace_secs,
            "grace period elapsed with connections still open; exiting anyway"
        ),
    }

    tracing::info!("gateway stopped");
    Ok(())
}
