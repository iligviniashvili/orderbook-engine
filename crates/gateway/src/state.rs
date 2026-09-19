//! Shared handler state.

use std::sync::Arc;
use std::time::{Duration, Instant};

use obe_core::{Backoff, Settings};
use obe_storage::{BookCache, Store, StreamSubscriber};

use crate::hub::{self, Hub};

#[derive(Debug, Clone)]
pub struct AppState(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    service_name: String,
    started_at: Instant,
    readiness_timeout: Duration,
    store: Store,
    cache: BookCache,
    hub: Hub,
}

impl AppState {
    /// Builds the pools without connecting, so the gateway can start ahead of
    /// its dependencies and report them through `/health/ready`.
    ///
    /// The hub starts with nobody feeding it: `/v1/stream` accepts clients and
    /// serves them their opening book from the cache whether or not Redis
    /// pub/sub is reachable. [`Self::follow_stream`] attaches the reader.
    pub fn new(settings: &Settings) -> Result<Self, obe_storage::Error> {
        Ok(Self(Arc::new(Inner {
            service_name: settings.telemetry.service_name.clone(),
            started_at: Instant::now(),
            readiness_timeout: settings.server.readiness_timeout(),
            store: Store::connect_lazy(&settings.database)?,
            cache: BookCache::connect_lazy(&settings.redis)?,
            hub: Hub::default(),
        })))
    }

    /// Spawns the Redis pub/sub reader that feeds this state's hub, and hands
    /// back its join handle so shutdown can wait for it.
    ///
    /// One subscription per process: every connected client reads from the
    /// same `broadcast` channel, so what Redis sees does not grow with the
    /// number of clients.
    pub fn follow_stream(
        &self,
        settings: &Settings,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<()>, obe_storage::Error> {
        let subscriber = StreamSubscriber::connect_lazy(&settings.redis)?;
        let backoff = Backoff::new(
            settings.ingest.reconnect_base(),
            settings.ingest.reconnect_max(),
        );

        Ok(tokio::spawn(hub::run(
            subscriber,
            self.0.hub.clone(),
            backoff,
            settings.redis.book_ttl(),
            shutdown,
        )))
    }

    pub fn service_name(&self) -> &str {
        &self.0.service_name
    }

    pub fn uptime_secs(&self) -> u64 {
        self.0.started_at.elapsed().as_secs()
    }

    pub fn readiness_timeout(&self) -> Duration {
        self.0.readiness_timeout
    }

    pub fn store(&self) -> &Store {
        &self.0.store
    }

    pub fn cache(&self) -> &BookCache {
        &self.0.cache
    }

    pub fn hub(&self) -> &Hub {
        &self.0.hub
    }
}
