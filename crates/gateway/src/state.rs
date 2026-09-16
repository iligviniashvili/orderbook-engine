//! Shared handler state.

use std::sync::Arc;
use std::time::{Duration, Instant};

use obe_core::Settings;
use obe_storage::{BookCache, Store};

#[derive(Debug, Clone)]
pub struct AppState(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    service_name: String,
    started_at: Instant,
    readiness_timeout: Duration,
    store: Store,
    cache: BookCache,
}

impl AppState {
    /// Builds the pools without connecting, so the gateway can start ahead of
    /// its dependencies and report them through `/health/ready`.
    pub fn new(settings: &Settings) -> Result<Self, obe_storage::Error> {
        Ok(Self(Arc::new(Inner {
            service_name: settings.telemetry.service_name.clone(),
            started_at: Instant::now(),
            readiness_timeout: settings.server.readiness_timeout(),
            store: Store::connect_lazy(&settings.database)?,
            cache: BookCache::connect_lazy(&settings.redis)?,
        })))
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
}
