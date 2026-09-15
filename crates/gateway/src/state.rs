//! Shared handler state.

use std::sync::Arc;
use std::time::Instant;

use obe_core::Settings;

#[derive(Debug, Clone)]
pub struct AppState(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    service_name: String,
    started_at: Instant,
}

impl AppState {
    pub fn new(settings: &Settings) -> Self {
        Self(Arc::new(Inner {
            service_name: settings.telemetry.service_name.clone(),
            started_at: Instant::now(),
        }))
    }

    pub fn service_name(&self) -> &str {
        &self.0.service_name
    }

    pub fn uptime_secs(&self) -> u64 {
        self.0.started_at.elapsed().as_secs()
    }
}
