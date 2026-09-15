//! Shared building blocks for the orderbook-engine services: layered
//! configuration, tracing setup and graceful-shutdown signalling.

pub mod config;
pub mod shutdown;
pub mod telemetry;

pub use config::{Error as ConfigError, LogFormat, ServerConfig, Settings, TelemetryConfig};
pub use shutdown::shutdown_signal;
