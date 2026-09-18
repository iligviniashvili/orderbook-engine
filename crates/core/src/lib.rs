//! Shared building blocks for the orderbook-engine services: layered
//! configuration, tracing setup and graceful-shutdown signalling.

pub mod config;
pub mod secret;
pub mod shutdown;
pub mod telemetry;

pub use config::{
    DatabaseConfig, Error as ConfigError, IngestConfig, InstrumentConfig, LogFormat, RedisConfig,
    ServerConfig, Settings, TelemetryConfig,
};
pub use secret::Dsn;
pub use shutdown::shutdown_signal;
