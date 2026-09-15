//! Structured logging / tracing setup.

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

use crate::config::{LogFormat, TelemetryConfig};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid tracing filter `{filter}`")]
    Filter {
        filter: String,
        #[source]
        source: tracing_subscriber::filter::ParseError,
    },
    #[error("a global tracing subscriber is already installed")]
    AlreadyInitialised,
}

/// Installs the process-wide tracing subscriber.
///
/// `RUST_LOG` wins over `telemetry.level` when it is set, so operators can
/// turn up logging without editing configuration.
pub fn init(cfg: &TelemetryConfig) -> Result<(), Error> {
    let filter = resolve_filter(cfg, std::env::var("RUST_LOG").ok())?;
    let registry = Registry::default().with(filter);

    let result = match cfg.format {
        LogFormat::Json => registry
            .with(
                fmt::layer()
                    .json()
                    .flatten_event(true)
                    .with_current_span(true)
                    .with_span_list(false),
            )
            .try_init(),
        LogFormat::Compact => registry.with(fmt::layer().compact()).try_init(),
        LogFormat::Pretty => registry.with(fmt::layer().pretty()).try_init(),
    };

    result.map_err(|_| Error::AlreadyInitialised)?;

    tracing::info!(service = %cfg.service_name, format = ?cfg.format, "telemetry initialised");
    Ok(())
}

fn resolve_filter(cfg: &TelemetryConfig, env_override: Option<String>) -> Result<EnvFilter, Error> {
    let directives = match env_override {
        Some(value) if !value.trim().is_empty() => value,
        _ => cfg.level.clone(),
    };

    EnvFilter::try_new(&directives).map_err(|source| Error::Filter {
        filter: directives,
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(level: &str) -> TelemetryConfig {
        TelemetryConfig {
            service_name: "test".into(),
            level: level.into(),
            format: LogFormat::Json,
        }
    }

    #[test]
    fn config_level_is_used_without_an_env_override() {
        let filter = resolve_filter(&cfg("obe_core=debug,info"), None).unwrap();
        assert!(filter.to_string().contains("obe_core=debug"));
    }

    #[test]
    fn env_override_wins() {
        let filter = resolve_filter(&cfg("info"), Some("obe_gateway=trace".into())).unwrap();
        assert!(filter.to_string().contains("obe_gateway=trace"));
    }

    #[test]
    fn blank_env_override_is_ignored() {
        let filter = resolve_filter(&cfg("warn"), Some("   ".into())).unwrap();
        assert_eq!(filter.to_string(), "warn");
    }

    #[test]
    fn invalid_level_is_an_error() {
        assert!(matches!(
            resolve_filter(&cfg("obe_core=notalevel"), None),
            Err(Error::Filter { .. })
        ));
    }
}
