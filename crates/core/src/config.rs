//! Layered configuration.
//!
//! Sources are applied in increasing order of precedence:
//! 1. the `config/default.toml` baked into the binary at compile time,
//! 2. `<config_dir>/default.toml` (optional, lets an operator ship a file),
//! 3. `<config_dir>/<RUN_ENV>.toml` (optional, e.g. `production.toml`),
//! 4. `<config_dir>/local.toml` (optional, git-ignored developer overrides),
//! 5. environment variables such as `OBE__SERVER__PORT`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;

use crate::secret::Dsn;

/// Baseline values, compiled in so the binary runs without a config directory.
const EMBEDDED_DEFAULTS: &str = include_str!("../../../config/default.toml");

const ENV_PREFIX: &str = "OBE";
const ENV_SEPARATOR: &str = "__";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not read configuration")]
    Source(#[from] config::ConfigError),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub server: ServerConfig,
    pub telemetry: TelemetryConfig,
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub shutdown_grace_secs: u64,
    /// Budget for the whole readiness probe, so a wedged dependency cannot
    /// make `/health/ready` hang instead of answering "down".
    pub readiness_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    pub url: Dsn,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    /// Apply pending migrations during service startup. Convenient for local
    /// work; deployments should run `obe-migrate` as a separate step instead.
    pub migrate_on_start: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub url: Dsn,
    pub pool_size: usize,
    /// Namespace for every key this service writes.
    pub key_prefix: String,
    /// How long a cached book stays valid without a refresh from the ingestor.
    pub book_ttl_secs: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    pub service_name: String,
    /// Default tracing filter; `RUST_LOG` overrides it when set.
    pub level: String,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Compact,
    Json,
}

impl Settings {
    /// Loads settings from the directory named by `OBE_CONFIG_DIR`, falling
    /// back to `./config`.
    pub fn load() -> Result<Self, Error> {
        let dir = std::env::var("OBE_CONFIG_DIR").unwrap_or_else(|_| "config".to_owned());
        Self::load_from(dir)
    }

    /// Loads settings using `dir` as the configuration directory. Missing
    /// files are not an error; the embedded defaults always apply.
    pub fn load_from(dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = dir.as_ref();
        let run_env = std::env::var("RUN_ENV").unwrap_or_else(|_| "development".to_owned());

        let settings: Self = Config::builder()
            .add_source(File::from_str(EMBEDDED_DEFAULTS, FileFormat::Toml))
            .add_source(optional_file(dir, "default"))
            .add_source(optional_file(dir, &run_env))
            .add_source(optional_file(dir, "local"))
            .add_source(
                Environment::with_prefix(ENV_PREFIX)
                    .separator(ENV_SEPARATOR)
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()?;

        settings.validate()?;
        Ok(settings)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.server.host.trim().is_empty() {
            return Err(Error::Invalid("server.host must not be empty".into()));
        }
        if self.server.port == 0 {
            return Err(Error::Invalid("server.port must not be 0".into()));
        }
        if self.telemetry.service_name.trim().is_empty() {
            return Err(Error::Invalid(
                "telemetry.service_name must not be empty".into(),
            ));
        }
        if self.server.readiness_timeout_ms == 0 {
            return Err(Error::Invalid(
                "server.readiness_timeout_ms must not be 0".into(),
            ));
        }
        if self.database.url.is_empty() {
            return Err(Error::Invalid("database.url must not be empty".into()));
        }
        if self.database.max_connections == 0 {
            return Err(Error::Invalid(
                "database.max_connections must be at least 1".into(),
            ));
        }
        if self.database.min_connections > self.database.max_connections {
            return Err(Error::Invalid(
                "database.min_connections must not exceed database.max_connections".into(),
            ));
        }
        if self.redis.url.is_empty() {
            return Err(Error::Invalid("redis.url must not be empty".into()));
        }
        if self.redis.pool_size == 0 {
            return Err(Error::Invalid("redis.pool_size must be at least 1".into()));
        }
        if self.redis.key_prefix.trim().is_empty() {
            return Err(Error::Invalid("redis.key_prefix must not be empty".into()));
        }
        if self.redis.book_ttl_secs == 0 {
            return Err(Error::Invalid("redis.book_ttl_secs must not be 0".into()));
        }
        Ok(())
    }
}

impl DatabaseConfig {
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout_secs)
    }

    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_secs)
    }
}

impl RedisConfig {
    pub fn book_ttl(&self) -> Duration {
        Duration::from_secs(self.book_ttl_secs)
    }
}

impl ServerConfig {
    /// Resolves `host:port` to a socket address.
    pub fn socket_addr(&self) -> Result<SocketAddr, Error> {
        let raw = format!("{}:{}", self.host, self.port);
        raw.parse()
            .map_err(|_| Error::Invalid(format!("server address `{raw}` is not a socket address")))
    }

    pub fn shutdown_grace(&self) -> Duration {
        Duration::from_secs(self.shutdown_grace_secs)
    }

    pub fn readiness_timeout(&self) -> Duration {
        Duration::from_millis(self.readiness_timeout_ms)
    }
}

fn optional_file(dir: &Path, stem: &str) -> File<config::FileSourceFile, FileFormat> {
    let path: PathBuf = dir.join(stem);
    File::with_name(&path.to_string_lossy()).required(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env vars are process-wide, so the tests that touch them are serialised
    /// behind this guard rather than run in parallel.
    static ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn embedded_only() -> Settings {
        // A directory that holds no config files, so only the embedded
        // defaults and the environment apply.
        Settings::load_from(std::env::temp_dir().join("obe-nonexistent-config")).unwrap()
    }

    #[test]
    fn embedded_defaults_are_valid() {
        let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let settings = embedded_only();

        assert_eq!(settings.server.port, 8080);
        assert_eq!(settings.server.shutdown_grace_secs, 10);
        assert_eq!(settings.telemetry.format, LogFormat::Pretty);
        assert_eq!(settings.telemetry.service_name, "orderbook-engine");
        assert!(settings.database.url.expose().starts_with("postgres://"));
        assert!(!settings.database.migrate_on_start);
        assert_eq!(settings.redis.key_prefix, "obe");
    }

    #[test]
    fn debug_output_never_contains_a_password() {
        let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var(
            "OBE__DATABASE__URL",
            "postgres://orderbook:hunter2@localhost:5432/orderbook",
        );

        let settings = embedded_only();

        std::env::remove_var("OBE__DATABASE__URL");
        let rendered = format!("{settings:?}");

        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("***"), "{rendered}");
    }

    #[test]
    fn repository_config_dir_matches_embedded_defaults() {
        let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let repo_config = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");

        assert_eq!(Settings::load_from(repo_config).unwrap(), embedded_only());
    }

    #[test]
    fn environment_overrides_files() {
        let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY-ish: single-threaded section guarded by ENV_GUARD.
        std::env::set_var("OBE__SERVER__PORT", "9999");
        std::env::set_var("OBE__TELEMETRY__FORMAT", "json");

        let settings = embedded_only();

        std::env::remove_var("OBE__SERVER__PORT");
        std::env::remove_var("OBE__TELEMETRY__FORMAT");

        assert_eq!(settings.server.port, 9999);
        assert_eq!(settings.telemetry.format, LogFormat::Json);
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("OBE__SERVER__PORTT", "9999");

        let result = Settings::load_from(std::env::temp_dir().join("obe-nonexistent-config"));

        std::env::remove_var("OBE__SERVER__PORTT");
        assert!(result.is_err(), "typo in an override should fail loudly");
    }

    #[test]
    fn socket_addr_is_resolved() {
        let cfg = ServerConfig {
            host: "127.0.0.1".into(),
            port: 8081,
            shutdown_grace_secs: 5,
            readiness_timeout_ms: 1_500,
        };

        assert_eq!(cfg.socket_addr().unwrap().to_string(), "127.0.0.1:8081");
        assert_eq!(cfg.shutdown_grace(), Duration::from_secs(5));
        assert_eq!(cfg.readiness_timeout(), Duration::from_millis(1_500));
    }

    fn valid_settings() -> Settings {
        Settings {
            server: ServerConfig {
                host: "127.0.0.1".into(),
                port: 8080,
                shutdown_grace_secs: 1,
                readiness_timeout_ms: 1_000,
            },
            telemetry: TelemetryConfig {
                service_name: "test".into(),
                level: "info".into(),
                format: LogFormat::Json,
            },
            database: DatabaseConfig {
                url: "postgres://u:p@localhost/db".into(),
                max_connections: 4,
                min_connections: 1,
                acquire_timeout_secs: 5,
                idle_timeout_secs: 300,
                migrate_on_start: false,
            },
            redis: RedisConfig {
                url: "redis://localhost:6379".into(),
                pool_size: 4,
                key_prefix: "obe".into(),
                book_ttl_secs: 60,
            },
        }
    }

    #[test]
    fn reference_settings_are_valid() {
        assert!(valid_settings().validate().is_ok());
    }

    #[test]
    fn empty_host_is_rejected() {
        let mut settings = valid_settings();
        settings.server.host = "  ".into();

        assert!(settings.validate().is_err());
    }

    #[test]
    fn pool_bounds_must_be_consistent() {
        let mut settings = valid_settings();
        settings.database.min_connections = settings.database.max_connections + 1;

        assert!(settings.validate().is_err());
    }

    #[test]
    fn empty_redis_prefix_is_rejected() {
        let mut settings = valid_settings();
        settings.redis.key_prefix = " ".into();

        assert!(settings.validate().is_err());
    }
}
