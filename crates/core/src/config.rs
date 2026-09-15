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
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub shutdown_grace_secs: u64,
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
        Ok(())
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
        };

        assert_eq!(cfg.socket_addr().unwrap().to_string(), "127.0.0.1:8081");
        assert_eq!(cfg.shutdown_grace(), Duration::from_secs(5));
    }

    #[test]
    fn empty_host_is_rejected() {
        let settings = Settings {
            server: ServerConfig {
                host: "  ".into(),
                port: 8080,
                shutdown_grace_secs: 1,
            },
            telemetry: TelemetryConfig {
                service_name: "test".into(),
                level: "info".into(),
                format: LogFormat::Json,
            },
        };

        assert!(settings.validate().is_err());
    }
}
