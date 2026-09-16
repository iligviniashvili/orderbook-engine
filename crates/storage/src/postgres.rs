//! PostgreSQL connection handling and schema migration.

use std::time::Duration;

use obe_core::DatabaseConfig;
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::{Pool, Transaction};

use crate::error::Result;

/// The `migrations/` directory, embedded in the binary at compile time, so a
/// deployed image carries its own schema and needs no files on disk.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// A handle to the market-data database. Cheap to clone; the pool is shared.
#[derive(Debug, Clone)]
pub struct Store {
    pool: Pool<Postgres>,
}

impl Store {
    /// Builds the pool without opening a connection.
    ///
    /// Lazy on purpose: the gateway must start and answer `/health/live` even
    /// when PostgreSQL is down, and report that through `/health/ready`
    /// instead of crash-looping before it can serve anything.
    pub fn connect_lazy(cfg: &DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .min_connections(cfg.min_connections)
            .acquire_timeout(cfg.acquire_timeout())
            .idle_timeout(Some(cfg.idle_timeout()))
            .connect_lazy(cfg.url.expose())?;

        Ok(Self { pool })
    }

    pub fn from_pool(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>> {
        Ok(self.pool.begin().await?)
    }

    /// Applies any pending migrations. Safe to call concurrently: sqlx takes a
    /// PostgreSQL advisory lock for the duration, so a rolling deploy of N
    /// replicas still applies each migration exactly once.
    pub async fn migrate(&self) -> Result<()> {
        MIGRATOR.run(&self.pool).await?;
        tracing::info!("database migrations applied");
        Ok(())
    }

    /// Round-trips a trivial query and reports how long it took.
    pub async fn ping(&self) -> Result<Duration> {
        let started = std::time::Instant::now();
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await?;
        Ok(started.elapsed())
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obe_core::Settings;

    fn settings() -> Settings {
        Settings::load_from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config"))
            .expect("repository config should load")
    }

    #[test]
    fn migrations_are_embedded_and_ordered() {
        let versions: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();

        assert!(!versions.is_empty(), "no migrations were embedded");
        assert!(
            versions.windows(2).all(|w| w[0] < w[1]),
            "migration versions must increase: {versions:?}"
        );
    }

    #[tokio::test]
    async fn connect_lazy_does_not_touch_the_network() {
        // Nothing is listening on this port; building the pool must still
        // succeed, otherwise the gateway could not start ahead of Postgres.
        let mut cfg = settings().database;
        cfg.url = "postgres://nobody:nothing@127.0.0.1:1/absent".into();

        assert!(Store::connect_lazy(&cfg).is_ok());
    }

    #[test]
    fn a_malformed_url_is_rejected_immediately() {
        let mut cfg = settings().database;
        cfg.url = "not-a-connection-string".into();

        assert!(Store::connect_lazy(&cfg).is_err());
    }
}
