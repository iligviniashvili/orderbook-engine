//! Storage errors.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error")]
    Database(#[from] sqlx::Error),
    #[error("migration failed")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("cache error")]
    Cache(#[from] redis::RedisError),
    #[error("cache pool exhausted")]
    CachePool(#[from] deadpool_redis::PoolError),
    #[error("could not build the cache pool")]
    CacheConfig(#[from] deadpool_redis::CreatePoolError),
    #[error("could not decode a cached value")]
    Decode(#[from] serde_json::Error),
    #[error("invalid argument: {0}")]
    Invalid(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}
