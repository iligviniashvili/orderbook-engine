//! Level-2 snapshot persistence.

use sqlx::types::Json;
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::model::{BookSnapshot, Level};
use crate::postgres::Store;

impl Store {
    /// Stores a snapshot, ignoring it if that `(symbol_id, sequence)` is
    /// already on disk. Returns `true` when the row was new.
    ///
    /// Malformed books are refused here rather than persisted: a crossed or
    /// misordered snapshot means the reconstruction upstream has drifted, and
    /// writing it would hide the bug behind bad history.
    pub async fn insert_snapshot(&self, snapshot: &BookSnapshot) -> Result<bool> {
        if !snapshot.is_well_formed() {
            return Err(Error::invalid(format!(
                "snapshot {} for symbol {} is crossed or misordered",
                snapshot.sequence, snapshot.symbol_id
            )));
        }

        let inserted = sqlx::query(
            "INSERT INTO book_snapshots (symbol_id, sequence, captured_at, bids, asks)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (symbol_id, sequence) DO NOTHING",
        )
        .bind(snapshot.symbol_id)
        .bind(snapshot.sequence)
        .bind(snapshot.captured_at)
        .bind(Json(&snapshot.bids))
        .bind(Json(&snapshot.asks))
        .execute(self.pool())
        .await?
        .rows_affected();

        Ok(inserted == 1)
    }

    pub async fn latest_snapshot(&self, symbol_id: i32) -> Result<Option<BookSnapshot>> {
        let row = sqlx::query_as::<_, SnapshotRow>(
            "SELECT symbol_id, sequence, captured_at, bids, asks
             FROM book_snapshots
             WHERE symbol_id = $1
             ORDER BY sequence DESC
             LIMIT 1",
        )
        .bind(symbol_id)
        .fetch_optional(self.pool())
        .await?;

        Ok(row.map(Into::into))
    }

    /// The last snapshot captured at or before `at` — the starting point for
    /// replaying deltas to reconstruct the book at a past moment.
    pub async fn snapshot_as_of(
        &self,
        symbol_id: i32,
        at: OffsetDateTime,
    ) -> Result<Option<BookSnapshot>> {
        let row = sqlx::query_as::<_, SnapshotRow>(
            "SELECT symbol_id, sequence, captured_at, bids, asks
             FROM book_snapshots
             WHERE symbol_id = $1 AND captured_at <= $2
             ORDER BY captured_at DESC, sequence DESC
             LIMIT 1",
        )
        .bind(symbol_id)
        .bind(at)
        .fetch_optional(self.pool())
        .await?;

        Ok(row.map(Into::into))
    }

    pub async fn prune_snapshots_before(&self, cutoff: OffsetDateTime) -> Result<u64> {
        let deleted = sqlx::query("DELETE FROM book_snapshots WHERE captured_at < $1")
            .bind(cutoff)
            .execute(self.pool())
            .await?
            .rows_affected();

        Ok(deleted)
    }
}

/// `Vec<Level>` arrives as `jsonb`, so it needs the `Json` wrapper on the way
/// out of sqlx; `BookSnapshot` itself stays free of storage concerns.
#[derive(Debug, sqlx::FromRow)]
struct SnapshotRow {
    symbol_id: i32,
    sequence: i64,
    captured_at: OffsetDateTime,
    bids: Json<Vec<Level>>,
    asks: Json<Vec<Level>>,
}

impl From<SnapshotRow> for BookSnapshot {
    fn from(row: SnapshotRow) -> Self {
        Self {
            symbol_id: row.symbol_id,
            sequence: row.sequence,
            captured_at: row.captured_at,
            bids: row.bids.0,
            asks: row.asks.0,
        }
    }
}
