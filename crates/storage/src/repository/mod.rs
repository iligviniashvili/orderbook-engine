//! Query layer. Each module adds an `impl Store` block for one table, so the
//! SQL for a table lives in exactly one file.

mod snapshots;
mod symbols;
mod trades;

pub use trades::{TradeQuery, MAX_TRADE_LIMIT};
