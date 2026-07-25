//! Migration runner (`DESIGN.md` §11.5). Migrations are embedded into the binary at compile time by
//! the `sqlx::migrate!` macro, so a deployed proxy needs no migration files on disk.

use sqlx::SqlitePool;
use sqlx::migrate::Migrator;

use crate::error::StoreError;

/// The compiled-in SQLite migration set (`migrations/sqlite/`).
pub static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

/// Apply all pending SQLite migrations to `pool`.
pub async fn run_sqlite(pool: &SqlitePool) -> Result<(), StoreError> {
    SQLITE_MIGRATOR.run(pool).await.map_err(StoreError::from)
}
