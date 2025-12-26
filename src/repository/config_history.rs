//! Configuration history repository for tracking config changes.
//!
//! This module provides async database access for configuration history using sqlx.

use chrono::{DateTime, Utc};
use sqlx::sqlite::SqlitePool;
use uuid::Uuid;

use super::{parse_datetime, Result};

/// Maximum number of configuration history entries to retain.
const MAX_HISTORY_ENTRIES: i32 = 16;

/// Represents a stored configuration entry.
#[derive(Debug, Clone)]
pub struct ConfigHistoryEntry {
    pub uuid: String,
    pub created_at: DateTime<Utc>,
    pub data: String,
    pub format: String,
    pub hash: String,
}

/// Row type for SQLx query mapping.
#[derive(sqlx::FromRow)]
struct ConfigHistoryRow {
    uuid: String,
    created_at: String,
    data: String,
    format: String,
    hash: String,
}

impl From<ConfigHistoryRow> for ConfigHistoryEntry {
    fn from(row: ConfigHistoryRow) -> Self {
        ConfigHistoryEntry {
            uuid: row.uuid,
            created_at: parse_datetime(&row.created_at),
            data: row.data,
            format: row.format,
            hash: row.hash,
        }
    }
}

/// Async SQLx-backed configuration history repository.
#[derive(Clone)]
pub struct AsyncConfigHistoryRepository {
    pool: SqlitePool,
}

impl AsyncConfigHistoryRepository {
    /// Create a new async configuration history repository.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Check if a config with the given hash already exists.
    pub async fn hash_exists(&self, hash: &str) -> Result<bool> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM config_history WHERE hash = ?"#,
            hash
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    /// Insert a new configuration entry if the hash doesn't already exist.
    /// Returns true if inserted, false if hash already exists.
    pub async fn insert_if_new(&self, data: &str, format: &str, hash: &str) -> Result<bool> {
        if self.hash_exists(hash).await? {
            return Ok(false);
        }

        let uuid = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        sqlx::query!(
            r#"INSERT INTO config_history (uuid, created_at, data, format, hash)
               VALUES (?1, ?2, ?3, ?4, ?5)"#,
            uuid,
            now,
            data,
            format,
            hash
        )
        .execute(&self.pool)
        .await?;

        // Prune old entries
        self.prune_old_entries().await?;

        Ok(true)
    }

    /// Get the most recent configuration entry.
    pub async fn get_latest(&self) -> Result<Option<ConfigHistoryEntry>> {
        let row = sqlx::query_as!(
            ConfigHistoryRow,
            r#"SELECT
                uuid as "uuid!",
                created_at as "created_at!",
                data as "data!",
                format as "format!",
                hash as "hash!"
               FROM config_history
               ORDER BY created_at DESC
               LIMIT 1"#
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(ConfigHistoryEntry::from))
    }

    /// Get all configuration history entries (most recent first).
    pub async fn get_all(&self) -> Result<Vec<ConfigHistoryEntry>> {
        let rows = sqlx::query_as!(
            ConfigHistoryRow,
            r#"SELECT
                uuid as "uuid!",
                created_at as "created_at!",
                data as "data!",
                format as "format!",
                hash as "hash!"
               FROM config_history
               ORDER BY created_at DESC"#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(ConfigHistoryEntry::from).collect())
    }

    /// Get just the hash of the most recent configuration entry.
    pub async fn get_latest_hash(&self) -> Result<Option<String>> {
        let hash = sqlx::query_scalar!(
            r#"SELECT hash as "hash!" FROM config_history ORDER BY created_at DESC LIMIT 1"#
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(hash)
    }

    /// Prune old entries to keep only the last MAX_HISTORY_ENTRIES.
    async fn prune_old_entries(&self) -> Result<()> {
        sqlx::query!(
            r#"DELETE FROM config_history
               WHERE uuid NOT IN (
                   SELECT uuid FROM config_history
                   ORDER BY created_at DESC
                   LIMIT ?
               )"#,
            MAX_HISTORY_ENTRIES
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
