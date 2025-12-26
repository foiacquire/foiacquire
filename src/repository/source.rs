//! Source repository for SQLite persistence.
//!
//! This module provides async database access for source operations using sqlx.

use chrono::{DateTime, Utc};
use sqlx::sqlite::SqlitePool;

use super::{parse_datetime, parse_datetime_opt, Result};
use crate::models::{Source, SourceType};

/// Row type for SQLx query mapping.
#[derive(sqlx::FromRow)]
struct SourceRow {
    id: String,
    source_type: String,
    name: String,
    base_url: String,
    metadata: String,
    created_at: String,
    last_scraped: Option<String>,
}

impl From<SourceRow> for Source {
    fn from(row: SourceRow) -> Self {
        Source {
            id: row.id,
            source_type: SourceType::from_str(&row.source_type).unwrap_or(SourceType::Custom),
            name: row.name,
            base_url: row.base_url,
            metadata: serde_json::from_str(&row.metadata).unwrap_or_default(),
            created_at: parse_datetime(&row.created_at),
            last_scraped: parse_datetime_opt(row.last_scraped),
        }
    }
}

/// Async SQLx-backed source repository.
#[derive(Clone)]
pub struct AsyncSourceRepository {
    pool: SqlitePool,
}

impl AsyncSourceRepository {
    /// Create a new async source repository with an existing pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Get a source by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Source>> {
        let row = sqlx::query_as!(
            SourceRow,
            r#"SELECT
                id as "id!",
                source_type as "source_type!",
                name as "name!",
                base_url as "base_url!",
                metadata as "metadata!",
                created_at as "created_at!",
                last_scraped
               FROM sources WHERE id = ?"#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Source::from))
    }

    /// Get all sources.
    pub async fn get_all(&self) -> Result<Vec<Source>> {
        let rows = sqlx::query_as!(
            SourceRow,
            r#"SELECT
                id as "id!",
                source_type as "source_type!",
                name as "name!",
                base_url as "base_url!",
                metadata as "metadata!",
                created_at as "created_at!",
                last_scraped
               FROM sources"#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Source::from).collect())
    }

    /// Save a source (insert or update).
    pub async fn save(&self, source: &Source) -> Result<()> {
        let metadata_json = serde_json::to_string(&source.metadata)?;
        let created_at = source.created_at.to_rfc3339();
        let last_scraped = source.last_scraped.map(|dt| dt.to_rfc3339());
        let source_type = source.source_type.as_str();

        sqlx::query!(
            r#"INSERT INTO sources (id, source_type, name, base_url, metadata, created_at, last_scraped)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
               ON CONFLICT(id) DO UPDATE SET
                   source_type = excluded.source_type,
                   name = excluded.name,
                   base_url = excluded.base_url,
                   metadata = excluded.metadata,
                   last_scraped = excluded.last_scraped"#,
            source.id,
            source_type,
            source.name,
            source.base_url,
            metadata_json,
            created_at,
            last_scraped
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete a source.
    pub async fn delete(&self, id: &str) -> Result<bool> {
        let result = sqlx::query!("DELETE FROM sources WHERE id = ?", id)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Check if a source exists.
    pub async fn exists(&self, id: &str) -> Result<bool> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM sources WHERE id = ?"#,
            id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    /// Update last scraped timestamp.
    pub async fn update_last_scraped(&self, id: &str, timestamp: DateTime<Utc>) -> Result<()> {
        let ts = timestamp.to_rfc3339();

        sqlx::query!("UPDATE sources SET last_scraped = ? WHERE id = ?", ts, id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
