//! URL CRUD operations for AsyncCrawlRepository.

use chrono::{DateTime, Utc};

use super::types::CrawlUrlRow;
use super::AsyncCrawlRepository;
use crate::models::CrawlUrl;
use crate::repository::Result;

impl AsyncCrawlRepository {
    /// Add a discovered URL if not already known.
    pub async fn add_url(&self, crawl_url: &CrawlUrl) -> Result<bool> {
        let status = crawl_url.status.as_str();
        let discovery_method = crawl_url.discovery_method.as_str();
        let discovery_context = serde_json::to_string(&crawl_url.discovery_context)?;
        let depth = crawl_url.depth as i32;
        let discovered_at = crawl_url.discovered_at.to_rfc3339();
        let retry_count = crawl_url.retry_count as i32;

        let result = sqlx::query!(
            r#"INSERT OR IGNORE INTO crawl_urls (
                url, source_id, status, discovery_method, parent_url,
                discovery_context, depth, discovered_at, retry_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
            crawl_url.url,
            crawl_url.source_id,
            status,
            discovery_method,
            crawl_url.parent_url,
            discovery_context,
            depth,
            discovered_at,
            retry_count
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Get a specific URL's crawl state.
    pub async fn get_url(&self, source_id: &str, url: &str) -> Result<Option<CrawlUrl>> {
        let row = sqlx::query_as!(
            CrawlUrlRow,
            r#"SELECT
                id as "id!",
                url as "url!",
                source_id as "source_id!",
                status as "status!",
                discovery_method as "discovery_method!",
                parent_url,
                discovery_context as "discovery_context!",
                depth as "depth!",
                discovered_at as "discovered_at!",
                fetched_at,
                retry_count as "retry_count!",
                last_error,
                next_retry_at,
                etag,
                last_modified,
                content_hash,
                document_id
               FROM crawl_urls WHERE source_id = ? AND url = ?"#,
            source_id,
            url
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(CrawlUrl::from))
    }

    /// Check if a URL has already been discovered.
    pub async fn url_exists(&self, source_id: &str, url: &str) -> Result<bool> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM crawl_urls
               WHERE source_id = ? AND url = ?"#,
            source_id,
            url
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    /// Update an existing URL's state.
    pub async fn update_url(&self, crawl_url: &CrawlUrl) -> Result<()> {
        let status = crawl_url.status.as_str();
        let fetched_at = crawl_url.fetched_at.map(|dt| dt.to_rfc3339());
        let retry_count = crawl_url.retry_count as i32;
        let next_retry_at = crawl_url.next_retry_at.map(|dt| dt.to_rfc3339());

        sqlx::query!(
            r#"UPDATE crawl_urls SET
                status = ?1,
                fetched_at = ?2,
                retry_count = ?3,
                last_error = ?4,
                next_retry_at = ?5,
                etag = ?6,
                last_modified = ?7,
                content_hash = ?8,
                document_id = ?9
            WHERE source_id = ?10 AND url = ?11"#,
            status,
            fetched_at,
            retry_count,
            crawl_url.last_error,
            next_retry_at,
            crawl_url.etag,
            crawl_url.last_modified,
            crawl_url.content_hash,
            crawl_url.document_id,
            crawl_url.source_id,
            crawl_url.url
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark a URL for refresh by changing its status back to 'discovered'.
    pub async fn mark_url_for_refresh(&self, source_id: &str, url: &str) -> Result<()> {
        sqlx::query!(
            r#"UPDATE crawl_urls SET status = 'discovered' WHERE source_id = ? AND url = ?"#,
            source_id,
            url
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get URLs that haven't been checked since a given time.
    pub async fn get_urls_needing_refresh(
        &self,
        source_id: &str,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>> {
        let older_than_str = older_than.to_rfc3339();
        let limit = limit as i32;

        let rows = sqlx::query_as!(
            CrawlUrlRow,
            r#"SELECT
                id as "id!",
                url as "url!",
                source_id as "source_id!",
                status as "status!",
                discovery_method as "discovery_method!",
                parent_url,
                discovery_context as "discovery_context!",
                depth as "depth!",
                discovered_at as "discovered_at!",
                fetched_at,
                retry_count as "retry_count!",
                last_error,
                next_retry_at,
                etag,
                last_modified,
                content_hash,
                document_id
               FROM crawl_urls
               WHERE source_id = ?
               AND status = 'fetched'
               AND fetched_at < ?
               ORDER BY fetched_at ASC
               LIMIT ?"#,
            source_id,
            older_than_str,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(CrawlUrl::from).collect())
    }

    /// Get recently fetched URLs (successfully completed).
    pub async fn get_recent_downloads(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>> {
        let limit = limit as i32;

        let rows = sqlx::query_as!(
            CrawlUrlRow,
            r#"SELECT
                id as "id!",
                url as "url!",
                source_id as "source_id!",
                status as "status!",
                discovery_method as "discovery_method!",
                parent_url,
                discovery_context as "discovery_context!",
                depth as "depth!",
                discovered_at as "discovered_at!",
                fetched_at,
                retry_count as "retry_count!",
                last_error,
                next_retry_at,
                etag,
                last_modified,
                content_hash,
                document_id
               FROM crawl_urls
               WHERE (?1 IS NULL OR source_id = ?1) AND status = 'fetched'
               ORDER BY fetched_at DESC
               LIMIT ?2"#,
            source_id,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(CrawlUrl::from).collect())
    }

    /// Get failed URLs with their error messages.
    pub async fn get_failed_urls(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>> {
        let limit = limit as i32;

        let rows = sqlx::query_as!(
            CrawlUrlRow,
            r#"SELECT
                id as "id!",
                url as "url!",
                source_id as "source_id!",
                status as "status!",
                discovery_method as "discovery_method!",
                parent_url,
                discovery_context as "discovery_context!",
                depth as "depth!",
                discovered_at as "discovered_at!",
                fetched_at,
                retry_count as "retry_count!",
                last_error,
                next_retry_at,
                etag,
                last_modified,
                content_hash,
                document_id
               FROM crawl_urls
               WHERE (?1 IS NULL OR source_id = ?1) AND status IN ('failed', 'exhausted')
               ORDER BY fetched_at DESC NULLS LAST
               LIMIT ?2"#,
            source_id,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(CrawlUrl::from).collect())
    }

    /// Clear pending crawl state for a source (keeps fetched URLs).
    pub async fn clear_source(&self, source_id: &str) -> Result<()> {
        sqlx::query!(
            r#"DELETE FROM crawl_urls
               WHERE source_id = ? AND status IN ('discovered', 'fetching', 'failed')"#,
            source_id
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!("DELETE FROM crawl_requests WHERE source_id = ?", source_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Clear ALL crawl state for a source (including fetched URLs).
    pub async fn clear_source_all(&self, source_id: &str) -> Result<()> {
        sqlx::query!("DELETE FROM crawl_urls WHERE source_id = ?", source_id)
            .execute(&self.pool)
            .await?;

        sqlx::query!("DELETE FROM crawl_requests WHERE source_id = ?", source_id)
            .execute(&self.pool)
            .await?;

        sqlx::query!("DELETE FROM crawl_config WHERE source_id = ?", source_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
