//! URL claiming operations for AsyncCrawlRepository.

use chrono::Utc;

use super::types::CrawlUrlRow;
use super::AsyncCrawlRepository;
use crate::models::{CrawlUrl, UrlStatus};
use crate::repository::Result;

impl AsyncCrawlRepository {
    /// Get URLs that need to be fetched.
    pub async fn get_pending_urls(&self, source_id: &str, limit: u32) -> Result<Vec<CrawlUrl>> {
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
               AND status IN ('discovered', 'fetching')
               ORDER BY depth ASC, discovered_at ASC
               LIMIT ?"#,
            source_id,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(CrawlUrl::from).collect())
    }

    /// Atomically claim a pending URL for processing.
    pub async fn claim_pending_url(&self, source_id: Option<&str>) -> Result<Option<CrawlUrl>> {
        let mut tx = self.pool.begin().await?;

        // Find a pending URL
        let row = if let Some(sid) = source_id {
            sqlx::query_as!(
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
                   WHERE source_id = ? AND status = 'discovered'
                   ORDER BY depth ASC, discovered_at ASC
                   LIMIT 1"#,
                sid
            )
            .fetch_optional(&mut *tx)
            .await?
        } else {
            sqlx::query_as!(
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
                   WHERE status = 'discovered'
                   ORDER BY depth ASC, discovered_at ASC
                   LIMIT 1"#
            )
            .fetch_optional(&mut *tx)
            .await?
        };

        if let Some(row) = row {
            let mut crawl_url = CrawlUrl::from(row);

            // Mark as fetching
            sqlx::query!(
                "UPDATE crawl_urls SET status = 'fetching' WHERE source_id = ? AND url = ?",
                crawl_url.source_id,
                crawl_url.url
            )
            .execute(&mut *tx)
            .await?;

            crawl_url.status = UrlStatus::Fetching;
            tx.commit().await?;
            Ok(Some(crawl_url))
        } else {
            tx.commit().await?;
            Ok(None)
        }
    }

    /// Atomically claim multiple pending URLs for processing.
    pub async fn claim_pending_urls(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>> {
        let mut tx = self.pool.begin().await?;
        let limit = limit as i32;

        // Find pending URLs
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
               WHERE (?1 IS NULL OR source_id = ?1) AND status = 'discovered'
               ORDER BY depth ASC, discovered_at ASC
               LIMIT ?2"#,
            source_id,
            limit
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut urls: Vec<CrawlUrl> = Vec::with_capacity(rows.len());

        for row in rows {
            let mut crawl_url = CrawlUrl::from(row);

            // Mark as fetching
            sqlx::query!(
                "UPDATE crawl_urls SET status = 'fetching' WHERE source_id = ? AND url = ?",
                crawl_url.source_id,
                crawl_url.url
            )
            .execute(&mut *tx)
            .await?;

            crawl_url.status = UrlStatus::Fetching;
            urls.push(crawl_url);
        }

        tx.commit().await?;
        Ok(urls)
    }

    /// Get failed URLs that are ready for retry.
    pub async fn get_retryable_urls(&self, source_id: &str, limit: u32) -> Result<Vec<CrawlUrl>> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let exhausted_cutoff = (now - chrono::Duration::days(70)).to_rfc3339();
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
               AND (
                   (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= ?))
                   OR (status = 'exhausted' AND (next_retry_at IS NULL OR next_retry_at < ?))
               )
               ORDER BY retry_count ASC, discovered_at ASC
               LIMIT ?"#,
            source_id,
            now_str,
            exhausted_cutoff,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(CrawlUrl::from).collect())
    }
}
