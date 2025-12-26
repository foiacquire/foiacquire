//! State and statistics operations for AsyncCrawlRepository.

use std::collections::HashMap;

use super::AsyncCrawlRepository;
use crate::models::{CrawlState, RequestStats};
use crate::repository::{parse_datetime_opt, Result};

impl AsyncCrawlRepository {
    /// Get aggregate crawl state for a source.
    pub async fn get_crawl_state(&self, source_id: &str) -> Result<CrawlState> {
        // Query 1: Get status counts
        let status_rows = sqlx::query!(
            r#"SELECT status as "status!", COUNT(*) as "count!: i64"
               FROM crawl_urls WHERE source_id = ?
               GROUP BY status"#,
            source_id
        )
        .fetch_all(&self.pool)
        .await?;

        let mut status_counts: HashMap<String, u64> = HashMap::new();
        for row in status_rows {
            status_counts.insert(row.status, row.count as u64);
        }

        // Query 2: Get timing info
        let timing = sqlx::query!(
            r#"SELECT
                MIN(discovered_at) as "first_discovered: String",
                MAX(fetched_at) as "last_fetched: String",
                MIN(CASE WHEN status IN ('discovered', 'fetching')
                    THEN discovered_at END) as "oldest_pending: String"
               FROM crawl_urls WHERE source_id = ?"#,
            source_id
        )
        .fetch_one(&self.pool)
        .await?;

        // Query 3: Get unexplored branch count
        let unexplored_count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM crawl_urls u1
               WHERE u1.source_id = ?
               AND u1.status = 'fetched'
               AND u1.discovery_method IN ('html_link', 'pagination', 'api_result')
               AND NOT EXISTS (
                   SELECT 1 FROM crawl_urls u2
                   WHERE u2.source_id = u1.source_id
                   AND u2.parent_url = u1.url
               )
               AND u1.depth < 10"#,
            source_id
        )
        .fetch_one(&self.pool)
        .await?;

        let urls_discovered: u64 = status_counts.values().sum();
        let urls_fetched = *status_counts.get("fetched").unwrap_or(&0);
        let urls_failed = status_counts.get("failed").unwrap_or(&0)
            + status_counts.get("exhausted").unwrap_or(&0);
        let urls_pending = status_counts.get("discovered").unwrap_or(&0)
            + status_counts.get("fetching").unwrap_or(&0);

        Ok(CrawlState {
            source_id: source_id.to_string(),
            last_crawl_started: parse_datetime_opt(timing.first_discovered),
            last_crawl_completed: if urls_pending == 0 {
                parse_datetime_opt(timing.last_fetched)
            } else {
                None
            },
            urls_discovered,
            urls_fetched,
            urls_failed,
            urls_pending,
            has_pending_urls: urls_pending > 0,
            has_unexplored_branches: unexplored_count > 0,
            oldest_pending_url: parse_datetime_opt(timing.oldest_pending),
        })
    }

    /// Count crawl URLs for a source.
    pub async fn count_by_source(&self, source_id: &str) -> Result<u64> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM crawl_urls WHERE source_id = ?"#,
            source_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count as u64)
    }

    /// Get request statistics for a source.
    pub async fn get_request_stats(&self, source_id: &str) -> Result<RequestStats> {
        let stats = sqlx::query!(
            r#"SELECT
                COUNT(*) as "total_requests!: i64",
                SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END) as "success_200: i64",
                SUM(CASE WHEN response_status = 304 THEN 1 ELSE 0 END) as "not_modified_304: i64",
                SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END) as "errors: i64",
                SUM(was_conditional) as "conditional_requests: i64",
                AVG(duration_ms) as "avg_duration_ms: f64",
                SUM(response_size) as "total_bytes: i64"
               FROM crawl_requests
               WHERE source_id = ?"#,
            source_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(RequestStats {
            total_requests: stats.total_requests as u64,
            success_200: stats.success_200.unwrap_or(0) as u64,
            not_modified_304: stats.not_modified_304.unwrap_or(0) as u64,
            errors: stats.errors.unwrap_or(0) as u64,
            conditional_requests: stats.conditional_requests.unwrap_or(0) as u64,
            avg_duration_ms: stats.avg_duration_ms.unwrap_or(0.0),
            total_bytes: stats.total_bytes.unwrap_or(0) as u64,
        })
    }

    /// Get request statistics for all sources (bulk query).
    pub async fn get_all_request_stats(&self) -> Result<HashMap<String, RequestStats>> {
        let rows = sqlx::query!(
            r#"SELECT
                source_id as "source_id!",
                COUNT(*) as "total_requests!: i64",
                COALESCE(SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END), 0) as "success_200!: i64",
                COALESCE(SUM(CASE WHEN response_status = 304 THEN 1 ELSE 0 END), 0) as "not_modified_304!: i64",
                COALESCE(SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END), 0) as "errors!: i64",
                COALESCE(SUM(was_conditional), 0) as "conditional_requests!: i64",
                COALESCE(AVG(duration_ms), 0.0) as "avg_duration_ms!: f64",
                COALESCE(SUM(response_size), 0) as "total_bytes!: i64"
               FROM crawl_requests
               GROUP BY source_id"#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut stats = HashMap::new();
        for row in rows {
            stats.insert(
                row.source_id,
                RequestStats {
                    total_requests: row.total_requests as u64,
                    success_200: row.success_200 as u64,
                    not_modified_304: row.not_modified_304 as u64,
                    errors: row.errors as u64,
                    conditional_requests: row.conditional_requests as u64,
                    avg_duration_ms: row.avg_duration_ms,
                    total_bytes: row.total_bytes as u64,
                },
            );
        }

        Ok(stats)
    }

    /// Get aggregate stats across all sources (bulk query).
    pub async fn get_all_stats(&self) -> Result<HashMap<String, CrawlState>> {
        // Bulk query 1: Get all status counts grouped by source
        let status_rows = sqlx::query!(
            r#"SELECT
                source_id as "source_id!",
                status as "status!",
                COUNT(*) as "count!: i64"
               FROM crawl_urls
               GROUP BY source_id, status"#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut status_by_source: HashMap<String, HashMap<String, u64>> = HashMap::new();
        for row in status_rows {
            status_by_source
                .entry(row.source_id)
                .or_default()
                .insert(row.status, row.count as u64);
        }

        // Bulk query 2: Get timing info for all sources
        let timing_rows = sqlx::query!(
            r#"SELECT
                source_id as "source_id!",
                MIN(discovered_at) as "first_discovered: String",
                MAX(fetched_at) as "last_fetched: String",
                MIN(CASE WHEN status IN ('discovered', 'fetching')
                    THEN discovered_at END) as "oldest_pending: String"
               FROM crawl_urls
               GROUP BY source_id"#
        )
        .fetch_all(&self.pool)
        .await?;

        #[allow(clippy::type_complexity)]
        let mut timing_by_source: HashMap<
            String,
            (Option<String>, Option<String>, Option<String>),
        > = HashMap::new();
        for row in timing_rows {
            timing_by_source.insert(
                row.source_id,
                (
                    Some(row.first_discovered),
                    row.last_fetched.map(|s| s.to_string()),
                    row.oldest_pending.map(|s| s.to_string()),
                ),
            );
        }

        // Bulk query 3: Get unexplored branch counts for all sources
        let unexplored_rows = sqlx::query!(
            r#"SELECT u1.source_id as "source_id!", COUNT(*) as "count!: i64"
               FROM crawl_urls u1
               WHERE u1.status = 'fetched'
               AND u1.discovery_method IN ('html_link', 'pagination', 'api_result')
               AND NOT EXISTS (
                   SELECT 1 FROM crawl_urls u2
                   WHERE u2.source_id = u1.source_id
                   AND u2.parent_url = u1.url
               )
               AND u1.depth < 10
               GROUP BY u1.source_id"#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut unexplored_by_source: HashMap<String, i64> = HashMap::new();
        for row in unexplored_rows {
            unexplored_by_source.insert(row.source_id, row.count);
        }

        // Build CrawlState for each source
        let mut stats = HashMap::new();
        for (source_id, status_counts) in status_by_source {
            let timing = timing_by_source
                .get(&source_id)
                .cloned()
                .unwrap_or((None, None, None));

            let unexplored_count = unexplored_by_source.get(&source_id).copied().unwrap_or(0);

            let urls_discovered: u64 = status_counts.values().sum();
            let urls_fetched = *status_counts.get("fetched").unwrap_or(&0);
            let urls_failed = status_counts.get("failed").unwrap_or(&0)
                + status_counts.get("exhausted").unwrap_or(&0);
            let urls_pending = status_counts.get("discovered").unwrap_or(&0)
                + status_counts.get("fetching").unwrap_or(&0);

            let state = CrawlState {
                source_id: source_id.clone(),
                last_crawl_started: parse_datetime_opt(timing.0),
                last_crawl_completed: if urls_pending == 0 {
                    parse_datetime_opt(timing.1)
                } else {
                    None
                },
                urls_discovered,
                urls_fetched,
                urls_failed,
                urls_pending,
                has_pending_urls: urls_pending > 0,
                has_unexplored_branches: unexplored_count > 0,
                oldest_pending_url: parse_datetime_opt(timing.2),
            };

            stats.insert(source_id, state);
        }

        Ok(stats)
    }
}
