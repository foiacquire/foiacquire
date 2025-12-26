//! Request logging operations for AsyncCrawlRepository.

use super::types::CrawlRequestRow;
use super::AsyncCrawlRepository;
use crate::models::CrawlRequest;
use crate::repository::Result;

impl AsyncCrawlRepository {
    /// Log an HTTP request and return its ID.
    pub async fn log_request(&self, request: &CrawlRequest) -> Result<i64> {
        let request_headers = serde_json::to_string(&request.request_headers)?;
        let request_at = request.request_at.to_rfc3339();
        let response_status = request.response_status.map(|s| s as i32);
        let response_headers = serde_json::to_string(&request.response_headers)?;
        let response_at = request.response_at.map(|dt| dt.to_rfc3339());
        let response_size = request.response_size.map(|s| s as i64);
        let duration_ms = request.duration_ms.map(|d| d as i64);
        let was_conditional = request.was_conditional as i32;
        let was_not_modified = request.was_not_modified as i32;

        let result = sqlx::query!(
            r#"INSERT INTO crawl_requests (
                source_id, url, method, request_headers, request_at,
                response_status, response_headers, response_at,
                response_size, duration_ms, error,
                was_conditional, was_not_modified
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
            request.source_id,
            request.url,
            request.method,
            request_headers,
            request_at,
            response_status,
            response_headers,
            response_at,
            response_size,
            duration_ms,
            request.error,
            was_conditional,
            was_not_modified
        )
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Get the most recent request for a URL.
    pub async fn get_last_request(
        &self,
        source_id: &str,
        url: &str,
    ) -> Result<Option<CrawlRequest>> {
        let row = sqlx::query_as!(
            CrawlRequestRow,
            r#"SELECT
                id as "id!",
                source_id as "source_id!",
                url as "url!",
                method as "method!",
                request_headers as "request_headers!",
                request_at as "request_at!",
                response_status,
                response_headers as "response_headers!",
                response_at,
                response_size,
                duration_ms,
                error,
                was_conditional as "was_conditional!",
                was_not_modified as "was_not_modified!"
               FROM crawl_requests
               WHERE source_id = ? AND url = ?
               ORDER BY request_at DESC
               LIMIT 1"#,
            source_id,
            url
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(CrawlRequest::from))
    }
}
