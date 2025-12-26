//! Type definitions for crawl repository database rows.

use std::collections::HashMap;

use crate::models::{CrawlRequest, CrawlUrl, DiscoveryMethod, UrlStatus};
use crate::repository::{parse_datetime, parse_datetime_opt};

/// Row type for CrawlUrl SQLx query mapping.
#[derive(sqlx::FromRow)]
pub struct CrawlUrlRow {
    #[allow(dead_code)]
    pub id: i64,
    pub url: String,
    pub source_id: String,
    pub status: String,
    pub discovery_method: String,
    pub parent_url: Option<String>,
    pub discovery_context: String,
    pub depth: i64,
    pub discovered_at: String,
    pub fetched_at: Option<String>,
    pub retry_count: i64,
    pub last_error: Option<String>,
    pub next_retry_at: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_hash: Option<String>,
    pub document_id: Option<String>,
}

impl From<CrawlUrlRow> for CrawlUrl {
    fn from(row: CrawlUrlRow) -> Self {
        let discovery_context: HashMap<String, serde_json::Value> =
            serde_json::from_str(&row.discovery_context).unwrap_or_default();

        CrawlUrl {
            url: row.url,
            source_id: row.source_id,
            status: UrlStatus::from_str(&row.status).unwrap_or(UrlStatus::Discovered),
            discovery_method: DiscoveryMethod::from_str(&row.discovery_method)
                .unwrap_or(DiscoveryMethod::Seed),
            parent_url: row.parent_url,
            discovery_context,
            depth: row.depth as u32,
            discovered_at: parse_datetime(&row.discovered_at),
            fetched_at: parse_datetime_opt(row.fetched_at),
            retry_count: row.retry_count as u32,
            last_error: row.last_error,
            next_retry_at: parse_datetime_opt(row.next_retry_at),
            etag: row.etag,
            last_modified: row.last_modified,
            content_hash: row.content_hash,
            document_id: row.document_id,
        }
    }
}

/// Row type for CrawlRequest SQLx query mapping.
#[derive(sqlx::FromRow)]
pub struct CrawlRequestRow {
    pub id: i64,
    pub source_id: String,
    pub url: String,
    pub method: String,
    pub request_headers: String,
    pub request_at: String,
    pub response_status: Option<i64>,
    pub response_headers: String,
    pub response_at: Option<String>,
    pub response_size: Option<i64>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
    pub was_conditional: i64,
    pub was_not_modified: i64,
}

impl From<CrawlRequestRow> for CrawlRequest {
    fn from(row: CrawlRequestRow) -> Self {
        CrawlRequest {
            id: Some(row.id),
            source_id: row.source_id,
            url: row.url,
            method: row.method,
            request_headers: serde_json::from_str(&row.request_headers).unwrap_or_default(),
            request_at: parse_datetime(&row.request_at),
            response_status: row.response_status.map(|s| s as u16),
            response_headers: serde_json::from_str(&row.response_headers).unwrap_or_default(),
            response_at: parse_datetime_opt(row.response_at),
            response_size: row.response_size.map(|s| s as u64),
            duration_ms: row.duration_ms.map(|d| d as u64),
            error: row.error,
            was_conditional: row.was_conditional != 0,
            was_not_modified: row.was_not_modified != 0,
        }
    }
}
