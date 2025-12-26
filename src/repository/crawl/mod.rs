//! Crawl state repository for tracking URL discovery and request history.
//!
//! This module provides async database access for crawl operations using sqlx.

mod async_claims;
mod async_config;
mod async_requests;
mod async_stats;
mod async_urls;
mod types;

use sqlx::sqlite::SqlitePool;

/// Async SQLx-backed repository for crawl state.
#[derive(Clone)]
pub struct AsyncCrawlRepository {
    pub(crate) pool: SqlitePool,
}

impl AsyncCrawlRepository {
    /// Create a new async crawl repository with an existing pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
