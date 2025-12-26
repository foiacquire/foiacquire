//! Database persistence for rate limit state.

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use tracing::{debug, info};

use super::domain_state::DomainState;
use super::RateLimiter;

/// Create a SQLite pool for rate limit persistence.
async fn create_pool(db_path: &Path) -> anyhow::Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(30));

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    // Initialize the table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS rate_limit_state (
            domain TEXT PRIMARY KEY,
            current_delay_ms INTEGER NOT NULL,
            in_backoff INTEGER NOT NULL DEFAULT 0,
            total_requests INTEGER NOT NULL DEFAULT 0,
            rate_limit_hits INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

/// Load rate limit state from database into a RateLimiter.
pub async fn load_rate_limit_state(limiter: &RateLimiter, db_path: &Path) -> anyhow::Result<usize> {
    let pool = create_pool(db_path).await?;

    let rows = sqlx::query_as::<_, (String, i64, i32, i64, i64)>(
        "SELECT domain, current_delay_ms, in_backoff, total_requests, rate_limit_hits FROM rate_limit_state",
    )
    .fetch_all(&pool)
    .await?;

    let mut domains = limiter.domains.write().await;
    let base_delay = limiter.config.base_delay;
    let mut count = 0;

    for (domain, delay_ms, in_backoff, total_requests, rate_limit_hits) in rows {
        let in_backoff = in_backoff != 0;

        // Only load domains that are still in backoff (have meaningful state)
        if in_backoff || delay_ms > base_delay.as_millis() as i64 {
            let state = DomainState {
                current_delay: Duration::from_millis(delay_ms as u64),
                last_request: None, // Can't restore Instant from DB
                consecutive_successes: 0,
                recent_403s: Vec::new(),
                in_backoff,
                total_requests: total_requests as u64,
                rate_limit_hits: rate_limit_hits as u64,
            };
            info!(
                "Restored rate limit state for {}: delay={}ms, in_backoff={}",
                domain, delay_ms, in_backoff
            );
            domains.insert(domain, state);
            count += 1;
        }
    }

    if count > 0 {
        info!(
            "Loaded rate limit state for {} domains from database",
            count
        );
    }

    Ok(count)
}

/// Save rate limit state to database.
pub async fn save_rate_limit_state(limiter: &RateLimiter, db_path: &Path) -> anyhow::Result<usize> {
    let pool = create_pool(db_path).await?;

    let domains = limiter.domains.read().await;
    let base_delay = limiter.config.base_delay;
    let mut count = 0;

    for (domain, state) in domains.iter() {
        // Only save domains with non-default state
        if state.in_backoff || state.current_delay > base_delay {
            let delay = state.current_delay.as_millis() as i64;
            let in_backoff = state.in_backoff as i32;
            let total = state.total_requests as i64;
            let hits = state.rate_limit_hits as i64;

            sqlx::query(
                r#"INSERT OR REPLACE INTO rate_limit_state
                   (domain, current_delay_ms, in_backoff, total_requests, rate_limit_hits, updated_at)
                   VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"#,
            )
            .bind(&domain)
            .bind(delay)
            .bind(in_backoff)
            .bind(total)
            .bind(hits)
            .execute(&pool)
            .await?;
            count += 1;
        }
    }

    // Clean up old entries that are no longer in backoff
    let base_delay_ms = base_delay.as_millis() as i64;
    sqlx::query("DELETE FROM rate_limit_state WHERE in_backoff = 0 AND current_delay_ms <= ?")
        .bind(base_delay_ms)
        .execute(&pool)
        .await?;

    if count > 0 {
        debug!("Saved rate limit state for {} domains to database", count);
    }

    Ok(count)
}

/// Save state for a single domain (call after rate limit events).
pub async fn save_domain_state(
    limiter: &RateLimiter,
    domain: &str,
    db_path: &Path,
) -> anyhow::Result<()> {
    let domains = limiter.domains.read().await;
    let base_delay = limiter.config.base_delay;

    if let Some(state) = domains.get(domain) {
        if state.in_backoff || state.current_delay > base_delay {
            let pool = create_pool(db_path).await?;

            let delay = state.current_delay.as_millis() as i64;
            let in_backoff = state.in_backoff as i32;
            let total = state.total_requests as i64;
            let hits = state.rate_limit_hits as i64;

            sqlx::query(
                r#"INSERT OR REPLACE INTO rate_limit_state
                   (domain, current_delay_ms, in_backoff, total_requests, rate_limit_hits, updated_at)
                   VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"#,
            )
            .bind(domain)
            .bind(delay)
            .bind(in_backoff)
            .bind(total)
            .bind(hits)
            .execute(&pool)
            .await?;
        }
    }

    Ok(())
}
