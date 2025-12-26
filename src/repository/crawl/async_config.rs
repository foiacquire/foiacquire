//! Config hash operations for AsyncCrawlRepository.

use chrono::Utc;
use sha2::{Digest, Sha256};

use super::AsyncCrawlRepository;
use crate::repository::Result;

impl AsyncCrawlRepository {
    /// Check if the scraper config has changed since last crawl.
    /// Returns (has_changed, should_clear) - should_clear is true if there are pending URLs.
    pub async fn check_config_changed(
        &self,
        source_id: &str,
        config: &impl serde::Serialize,
    ) -> Result<(bool, bool)> {
        // Compute hash of current config
        let config_json = serde_json::to_string(config).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(config_json.as_bytes());
        let current_hash = hex::encode(hasher.finalize());

        // Get stored hash
        let stored_hash: Option<String> = sqlx::query_scalar!(
            r#"SELECT config_hash as "config_hash!" FROM crawl_config WHERE source_id = ?"#,
            source_id
        )
        .fetch_optional(&self.pool)
        .await?;

        let has_changed = stored_hash.as_ref() != Some(&current_hash);

        // Check if there are pending URLs that would be affected
        let pending_count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM crawl_urls
               WHERE source_id = ? AND status IN ('discovered', 'fetching')"#,
            source_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok((has_changed, has_changed && pending_count > 0))
    }

    /// Store the current config hash for a source.
    pub async fn store_config_hash(
        &self,
        source_id: &str,
        config: &impl serde::Serialize,
    ) -> Result<()> {
        let config_json = serde_json::to_string(config).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(config_json.as_bytes());
        let config_hash = hex::encode(hasher.finalize());
        let now = Utc::now().to_rfc3339();

        sqlx::query!(
            r#"INSERT OR REPLACE INTO crawl_config (source_id, config_hash, updated_at)
               VALUES (?, ?, ?)"#,
            source_id,
            config_hash,
            now
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
