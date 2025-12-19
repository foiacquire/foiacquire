//! Configuration management for FOIAcquire using the prefer crate.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::llm::LlmConfig;
use crate::scrapers::ScraperConfig;

/// Default refresh TTL in days (14 days).
pub const DEFAULT_REFRESH_TTL_DAYS: u64 = 14;

/// Application settings.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Base data directory.
    pub data_dir: PathBuf,
    /// Database filename.
    pub database_filename: String,
    /// Directory for storing documents.
    pub documents_dir: PathBuf,
    /// User agent for HTTP requests.
    pub user_agent: String,
    /// Request timeout in seconds.
    pub request_timeout: u64,
    /// Delay between requests in milliseconds.
    pub request_delay_ms: u64,
    /// Rate limit backend URL (None = in-memory, "sqlite" = local DB, "redis://..." = Redis).
    pub rate_limit_backend: Option<String>,
    /// Worker queue broker URL (None = local DB, "amqp://..." = RabbitMQ).
    pub broker_url: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        // Default to ~/Documents/foia/ for user data
        // Falls back gracefully: Documents dir -> Home dir -> Current dir
        let data_dir = dirs::document_dir()
            .or_else(dirs::home_dir)
            .unwrap_or_else(|| PathBuf::from("."))
            .join("foia");

        Self {
            documents_dir: data_dir.join("documents"),
            data_dir,
            database_filename: "foiacquire.db".to_string(),
            user_agent: "FOIAcquire/0.1 (academic research)".to_string(),
            request_timeout: 30,
            request_delay_ms: 500,
            rate_limit_backend: None, // In-memory by default
            broker_url: None,         // Local DB by default
        }
    }
}

impl Settings {
    /// Create settings with a custom data directory.
    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            documents_dir: data_dir.join("documents"),
            data_dir,
            ..Default::default()
        }
    }

    /// Get the full path to the database.
    pub fn database_path(&self) -> PathBuf {
        self.data_dir.join(&self.database_filename)
    }

    /// Ensure all directories exist.
    pub fn ensure_directories(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.data_dir)?;
        fs::create_dir_all(&self.documents_dir)?;
        Ok(())
    }
}

/// Configuration file structure.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Target directory for data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Database filename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// User agent string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Request timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout: Option<u64>,
    /// Delay between requests in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_delay_ms: Option<u64>,
    /// Rate limit backend URL.
    /// - None or "memory": In-memory (single process only)
    /// - "sqlite": Use local SQLite database (multi-process safe)
    /// - "redis://host:port": Use Redis (distributed)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_backend: Option<String>,
    /// Worker queue broker URL.
    /// - None or "database": Use local SQLite database
    /// - "amqp://host:port": Use RabbitMQ
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_url: Option<String>,
    /// Default refresh TTL in days for re-checking fetched URLs.
    /// Individual scrapers can override this with their own refresh_ttl_days.
    /// Defaults to 14 days if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_refresh_ttl_days: Option<u64>,
    /// Scraper configurations.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub scrapers: HashMap<String, ScraperConfig>,
    /// LLM configuration for document summarization.
    #[serde(default, skip_serializing_if = "LlmConfig::is_default")]
    pub llm: LlmConfig,

    /// Path to the config file this was loaded from (not serialized).
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
}

impl Config {
    /// Load configuration using prefer crate.
    /// Automatically discovers foiacquire config files in standard locations.
    pub async fn load() -> Self {
        match prefer::load("foiacquire").await {
            Ok(pref_config) => {
                // Extract values from prefer config using dot notation
                let target: Option<String> = pref_config.get("target").await.ok();
                let database: Option<String> = pref_config.get("database").await.ok();
                let user_agent: Option<String> = pref_config.get("user_agent").await.ok();
                let request_timeout: Option<u64> = pref_config.get("request_timeout").await.ok();
                let request_delay_ms: Option<u64> = pref_config.get("request_delay_ms").await.ok();
                let rate_limit_backend: Option<String> =
                    pref_config.get("rate_limit_backend").await.ok();
                let broker_url: Option<String> = pref_config.get("broker_url").await.ok();
                let default_refresh_ttl_days: Option<u64> =
                    pref_config.get("default_refresh_ttl_days").await.ok();
                let scrapers: HashMap<String, ScraperConfig> =
                    pref_config.get("scrapers").await.unwrap_or_default();
                let llm: LlmConfig = pref_config.get("llm").await.unwrap_or_default();

                // Get the source path from prefer
                let source_path = pref_config.source_path().cloned();

                Config {
                    target,
                    database,
                    user_agent,
                    request_timeout,
                    request_delay_ms,
                    rate_limit_backend,
                    broker_url,
                    default_refresh_ttl_days,
                    scrapers,
                    llm,
                    source_path,
                }
            }
            Err(_) => {
                // No config file found, use defaults
                Self::default()
            }
        }
    }

    /// Load configuration from a specific file path.
    pub async fn load_from_path(path: &Path) -> Result<Self, String> {
        let contents = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| format!("Failed to read config file: {}", e))?;

        let mut config: Config = serde_json::from_str(&contents)
            .map_err(|e| format!("Failed to parse config file: {}", e))?;

        config.source_path = Some(path.to_path_buf());
        Ok(config)
    }

    /// Get the base directory for resolving relative paths.
    /// Returns the config file's parent directory if available, otherwise None.
    pub fn base_dir(&self) -> Option<PathBuf> {
        self.source_path.as_ref().and_then(|p| p.parent().map(|p| p.to_path_buf()))
    }

    /// Resolve a path that may be relative to the config file.
    /// - Absolute paths are returned as-is
    /// - Paths starting with ~ are expanded
    /// - Relative paths are resolved relative to `base_dir` (config file location or CWD)
    pub fn resolve_path(&self, path_str: &str, base_dir: &Path) -> PathBuf {
        let expanded = shellexpand::tilde(path_str);
        let path = Path::new(expanded.as_ref());

        if path.is_absolute() {
            path.to_path_buf()
        } else {
            base_dir.join(path)
        }
    }

    /// Apply configuration to settings.
    /// `base_dir` is used to resolve relative paths (typically config file dir or CWD).
    pub fn apply_to_settings(&self, settings: &mut Settings, base_dir: &Path) {
        if let Some(ref target) = self.target {
            settings.data_dir = self.resolve_path(target, base_dir);
            settings.documents_dir = settings.data_dir.join("documents");
        }
        if let Some(ref database) = self.database {
            settings.database_filename = database.clone();
        }
        if let Some(ref user_agent) = self.user_agent {
            settings.user_agent = user_agent.clone();
        }
        if let Some(timeout) = self.request_timeout {
            settings.request_timeout = timeout;
        }
        if let Some(delay) = self.request_delay_ms {
            settings.request_delay_ms = delay;
        }
        if let Some(ref backend) = self.rate_limit_backend {
            settings.rate_limit_backend = Some(backend.clone());
        }
        if let Some(ref broker) = self.broker_url {
            settings.broker_url = Some(broker.clone());
        }
    }

    /// Get the effective refresh TTL in days for a scraper.
    /// Priority: scraper config > global config > default constant.
    pub fn get_refresh_ttl_days(&self, source_id: &str) -> u64 {
        // First check scraper-specific config
        if let Some(scraper_config) = self.scrapers.get(source_id) {
            if let Some(ttl) = scraper_config.refresh_ttl_days {
                return ttl;
            }
        }
        // Fall back to global config or default
        self.default_refresh_ttl_days
            .unwrap_or(DEFAULT_REFRESH_TTL_DAYS)
    }
}

/// Options for loading settings.
#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// Explicit config file path (overrides auto-discovery).
    pub config_path: Option<PathBuf>,
    /// Use CWD for relative paths instead of config file directory.
    pub use_cwd: bool,
    /// Override data directory (--data-dir flag).
    pub data_dir: Option<PathBuf>,
}

/// Load settings with explicit options.
pub async fn load_settings_with_options(options: LoadOptions) -> Settings {
    // Load config from explicit path or auto-discover
    let config = match &options.config_path {
        Some(path) => Config::load_from_path(path).await.unwrap_or_default(),
        None => Config::load().await,
    };

    let mut settings = Settings::default();

    // Determine base directory for resolving relative paths
    let base_dir = if options.use_cwd {
        // --cwd flag: use current working directory
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    } else {
        // Default: use config file's directory, fall back to CWD
        config.base_dir().unwrap_or_else(|| {
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
        })
    };

    config.apply_to_settings(&mut settings, &base_dir);

    // --data-dir override takes precedence
    if let Some(data_dir) = options.data_dir {
        settings = Settings::with_data_dir(data_dir);
    }

    settings
}
