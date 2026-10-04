use anyhow::{Result, ensure};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    #[default]
    Sqlite,
    Clickhouse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouse {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: Option<tg_backup_credentials::Secret>,
    pub timeout_seconds: u64,
}
impl Default for ClickHouse {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8123".into(),
            database: "tg_backup".into(),
            user: "default".into(),
            password: None,
            timeout_seconds: 30,
        }
    }
}
impl ClickHouse {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.url.starts_with("http://") || self.url.starts_with("https://"),
            "ClickHouse URL must use HTTP or HTTPS"
        );
        ensure!(
            !self.database.is_empty()
                && self
                    .database
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid ClickHouse database name"
        );
        ensure!(
            self.timeout_seconds > 0,
            "ClickHouse timeout must be positive"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum EpochPeriod {
    Weekly,
    #[default]
    Monthly,
    Yearly,
}
impl EpochPeriod {
    pub fn key(self, time: DateTime<Utc>) -> String {
        match self {
            Self::Monthly => time.format("%Y-%m").to_string(),
            Self::Yearly => time.format("%Y").to_string(),
            Self::Weekly => format!("{}-W{:02}", time.iso_week().year(), time.iso_week().week()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub backend: Backend,
    pub clickhouse: Option<ClickHouse>,
    pub dataset_id: Option<String>,
    pub epoch: EpochPeriod,
    pub compression_level: i32,
    pub block_bytes: usize,
    pub retrain: bool,
    pub index_history: bool,
    pub history_selector: String,
    pub attachment_selector: String,
    pub metadata_refresh_seconds: u64,
    pub audit_interval_seconds: u64,
    pub max_file_bytes: u64,
    /// Logical SQLite allocation target; zero disables size rollover.
    pub max_epoch_bytes: u64,
    pub resources: crate::work::Resources,
    pub schedule: crate::work::Schedule,
    pub transcode: crate::transcode::Policy,
    pub api_bind: String,
    pub api_token: Option<tg_backup_credentials::Secret>,
    pub metrics_bind: Option<String>,
    pub metrics_token: Option<tg_backup_credentials::Secret>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            backend: Backend::Sqlite,
            clickhouse: None,
            dataset_id: None,
            epoch: EpochPeriod::Monthly,
            compression_level: 9,
            block_bytes: 1024 * 1024,
            retrain: true,
            index_history: false,
            history_selector: "true".into(),
            attachment_selector: "true".into(),
            metadata_refresh_seconds: 3600,
            audit_interval_seconds: 86400,
            max_file_bytes: 4 * 1024 * 1024 * 1024,
            max_epoch_bytes: 4 * 1024 * 1024 * 1024,
            resources: Default::default(),
            schedule: Default::default(),
            transcode: Default::default(),
            api_bind: "127.0.0.1:8080".into(),
            api_token: None,
            metrics_bind: None,
            metrics_token: None,
        }
    }
}
impl Config {
    pub fn load(root: &Path) -> Result<Self> {
        let value: Self = toml::from_str(&std::fs::read_to_string(root.join("config.toml"))?)?;
        if value.backend == Backend::Clickhouse {
            value
                .clickhouse
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("ClickHouse configuration is missing"))?
                .validate()?;
        }
        ensure!(
            (4096..=64 * 1024 * 1024).contains(&value.block_bytes),
            "block_bytes must be 4 KiB–64 MiB"
        );
        ensure!(
            (1..=19).contains(&value.compression_level),
            "compression_level must be 1–19"
        );
        ensure!(
            value.metadata_refresh_seconds > 0 && value.audit_interval_seconds > 0,
            "refresh intervals must be positive"
        );
        value.resources.validate()?;
        value.schedule.validate()?;
        value.transcode.validate()?;
        crate::selector::Selector::parse(&value.history_selector)?;
        crate::selector::Selector::parse(&value.attachment_selector)?;
        Ok(value)
    }
}
