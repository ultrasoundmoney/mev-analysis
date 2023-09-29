use lazy_static::lazy_static;
use reqwest::Url;
use serde::Deserialize;

use crate::env::{deserialize_urls, get_app_config, Env};

#[derive(Deserialize)]
pub struct AppConfig {
    pub env: Env,
    pub port: u16,
    pub database_url: String,
    pub relay_database_url: String,
    #[serde(deserialize_with = "deserialize_urls")]
    pub consensus_nodes: Vec<Url>,
    #[serde(deserialize_with = "deserialize_urls")]
    pub validation_nodes: Vec<Url>,
    pub opsgenie_api_key: String,
    pub telegram_api_key: String,
    pub telegram_channel_id: String,
    #[serde(default = "default_wait")]
    pub canonical_wait_minutes: i64,
    /// Slot range to check for counting missed slots
    #[serde(default = "default_missed_slots_range")]
    pub missed_slots_check_range: i64,
    /// Minimum number of missed slots per check interval to trigger an alert
    #[serde(default = "default_missed_slots_alert_threshold")]
    pub missed_slots_alert_threshold: i64,
}

fn default_wait() -> i64 {
    2
}

fn default_missed_slots_range() -> i64 {
    30 // 6 minutes
}

fn default_missed_slots_alert_threshold() -> i64 {
    3
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
