use std::{collections::HashSet, fmt, str, sync::LazyLock};

use reqwest::Url;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};

use crate::env::{
    deserialize_hash_set, deserialize_network, deserialize_urls, get_app_config, Network,
};

#[serde_as]
#[derive(Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_wait")]
    pub canonical_wait_minutes: i64,
    #[serde(deserialize_with = "deserialize_urls")]
    pub consensus_nodes: Vec<Url>,
    pub database_url: String,
    /// Skip global checks in `run_ops_monitors` and only check for beacon/sim node status.
    #[serde(default)]
    pub ff_node_check_only: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub geo: Geo,
    pub loki_url: String,
    /// Minimum number of missed slots per check interval to trigger an alert
    #[serde(default = "default_missed_slots_alert_threshold")]
    pub missed_slots_alert_threshold: i64,
    /// Slot range to check for counting missed slots
    #[serde(default = "default_missed_slots_range")]
    pub missed_slots_check_range: i64,
    #[serde(deserialize_with = "deserialize_network")]
    pub network: Network,
    pub opsgenie_api_key: String,
    pub port: u16,
    pub relay_database_url: String,
    pub telegram_api_key: String,
    pub telegram_alerts_channel_id: String,
    pub telegram_block_not_found_channel_id: String,
    pub telegram_demotions_channel_id: String,
    pub telegram_warnings_channel_id: String,
    #[serde(deserialize_with = "deserialize_urls")]
    pub validation_nodes: Vec<Url>,
    #[serde(default = "default_unsynced_nodes_threshold_tg_warning")]
    pub unsynced_nodes_threshold_tg_warning: usize,
    #[serde(default = "default_unsynced_nodes_threshold_og_alert")]
    pub unsynced_nodes_threshold_og_alert: usize,
    #[serde(default = "default_max_auction_analysis_slot_lag")]
    pub max_auction_analysis_slot_lag: u32,
    /// List of builder ids that we allow some extra leniency in demotions
    #[serde(default, deserialize_with = "deserialize_hash_set")]
    pub trusted_builder_ids: HashSet<String>,
}

fn default_max_auction_analysis_slot_lag() -> u32 {
    50
}

fn default_unsynced_nodes_threshold_og_alert() -> usize {
    2
}

fn default_unsynced_nodes_threshold_tg_warning() -> usize {
    1
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

/// Auction geography
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(type_name = "geo")]
pub enum Geo {
    #[sqlx(rename = "rbx")]
    RBX,
    #[sqlx(rename = "vin")]
    VIN,
    #[sqlx(rename = "tyo")]
    TYO,
}

impl fmt::Display for Geo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match &self {
            Geo::RBX => "rbx",
            Geo::VIN => "vin",
            Geo::TYO => "tyo",
        };
        write!(f, "{}", str)
    }
}

impl str::FromStr for Geo {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "rbx" => Ok(Geo::RBX),
            "vin" => Ok(Geo::VIN),
            "tyo" => Ok(Geo::TYO),
            _ => Err(format!("invalid auction geo: {}", s)),
        }
    }
}

pub static APP_CONFIG: LazyLock<AppConfig> = LazyLock::new(get_app_config);

impl AppConfig {
    pub fn relay_analytics_url(&self) -> &str {
        match self.network {
            Network::Mainnet => "https://relay-analytics.ultrasound.money",
            Network::Holesky => "https://relay-analytics-holesky.ultrasound.money",
            Network::Hoodi => "https://relay-analytics-hoodi.ultrasound.money",
        }
    }
}
