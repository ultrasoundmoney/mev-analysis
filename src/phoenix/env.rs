use std::{fmt, str};

use lazy_static::lazy_static;
use reqwest::Url;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};

use crate::env::{deserialize_network, deserialize_urls, get_app_config, Env, Network};

#[serde_as]
#[derive(Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_wait")]
    pub canonical_wait_minutes: i64,
    #[serde(deserialize_with = "deserialize_urls")]
    pub consensus_nodes: Vec<Url>,
    pub database_url: String,
    pub env: Env,
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
    pub telegram_warnings_channel_id: String,
    #[serde(deserialize_with = "deserialize_urls")]
    pub validation_nodes: Vec<Url>,
    #[serde(default = "default_unsynced_nodes_threshold_tg_warning")]
    pub unsynced_nodes_threshold_tg_warning: usize,
    #[serde(default = "default_unsynced_nodes_threshold_og_alert")]
    pub unsynced_nodes_threshold_og_alert: usize,
    #[serde(default = "default_max_auction_analysis_slot_lag")]
    pub max_auction_analysis_slot_lag: u32,
    #[serde(default = "default_max_header_delay_updates_slot_lag")]
    pub max_header_delay_updates_slot_lag: u32,
    #[serde(default = "default_max_lookback_updates_slot_lag")]
    pub max_lookback_updates_slot_lag: u32,
}

fn default_max_lookback_updates_slot_lag() -> u32 {
    // Note that this is more heuristic: since theoretically there could just be no updates because
    // of the way the update algorithm works
    600
}

fn default_max_header_delay_updates_slot_lag() -> u32 {
    // Should be configured considering the configured schedule for the update cron job
    60
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
pub enum Geo {
    RBX,
    VIN,
}

impl fmt::Display for Geo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match &self {
            Geo::RBX => "rbx",
            Geo::VIN => "vin",
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
            _ => Err(format!("invalid auction geo: {}", s)),
        }
    }
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
