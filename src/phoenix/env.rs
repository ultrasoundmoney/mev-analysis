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
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
