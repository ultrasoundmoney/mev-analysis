use std::sync::LazyLock;

use reqwest::Url;
use serde::Deserialize;

use crate::env::{deserialize_urls, get_app_config, Env};

#[derive(Deserialize)]
pub struct AppConfig {
    pub env: Env,
    pub port: u16,
    pub database_url: String,
    pub global_database_url: String,
    pub redis_uri: String,
    #[serde(deserialize_with = "deserialize_urls")]
    pub consensus_nodes: Vec<Url>,
}

pub static APP_CONFIG: LazyLock<AppConfig> = LazyLock::new(get_app_config);
