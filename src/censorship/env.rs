use std::sync::LazyLock;

use serde::Deserialize;

use crate::env::get_app_config;

#[derive(Deserialize)]
pub struct AppConfig {
    pub port: u16,
    pub database_url: String,
    pub backfill_until_slot: i64,
}

pub static APP_CONFIG: LazyLock<AppConfig> = LazyLock::new(get_app_config);
