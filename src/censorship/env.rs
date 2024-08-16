use lazy_static::lazy_static;
use serde::Deserialize;

use crate::env::get_app_config;

#[derive(Deserialize)]
pub struct AppConfig {
    pub port: u16,
    pub database_url: String,
    pub backfill_until_slot: i64,
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
