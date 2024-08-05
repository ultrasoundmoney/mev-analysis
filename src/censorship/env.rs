use chrono::{DateTime, Duration, Utc};
use lazy_static::lazy_static;
use serde::Deserialize;

use crate::env::{deserialize_duration_minutes, get_app_config};

#[derive(Deserialize)]
pub struct AppConfig {
    pub port: u16,
    pub database_url: String,
    pub zeromev_database_url: String,
    pub bigquery_service_account: String,
    pub infura_api_key: String,
    pub backfill_until: DateTime<Utc>,
    pub backfill_until_slot: i64,
    #[serde(deserialize_with = "deserialize_duration_minutes")]
    pub chain_data_interval: Duration,
    #[serde(deserialize_with = "deserialize_duration_minutes")]
    pub chain_data_batch_size: Duration,
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
