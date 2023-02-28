use chrono::{DateTime, Duration, Utc};
use lazy_static::lazy_static;

pub struct AppConfig {
    pub port: u16,
    pub database_url: String,
    pub zeromev_database_url: String,
    pub bigquery_service_account: String,
    pub backfill_until: DateTime<Utc>,
    pub backfill_until_slot: i64,
    pub chain_data_interval: Duration,
    pub block_production_interval: Duration,
}

fn get_app_config() -> AppConfig {
    let port = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(3002);
    let database_url = std::env::var("DATABASE_URL");
    let zeromev_database_url = std::env::var("ZEROMEV_DATABASE_URL");
    let backfill_until = std::env::var("BACKFILL_UNTIL");
    let backfill_until_slot = std::env::var("BACKFILL_UNTIL_SLOT");
    let chain_data_interval = std::env::var("CHAIN_DATA_INTERVAL");
    let block_production_interval = std::env::var("BLOCK_PRODUCTION_INTERVAL");
    let bigquery_service_account = std::env::var("BIGQUERY_SERVICE_ACCOUNT");

    if let (
        Ok(database_url),
        Ok(zeromev_database_url),
        Ok(backfill_until),
        Ok(backfill_until_slot),
        Ok(chain_data_interval),
        Ok(block_production_interval),
        Ok(bigquery_service_account),
    ) = (
        database_url,
        zeromev_database_url,
        backfill_until,
        backfill_until_slot,
        chain_data_interval,
        block_production_interval,
        bigquery_service_account,
    ) {
        AppConfig {
            port,
            database_url,
            zeromev_database_url,
            backfill_until: DateTime::parse_from_rfc3339(&backfill_until)
                .unwrap()
                .with_timezone(&Utc),
            backfill_until_slot: backfill_until_slot.parse().unwrap(),
            chain_data_interval: Duration::minutes(chain_data_interval.parse().unwrap()),
            block_production_interval: Duration::minutes(
                block_production_interval.parse().unwrap(),
            ),
            bigquery_service_account,
        }
    } else {
        panic!("missing environment variable(s)");
    }
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
