use chrono::{DateTime, Duration, Utc};
use lazy_static::lazy_static;

pub struct AppConfig {
    pub db_connection_str: String,
    pub zeromev_connection_str: String,
    pub backfill_until: DateTime<Utc>,
    pub chain_data_interval: Duration,
    pub block_production_interval: Duration,
    pub bigquery_service_account: String,
}

fn get_app_config() -> AppConfig {
    let db_connection_str = std::env::var("DATABASE_URL");
    let zeromev_connection_str = std::env::var("ZEROMEV_DATABASE_URL");
    let backfill_until = std::env::var("BACKFILL_UNTIL");
    let chain_data_interval = std::env::var("CHAIN_DATA_INTERVAL");
    let block_production_interval = std::env::var("BLOCK_PRODUCTION_INTERVAL");
    let bigquery_service_account = std::env::var("BIGQUERY_SERVICE_ACCOUNT");

    if let (
        Ok(db_connection_str),
        Ok(zeromev_connection_str),
        Ok(backfill_until),
        Ok(chain_data_interval),
        Ok(block_production_interval),
        Ok(bigquery_service_account),
    ) = (
        db_connection_str,
        zeromev_connection_str,
        backfill_until,
        chain_data_interval,
        block_production_interval,
        bigquery_service_account,
    ) {
        AppConfig {
            db_connection_str,
            zeromev_connection_str,
            backfill_until: DateTime::parse_from_rfc3339(&backfill_until)
                .unwrap()
                .with_timezone(&Utc),
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
