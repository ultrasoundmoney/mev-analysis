use chrono::Duration;
use sqlx::{postgres::PgPoolOptions, Row};
use tracing::{error, info};

use crate::beacon_api::BeaconAPI;

use super::env::APP_CONFIG;

#[derive(Debug)]
#[allow(dead_code)]
struct DeliveredPayload {
    slot: i64,
    block_hash: String,
}

#[allow(dead_code)]
pub async fn check_delivered_payloads() {
    let db_pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::seconds(3).to_std().unwrap())
        .connect(&APP_CONFIG.db_connection_str)
        .await
        .expect("can't connect to database");

    let beacon_api = BeaconAPI::new(&APP_CONFIG.consensus_nodes);

    let query = format!(
        "
            SELECT
                slot,
                block_hash
            FROM {}_payload_delivered
            ORDER BY slot DESC
            LIMIT 5
        ",
        &APP_CONFIG.network.to_string()
    );

    let payloads: Vec<DeliveredPayload> = sqlx::query(&query)
        .fetch_all(&db_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| DeliveredPayload {
                    slot: row.get("slot"),
                    block_hash: row.get("block_hash"),
                })
                .collect()
        })
        .unwrap();

    for payload in payloads {
        let block_hash = beacon_api.get_block_hash(&payload.slot).await.unwrap();

        if payload.block_hash == block_hash {
            info!("found matching block hash for slot {}", payload.slot);
        } else {
            error!("block hash mismatch for slot {}", payload.slot);
        }
    }
}
