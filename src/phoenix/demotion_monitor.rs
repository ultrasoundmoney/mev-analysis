use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use sqlx::{PgPool, Row};
use tracing::info;

use crate::env::{ToBeaconExplorerUrl, ToNetwork};

use super::{
    alert,
    checkpoint::{self, CheckpointId},
    env::APP_CONFIG,
};

#[derive(Debug, Clone)]
pub struct BuilderDemotion {
    pub inserted_at: DateTime<Utc>,
    pub builder_pubkey: String,
    pub builder_id: Option<String>,
    pub slot: i64,
    pub sim_error: String,
}

pub async fn get_builder_demotions(
    relay_pool: &PgPool,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Result<Vec<BuilderDemotion>> {
    let query = format!(
        "
        SELECT
            bd.inserted_at,
            bd.builder_pubkey,
            bb.builder_id,
            bd.slot,
            bd.sim_error
        FROM {network}_builder_demotions bd
        INNER JOIN {network}_blockbuilder bb
          ON bd.builder_pubkey = bb.builder_pubkey
        WHERE bd.inserted_at > $1
          AND bd.inserted_at <= $2
        ORDER BY bd.inserted_at ASC
     ",
        network = &APP_CONFIG.env.to_network().to_string()
    );

    sqlx::query(&query)
        .bind(start)
        .bind(end)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| BuilderDemotion {
                    inserted_at: Utc.from_utc_datetime(&row.get("inserted_at")),
                    builder_pubkey: row.get("builder_pubkey"),
                    builder_id: row.try_get("builder_id").ok(),
                    slot: row.get("slot"),
                    sim_error: row.get::<String, _>("sim_error").trim().to_string(),
                })
                .collect()
        })
        .map_err(Into::into)
}

pub async fn run_demotion_monitor(relay_pool: &PgPool, mev_pool: &PgPool) -> Result<()> {
    let checkpoint = match checkpoint::get_checkpoint(&mev_pool, CheckpointId::Demotion).await? {
        Some(c) => c,
        None => {
            info!("no checkpoint found, initializing");
            let now = Utc::now();
            checkpoint::put_checkpoint(&mev_pool, CheckpointId::Demotion, &now).await?;
            now
        }
    };

    let now = Utc::now();

    info!("checking demotions between {} and {}", &checkpoint, &now);

    let demotions = get_builder_demotions(&relay_pool, &checkpoint, &now).await?;
    // reduce alert noise by filtering out duplicate demotions
    let unique_demotions = demotions
        .iter()
        .unique_by(|d| format!("{}{}{}", d.builder_pubkey, d.slot, d.sim_error))
        .collect_vec();

    for demotion in &unique_demotions {
        let message = format!(
            "*{name}* `{pubkey}` was demoted during slot [{slot}]({url}/slot/{slot}) with the following error:\n\n{error}",
            name = demotion.builder_id.clone().unwrap_or("unknown builder_id".to_string()),
            pubkey = demotion.builder_pubkey,
            slot = demotion.slot,
            url = &APP_CONFIG.env.to_beacon_explorer_url(),
            error = demotion.sim_error
        );
        info!("{}", &message);
        alert::send_telegram_alert(&message).await?;
    }

    checkpoint::put_checkpoint(&mev_pool, CheckpointId::Demotion, &now).await?;

    Ok(())
}
