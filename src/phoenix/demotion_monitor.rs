use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use itertools::Itertools;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tracing::info;

use crate::{
    env::{ToBeaconExplorerUrl, ToNetwork},
    phoenix::alert,
};

use super::env::APP_CONFIG;

#[derive(Debug)]
struct BuilderDemotion {
    inserted_at: DateTime<Utc>,
    builder_pubkey: String,
    builder_description: String,
    slot: i64,
    sim_error: String,
}

async fn get_checkpoint(mev_pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    sqlx::query_scalar!(
        r#"
        SELECT timestamp
        FROM monitor_checkpoints
        WHERE monitor_id = 'demotion_monitor'
        LIMIT 1
        "#
    )
    .fetch_optional(mev_pool)
    .await
    .map_err(Into::into)
}

async fn put_checkpoint(mev_pool: &PgPool, checkpoint: &DateTime<Utc>) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO monitor_checkpoints (monitor_id, timestamp)
        VALUES ('demotion_monitor', $1)
        ON CONFLICT (monitor_id) DO UPDATE SET timestamp = $1
        "#,
        checkpoint
    )
    .execute(mev_pool)
    .await
    .map(|_| ())
    .map_err(Into::into)
}

async fn get_builder_demotions(
    relay_pool: &PgPool,
    checkpoint: &DateTime<Utc>,
) -> Result<Vec<BuilderDemotion>> {
    let query = format!(
        "
        SELECT
            bd.inserted_at,
            bd.builder_pubkey,
            bb.description,
            bd.slot,
            bd.sim_error
        FROM {network}_builder_demotions bd
        INNER JOIN {network}_blockbuilder bb
          ON bd.builder_pubkey = bb.builder_pubkey
        WHERE bd.inserted_at > $1
        ORDER BY bd.inserted_at ASC
     ",
        network = &APP_CONFIG.env.to_network().to_string()
    );

    sqlx::query(&query)
        .bind(checkpoint)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| BuilderDemotion {
                    inserted_at: Utc.from_utc_datetime(&row.get("inserted_at")),
                    builder_pubkey: row.get("builder_pubkey"),
                    builder_description: row
                        .try_get("description")
                        .unwrap_or("unknown builder".to_string()),
                    slot: row.get("slot"),
                    sim_error: row.get("sim_error"),
                })
                .collect()
        })
        .map_err(Into::into)
}

pub async fn start_demotion_monitor() -> Result<()> {
    let relay_pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::seconds(3).to_std()?)
        .connect(&APP_CONFIG.relay_database_url)
        .await?;
    let mev_pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::seconds(3).to_std()?)
        .connect(&APP_CONFIG.database_url)
        .await?;

    loop {
        let checkpoint = match get_checkpoint(&mev_pool).await? {
            Some(c) => c,
            None => {
                info!("no checkpoint found, initializing");
                let now = Utc::now();
                put_checkpoint(&mev_pool, &now).await?;
                now
            }
        };

        info!("checking demotions since {}", checkpoint);

        let demotions = get_builder_demotions(&relay_pool, &checkpoint).await?;
        // reduce alert noise by filtering out duplicate demotions
        let unique_demotions = demotions
            .iter()
            .unique_by(|d| format!("{}{}{}", d.builder_pubkey, d.slot, d.sim_error))
            .collect_vec();

        for demotion in &unique_demotions {
            let message = format!(
                "*{name}* `{pubkey}` was demoted during slot [{slot}]({url}/slot/{slot}) with the following error:\n\n{error}",
                name = demotion.builder_description,
                pubkey = demotion.builder_pubkey,
                slot = demotion.slot,
                url = &APP_CONFIG.env.to_beacon_explorer_url(),
                error = demotion.sim_error
            );
            info!("{}", &message);
            alert::send_telegram_alert(&message).await?;
        }

        let new_checkpoint = &demotions.last().map(|d| d.inserted_at);

        if let Some(new) = new_checkpoint {
            info!("updating checkpoint to {}", new);
            put_checkpoint(&mev_pool, &new).await?;
        }

        tokio::time::sleep(Duration::minutes(1).to_std()?).await;
    }
}
