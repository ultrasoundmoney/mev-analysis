use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use indoc::formatdoc;
use itertools::Itertools;
use sqlx::{PgPool, Row};
use tracing::{error, info};

use crate::{
    env::{ToBeaconExplorerUrl, ToNetwork},
    phoenix::telegram::{self, escape_code_block, telegram_escape},
};

use super::{
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

/// Demotion errors that shouldn't be broadcast on telegram
pub const SILENT_ERRORS: &[&str] = &[
    "HTTP status server error (500 Internal Server Error) for url (http://prio-load-balancer/)",
    "Post \"http://prio-load-balancer:80\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
    "json error: request timeout hit before processing",
    "simulation failed: unknown ancestor",
];

pub async fn run_demotion_monitor(relay_pool: &PgPool, mev_pool: &PgPool) -> Result<()> {
    let explorer_url = APP_CONFIG.env.to_beacon_explorer_url();
    let checkpoint = match checkpoint::get_checkpoint(mev_pool, CheckpointId::Demotion).await? {
        Some(c) => c,
        None => {
            info!("no checkpoint found, initializing");
            let now = Utc::now();
            checkpoint::put_checkpoint(mev_pool, CheckpointId::Demotion, &now).await?;
            now
        }
    };

    let now = Utc::now();

    info!("checking demotions between {} and {}", &checkpoint, &now);

    let demotions = get_builder_demotions(relay_pool, &checkpoint, &now)
        .await?
        .into_iter()
        // reduce alert noise by filtering out duplicate demotions and auto-promotable ones
        .unique_by(|d| format!("{}{}{}", d.builder_pubkey, d.slot, d.sim_error))
        .filter(|d| !SILENT_ERRORS.contains(&d.sim_error.as_str()))
        .collect_vec();

    if !demotions.is_empty() {
        let message = {
            let header = "*builder demoted*";
            let demotion_messages: Vec<String> = demotions
                .into_iter()
                .map(|demotion| {
                    let builder_id = {
                        let builder_id =
                            demotion.builder_id.unwrap_or_else(|| "unknown".to_string());
                        telegram_escape(&builder_id)
                    };
                    let builder_pubkey = demotion.builder_pubkey;
                    let error = escape_code_block(&demotion.sim_error);
                    let slot = demotion.slot;
                    formatdoc!(
                        "
                        [beaconcha\\.in/slot/{slot}]({explorer_url}/slot/{slot})
                        slot: `{slot}`
                        builder\\_id: `{builder_id}`
                        builder\\_pubkey: `{builder_pubkey}`
                        ```
                        {error}
                        ```
                        "
                    )
                })
                .collect();

            format!("{}\n\n{}", header, demotion_messages.join("\n\n"))
        };

        info!(?message, "sending telegram alert");

        if let Err(error) = telegram::send_telegram_alert(&message).await {
            error!(?error, ?message, "failed to send telegram alert");
            telegram::send_telegram_alert(
                "there were builder demotions, but the telegram alert failed",
            )
            .await?;
        }
    }

    checkpoint::put_checkpoint(mev_pool, CheckpointId::Demotion, &now).await?;

    Ok(())
}
