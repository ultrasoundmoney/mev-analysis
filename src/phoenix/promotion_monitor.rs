use anyhow::Result;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

use crate::env::ToNetwork;

use super::{
    checkpoint::{self, CheckpointId},
    demotion_monitor::{get_builder_demotions, BuilderDemotion},
    env::APP_CONFIG,
};

async fn get_missed_slots(mev_pool: &PgPool, start: &DateTime<Utc>) -> Result<Vec<i64>> {
    sqlx::query_scalar!(
        r#"
        SELECT slot_number
        FROM missed_slots
        WHERE inserted_at > $1
        "#,
        start,
    )
    .fetch_all(mev_pool)
    .await
    .map_err(Into::into)
}

async fn promote_builder_ids(
    relay_pool: &PgPool,
    builder_ids: &Vec<String>,
) -> Result<Vec<(String, String)>> {
    let query = format!(
        "
        UPDATE {}_blockbuilder
        SET is_optimistic = true
        WHERE
            builder_id = ANY($1)
        AND collateral > 0
        AND is_optimistic = false
        RETURNING builder_id, builder_pubkey
        ",
        APP_CONFIG.env.to_network()
    );

    sqlx::query(&query)
        .bind(builder_ids)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| (row.get("builder_id"), row.get("builder_pubkey")))
                .collect_vec()
        })
        .map_err(Into::into)
}

#[allow(dead_code, clippy::ptr_arg)]
fn format_builder_list(builders: &Vec<(String, String)>) -> String {
    builders
        .iter()
        .sorted_by_key(|(id, _)| id)
        .group_by(|(id, _)| id.clone())
        .into_iter()
        .fold(String::new(), |acc, (id, group)| {
            let pubkeys = group
                .map(|(_, pubkey)| format!("`{}`", &pubkey))
                .collect_vec()
                .join("\n");

            if acc.is_empty() {
                format!("*{}*\n{}", id, pubkeys)
            } else {
                format!("{}\n\n*{}*\n{}", acc, id, pubkeys)
            }
        })
}

/// Demotion errors eligible for re-promotion if no slot was missed
const PROMOTABLE_ERRORS: &[&str] = &[
    "HTTP status server error (500 Internal Server Error) for url (http://prio-load-balancer/)",
    "Post \"http://prio-load-balancer:80\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
    "json error: request timeout hit before processing",
    "simulation failed: unknown ancestor",
    "simulation failed: incorrect gas limit set"
];

pub fn is_promotable_error(error: &str) -> bool {
    PROMOTABLE_ERRORS
        .iter()
        // Use starts_with to account for additional info in gas limit error
        .any(|promotable_error| error.starts_with(promotable_error))
}

fn get_eligible_builders(demotions: Vec<BuilderDemotion>, missed_slots: Vec<i64>) -> Vec<String> {
    debug!(
        "get_eligible_builders: demotions: {:?}, missed_slots {:?}",
        &demotions, &missed_slots
    );
    demotions
        .into_iter()
        .sorted_by_key(|d| d.builder_id.clone())
        .group_by(|d| d.builder_id.clone())
        .into_iter()
        .filter_map(|(builder_id, group)| match builder_id {
            None => {
                warn!(
                    "{} pubkeys without builder_id, unable to promote",
                    &group.count()
                );
                None
            }
            Some(builder_id) => {
                let demotions = group.collect_vec();
                debug!("grouped demotions for {}: {:?}", &builder_id, &demotions);
                let no_missed_slots = demotions.iter().all(|d| !missed_slots.contains(&d.slot));
                let only_eligible_errors = demotions
                    .iter()
                    .all(|d| is_promotable_error(d.sim_error.as_str()));

                if no_missed_slots && only_eligible_errors {
                    Some(builder_id)
                } else {
                    None
                }
            }
        })
        .collect()
}

pub async fn run_promotion_monitor(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    canonical_horizon: &DateTime<Utc>,
) -> Result<()> {
    let checkpoint = match checkpoint::get_checkpoint(mev_pool, CheckpointId::Promotion).await? {
        Some(c) => c,
        None => {
            info!("no checkpoint found, initializing");
            let now = Utc::now();
            checkpoint::put_checkpoint(mev_pool, CheckpointId::Promotion, &now).await?;
            now
        }
    };

    debug!(
        "checking promotions between {} and {}",
        &checkpoint, &canonical_horizon
    );

    let demotions = get_builder_demotions(relay_pool, &checkpoint, canonical_horizon).await?;
    let missed_slots = get_missed_slots(mev_pool, &checkpoint).await?;
    let eligible = get_eligible_builders(demotions, missed_slots);

    if !eligible.is_empty() {
        info!("found builder ids eligible for promotion: {:?}", &eligible);

        let _promoted = promote_builder_ids(relay_pool, &eligible).await?;
    }

    checkpoint::put_checkpoint(mev_pool, CheckpointId::Promotion, canonical_horizon).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_get_eligible_builders_all_eligible() {
        let inserted_at = Utc::now();
        let demotions = vec![
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "json error: request timeout hit before processing".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![];

        let result = get_eligible_builders(demotions, missed_slots);
        assert_eq!(result, vec!["builder1".to_string(), "builder2".to_string()]);
    }

    #[test]
    fn test_get_eligible_builders_none_eligible() {
        let inserted_at = Utc::now();
        let demotions = vec![
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "invalid error".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![2];

        let result = get_eligible_builders(demotions, missed_slots);

        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn test_get_eligible_builders_some_eligible() {
        let inserted_at = Utc::now();
        let demotions = vec![
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "json error: request timeout hit before processing".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "invalid error".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 3,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![2];

        let result = get_eligible_builders(demotions, missed_slots);

        assert_eq!(result, vec!["builder1".to_string()]);
    }

    #[test]
    fn test_same_slot_both_valid_and_invalid() {
        let inserted_at = Utc::now();
        let demotions = vec![
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "invalid error".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "json error: request timeout hit before processing".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![];

        let result = get_eligible_builders(demotions, missed_slots);

        assert_eq!(result, vec!["builder1".to_string()]);
    }

    #[test]
    fn test_format_builder_list() {
        let builder_ids = vec![
            ("builder1".to_string(), "pkey1".to_string()),
            ("builder2".to_string(), "pkey2".to_string()),
            ("builder1".to_string(), "pkey3".to_string()),
        ];
        let result = format_builder_list(&builder_ids);
        assert_eq!(
            result,
            "*builder1*\n`pkey1`\n`pkey3`\n\n*builder2*\n`pkey2`"
        );
    }
}
