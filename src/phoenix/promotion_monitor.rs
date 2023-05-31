use anyhow::Result;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use sqlx::PgPool;
use tracing::info;

use super::{
    alert,
    checkpoint::{self, CheckpointId},
    demotion_monitor::{get_builder_demotions, BuilderDemotion},
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

#[derive(Debug, PartialEq)]
struct BuilderPromotion {
    builder_pubkey: String,
    builder_description: String,
}

fn get_eligible_builders(
    demotions: Vec<BuilderDemotion>,
    missed_slots: Vec<i64>,
) -> Vec<BuilderPromotion> {
    let eligible_errors = vec![
        "json error: request timeout hit before processing",
        "simulation failed: unknown ancestor",
    ];
    demotions
        .into_iter()
        .group_by(|d| d.builder_pubkey.clone())
        .into_iter()
        .filter_map(|(pubkey, group)| {
            let demotions = group.collect_vec();
            let no_missed_slots = demotions.iter().all(|d| !missed_slots.contains(&d.slot));
            let only_eligible_errors = demotions
                .iter()
                .all(|d| eligible_errors.contains(&d.sim_error.as_str()));

            if no_missed_slots && only_eligible_errors {
                Some(BuilderPromotion {
                    builder_pubkey: pubkey,
                    builder_description: demotions[0].builder_description.clone(),
                })
            } else {
                None
            }
        })
        .collect()
}

pub async fn run_promotion_monitor(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    canonical_horizon: &DateTime<Utc>,
) -> Result<()> {
    let checkpoint = match checkpoint::get_checkpoint(&mev_pool, CheckpointId::Promotion).await? {
        Some(c) => c,
        None => {
            info!("no checkpoint found, initializing");
            let now = Utc::now();
            checkpoint::put_checkpoint(&mev_pool, CheckpointId::Promotion, &now).await?;
            now
        }
    };

    info!(
        "checking promotions between {} and {}",
        &checkpoint, &canonical_horizon
    );

    let demotions = get_builder_demotions(&relay_pool, &checkpoint, canonical_horizon).await?;
    let missed_slots = get_missed_slots(&mev_pool, &checkpoint).await?;
    let eligible = get_eligible_builders(demotions, missed_slots);

    if !eligible.is_empty() {
        info!("found {} builders eligible for promotion", &eligible.len());

        let builder_list = eligible
            .iter()
            .map(|b| format!("*{}* `{}`", b.builder_description, b.builder_pubkey))
            .collect_vec()
            .join("\n");

        let message = format!("promoted the following builders:\n\n{}", builder_list);

        alert::send_telegram_alert(&message).await?;
    }

    checkpoint::put_checkpoint(&mev_pool, CheckpointId::Promotion, canonical_horizon).await?;

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
                builder_description: "builder1".to_string(),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_description: "builder2".to_string(),
            },
        ];
        let missed_slots = vec![];

        let result = get_eligible_builders(demotions, missed_slots);
        assert_eq!(
            result,
            vec![
                BuilderPromotion {
                    builder_pubkey: "pubkey1".to_string(),
                    builder_description: "builder1".to_string(),
                },
                BuilderPromotion {
                    builder_pubkey: "pubkey2".to_string(),
                    builder_description: "builder2".to_string(),
                }
            ]
        );
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
                builder_description: "builder1".to_string(),
            },
            BuilderDemotion {
                inserted_at,
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_description: "builder2".to_string(),
            },
        ];
        let missed_slots = vec![2];

        let result = get_eligible_builders(demotions, missed_slots);

        assert_eq!(result, vec![]);
    }
}
