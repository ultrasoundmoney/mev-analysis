mod db;
mod env;
mod relay;

use anyhow::Result;
use axum::{http::StatusCode, routing::get, Router};
use chrono::{Duration, Utc};
use enum_iterator::all;
use futures::future;
use itertools::Itertools;
use sqlx::{Connection, PgConnection};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process;
use tracing::{error, info, warn};

use crate::log;

use self::db::{CensorshipDB, PostgresCensorshipDB};
use self::env::APP_CONFIG;
use self::relay::{DeliveredPayload, RelayApi};

pub use self::relay::RelayId;

pub async fn start_block_production_ingest() -> Result<()> {
    log::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.database_url).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let db = PostgresCensorshipDB::new().await?;

    tokio::spawn(mount_health_route());

    let result = tokio::try_join!(
        ingest_block_production_data(&db),
        backfill_block_production_data(&db)
    );

    match result {
        Ok(_) => {
            error!("block production data ingestion completed unexpectedly without an error");
            process::exit(1);
        }
        Err(err) => {
            error!("block production data ingestion failed: {}", err);
            process::exit(1);
        }
    }
}

/*
   The relay api has no way of providing both start and end slot for the payload request.
   This makes it difficult to deterministically fetch all the payloads for an interval.
   Instead we poll "often enough" and upsert the data into the db.

   The lowest max is bloxroute with 100. In the unlikely event that one
   relay relays every single block for 100 slots straight, the time interval for the response
   will be 100 * 12 seconds = 20 minutes. So theoretically as long as we poll more often than
   every 20 minutes we should be fine.
*/
async fn ingest_block_production_data(db: &impl CensorshipDB) -> Result<()> {
    let fetch_interval = Duration::minutes(5).to_std().unwrap();
    let relay_count = all::<RelayId>().count();

    loop {
        let begin = Utc::now();

        info!("fetching delivered payloads from {} relays", &relay_count);

        let payloads = fetch_block_production_batch(&None).await?;
        let interval = get_fully_traversed_interval(payloads);

        db.upsert_delivered_payloads(interval).await?;

        info!(
            "persisted delivered payloads in {} seconds",
            (Utc::now() - begin).num_seconds()
        );

        tokio::time::sleep(fetch_interval).await;
    }
}

/*
  Beacause the relay API only takes an end slot as cursor, we need to crawl
  the data backwards like so:
  1. Fetch 100 payloads with end slot_number `s` from each relay
  2. Find the lowest slot_number in the response for each relay
  3. Find the highest slot_number out of those
  4. Repeat with cursor `s` set to that number
*/

async fn backfill_block_production_data(db: &impl CensorshipDB) -> Result<()> {
    let goal = APP_CONFIG.backfill_until_slot;
    loop {
        let checkpoint = db.get_block_production_checkpoint().await?;

        match checkpoint {
            Some(slot_number) if slot_number <= goal => {
                info!(
                    "block production backfill reached slot {}, goal was {}. exiting",
                    slot_number, goal
                );
                break;
            }
            slot_option => {
                if slot_option.is_none() {
                    info!("backfilling block production from now until slot {}", goal);
                }
                if let Some(slot_number) = slot_option {
                    info!("backfilling block production from slot {}", slot_number);
                }

                let result = fetch_block_production_batch(&slot_option).await;

                match result {
                    Ok(payloads) => {
                        let interval = get_fully_traversed_interval(payloads);
                        db.upsert_delivered_payloads(interval).await?;
                    }
                    Err(err) => {
                        warn!("failed fetching block production data, retrying: {}", err);
                    }
                }
            }
        }

        // avoid rate-limits
        tokio::time::sleep(Duration::seconds(1).to_std().unwrap()).await;
    }

    Ok(())
}

pub async fn patch_block_production_interval(start_slot: i64, end_slot: i64) -> Result<()> {
    log::init();

    let db = PostgresCensorshipDB::new().await?;
    let mut checkpoints: HashMap<RelayId, i64> = all::<RelayId>()
        .map(|relay| (relay, end_slot + 1))
        .collect();

    info!(
        "patching block production data from slot {} to {}",
        start_slot, end_slot
    );

    loop {
        if checkpoints.is_empty() {
            info!("block production patch completed");
            break;
        }

        info!("{:?}", &checkpoints);

        let futs = checkpoints.iter().map(|(relay, checkpoint)| async move {
            let mut relay_payloads = relay
                .fetch_delivered_payloads(&Some(*checkpoint - 1))
                .await?;
            relay_payloads.sort_by(|a, b| b.slot_number.cmp(&a.slot_number));

            info!("fetched {} payloads from {}", relay_payloads.len(), &relay);

            Ok::<_, anyhow::Error>((relay.clone(), relay_payloads))
        });

        let all_payloads = future::try_join_all(futs).await?;

        for (relay, payloads) in &all_payloads {
            match payloads.last() {
                Some(payload) if payload.slot_number <= start_slot => {
                    info!(
                        "reached start slot {} for {}, removing from checkpoints",
                        start_slot, &relay
                    );
                    checkpoints.remove(relay);
                }
                Some(payload) => {
                    checkpoints.insert(relay.clone(), payload.slot_number);
                }
                None => {
                    let lowest_slot = checkpoints.get(relay).unwrap();
                    info!("reached lowest slot {} for {}", lowest_slot, &relay);
                    checkpoints.remove(relay);
                }
            }
        }

        db.upsert_delivered_payloads(
            all_payloads
                .into_iter()
                .flat_map(|(_, payloads)| payloads)
                .collect_vec(),
        )
        .await?;
    }

    Ok(())
}

type BlockProductionBatch = Vec<(RelayId, Vec<DeliveredPayload>)>;

async fn fetch_block_production_batch(end_slot: &Option<i64>) -> Result<BlockProductionBatch> {
    let futs = all::<RelayId>()
        .map(|relay| async move {
            let mut payloads = relay.fetch_delivered_payloads(end_slot).await?;

            // Payloads should be sorted descending by default, just making sure
            // since we rely on that assumption
            payloads.sort_by(|a, b| b.slot_number.cmp(&a.slot_number));

            Ok::<_, anyhow::Error>((relay, payloads))
        })
        .collect_vec();

    future::try_join_all(futs).await
}

/*
  When asking each relay for a 100 payloads, we're going to cover a different time window
  for each. This function filters payloads from all relays to match the shortest window covered
  so we can correctly set the checkpoint.
*/
fn get_fully_traversed_interval(batch: BlockProductionBatch) -> Vec<DeliveredPayload> {
    let highest_end_slot = &batch
        .iter()
        .filter_map(|(_, payloads)| payloads.last().map(|p| p.slot_number))
        .sorted()
        .next_back()
        .expect("at least one payload should be present");

    batch
        .into_iter()
        .flat_map(|(_, payloads)| payloads)
        .filter(|payload| payload.slot_number >= *highest_end_slot)
        .collect_vec()
}

async fn mount_health_route() {
    let addr = SocketAddr::from(([0, 0, 0, 0], APP_CONFIG.port));
    let app = Router::new().route("/", get(|| async { StatusCode::OK }));

    info!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
