mod chain;
mod db;
mod env;
mod mempool;
mod relay;

use anyhow::Result;
use axum::{http::StatusCode, routing::get, Router};
use chrono::{Duration, Utc};
use enum_iterator::all;
use futures::future;
use gcp_bigquery_client::Client;
use itertools::Itertools;
use sqlx::{Connection, PgConnection};
use std::net::SocketAddr;
use std::ops::Add;
use std::process;
use tracing::{error, info, warn};

use self::db::{CensorshipDB, PostgresCensorshipDB};
use self::env::APP_CONFIG;
use self::relay::{DeliveredPayload, RelayApi, RelayId};
use self::{
    chain::ChainStore,
    mempool::{MempoolStore, ZeroMev},
};

async fn ingest_chain_data(
    db: &impl CensorshipDB,
    chain_store: &mut impl ChainStore,
    mempool_store: &impl MempoolStore,
) -> Result<()> {
    let fetch_interval = APP_CONFIG.chain_data_interval;

    loop {
        let checkpoint = db
            .get_chain_checkpoint()
            .await?
            .unwrap_or(APP_CONFIG.backfill_until);

        let begin = Utc::now();
        let is_backfilling = begin - checkpoint > fetch_interval;

        let start_time = checkpoint;
        let end_time = start_time.add(Duration::hours(3));

        info!(
            "starting chain data ingestion from {} until {}, interval: {} minutes",
            start_time,
            end_time,
            fetch_interval.num_minutes()
        );

        let blocks = chain_store.fetch_blocks(&start_time, &end_time).await?;
        let txs = chain_store.fetch_txs(&start_time, &end_time).await?;

        let block_count = blocks.len();
        let tx_count = txs.len();

        info!("received {} blocks and {} txs", &block_count, &tx_count);

        let timestamped_txs = mempool_store.fetch_tx_timestamps(txs).await?;

        db.put_chain_data(blocks, timestamped_txs).await?;

        info!(
            "persisted chain data in {} seconds",
            (Utc::now() - begin).num_seconds()
        );

        if !is_backfilling {
            info!(
                "reached current time, sleeping for {} minutes",
                APP_CONFIG.chain_data_interval.num_minutes()
            );
            tokio::time::sleep(fetch_interval.to_std().unwrap()).await;
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

        let payloads = fetch_block_production_batch(&None)
            .await?
            .into_iter()
            .flat_map(|(_, ps)| ps)
            .collect_vec();

        db.upsert_delivered_payloads(payloads).await?;

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
                info!("block production backfill reached goal, exiting");
                break;
            }
            slot_option => {
                if let None = slot_option {
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

async fn mount_health_route() {
    let addr = SocketAddr::from(([0, 0, 0, 0], APP_CONFIG.port));
    let app = Router::new().route("/", get(|| async { StatusCode::OK }));

    info!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

pub async fn start_ingestion() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.database_url).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let db = PostgresCensorshipDB::new().await?;
    let mempool_store = ZeroMev::new().await;
    let mut chain_store =
        Client::from_service_account_key_file(&APP_CONFIG.bigquery_service_account).await?;

    tokio::spawn(mount_health_route());

    let result = tokio::try_join!(
        ingest_chain_data(&db, &mut chain_store, &mempool_store),
        ingest_block_production_data(&db),
        backfill_block_production_data(&db)
    );

    match result {
        Ok(_) => {
            error!("ingestion task(s) completed unexpectedly without an error");
            process::exit(1);
        }
        Err(err) => {
            error!("ingestion task(s) failed: {}", err);
            process::exit(1);
        }
    }
}

type BlockProductionBatch = Vec<(RelayId, Vec<DeliveredPayload>)>;

async fn fetch_block_production_batch(end_slot: &Option<i64>) -> Result<BlockProductionBatch> {
    let futs = all::<RelayId>()
        .map(|relay| async move {
            let payloads = relay.fetch_delivered_payloads(end_slot).await?;
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
        .last()
        .expect("at least one payload should be present");

    batch
        .into_iter()
        .flat_map(|(_, payloads)| payloads)
        .filter(|payload| payload.slot_number >= *highest_end_slot)
        .collect_vec()
}
