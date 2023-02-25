mod chain_store;
mod db;
mod env;
mod relay;
mod timestamp_service;

use anyhow::Result;
use chrono::{Duration, Utc};
use enum_iterator::all;
use futures::future;
use gcp_bigquery_client::Client;
use itertools::Itertools;
use sqlx::{Connection, PgConnection};
use std::ops::Add;
use tracing::{error, info};

use self::db::{CensorshipDB, PostgresCensorshipDB};
use self::env::APP_CONFIG;
use self::relay::{RelayApi, RelayId};
use self::{
    chain_store::ChainStore,
    timestamp_service::{TimestampService, ZeroMev},
};

async fn ingest_chain_data() -> Result<()> {
    let chain_store =
        Client::from_service_account_key_file("ultra-sound-relay-ae2ce601f320.json").await?;
    let ts_service = ZeroMev::new().await;
    let db = PostgresCensorshipDB::new().await?;
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

        let (blocks_result, txs_result) = tokio::join!(
            chain_store.fetch_blocks(&start_time, &end_time),
            chain_store.fetch_txs(&start_time, &end_time)
        );
        let blocks = blocks_result?;
        let txs = txs_result?;

        let block_count = blocks.len();
        let tx_count = txs.len();

        info!("received {} blocks and {} txs", &block_count, &tx_count);

        let timestamped_txs = ts_service.fetch_tx_timestamps(txs).await?;

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
async fn ingest_block_production_data() -> Result<()> {
    let db = PostgresCensorshipDB::new().await?;
    let fetch_interval = Duration::minutes(5).to_std().unwrap();
    let relay_count = all::<RelayId>().count();

    loop {
        let begin = Utc::now();

        info!("fetching delivered payloads from {} relays", &relay_count);

        let futs = all::<RelayId>()
            .map(|relay| async move {
                let payloads = relay.fetch_delivered_payloads(&None).await?;

                info!("received {} payloads from {}", payloads.len(), relay);

                Ok::<_, anyhow::Error>(payloads)
            })
            .collect_vec();

        let results = future::try_join_all(futs).await?;
        let payloads = results.into_iter().flatten().collect_vec();

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
// async fn backfill_block_production_data() -> Result<()> {
//     let db = PostgresCensorshipDB::new().await?;
//     Ok(())
// }

pub async fn start_ingestion() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.db_connection_str).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let result = tokio::try_join!(ingest_chain_data(), ingest_block_production_data());

    match result {
        Ok(_) => {
            error!("ingestion task(s) completed unexpectedly without an error");
        }
        Err(err) => {
            error!("ingestion task(s) failed: {}", err);
        }
    }

    Ok(())
}
