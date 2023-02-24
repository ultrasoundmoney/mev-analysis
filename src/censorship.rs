mod chain_store;
mod db;
mod env;
mod relay;
mod timestamp_service;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use gcp_bigquery_client::Client;
use lazy_static::lazy_static;
use sqlx::{Connection, PgConnection};
use std::ops::Add;
use tracing::info;

use self::db::{CensorshipDB, PostgresCensorshipDB};
use self::env::APP_CONFIG;
use self::relay::RelayServiceHttp;
use self::{
    chain_store::ChainStore,
    timestamp_service::{TimestampService, ZeroMev},
};

lazy_static! {
    pub static ref MERGE_CHECKPOINT: DateTime<Utc> =
        DateTime::parse_from_rfc3339("2022-09-15T06:42:42Z")
            .unwrap()
            .with_timezone(&Utc);
}

pub async fn start_ingestion() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.db_connection_str).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let chain_store =
        Client::from_service_account_key_file("ultra-sound-relay-ae2ce601f320.json").await?;

    let ts_service = ZeroMev::new().await;

    let db = PostgresCensorshipDB::new().await?;

    loop {
        let checkpoint = db
            .get_chain_checkpoint()
            .await?
            .unwrap_or(*MERGE_CHECKPOINT);

        let start_time = checkpoint;
        let end_time = start_time.add(Duration::hours(2));

        info!(
            "starting chain data ingestion from {} until {}",
            start_time, end_time
        );

        let begin = Utc::now();

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

        db.persist_chain_data(blocks, timestamped_txs).await?;

        info!(
            "persisted chain data in {} seconds",
            (Utc::now() - begin).num_seconds()
        );
    }
}

async fn ingest_block_production_data() -> Result<()> {
    let url = APP_CONFIG.relay_urls.first().unwrap().clone();
    let relay_api = RelayServiceHttp::new(url);
    let db = PostgresCensorshipDB::new().await?;

    let checkpoint = db.get_block_production_checkpoint().await?;

    Ok(())
}
