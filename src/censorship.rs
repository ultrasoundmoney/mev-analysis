mod chain_store;
mod db;
mod env;
mod relay_service;
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

    let checkpoint = *MERGE_CHECKPOINT;

    let start_time = checkpoint;
    let end_time = start_time.add(Duration::minutes(60));

    info!(
        "starting chain data ingestion from {} until {}",
        start_time, end_time
    );

    let blocks = chain_store.fetch_blocks(&start_time, &end_time).await?;
    let txs = chain_store.fetch_txs(&start_time, &end_time).await?;

    info!("received {} blocks and {} txs", blocks.len(), txs.len());

    let timestamped_txs = ts_service.fetch_tx_timestamps(txs).await?;

    db.persist_chain_data(blocks, timestamped_txs).await?;

    Ok(())
}
