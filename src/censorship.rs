mod chain_store;
mod env;
mod relay_service;
mod timestamp_service;

use anyhow::Result;
use chrono::{DateTime, Utc};
use gcp_bigquery_client::Client;
use reqwest::Url;

use self::{
    chain_store::ChainStore,
    relay_service::{RelayService, RelayServiceHttp},
    timestamp_service::{TimestampService, ZeroMev},
};

pub async fn start_ingestion() -> Result<()> {
    let chain_store = Client::from_service_account_key_file("insert_key_file_here.json").await?;

    let ts_service = ZeroMev::new().await;

    // block nr 16648338
    let start = DateTime::parse_from_rfc3339("2023-02-17T12:09:35Z")
        .expect("failed to parse date")
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339("2023-02-17T12:09:36Z")
        .expect("failed to parse date")
        .with_timezone(&Utc);

    let blocks = chain_store.fetch_blocks(&start, &end).await?;
    let txs = chain_store.fetch_txs(&start, &end).await?;
    let timestamped_txs = ts_service.fetch_tx_timestamps(txs).await?;

    // this data will be collected separately from block/tx data
    let relay = RelayServiceHttp::new(Url::parse("https://relay.ultrasound.money").unwrap());
    let delivered_payloads = relay.fetch_delivered_payloads(&5810247).await?;

    println!(
        "got {} blocks, {} txs and {} delivered payloads",
        blocks.len(),
        timestamped_txs.len(),
        delivered_payloads.len()
    );

    Ok(())
}
