mod http;

use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

pub use http::RelayServiceHttp;

#[derive(Deserialize, Debug)]
pub struct DeliveredPayload {
    pub slot_number: i64,
    pub block_number: i64,
    pub block_hash: String,
    pub builder_pubkey: String,
    pub proposer_pubkey: String,
    pub proposer_fee_recipient: String,
}

#[async_trait]
pub trait RelayApi {
    async fn fetch_delivered_payloads(&self, end_slot: &i64) -> Result<Vec<DeliveredPayload>>;
}
