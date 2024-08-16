mod postgres;

use anyhow::Result;
use async_trait::async_trait;

use super::relay::DeliveredPayload;

pub use postgres::PostgresCensorshipDB;

#[async_trait]
pub trait CensorshipDB {
    // slot_number checkpoint for offchain relay data
    async fn get_block_production_checkpoint(&self) -> Result<Option<i64>>;
    // this method is idempotent for a given set of relays
    async fn upsert_delivered_payloads(&self, payloads: Vec<DeliveredPayload>) -> Result<()>;
}
