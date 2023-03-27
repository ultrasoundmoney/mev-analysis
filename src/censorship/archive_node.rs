mod infura;

pub use infura::InfuraClient;

use anyhow::Result;
use async_trait::async_trait;

pub struct TxLowBalanceCheck {
    pub transaction_hash: String,
    pub from_address: String,
    pub block_number: i64,
    pub total_value: i128, // wei
}

#[async_trait]
pub trait ArchiveNode {
    async fn check_low_balance(&self, check: &TxLowBalanceCheck) -> Result<bool>;
}
