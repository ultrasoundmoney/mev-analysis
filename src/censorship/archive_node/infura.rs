use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tracing::info;

use crate::censorship::env::APP_CONFIG;

use super::{ArchiveNode, TxLowBalanceCheck};

pub struct InfuraClient {
    client: reqwest::Client,
}

impl InfuraClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct InfuraResponse {
    jsonrpc: String,
    id: u32,
    result: String,
}

fn encode_block_number(block_number: &i64) -> String {
    format!(
        "0x{}",
        hex::encode(block_number.to_be_bytes()).trim_start_matches('0')
    )
}

#[async_trait]
impl ArchiveNode for InfuraClient {
    async fn check_low_balance(
        &self,
        TxLowBalanceCheck {
            from_address,
            block_number,
            total_value,
            ..
        }: &TxLowBalanceCheck,
    ) -> Result<bool> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBalance",
            "params": [from_address, encode_block_number(block_number)]
        });

        info!("{}, block_number {}", payload, block_number);

        let response: InfuraResponse = self
            .client
            .post(format!(
                "https://mainnet.infura.io/v3/{}",
                &APP_CONFIG.infura_api_key
            ))
            .json(&payload)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let balance = i128::from_str_radix(&response.result[2..], 16)?;
        let low_balance = balance < *total_value;

        info!(
            "balance: {}, required: {}, is_low {}",
            balance, total_value, low_balance
        );

        Ok(low_balance)
    }
}
