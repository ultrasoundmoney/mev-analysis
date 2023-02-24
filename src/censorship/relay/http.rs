use anyhow::Result;
use async_trait::async_trait;
use reqwest::Url;
use serde::Deserialize;

use super::{DeliveredPayload, RelayApi};

pub struct RelayServiceHttp {
    url: Url,
}

impl RelayServiceHttp {
    pub fn new(url: Url) -> Self {
        Self { url }
    }
}

#[derive(Deserialize)]
struct DeliveredPayloadResponse {
    slot: String,
    block_number: String,
    block_hash: String,
    builder_pubkey: String,
    proposer_pubkey: String,
    proposer_fee_recipient: String,
}

#[async_trait]
impl RelayApi for RelayServiceHttp {
    async fn fetch_delivered_payloads(&self, end_slot: &i64) -> Result<Vec<DeliveredPayload>> {
        let url = format!(
            "{}relay/v1/data/bidtraces/proposer_payload_delivered?cursor={}",
            &self.url, end_slot
        );
        reqwest::get(url)
            .await?
            .error_for_status()?
            .json::<Vec<DeliveredPayloadResponse>>()
            .await
            .map(|res| {
                res.into_iter()
                    .map(
                        |DeliveredPayloadResponse {
                             slot,
                             block_number,
                             block_hash,
                             builder_pubkey,
                             proposer_pubkey,
                             proposer_fee_recipient,
                         }| DeliveredPayload {
                            slot_number: slot.parse().unwrap(),
                            block_number: block_number.parse().unwrap(),
                            block_hash,
                            builder_pubkey,
                            proposer_pubkey,
                            proposer_fee_recipient,
                        },
                    )
                    .collect()
            })
            .map_err(|err| err.into())
    }
}
