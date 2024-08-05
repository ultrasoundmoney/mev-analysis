use anyhow::Result;
use async_trait::async_trait;
use reqwest::Url;
use serde::Deserialize;

use super::{DeliveredPayload, RelayApi, RelayId};

#[derive(Deserialize)]
struct DeliveredPayloadResponse {
    slot: String,
    block_number: String,
    block_hash: String,
    builder_pubkey: String,
    proposer_pubkey: String,
    value: String,
}

// Set to the lowest max which is bloxroute
const PAYLOAD_LIMIT: usize = 100;

#[async_trait]
impl RelayApi for RelayId {
    async fn fetch_delivered_payloads(
        &self,
        end_slot: &Option<i64>,
    ) -> Result<Vec<DeliveredPayload>> {
        let url: Url = self.clone().into();
        let limit = format!("?limit={}", PAYLOAD_LIMIT);
        let query = end_slot
            .map(|end_slot| format!("{}&cursor={}", &limit, &end_slot))
            .unwrap_or(limit);

        let url = format!(
            "{}relay/v1/data/bidtraces/proposer_payload_delivered{}",
            url, &query
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
                             value,
                         }| DeliveredPayload {
                            relay_id: self.clone(),
                            slot_number: slot.parse().unwrap(),
                            block_number: block_number.parse().unwrap(),
                            block_hash,
                            builder_pubkey,
                            proposer_pubkey,
                            value,
                        },
                    )
                    .collect()
            })
            .map_err(Into::into)
    }
}
