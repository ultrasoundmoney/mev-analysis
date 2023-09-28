mod http;

use anyhow::Result;
use async_trait::async_trait;
use enum_iterator::Sequence;
use reqwest::Url;
use std::fmt;

#[async_trait]
pub trait RelayApi {
    async fn fetch_delivered_payloads(
        &self,
        end_slot: &Option<i64>,
    ) -> Result<Vec<DeliveredPayload>>;
}

pub struct DeliveredPayload {
    pub relay_id: RelayId,
    pub slot_number: i64,
    pub block_number: i64,
    pub block_hash: String,
    pub builder_pubkey: String,
    pub proposer_pubkey: String,
    pub proposer_fee_recipient: String,
    pub value: String,
}

#[derive(PartialEq, Eq, Hash, Sequence, Clone, Debug)]
pub enum RelayId {
    UltraSound,
    Agnostic,
    Flashbots,
    BlxrMaxProfit,
    BlxrRegulated,
    Eden,
    Aestus,
    Manifold,
}

impl fmt::Display for RelayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelayId::UltraSound => write!(f, "ultrasound"),
            RelayId::Agnostic => write!(f, "agnostic"),
            RelayId::Flashbots => write!(f, "flashbots"),
            RelayId::BlxrMaxProfit => write!(f, "blxr-max-profit"),
            RelayId::BlxrRegulated => write!(f, "blxr-regulated"),
            RelayId::Eden => write!(f, "eden"),
            RelayId::Aestus => write!(f, "aestus"),
            RelayId::Manifold => write!(f, "manifold"),
        }
    }
}

impl From<RelayId> for Url {
    fn from(val: RelayId) -> Self {
        match val {
            // Use k8s service name for our relay
            RelayId::UltraSound => Url::parse("http://data-api").unwrap(),
            RelayId::Agnostic => Url::parse("https://agnostic-relay.net").unwrap(),
            RelayId::Flashbots => Url::parse("https://boost-relay.flashbots.net").unwrap(),
            RelayId::BlxrMaxProfit => {
                Url::parse("https://bloxroute.max-profit.blxrbdn.com").unwrap()
            }
            RelayId::BlxrRegulated => {
                Url::parse("https://bloxroute.regulated.blxrbdn.com").unwrap()
            }
            RelayId::Eden => Url::parse("https://relay.edennetwork.io").unwrap(),
            RelayId::Aestus => Url::parse("https://mainnet.aestus.live").unwrap(),
            RelayId::Manifold => Url::parse("https://mainnet-relay.securerpc.com").unwrap(),
        }
    }
}
