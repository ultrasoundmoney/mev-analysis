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
}

#[derive(PartialEq, Sequence, Clone)]
pub enum RelayId {
    UltraSound,
    Agnostic,
    Flashbots,
    BlxrMaxProfit,
    BlxrRegulated,
    BlxrEthical,
    Blocknative,
    Eden,
    Relayoor,
    Aestus,
}

impl fmt::Display for RelayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelayId::UltraSound => write!(f, "ultrasound"),
            RelayId::Agnostic => write!(f, "agnostic"),
            RelayId::Flashbots => write!(f, "flashbots"),
            RelayId::BlxrMaxProfit => write!(f, "blxr-max-profit"),
            RelayId::BlxrRegulated => write!(f, "blxr-regulated"),
            RelayId::BlxrEthical => write!(f, "blxr-ethical"),
            RelayId::Blocknative => write!(f, "blocknative"),
            RelayId::Eden => write!(f, "eden"),
            RelayId::Relayoor => write!(f, "relayoor"),
            RelayId::Aestus => write!(f, "aestus"),
        }
    }
}

impl Into<Url> for RelayId {
    fn into(self) -> Url {
        match self {
            RelayId::UltraSound => Url::parse("https://relay.ultrasound.money").unwrap(),
            RelayId::Agnostic => Url::parse("https://agnostic-relay.net").unwrap(),
            RelayId::Flashbots => Url::parse("https://boost-relay.flashbots.net").unwrap(),
            RelayId::BlxrMaxProfit => {
                Url::parse("https://bloxroute.max-profit.blxrbdn.com").unwrap()
            }
            RelayId::BlxrRegulated => {
                Url::parse("https://bloxroute.regulated.blxrbdn.com").unwrap()
            }
            RelayId::BlxrEthical => Url::parse("https://bloxroute.ethical.blxrbdn.com").unwrap(),
            RelayId::Blocknative => {
                Url::parse("https://builder-relay-mainnet.blocknative.com").unwrap()
            }
            RelayId::Eden => Url::parse("https://relay.edennetwork.io").unwrap(),
            RelayId::Relayoor => Url::parse("https://relayooor.wtf").unwrap(),
            RelayId::Aestus => Url::parse("https://mainnet.aestus.live").unwrap(),
        }
    }
}
