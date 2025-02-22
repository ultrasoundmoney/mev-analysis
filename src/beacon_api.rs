use anyhow::anyhow;
use rand::seq::SliceRandom;
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use tracing::debug;

#[derive(Deserialize)]
struct BeaconResponse<T> {
    data: T,
}

#[derive(Deserialize)]
struct Validator {
    index: String,
}

#[derive(Deserialize)]
pub struct SyncStatus {
    pub is_syncing: bool,
}

#[derive(Deserialize, Clone)]
pub struct ExecutionPayload {
    pub block_hash: String,
    #[serde(deserialize_with = "parse_i64_from_string")]
    pub block_number: i64,
}

#[derive(Deserialize)]
pub struct BlockBody {
    execution_payload: ExecutionPayload,
}

#[derive(Deserialize)]
pub struct BlockMessage {
    body: BlockBody,
}

#[allow(dead_code)]
#[derive(Deserialize)]
pub struct BlockResponse {
    message: BlockMessage,
}

fn parse_i64_from_string<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<i64>().map_err(serde::de::Error::custom)
}

#[derive(Clone)]
pub struct BeaconApi {
    node_hosts: Vec<Url>,
    client: reqwest::Client,
}

impl BeaconApi {
    pub fn new(node_hosts: &[Url]) -> Self {
        assert!(
            !node_hosts.is_empty(),
            "tried to instantiate BeaconAPI without at least one url"
        );

        Self {
            node_hosts: node_hosts.to_vec(),
            client: reqwest::Client::new(),
        }
    }

    /// Pick a random beacon node from the list we've been initialized with.
    fn random_host(&self) -> &Url {
        self.node_hosts.choose(&mut rand::thread_rng()).unwrap()
    }

    /// Fetch a validator index from a pubkey.
    pub async fn validator_index(&self, pubkey: &String) -> reqwest::Result<String> {
        let url = format!(
            "{}eth/v1/beacon/states/head/validators/{}",
            self.random_host(),
            pubkey
        );

        self.client
            .get(url)
            .send()
            .await?
            .json::<BeaconResponse<Validator>>()
            .await
            .map(|body| body.data.index)
    }

    /// Method to fetch the payload from a node and a slot.
    async fn block_by_slot(
        &self,
        node: &Url,
        slot: i64,
    ) -> anyhow::Result<Option<ExecutionPayload>> {
        let url = format!("{}eth/v2/beacon/blocks/{}", node, slot);
        let res = self.client.get(&url).send().await?;
        match res.status() {
            // Could mean:
            // 1. Slot doesn't have a block.
            // 2. Slot is in the future and doesn't have a block yet.
            // 3. Slot is before the beacon node backfill limit.
            StatusCode::NOT_FOUND => {
                debug!("no block for slot {} on node {}", slot, node);
                Ok(None)
            }
            StatusCode::OK => {
                debug!("found block for slot {} on node {}", slot, node);
                let block = res
                    .json::<BeaconResponse<BlockResponse>>()
                    .await
                    .map(|envelope| envelope.data.message)?;
                Ok(Some(block.body.execution_payload))
            }
            status => Err(anyhow!(
                "failed to fetch block by slot. slot = {} status = {} url = {}",
                slot,
                status,
                res.url()
            )),
        }
    }

    /// Fetch a beacon block by slot.
    ///
    /// This function is intended to be highly reliable, it does so by calling as many nodes as it
    /// can and returning the first Ok(Some) if any, then Ok(None) if any, and finally the first
    /// error.
    pub async fn block_by_slot_any(&self, slot: i64) -> anyhow::Result<Option<ExecutionPayload>> {
        let futures = self
            .node_hosts
            .iter()
            .map(|node| self.block_by_slot(node, slot));
        let results = futures::future::join_all(futures).await;

        // Attempt to return the first Ok(Some) if any.
        for result in &results {
            match result {
                Ok(Some(payload)) => {
                    debug!("at least one node has a block for slot {}", slot);
                    return Ok(Some(payload.clone()));
                }
                Ok(None) => continue,
                Err(_) => continue,
            }
        }

        // Attempt to return the first Ok(None) if any.
        for result in &results {
            match result {
                Ok(None) => {
                    debug!("no node has a block for slot {}", slot);
                    return Ok(None);
                }
                Ok(Some(_)) => continue,
                Err(_) => continue,
            }
        }

        // Return the first error if all Ok(None) and Ok(Some) are exhausted.
        debug!(
            "failed to successfully fetch block for slot {} on all nodes",
            slot
        );
        results
            .into_iter()
            .next()
            .expect("expect to results to not be empty")
    }

    // Method to fetch the sync status from a node
    async fn sync_status(&self, node_url: &Url) -> reqwest::Result<SyncStatus> {
        let url = format!("{}eth/v1/node/syncing", node_url);
        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<BeaconResponse<SyncStatus>>()
            .await
            .map(|body| body.data)
    }

    pub async fn sync_status_all(&self) -> Vec<reqwest::Result<SyncStatus>> {
        let futures = self.node_hosts.iter().map(|node| self.sync_status(node));
        futures::future::join_all(futures).await
    }
}
