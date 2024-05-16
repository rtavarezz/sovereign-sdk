use core::{future::Future, pin::Pin};
pub use sov_rollup_interface::services::da::{DaService, SlotData};
use sov_rollup_interface::da::DaSpec;
//check repo: https://github.com/AnomalyFi/rust-seq-rpc
//for getting block information
use nodekit_seq_sdk;
use nodekit_seq_sdk::client::jsonrpc_client::*;
use nodekit_seq_sdk::types::types::*;
// relayer 
use nodekit_relay_rust;
use nodekit_relay_rust::rpc::*;
use nodekit_relay_rust::config::config::*;
//others
use std::sync::Arc;
use tokio::time::Duration;
use async_trait::async_trait;
use sha2::{Sha256, Digest};
use ::serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Error;
//in repo
use crate::da_spec::spec::{SEQTxs, NodeKitBlockInfo, NodeKitValidity, DaLayerSpec};
use crate::da_verifier::verifier::NodeKitVerifier;
use tokio::macros::support::Poll;
use futures::task::Context;

#[derive(Debug, Clone)]
pub struct NodeKitClient {
    //namespace is same as secondary chain id
    pub rollup_namespace: String,
    pub jsonrpc: JSONRPCClient,
    pub uri: String,
    pub secondary_chain_id: Vec<u8>,

}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeKitFilteredBlock {
    pub header: NodeKitBlockInfo,
    pub transactions: Vec<SEQTransaction>,
    //needs proofs(tbd in v1)
}

impl PartialEq for NodeKitFilteredBlock {
    fn eq(&self, other: &Self) -> bool {
        self.header.block.block_id == other.header.block.block_id &&
        self.header.block.timestamp == other.header.block.timestamp &&
        self.header.block.l1_head == other.header.block.l1_head &&
        self.header.block.height == other.header.block.height
    }
}
impl SlotData for NodeKitFilteredBlock {
    type BlockHeader = NodeKitBlockInfo;
    type Cond = NodeKitValidity;
    // Required methods
    fn hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        for tx in &self.transactions {
            hasher.update(&tx.transaction);
        }
        let result = hasher.finalize();
        result.into()
    }
    fn header(&self) -> &Self::BlockHeader {
        &self.header
    }
    fn validity_condition(&self) -> Self::Cond {
        //todo
        NodeKitValidity::default()
    }
}


impl NodeKitClient {
    pub fn new(uri: &str, network_id: u32, chain_id: String, rollup_namespace: String, secondary_chain_id: Vec<u8>) -> Result<Self, Box<dyn
    std::error::Error>> {
        let resp = JSONRPCClient::new(uri, network_id, chain_id);
        match resp {
            Ok(jsonrpc) => {
                Ok(Self {
                    jsonrpc,
                    rollup_namespace,
                    uri: uri.to_string(),
                    secondary_chain_id,
                })
            },
            Err(e) => Err(e),
        }
    }
}

pub struct NodeKitBlockHeaderStream {
    inner: tokio_stream::wrappers::BroadcastStream<NodeKitBlockInfo>,
}

impl futures::Stream for NodeKitBlockHeaderStream {
    type Item = Result<NodeKitBlockInfo, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

#[async_trait]
impl DaService for NodeKitClient {
    type Spec = DaLayerSpec;
    
    type Verifier = NodeKitVerifier;

    type FilteredBlock = NodeKitFilteredBlock;

    type TransactionId = ();

    type HeaderStream = NodeKitBlockHeaderStream;

    type Error = anyhow::Error;

    // Make an RPC call to the node to get the block at the given height
    // If no such block exists, block until one does.
    async fn get_block_at(&self, height: u64) -> Result<Self::FilteredBlock, Self::Error> {
        let client = NodeKitClient::new(
            &self.uri.clone(),
            self.jsonrpc.network_id.clone(),
            self.jsonrpc.chain_id.clone(),
            self.rollup_namespace.clone(),
            self.secondary_chain_id.clone(),
        ).expect("Failed to create client");

        let client_clone = client.clone();
        let client_ref = &client_clone;

        //Fetches all block headers starting from the requested height up to the time user made request.
        let start = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64 * 1000;
        let end = start - 120 * 1000;
        let args = GetBlockHeadersByHeightArgs {height, end};

        match client_ref.jsonrpc.get_block_headers_by_height(args.height, args.end) {
                
            Ok(block_headers_response) => {
                    
                if !block_headers_response.blocks.is_empty() {
                    // Extract the first header assuming it's the finalized block
                    let finalized_block = block_headers_response.blocks[0].clone();
                    // Fetch relevant transactions for the rollup namespace
                    let bytes = self.rollup_namespace.as_bytes();
                    let hex_namespace = hex::encode(bytes); 
                    let transactions = client.jsonrpc.get_block_transactions_by_namespace(height, hex_namespace);
                    let mut tx = Vec::new();
                    //checks if transactions returns a value and if so mark it as the tx.
                    if let Ok(transactions) = transactions {
                        tx = transactions.txs;
                    }
                    let block_info = NodeKitBlockInfo {
                        //block returns first block
                        block: finalized_block,
                        //header returns from(u64), blocks(Vec<BlockInfo>), prev(BlockInfo), next(BlockInfo)
                        //BlockInfo returns block_id, timestamp, l1_head, and height
                        header: block_headers_response,
                    };
                    //returns `FilteredBlock` with all relevant info
                    return Ok(Self::FilteredBlock {
                        //header is first block
                        header: block_info,
                        //tx of block submitted
                        transactions: tx,
                        //todo: inclusion_proof,
                    });
                }
                //if blocks field is empty; no blocks at height.
                else {
                    return Err(anyhow::anyhow!("Error: no blocks found at specified height {}", height));
                }
            }
            //rpc call failed
            Err(_e) => {
                return Err(anyhow::anyhow!("Error fetching block headers with rpc function. Double check the height inputted {}", height));
            }
        }
    }
    
    async fn get_extraction_proof(
        &self,
        block: &Self::FilteredBlock,
        blobs: &[<Self::Spec as DaSpec>::BlobTransaction],
    ) -> (
        <Self::Spec as DaSpec>::InclusionMultiProof,
        <Self::Spec as DaSpec>::CompletenessProof,
    ) {
        // TODO in v1
        (vec![],vec![])
    }

    // Send a transaction directly to SEQ. 
    // `blob` is the serialized and signed transaction. 
    // Returns nothing if the transaction was successfully sent.
    async fn send_transaction(&self, blob: &[u8]) -> Result<(), Self::Error> {
        let _ = self.jsonrpc.submit_tx(self.jsonrpc.chain_id.clone(),self.jsonrpc.network_id,self.secondary_chain_id.clone(), blob.to_vec());
        Ok(())
    }

    // Extract the blob transactions relevant to a particular rollup from a block.
    // This method is usually (but not always) parameterized by some configuration option,
    // such as the rollup's namespace. If configuration is needed, it should be provided
    // to the NodeKitClient struct through its constructor.
    fn extract_relevant_blobs(
        &self,
        block: &Self::FilteredBlock
    ) -> Vec<<Self::Spec as DaSpec>::BlobTransaction> {
        let mut relevant_txs = Vec::new();
        //Fetch all transactions for the block's height and rollup namespace
        let bytes = self.rollup_namespace.as_bytes();
        let hex_namespace = hex::encode(bytes);
        let block_transactions = self.jsonrpc.get_block_transactions_by_namespace(block.header.block.height, hex_namespace.clone());

        match block_transactions {
            Ok(block_transactions) => {
                for tx in &block_transactions.txs {
                    if tx.namespace != hex_namespace {
                        continue;
                    }
                    let blob_transaction = SEQTxs(tx.clone());
                    relevant_txs.push(blob_transaction);
                }
            },
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        }
        relevant_txs
    }
    
    // Extract the list blob transactions relevant to a particular rollup from a block, along with inclusion and
    // completeness proofs for that set of transactions. The output of this method will be passed to the verifier.
    #[allow(clippy::type_complexity)]
    async fn extract_relevant_blobs_with_proof(
        &self,
        block: &Self::FilteredBlock,
    ) -> (
        Vec<<Self::Spec as DaSpec>::BlobTransaction>,
        <Self::Spec as DaSpec>::InclusionMultiProof,
        <Self::Spec as DaSpec>::CompletenessProof,
    ) {
        let relevant_txs = self.extract_relevant_blobs(block);

        let (etx_proofs, rollup_row_proofs) = self
            .get_extraction_proof(block, relevant_txs.as_slice())
            .await;

        (relevant_txs, etx_proofs, rollup_row_proofs)
    }

    // new funcs from new updated version of DaService
    async fn get_last_finalized_block_header(
        &self,
    ) -> Result<<Self::Spec as DaSpec>::BlockHeader, Self::Error> { 
        todo!() 
    }

    async fn subscribe_finalized_header(&self) -> Result<Self::HeaderStream, Self::Error> { 
        todo!() 
    }

    async fn get_head_block_header(
        &self,
    ) -> Result<<Self::Spec as DaSpec>::BlockHeader, Self::Error> { 
        todo!() 
    }

    async fn send_aggregated_zk_proof(
        &self,
        aggregated_proof_data: &[u8],
    ) -> Result<u64, Self::Error> { 
        todo!() 
    }

    async fn get_aggregated_proofs_at(&self, height: u64) -> Result<Vec<Vec<u8>>, Self::Error> { 
        todo!() 
    }
}
