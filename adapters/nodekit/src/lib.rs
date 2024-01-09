#![cfg_attr(not(feature = "native"), no_std)]
use nodekit_seq_sdk;
use nodekit_seq_sdk::types::types::{BlockInfo, SEQTransaction, BlockHeadersResponse};
use sov_rollup_interface::da::DaSpec;
use sov_rollup_interface::BasicAddress as Address;
use sov_rollup_interface::da::BlockHeaderTrait as BlockHeader;
use sov_rollup_interface::da::BlockHashTrait as BlockHash;
use sov_rollup_interface::da::BlobReaderTrait as BlobReader;
use sov_rollup_interface::da::Time;
use sov_rollup_interface::da::NanoSeconds;
use sov_rollup_interface::zk::ValidityCondition as Validity;
use ::serde::{Serialize, Deserialize};
use borsh::{BorshDeserialize, BorshSerialize};

use bs58;
use core::convert::TryFrom;
use std::str::FromStr;

use sha2::Digest;
use core::fmt::Display;
use anyhow::Error;

#[cfg(feature = "native")]
mod service;
mod verifier;

#[derive(Debug, Eq, PartialEq)]
pub struct DaLayerSpec;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeKitBlockInfo {
    block: BlockInfo,
    header: BlockHeadersResponse
}

impl Eq for NodeKitBlockInfo {}
impl PartialEq for NodeKitBlockInfo {
    fn eq(&self, other: &Self) -> bool {
        self.block.block_id == other.block.block_id &&
        self.block.timestamp == other.block.timestamp &&
        self.block.l1_head == other.block.l1_head
    }
}


#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct NodeKitHash(pub [u8; 32]);

impl NodeKitHash {
    pub fn inner(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Display for NodeKitHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = hex::encode(&self.0);
        f.write_str(&s)
    }
}

impl AsRef<[u8]> for NodeKitHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<NodeKitHash> for [u8; 32] {
    fn from(val: NodeKitHash) -> Self {
        *val.inner()
    }
}

impl BlockHash for NodeKitHash {}


impl BlockHeader for NodeKitBlockInfo {
    type Hash = NodeKitHash;

    fn prev_hash(&self) -> Self::Hash {
        //uses bs58 lib and decode fn takes in &self..etc, decode fn returns DecodeBuilder.
        //fn into_vec() decodes the Base58 string into a byte vector (Vec<u8>).
        //if decode() is a success, it'll return a result(Vec<u8>).
        //With the Result<Vec<u8>>, itll call unwrap on the Result.
        //if Result is Ok() meaning it passed, then itll return the vector.
        //if Result fails, itll panic and program crashes, todo, (might need to fix will revisit).
        let decoded = bs58::decode(&self.header.prev.block_id).into_vec().unwrap();
        //fn try_into() tries to convert the vector above into array of 32 bytes.
        //if decoded == 32 bytes, itll return Ok([u8; 32]), otherwise itll fail
        //unwrap_. is called on the result from step above being 32 bytes.
        //if result is ok then itll return the array 32 bytes, otherwise fails and returns array of zeros.
        NodeKitHash(decoded.try_into().unwrap_or_else(|_| [0; 32]))
    }
    //same as prev_hash() but with curr block instead of prev.
    fn hash(&self) -> Self::Hash {
        let decoded = bs58::decode(&self.block.block_id).into_vec().unwrap();
        NodeKitHash(decoded.try_into().unwrap_or_else(|_| [0; 32]))
    }

    fn height(&self) -> u64 {
        self.block.l1_head
    }
    fn time(&self) -> Time {
        let millis = (self.block.timestamp % 1000) as u32;
        let nanos = millis * 1_000_000;
        let nanos = NanoSeconds::new(nanos).unwrap();
        let secs = self.block.timestamp / 1000;
        Time::new(secs, nanos)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct NodeKitAddress(Vec<u8>);

impl NodeKitAddress {
    pub fn new(id: Vec<u8>) -> Self {
        NodeKitAddress(id)
    }
}

impl<'a> TryFrom<&'a [u8]> for NodeKitAddress {
    type Error = anyhow::Error;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let array = <[u8; 32]>::try_from(value).map_err(|_| Error::msg("Failed to convert slice to array"))?;
        Ok(NodeKitAddress(array.to_vec()))
    }
}

impl FromStr for NodeKitAddress {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s).map_err(anyhow::Error::new)?;
        Ok(NodeKitAddress(bytes))
    }
}

impl AsRef<[u8]> for NodeKitAddress {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Display for NodeKitAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = hex::encode(&self.0);
        f.write_str(&s)
    }
}

impl Address for NodeKitAddress {}

#[derive(Serialize, Deserialize)]
pub struct SEQTxs(SEQTransaction);
//same idea and approach as BlockHeaderTrait
impl BlobReader for SEQTxs {
    type Address = NodeKitAddress;

    fn sender(&self) -> Self::Address {
        let decoded = bs58::decode(&self.0.namespace).into_vec().unwrap();
        NodeKitAddress(decoded)
    }
    fn hash(&self) -> [u8; 32] {
        let decoded = bs58::decode(&self.0.tx_id).into_vec().unwrap();
        decoded.try_into().unwrap_or_else(|_| [0; 32])
    }
    fn verified_data(&self) -> &[u8] {
        &self.0.transaction
    }
    fn total_len(&self) -> usize {
        self.0.transaction.len()
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Eq, Hash, Default, Clone, PartialEq, BorshDeserialize, BorshSerialize)]
pub struct NodeKitValidity {
    past: [u8; 32],
    block: [u8; 32],
}
//TODO: to be implemented in next version
impl Validity for NodeKitValidity {
    type Error = Error;

    fn combine<H: Digest>(&self, _rhs: Self) -> Result<Self, Self::Error> {
        Ok(_rhs)
    }
}

impl DaSpec for DaLayerSpec {

    //The hash of a DA layer block
    type SlotHash = NodeKitHash;

    //The block header type used by the DA layer
    type BlockHeader = NodeKitBlockInfo;

    //The transaction type used by the DA layer.
    type BlobTransaction = SEQTxs;

    //The type used to represent addresses on the DA layer.
    type Address = NodeKitAddress;

    //Any conditions imposed by the DA layer which need to be checked outside of the SNARK
    //todo
    type ValidityCondition = NodeKitValidity;

    //A proof that each tx in a set of blob transactions is included in a given block.
    //todo
    type InclusionMultiProof = Vec<u8>;

    //verifying the completeness of a set of transactions, 
    //such as a range proof indicating that the provided BlobTransactions 
    //correctly represents the entirety of the namespace within a certain block.
    //todo
    type CompletenessProof = Vec<u8>;

    //The built-in features of the rollup included in the state-transition function(STF),
    //like the rollup's namespace.
    //todo
    type ChainParams = ();
}