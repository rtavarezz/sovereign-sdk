use sov_rollup_interface::da::DaVerifier;
use sov_rollup_interface::da::DaSpec;
use anyhow::Error;
use crate::da_spec::spec::{NodeKitValidity, DaLayerSpec};

pub struct NodeKitVerifier;

impl DaVerifier for NodeKitVerifier {
    
    //The set of types required by the DA layer.
    type Spec = DaLayerSpec;

    //The error type returned by the DA layer’s verification function TODO: Should we add std::Error bound so it can be ()? ?
    type Error = anyhow::Error;

    //TODOs: Create a new da verifier with the given chain parameters
    fn new(_params: <Self::Spec as DaSpec>::ChainParams) -> Self {
        Self{}
    }

    //Verify a claimed set of transactions against a block header
    fn verify_relevant_tx_list(
        &self,
        _block_header: &<Self::Spec as DaSpec>::BlockHeader,
        _txs: &[<Self::Spec as DaSpec>::BlobTransaction],
        _inclusion_proof: <Self::Spec as DaSpec>::InclusionMultiProof,
        _completeness_proof: <Self::Spec as DaSpec>::CompletenessProof,
    ) -> Result<<Self::Spec as DaSpec>::ValidityCondition, Self::Error> {
        //placeholder, proofs will be included soon in version 1, tbd
        Ok(Default::default())
    }
}
