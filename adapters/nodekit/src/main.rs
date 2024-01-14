mod da_verifier;
mod da_service;
mod da_spec;
use sov_rollup_interface::services::da::DaService;
use nodekit_seq_sdk::client::jsonrpc_client::*;
use crate::da_service::service::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() {
    println!("Hello World");
    let chain_id = "?".to_string();
    let url_new = "?".to_string();
    let namespace = "?".to_string();
    let secondary_id = "?".as_bytes().to_vec();
    //insert the value, this is an example
    let network_id = 321; 
    //insert the value, this is an example
    let height = 321; 
    
    //todo: finalized still needs proofs. This tests functions get_finalized_at and get_block_at
    match test_block_height(chain_id.trim().to_string(), url_new.trim().to_string(), namespace.trim().to_string(), secondary_id.clone(), network_id, height).await {
        Ok(_) => println!("test_block_height succeeded"),
        Err(err) => {
            println!("test_block_height error occurred: {:?}", err);
        }
    }
    //1. Run test_block_height and test_send_transaction FIRST, then after getting txID and height of the submitted tx,
    //use that height and pass it as an arg into test_extract_relevant_blobs.
    //If you run all 3, then extract reel blob will fail. needs the height of submitted tx.
    match test_send_transaction().await {
        Ok(_) => println!("blob transaction sent!!"),
        Err(err) => println!("blob transaction sending error occurred: {:?}", err),
    }

    match test_extract_relevant_blobs().await {
        Ok(_) => println!("blobs extracted succeeded"),
        Err(err) => println!("blobs extracting error occurred: {:?}", err),
    }

}

async fn test_block_height(chain_id: String, url_new: String, namespace: String, secondary_id: Vec<u8>, network_id: u32, height: u64) -> Result<(NodeKitFilteredBlock, NodeKitFilteredBlock), Box<dyn std::error::Error>> {
        //pass args above
        let cli = NodeKitClient::new(&url_new, network_id, chain_id, namespace, secondary_id).map_err(|e| e as Box<dyn std::error::Error>)?;
        let start = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64 * 1000;
        let end = start - 120 * 1000;
        let block_head = match cli.jsonrpc.get_block_headers_by_height(height, end) {
            Ok(res) => res,
            Err(err) => return Err(err),
        };

        let temp = match block_head.blocks.last() {
            Some(block) => block,
            None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No blocks found"))),
        };

        let finalize = match cli.get_finalized_at(height).await {
            Ok(finalize) => finalize, 
            Err(err) => return Err(err.into()),
        };

        //tests get block at fn and uses same u64 height.
        let get_height = match cli.get_block_at(height).await {
            Ok(get_height) => get_height, 
            Err(err) => return Err(err.into()),
        };

        println!("get_finalize_at fn test: {:?}", finalize.header.header.get_blocks()[0]);
        println!("get_block_at fn test: {:?}", get_height.header.header.get_blocks()[0]);

        Ok((finalize, get_height))
}

//testing send tx function
async fn test_send_transaction() -> Result<(), Box<dyn std::error::Error>> {
    //setup args from SEQ
    let chain_id = "?".to_string();
    let url_new = "?".to_string();
    let namespace = "?".to_string();
    let secondary_id = "?".as_bytes().to_vec();
    let network_id = 321;
    //blob is the 'pub data: Vec<u8>' in jsonrpc SubmitMsgTxArgs.
    //but send tx expects &[u8] so we use 'as_bytes()' fn for that
    let blob = "?".as_bytes();
    let cli = NodeKitClient::new(&url_new, network_id, chain_id, namespace, secondary_id).unwrap();
    let result = cli.send_transaction(blob).await;
    assert!(result.is_ok(), "send_transaction failed: {:?}", result.err());
    Ok(())
}

//testing extract relevant blobs function
async fn test_extract_relevant_blobs() -> Result<(), Box<dyn std::error::Error>>  {
    //setup args from SEQ
    let chain_id = "?".to_string();
    let url_new = "?".to_string();
    let namespace = "?".to_string();
    let secondary_id = "?".as_bytes().to_vec();
    //insert the value, this is an example
    let network_id = 321;
    //insert the value, this is an example
    let height = 321;

    let cli = NodeKitClient::new(&url_new, network_id, chain_id, namespace.clone(), secondary_id).unwrap();

    // Need FilteredBlock instance to extract relevant blobs since that is passed through the func extract rel blob. 
    let block = match cli.get_block_at(height).await {
        Ok(res) => res,
        Err(err) => panic!("get_block_at failed: {:?}", err),
    };

    let hex_namespace = hex::encode(namespace);
    let test = match cli.jsonrpc.get_block_transactions_by_namespace(height, hex_namespace) {
        Ok(res) => res,
        Err(err) => panic!("get_block_at failed: {:?}", err),
    };
    let blobs = cli.extract_relevant_blobs(&block);
    println!("blobs: {:?}", blobs);
    assert!(!blobs.is_empty(), "No relevant blobs found");
    Ok(())
}