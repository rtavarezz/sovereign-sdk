mod da_verifier;
mod da_service;
mod da_spec;
use sov_rollup_interface::services::da::DaService;
use nodekit_seq_sdk::client::jsonrpc_client::*;
use crate::da_service::service::*;

#[tokio::main]
async fn main() {
    println!("Hello World");
    let chain_id = "?".to_string();
    let url_new = "?".to_string();
    let namespace = "?".to_string();
    let secondary_id = "?".as_bytes().to_vec();
    let network_id = 321; //insert the value, this is an example
    let height = 123; //insert the value, this is an example 
    
    match test_finalized(chain_id.trim().to_string(), url_new.trim().to_string(), namespace.trim().to_string(), secondary_id, network_id, height).await {
        Ok(_) => println!("test_finalized succeeded"),
        Err(err) => println!("test_finalized error occurred: {:?}", err),
    }
    match test_send_transaction().await {
        Ok(_) => println!("transaction sending succeeded"),
        Err(err) => println!("transaction sending error occurred: {:?}", err),
    }
    match test_extract_relevant_blobs().await {
        Ok(_) => println!("blobs extracted succeeded"),
        Err(err) => println!("blobs extracting error occurred: {:?}", err),
    }

}

//testing get finalized at function and get block at function
async fn test_finalized(chain_id: String, url_new: String, namespace: String, secondary_id: Vec<u8>, network_id: u32, height: u64) -> Result<(NodeKitFilteredBlock, NodeKitFilteredBlock), Box<dyn std::error::Error>> {
        let cli = NodeKitClient::new(&url_new, network_id, chain_id, namespace, secondary_id).map_err(|e| e as Box<dyn std::error::Error>)?;
        //testing finalized and block height funcs below
        let end = 0; //placeholder
        //let end be current time unix epoch millis like u did in rust sdk.
        let block_head = match cli.jsonrpc.get_block_headers_by_height(height, end) {
            Ok(res) => res,
            Err(err) => return Err(err),
        };
        println!("{:?}", block_head);
        //unwraps the last block from vec<blockinfo> so itll just be type blockinfo and can grab u64 from there
        //this case the l1_head
        let temp = match block_head.blocks.last() {
            Some(block) => block,
            None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No blocks found"))),
        };
        //gets l1 head of block or height u64 and passes that into fn finalized at.
        let param = temp.l1_head;
        let finalize = match cli.get_finalized_at(param).await {
            Ok(finalize) => finalize, 
            Err(err) => return Err(err),
        };
        //tests get block at fn and uses same u64 height.
        let get_height = match cli.get_block_at(param).await {
            Ok(get_height) => get_height, 
            Err(err) => return Err(err),
        };
        Ok((finalize, get_height))
}

//testing send tx function
async fn test_send_transaction() -> Result<(), Box<dyn std::error::Error>> {
    //setup args
    let chain_id = "?".to_string();
    let url_new = "?".to_string();
    let namespace = "?".to_string();
    //converts string to Vec<u8>
    let secondary_id = "?".to_string().into_bytes();
    let network_id = 1337;
    //blob is the 'pub data: Vec<u8>' in jsonrpc SubmitMsgTxArgs.
    //but send tx expects &[u8] so we use 'as_bytes()' fn for that
    //if we wanted string to Vec<u8>, do .into_bytes() like above
    let blob = "?".as_bytes();

    let cli = NodeKitClient::new(&url_new, network_id, chain_id, namespace, secondary_id).unwrap();

    let result = cli.send_transaction(blob).await;
    println!("{:?}", result);

    assert!(result.is_ok(), "send_transaction failed: {:?}", result.err());
    Ok(())
}

//testing extarct relevant blobs function
async fn test_extract_relevant_blobs() -> Result<(), Box<dyn std::error::Error>>  {
    //setup args
    let chain_id = "?".to_string();
    let url_new = "?".to_string();
    let namespace = "?".to_string();
    //converts string to Vec<u8>
    let secondary_id = "?".to_string().into_bytes();
    let network_id = 0;
    let height = "?".to_string();
    let height = height.trim().parse::<u64>().expect("Failed to parse height");

    //new NodeKitClient instance.
    let cli = NodeKitClient::new(&url_new, network_id, chain_id, namespace, secondary_id).unwrap();

    // Need FilteredBlock instance to extract relevant blobs since that is passed through the func extract rel blob. 
    let block = match cli.get_block_at(height).await {
        Ok(res) => res,
        Err(err) => panic!("get_block_at failed: {:?}", err),
    };

    //extract_relevant_blobs takes in block which is type Self::FilteredBlock which is correct by function definition
    //itll return a BlobTransaction which is SEQTxs(found in spec.rs under da_spec)
    let blobs = cli.extract_relevant_blobs(&block);

    // Check the blobs.
    assert!(!blobs.is_empty(), "No relevant blobs found");
    Ok(())
}