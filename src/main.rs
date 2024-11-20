use core::num;

use anyhow::Error;
use minotari_node_grpc_client::{
    grpc::{Empty, GetBlocksRequest, SubmitBlockRequest},
    BaseNodeGrpcClient,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let client_source = "http://localhost:18142";
    let client_dest = "http://localhost:18143";

    let mut gclient_source = BaseNodeGrpcClient::connect(client_source).await?;
    let mut gclient_dest = BaseNodeGrpcClient::connect(client_dest).await?;

    let tip_height = gclient_dest.get_tip_info(Empty {}).await?;

    let dest_height = tip_height.into_inner().metadata.unwrap().best_block_height;

    let tip_height = gclient_source.get_tip_info(Empty {}).await?;
    let source_height = tip_height.into_inner().metadata.unwrap().best_block_height;

    let mut num_blocks = 0;
    for height in dest_height.saturating_sub(20)..source_height {
        dbg!(height);
        let mut block_stream = gclient_source
            .get_blocks(GetBlocksRequest {
                heights: vec![height],
            })
            .await?
            .into_inner();
        while let Some(resp) = block_stream.message().await? {
            dbg!("Block received");
            let block = resp.block.unwrap();
            let submit_response = gclient_dest.submit_block(block).await?;
            dbg!(submit_response);
            num_blocks += 1;
            if num_blocks > 2000 {
                return Ok(());
                // dbg!(num_blocks);
            }
        }
    }

    Ok(())
}
