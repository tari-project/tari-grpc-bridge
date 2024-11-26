// Copyright 2021. The Tari Project
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
// following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
// disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
// following disclaimer in the documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
// products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
// USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use anyhow::Error;
use log::LevelFilter::{
    Debug as DebugL, Error as ErrorL, Info as InfoL, Trace as TraceL, Warn as WarnL,
};
use log::*;
use minotari_node_grpc_client::grpc::base_node_client::BaseNodeClient;
use minotari_node_grpc_client::grpc::{BlockHeader, ListHeadersRequest, MetaData};
use minotari_node_grpc_client::{
    grpc::{Empty, GetBlocksRequest},
    BaseNodeGrpcClient,
};
use primitive_types::U256;
use std::cmp::{max, min};
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::{env, fs};
use structopt::StructOpt;
use tari_common_types::types::FixedHash;
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
use tokio::time::{Duration, MissedTickBehavior};
use tokio::{signal, time};
use tonic::transport::Channel;

const LOG_SERVICE: &str = "grp_bridge::service";
const LOG_UPDATE: &str = "grp_bridge::update";
const LOG_CHAIN_SPLIT: &str = "grp_bridge::chain_split";

#[derive(StructOpt, Debug)]
struct CommandLineArgs {
    #[structopt(
        long,
        short = "c",
        default_value = "10",
        help = "The number of seconds between sync attempts"
    )]
    cadence_seconds: u64,
    #[structopt(
        long,
        default_value = "localhost",
        help = "The IP address of the first base node"
    )]
    ip1: String,
    #[structopt(
        long,
        default_value = "localhost",
        help = "The IP address of the second base node"
    )]
    ip2: String,
    #[structopt(
        long,
        short = "1",
        help = "The gRPC port number of the first base node"
    )]
    port1: String,
    #[structopt(
        long,
        short = "2",
        help = "The gRPC port number of the second base node"
    )]
    port2: String,
    #[structopt(
        long,
        short = "v",
        help = "Validate the chain with headers, otherwise just use the tip information"
    )]
    validate_with_headers: bool,
    #[structopt(
        long,
        short = "h",
        default_value = "100",
        help = "The initial number of headers to fetch from the other base node below its chain tip to find a chain split"
    )]
    initial_headers_back: u64,
    #[structopt(long, short = "g", default_value = "info")]
    log_level: String,
}

#[derive(Clone)]
struct SyncData {
    last_synced_header: Option<BlockHeader>,
}

impl SyncData {
    fn new() -> Self {
        SyncData {
            last_synced_header: Default::default(),
        }
    }
}

async fn get_tip_info(client: &mut BaseNodeGrpcClient<Channel>) -> Result<Option<MetaData>, Error> {
    let tip_info = client.get_tip_info(Empty {}).await?;
    Ok(tip_info.into_inner().metadata)
}

async fn fetch_and_submit_blocks(
    this_client: &mut BaseNodeGrpcClient<Channel>,
    other_client: &mut BaseNodeGrpcClient<Channel>,
    client_name: &str,
    sync_data: &mut SyncData,
    validate_with_headers: bool,
    initial_headers_back: u64,
    receiver: Receiver<()>,
) -> Result<(), Error> {
    // Fetch and compare meta data
    let this_meta_data = if let Some(data) = get_tip_info(this_client).await? {
        data
    } else {
        warn!(target: LOG_UPDATE, "Service '{}' - no tip info for this base node", client_name);
        return Ok(());
    };
    let this_accumulated_difficulty = U256::from_big_endian(&this_meta_data.accumulated_difficulty);
    debug!(
        target: LOG_UPDATE,
        "Service '{}' - this base node tip height: {}, hash: {}, accumulated difficulty: {}",
        client_name,
        this_meta_data.best_block_height,
        FixedHash::try_from(this_meta_data.best_block_hash.clone()).unwrap_or(FixedHash::zero()),
        this_accumulated_difficulty,
    );
    let other_meta_data = if let Some(data) = get_tip_info(other_client).await? {
        data
    } else {
        warn!(target: LOG_UPDATE, "Service '{}' - no tip info for other base node", client_name);
        return Ok(());
    };
    let other_accumulated_difficulty =
        U256::from_big_endian(&other_meta_data.accumulated_difficulty);
    debug!(
        target: LOG_UPDATE,
        "Service '{}' - other base node tip height: {}, hash: {}, accumulated difficulty: {}",
        client_name,
        other_meta_data.best_block_height,
        FixedHash::try_from(other_meta_data.best_block_hash.clone()).unwrap_or(FixedHash::zero()),
        other_accumulated_difficulty,
    );
    let last_height = sync_data
        .last_synced_header
        .as_ref()
        .map(|h| h.height)
        .unwrap_or(0);
    let last_block = sync_data
        .last_synced_header
        .as_ref()
        .map(|h| FixedHash::try_from(h.hash.clone()).unwrap_or(FixedHash::zero()))
        .unwrap_or(FixedHash::zero());
    info!(target: LOG_UPDATE, "Service '{}' - last synced {}, {}", client_name, last_height, last_block);

    if this_meta_data.best_block_hash == other_meta_data.best_block_hash {
        info!(target: LOG_UPDATE, "Service '{}' - nothing to do here, we are synced", client_name);
        return Ok(());
    } else if this_accumulated_difficulty >= other_accumulated_difficulty {
        info!(target: LOG_UPDATE, "Service '{}' - nothing to do here, we are ahead (or equal)", client_name);
        return Ok(());
    }

    // Fetch headers from the other client
    let last_synced_header = { sync_data.last_synced_header.clone() };
    let headers_back = if let Some(header) = last_synced_header {
        max(
            this_meta_data
                .best_block_height
                .saturating_sub(header.height),
            5,
        )
    } else {
        initial_headers_back
    };
    debug!(target: LOG_UPDATE, "Service '{}' - headers_back {}", client_name, headers_back);

    let mut other_headers = Vec::new();
    if validate_with_headers {
        let num_other_headers = other_meta_data
            .best_block_height
            .saturating_sub(this_meta_data.best_block_height)
            .saturating_add(headers_back) as usize;
        let mut other_headers_stream = other_client
            .list_headers(ListHeadersRequest {
                from_height: other_meta_data.best_block_height,
                num_headers: num_other_headers as u64,
                sorting: 0,
            })
            .await?
            .into_inner();
        while let Some(header) = other_headers_stream.message().await? {
            if receiver.has_changed()? {
                return Ok(());
            }
            let header = header.header.unwrap();
            other_headers.push(header.clone());
            trace!(
                target: LOG_UPDATE,
                "Service '{}' - received other base node header {}, {}",
                client_name, header.height, FixedHash::try_from(header.hash).unwrap_or(FixedHash::zero())
            );
        }
        debug!(target: LOG_UPDATE, "Service '{}' - streamed {} other base node headers", client_name, other_headers.len());
    }

    // See if our best header or one of our parent headers is in theirs, otherwise just fetch from a lower height
    let mut missing_blocks = vec![];
    if validate_with_headers {
        missing_blocks = if other_headers
            .iter()
            .any(|h| h.hash == this_meta_data.best_block_hash)
        {
            debug!(target: LOG_UPDATE, "Service '{}' - our best header is in the other chain", client_name);
            (this_meta_data.best_block_height..other_meta_data.best_block_height + 1)
                .collect::<Vec<_>>()
        } else if let Some(height) = find_chain_split(
            other_headers,
            this_client,
            this_meta_data.clone(),
            client_name,
        )
        .await?
        {
            debug!(target: LOG_UPDATE, "Service '{}' - chain split found at height {}", client_name, height);
            (height..other_meta_data.best_block_height + 1).collect::<Vec<_>>()
        } else {
            vec![]
        };
    }
    if missing_blocks.is_empty() {
        missing_blocks = if this_meta_data.best_block_height <= other_meta_data.best_block_height {
            debug!(target: LOG_UPDATE, "Service '{}' - our best block height <= theirs", client_name);
            (this_meta_data
                .best_block_height
                .saturating_sub(headers_back)..other_meta_data.best_block_height + 1)
                .collect::<Vec<_>>()
        } else {
            debug!(target: LOG_UPDATE, "Service '{}' - our best block height > theirs", client_name);
            (other_meta_data
                .best_block_height
                .saturating_sub(headers_back)..other_meta_data.best_block_height + 1)
                .collect::<Vec<_>>()
        };
    }

    // Stream and submit blocks - just one at a time
    debug!(
        target: LOG_UPDATE, "Service '{}' - attempting to stream blocks {} to {}",
        client_name, missing_blocks[0], missing_blocks.last().unwrap()
    );
    for block_number in &missing_blocks {
        if receiver.has_changed()? {
            return Ok(());
        }
        let mut block_stream = other_client
            .get_blocks(GetBlocksRequest {
                heights: vec![*block_number],
            })
            .await?
            .into_inner();
        while let Some(resp) = block_stream.message().await? {
            trace!(target: LOG_UPDATE, "Service '{}' - received block {} from other base node", client_name, block_number);
            let block = resp.block.unwrap();
            let header = block.header.clone().unwrap();
            let response = match this_client.submit_block(block).await {
                Ok(val) => val.into_inner(),
                Err(err) => {
                    warn!(
                        target: LOG_UPDATE,
                        "Service '{}' - error submitting block {} to this base node",
                        client_name, block_number
                    );
                    return Err(err.into());
                }
            };
            trace!(
                target: LOG_UPDATE,
                "Service '{}' - submitted block {} to this base node (hash response {})",
                client_name, block_number, FixedHash::try_from(response.block_hash).unwrap_or(FixedHash::zero())
            );
            sync_data.last_synced_header = Some(header);
        }
    }
    info!(target: LOG_UPDATE, "Service '{}' - synced {} blocks", client_name, missing_blocks.len());

    Ok(())
}

async fn find_chain_split(
    other_headers: Vec<BlockHeader>,
    this_client: &mut BaseNodeGrpcClient<Channel>,
    this_meta_data: MetaData,
    client_name: &str,
) -> Result<Option<u64>, Error> {
    info!(target: LOG_CHAIN_SPLIT, "Service '{}' - find chain split", client_name);
    let mut from_height = this_meta_data.best_block_height;
    const CHUNKS: u64 = 100;
    while from_height > 0 {
        let mut this_headers_stream = this_client
            .list_headers(ListHeadersRequest {
                from_height,
                num_headers: min(from_height, CHUNKS),
                sorting: 0,
            })
            .await?
            .into_inner();
        debug!(
            target: LOG_CHAIN_SPLIT, "Service '{}' - attempting to stream {} local headers from height {}",
            client_name, min(from_height, CHUNKS), from_height
        );
        while let Some(header) = this_headers_stream.message().await? {
            if let Some(split_header) = other_headers
                .iter()
                .find(|h| h.hash == header.header.as_ref().unwrap().hash)
            {
                debug!(target: LOG_CHAIN_SPLIT, "Service '{}' - found chain split", client_name);
                return Ok(Some(split_header.height));
            }
        }
        from_height = from_height.saturating_sub(CHUNKS);
    }
    Ok(None)
}

fn log_and_print(log_level: LevelFilter, msg: &str, add_newline: bool) {
    match log_level {
        TraceL => trace!(target: LOG_SERVICE, "{}", msg),
        DebugL => debug!(target: LOG_SERVICE, "{}", msg),
        InfoL => info!(target: LOG_SERVICE, "{}", msg),
        WarnL => warn!(target: LOG_SERVICE, "{}", msg),
        ErrorL => error!(target: LOG_SERVICE, "{}", msg),
        _ => {}
    }
    if add_newline {
        println!("\n{}", msg);
    } else {
        println!("{}", msg);
    }
}

#[allow(clippy::too_many_arguments)]
async fn sync_service(
    this_client_url: String,
    other_client_url: String,
    cadence: u64,
    client_name: String,
    validate_with_headers: bool,
    initial_headers_back: u64,
    mut shutdown_rx: watch::Receiver<()>,
    connected_tx: Sender<()>,
) -> Result<(), Error> {
    let mut sync_data = SyncData::new();
    info!(target: LOG_SERVICE, "Service '{}' starting", client_name);
    let mut count = 0u64;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                log_and_print(
                    InfoL,
                    &format!("Shutdown signal for '{}' received before connecting clients", client_name),
                    false
                );
                return Ok(());
            }

            result = async {
                let this_client = BaseNodeGrpcClient::connect(this_client_url.clone()).await;
                let other_client = BaseNodeGrpcClient::connect(other_client_url.clone()).await;
                match (this_client, other_client) {
                    (Ok(this_client), Ok(other_client)) => {
                        log_and_print(
                            InfoL,
                            &format!(
                                "Service '{}' connected to {} and {}",
                                client_name, this_client_url.clone(), other_client_url.clone()
                            ),
                            true
                        );
                        Ok::<(BaseNodeClient<Channel>, BaseNodeClient<Channel>), Error>((this_client, other_client))
                    },
                    (Ok(_), Err(_)) => {
                        log_and_print(
                            WarnL,
                            &format!(
                                "Service '{}' could not connect to {} ... ('Ctrl-C' to quit)",
                                client_name, other_client_url.clone()
                            ),
                            false
                        );
                        Err(anyhow::anyhow!("Failed to connect to clients"))
                    },
                    (Err(_), Ok(_)) => {
                        log_and_print(
                            WarnL,
                            &format!(
                                "Service '{}' could not connect to {} ... ('Ctrl-C' to quit)",
                                client_name, this_client_url.clone()
                            ),
                            false
                        );
                        Err(anyhow::anyhow!("Failed to connect to clients"))
                    },
                    _ => {
                        log_and_print(
                            WarnL,
                            &format!(
                                "Service '{}' could not connect to {} or {} ... ('Ctrl-C' to quit)",
                                client_name, this_client_url.clone(), other_client_url.clone()
                            ),
                            false
                        );
                        Err(anyhow::anyhow!("Failed to connect to clients"))
                    },
                }
            } => {
                match result {
                    Ok((mut this_client, mut other_client)) => {
                        let _ = connected_tx.send(());
                        let mut interval = time::interval(Duration::from_secs(cadence));
                        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                        tokio::pin!(interval);
                        loop {
                            count += 1;
                            debug!(target: LOG_SERVICE, "Service '{}' sync loop {}", client_name, count);
                            tokio::select! {
                                _ = interval.tick() => if let Err(e) = fetch_and_submit_blocks(
                                        &mut this_client,
                                        &mut other_client,
                                        &client_name,
                                        &mut sync_data,
                                        validate_with_headers,
                                        initial_headers_back,
                                        shutdown_rx.clone()
                                    ).await {
                                        log_and_print(
                                            ErrorL, &format!("Service '{}' error: {}", client_name, e), false
                                        );
                                        // Get new client connections
                                        break;
                                    },
                                _ = shutdown_rx.changed() => {
                                    log_and_print(
                                        InfoL, &format!("Service '{}' shutdown signal received", client_name), false
                                    );
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(2500)).await;
                    }
                }
            }
        }
    }
}

fn init_logger(log_level: LevelFilter) -> Result<(), Box<dyn std::error::Error>> {
    let log_path = env::current_exe()?
        .parent()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no exe path"))?
        .canonicalize()?
        .join("log");
    let log_file = log_path.join("bridge.log");
    println!("Logging data to '{}'", log_file.display());
    if !log_path.exists() {
        fs::create_dir(&log_path)?;
    }
    let file = File::create(log_file)?;

    env_logger::Builder::new()
        .format(move |buf, record| {
            let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            writeln!(buf, "{} {} - {}", timestamp, record.level(), record.args())
        })
        .filter(Some("grp_bridge"), log_level)
        .format_timestamp_millis()
        .target(env_logger::Target::Pipe(Box::new(file)))
        .init();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!();
    println!("gRPC Bridge Service Running - 'Ctrl-C' to quit");
    println!();

    let args = CommandLineArgs::from_args();
    init_logger(
        LevelFilter::from_str(&args.log_level)
            .map_err(|_e| {
                let levels = LevelFilter::iter()
                    .map(|l| l.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");
                format!("Invalid log level, choose from '{}'", levels)
            })
            .expect(""),
    )
    .expect("Failed to initialize logger");

    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let client1_url = format!("http://{}:{}", args.ip1, args.port1);
    let client2_url = format!("http://{}:{}", args.ip2, args.port2);

    let (client1_connected_tx, mut client1_connected_rx) = watch::channel(());
    let service1 = sync_service(
        client1_url.clone(),
        client2_url.clone(),
        args.cadence_seconds,
        args.port1.clone(),
        args.validate_with_headers,
        args.initial_headers_back,
        shutdown_rx.clone(),
        client1_connected_tx,
    );

    let (client2_connected_tx, mut client2_connected_rx) = watch::channel(());
    let service2 = sync_service(
        client2_url,
        client1_url,
        args.cadence_seconds,
        args.port2.clone(),
        args.validate_with_headers,
        args.initial_headers_back,
        shutdown_rx,
        client2_connected_tx,
    );

    let handle1 = tokio::spawn(service1);
    if client1_connected_rx.changed().await.is_ok() {
        println!();
        println!("Spawned service for {}", args.port1);
    }
    let handle2 = tokio::spawn(service2);
    if client2_connected_rx.changed().await.is_ok() {
        println!();
        println!("Spawned service for {}", args.port2);
    }

    // Wait for a shutdown signal (e.g., Ctrl+C)
    println!();
    println!("'Ctrl-C' to quit");
    println!();
    signal::ctrl_c().await?;
    println!("Shutdown signal received, shutting down services...");
    println!();
    let _ = shutdown_tx.send(());

    // Await the completion of the services
    let _ = handle1.await?;
    let _ = handle2.await?;
    println!();

    Ok(())
}
