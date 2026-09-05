use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================
// CONFIGURATION
// ============================================================

const MAX_RETRY_ROUNDS: usize = 12;
const INITIAL_RETRY_DELAY_SECS: u64 = 10;
const MAX_RETRY_DELAY_SECS: u64 = 120;

const PART_SIZE: u64 = 1_000_000;

const DEFAULT_BATCH_SIZE: u64 = 5;
const DEFAULT_CONCURRENCY: usize = 4;

// ============================================================
// RPC CONFIGURATION
// ============================================================

fn rpc_list(chain: &str) -> Vec<String> {
    match chain.to_lowercase().as_str() {
        "bnb" | "bsc" => vec![
            "https://bsc-dataseed.binance.org/".to_string(),
            "https://bsc-dataseed1.defibit.io/".to_string(),
            "https://bsc-dataseed1.ninicoin.io/".to_string(),
            "https://bsc-dataseed2.defibit.io/".to_string(),
            "https://bsc-dataseed2.ninicoin.io/".to_string(),
            "https://rpc.ankr.com/bsc".to_string(),
            "https://1rpc.io/bnb".to_string(),
        ],

        "ethereum" | "eth" => vec![
            "https://eth.llamarpc.com".to_string(),
            "https://cloudflare-eth.com".to_string(),
            "https://rpc.ankr.com/eth".to_string(),
            "https://1rpc.io/eth".to_string(),
        ],

        "polygon" | "matic" => vec![
            "https://polygon-rpc.com".to_string(),
            "https://rpc.ankr.com/polygon".to_string(),
            "https://1rpc.io/matic".to_string(),
        ],

        "arbitrum" | "arb" => vec![
            "https://arb1.arbitrum.io/rpc".to_string(),
            "https://rpc.ankr.com/arbitrum".to_string(),
            "https://1rpc.io/arb".to_string(),
        ],

        "base" => vec![
            "https://mainnet.base.org".to_string(),
            "https://base.llamarpc.com".to_string(),
            "https://1rpc.io/base".to_string(),
        ],

        "optimism" | "op" => vec![
            "https://mainnet.optimism.io".to_string(),
            "https://optimism.llamarpc.com".to_string(),
            "https://1rpc.io/op".to_string(),
        ],

        "avalanche_c" | "avalanche" | "avax" => vec![
            "https://api.avax.network/ext/bc/C/rpc".to_string(),
            "https://rpc.ankr.com/avalanche".to_string(),
            "https://1rpc.io/avax/c".to_string(),
        ],

        _ => vec![
            "https://bsc-dataseed.binance.org/".to_string(),
            "https://rpc.ankr.com/bsc".to_string(),
            "https://1rpc.io/bnb".to_string(),
        ],
    }
}

// ============================================================
// ERROR TYPE
// ============================================================

#[derive(Debug, Clone)]
struct RpcError {
    message: String,
}

impl RpcError {
    fn new<S: Into<String>>(message: S) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for RpcError {}

// ============================================================
// HEX PARSER
// ============================================================

fn parse_hex_u64(value: &str) -> Result<u64, RpcError> {
    let clean = value.trim().trim_start_matches("0x");

    if clean.is_empty() {
        return Err(RpcError::new("empty hexadecimal value"));
    }

    u64::from_str_radix(clean, 16)
        .map_err(|e| RpcError::new(format!("invalid hex number '{}': {}", value, e)))
}

// ============================================================
// SINGLE RPC BATCH REQUEST
// ============================================================

async fn request_batch(
    client: &Client,
    rpc: &str,
    blocks: &[u64],
) -> Result<HashMap<u64, Value>, RpcError> {
    if blocks.is_empty() {
        return Ok(HashMap::new());
    }

    // --------------------------------------------------------
    // JSON-RPC batch payload
    // --------------------------------------------------------

    let payload: Vec<Value> = blocks
        .iter()
        .map(|block| {
            json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [
                    format!("0x{:x}", block),
                    true
                ],
                "id": *block
            })
        })
        .collect();

    let response = client
        .post(rpc)
        .json(&payload)
        .send()
        .await
        .map_err(|e| {
            RpcError::new(format!(
                "HTTP request error: {}",
                e
            ))
        })?;

    let status = response.status();

    if !status.is_success() {
        return Err(RpcError::new(format!(
            "HTTP status {}",
            status
        )));
    }

    let text = response
        .text()
        .await
        .map_err(|e| {
            RpcError::new(format!(
                "response body error: {}",
                e
            ))
        })?;

    // --------------------------------------------------------
    // Parse JSON
    // --------------------------------------------------------

    let parsed: Value = serde_json::from_str(&text)
        .map_err(|e| {
            RpcError::new(format!(
                "invalid JSON: {} | body: {}",
                e,
                text.chars()
                    .take(300)
                    .collect::<String>()
            ))
        })?;

    // --------------------------------------------------------
    // Response must be an array
    // --------------------------------------------------------

    let array = match parsed {
        Value::Array(arr) => arr,

        Value::Object(obj) => {
            if let Some(error) = obj.get("error") {
                return Err(RpcError::new(format!(
                    "RPC error: {}",
                    error
                )));
            }

            return Err(RpcError::new(
                "RPC returned object instead of batch array",
            ));
        }

        _ => {
            return Err(RpcError::new(
                "RPC returned invalid JSON structure",
            ));
        }
    };

    // --------------------------------------------------------
    // Every requested block must be returned
    // --------------------------------------------------------

    if array.len() != blocks.len() {
        return Err(RpcError::new(format!(
            "incomplete batch: received {} / {} blocks",
            array.len(),
            blocks.len()
        )));
    }

    let requested: HashSet<u64> =
        blocks.iter().copied().collect();

    let mut results: HashMap<u64, Value> =
        HashMap::new();

    // --------------------------------------------------------
    // Validate each block
    // --------------------------------------------------------

    for item in array {
        if let Some(error) = item.get("error") {
            return Err(RpcError::new(format!(
                "RPC item error: {}",
                error
            )));
        }

        let id = item
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                RpcError::new(
                    "batch item missing numeric id",
                )
            })?;

        if !requested.contains(&id) {
            return Err(RpcError::new(format!(
                "unexpected block id {}",
                id
            )));
        }

        let result = item
            .get("result")
            .ok_or_else(|| {
                RpcError::new(format!(
                    "block {} missing result",
                    id
                ))
            })?;

        // ----------------------------------------------------
        // Null block is not accepted
        // ----------------------------------------------------

        if result.is_null() {
            return Err(RpcError::new(format!(
                "block {} returned null result",
                id
            )));
        }

        // ----------------------------------------------------
        // Verify block number
        // ----------------------------------------------------

        let block_number = result
            .get("number")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                RpcError::new(format!(
                    "block {} missing block number",
                    id
                ))
            })?;

        let actual_number =
            parse_hex_u64(block_number)?;

        if actual_number != id {
            return Err(RpcError::new(format!(
                "block mismatch: requested {}, received {}",
                id,
                actual_number
            )));
        }

        // ----------------------------------------------------
        // Verify transactions array
        // ----------------------------------------------------

        let transactions = result
            .get("transactions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                RpcError::new(format!(
                    "block {} missing transactions array",
                    id
                ))
            })?;

        // ----------------------------------------------------
        // DEBUG: transaction count
        // ----------------------------------------------------

        if !transactions.is_empty() {
            println!(
                "Validated block {} | Transactions: {}",
                id,
                transactions.len()
            );
        }

        results.insert(
            id,
            result.clone(),
        );
    }

    // --------------------------------------------------------
    // Final validation
    // --------------------------------------------------------

    if results.len() != blocks.len() {
        return Err(RpcError::new(format!(
            "validation failed: {} / {} blocks",
            results.len(),
            blocks.len()
        )));
    }

    Ok(results)
}

// ============================================================
// EXTRACT ADDRESSES FROM BATCH
// ============================================================

fn extract_addresses(
    blocks: &[u64],
    blocks_map: &HashMap<u64, Value>,
) -> Result<(Vec<String>, usize), RpcError> {
    let mut addresses: Vec<String> = Vec::new();

    let mut transaction_count = 0usize;

    for block_number in blocks {
        let block = blocks_map
            .get(block_number)
            .ok_or_else(|| {
                RpcError::new(format!(
                    "missing validated block {}",
                    block_number
                ))
            })?;

        let transactions = block
            .get("transactions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                RpcError::new(format!(
                    "block {} missing transactions array",
                    block_number
                ))
            })?;

        transaction_count += transactions.len();

        for tx in transactions {
            if let Some(from) =
                tx.get("from").and_then(|v| v.as_str())
            {
                addresses.push(
                    from.to_ascii_lowercase()
                );
            }

            if let Some(to) =
                tx.get("to").and_then(|v| v.as_str())
            {
                addresses.push(
                    to.to_ascii_lowercase()
                );
            }
        }
    }

    Ok((addresses, transaction_count))
}

// ============================================================
// FETCH BATCH WITH RPC FAILOVER + RETRY
// ============================================================

async fn fetch_batch_with_retry(
    client: &Client,
    rpcs: &[String],
    blocks: Vec<u64>,
) -> Result<Vec<String>, RpcError> {
    let first_block =
        *blocks.first().unwrap_or(&0);

    let last_block =
        *blocks.last().unwrap_or(&0);

    for retry_round in 1..=MAX_RETRY_ROUNDS {
        println!(
            "Trying batch {}-{} | retry round {}/{}",
            first_block,
            last_block,
            retry_round,
            MAX_RETRY_ROUNDS
        );

        // ----------------------------------------------------
        // Try every RPC
        // ----------------------------------------------------

        for (rpc_index, rpc) in
            rpcs.iter().enumerate()
        {
            let result =
                request_batch(
                    client,
                    rpc,
                    &blocks,
                )
                .await;

            match result {
                Ok(blocks_map) => {
                    match extract_addresses(
                        &blocks,
                        &blocks_map,
                    ) {
                        Ok((
                            addresses,
                            transaction_count,
                        )) => {
                            println!(
                                "Batch {}-{} recovered using RPC #{} (attempt {}) | Transactions: {} | Addresses: {}",
                                first_block,
                                last_block,
                                rpc_index + 1,
                                retry_round,
                                transaction_count,
                                addresses.len()
                            );

                            return Ok(addresses);
                        }

                        Err(error) => {
                            println!(
                                "Batch {}-{} RPC #{} extraction validation failed: {}",
                                first_block,
                                last_block,
                                rpc_index + 1,
                                error
                            );
                        }
                    }
                }

                Err(error) => {
                    println!(
                        "Batch {}-{} failed on RPC #{}: {}",
                        first_block,
                        last_block,
                        rpc_index + 1,
                        error
                    );
                }
            }

            // Small delay between providers.
            sleep(Duration::from_millis(250))
                .await;
        }

        // ----------------------------------------------------
        // All RPCs failed
        // ----------------------------------------------------

        if retry_round < MAX_RETRY_ROUNDS {
            let multiplier =
                2u64.saturating_pow(
                    (retry_round - 1)
                        .min(4) as u32,
                );

            let delay =
                (
                    INITIAL_RETRY_DELAY_SECS
                        * multiplier
                )
                .min(MAX_RETRY_DELAY_SECS);

            println!();
            println!(
                "ALL RPCs failed for batch {}-{}.",
                first_block,
                last_block
            );

            println!(
                "Waiting {} seconds before retry round {}/{}...",
                delay,
                retry_round + 1,
                MAX_RETRY_ROUNDS
            );
            println!();

            sleep(
                Duration::from_secs(delay)
            )
            .await;
        }
    }

    Err(RpcError::new(format!(
        "batch {}-{} failed after {} retry rounds across all RPCs",
        first_block,
        last_block,
        MAX_RETRY_ROUNDS
    )))
}

// ============================================================
// UPLOAD TO GITHUB RELEASE
// ============================================================

fn upload_to_release(
    tag: &str,
    file_name: &str,
) -> bool {
    println!();
    println!("==============================================");
    println!("UPLOADING FILE");
    println!("==============================================");
    println!("File    : {}", file_name);
    println!("Release : {}", tag);

    for attempt in 1..=5 {
        println!(
            "Upload attempt {}/5...",
            attempt
        );

        let status = Command::new("gh")
            .args([
                "release",
                "upload",
                tag,
                file_name,
                "--clobber",
            ])
            .status();

        match status {
            Ok(s) if s.success() => {
                println!(
                    "Successfully uploaded: {}",
                    file_name
                );

                if let Err(e) =
                    fs::remove_file(file_name)
                {
                    eprintln!(
                        "Warning: could not remove local file {}: {}",
                        file_name,
                        e
                    );
                }

                return true;
            }

            _ => {
                eprintln!(
                    "Upload failed on attempt {}",
                    attempt
                );

                std::thread::sleep(
                    Duration::from_secs(5)
                );
            }
        }
    }

    eprintln!(
        "FAILED to upload {} after 5 attempts.",
        file_name
    );

    false
}

// ============================================================
// WRITE COMPRESSED CSV
// ============================================================

fn write_addresses_file(
    chain: &str,
    part_num: u32,
    start_block: u64,
    end_block: u64,
    addresses: &HashSet<String>,
) -> Result<String, Box<dyn Error>> {
    fs::create_dir_all("output")?;

    let file_name = format!(
        "output/{}_blocks_{}_to_{}_part_{:03}.csv.gz",
        chain,
        start_block,
        end_block,
        part_num
    );

    let file =
        File::create(&file_name)?;

    let encoder =
        GzEncoder::new(
            file,
            Compression::default(),
        );

    let mut writer =
        csv::Writer::from_writer(
            encoder
        );

    writer.write_record(["address"])?;

    let mut sorted_addresses:
        Vec<&String> =
        addresses.iter().collect();

    sorted_addresses.sort_unstable();

    for address in sorted_addresses {
        writer.write_record([address])?;
    }

    writer.flush()?;

    println!();
    println!("==============================================");
    println!("PART COMPLETED");
    println!("==============================================");
    println!("Part           : {}", part_num);
    println!(
        "Blocks         : {} -> {}",
        start_block,
        end_block
    );
    println!(
        "Unique Address : {}",
        addresses.len()
    );
    println!("File           : {}", file_name);
    println!("==============================================");

    Ok(file_name)
}

// ============================================================
// GET LATEST BLOCK
// ============================================================

async fn get_latest_block(
    client: &Client,
    rpcs: &[String],
) -> Result<u64, RpcError> {
    for (index, rpc) in
        rpcs.iter().enumerate()
    {
        println!(
            "Checking RPC #{} for latest block...",
            index + 1
        );

        let request = client
            .post(rpc)
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            }))
            .send()
            .await;

        let response = match request {
            Ok(r) => r,

            Err(e) => {
                println!(
                    "RPC #{} connection failed: {}",
                    index + 1,
                    e
                );

                continue;
            }
        };

        if !response.status().is_success() {
            println!(
                "RPC #{} returned HTTP {}",
                index + 1,
                response.status()
            );

            continue;
        }

        let value =
            match response
                .json::<Value>()
                .await
            {
                Ok(v) => v,

                Err(e) => {
                    println!(
                        "RPC #{} returned invalid JSON: {}",
                        index + 1,
                        e
                    );

                    continue;
                }
            };

        if let Some(error) =
            value.get("error")
        {
            println!(
                "RPC #{} returned RPC error: {}",
                index + 1,
                error
            );

            continue;
        }

        if let Some(result) =
            value
                .get("result")
                .and_then(|v| v.as_str())
        {
            if let Ok(block) =
                parse_hex_u64(result)
            {
                println!(
                    "Connected successfully to RPC #{} | Latest block: {}",
                    index + 1,
                    block
                );

                return Ok(block);
            }
        }

        println!(
            "RPC #{} did not return valid block height.",
            index + 1
        );
    }

    Err(RpcError::new(
        "could not obtain latest block from any RPC",
    ))
}

// ============================================================
// ARGUMENT PARSER
// ============================================================

fn get_arg(name: &str) -> Option<String> {
    let args: Vec<String> =
        env::args().collect();

    for i in 0..args.len() {
        if args[i] == name {
            if i + 1 < args.len() {
                return Some(
                    args[i + 1].clone()
                );
            }
        }
    }

    None
}

// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!();
    println!("=======================================================");
    println!("        EVM BLOCKWISE ADDRESS EXTRACTOR v2.2");
    println!("=======================================================");
    println!();

    // --------------------------------------------------------
    // Chain
    // --------------------------------------------------------

    let chain =
        get_arg("--chain")
            .or_else(|| {
                env::var("CHAIN").ok()
            })
            .unwrap_or_else(|| {
                "bnb".to_string()
            });

    // --------------------------------------------------------
    // Start block
    // --------------------------------------------------------

    let start_block: u64 =
        get_arg("--start-block")
            .or_else(|| {
                env::var("START_BLOCK").ok()
            })
            .unwrap_or_else(|| {
                "0".to_string()
            })
            .parse()?;

    // --------------------------------------------------------
    // End block
    // --------------------------------------------------------

    let end_block: u64 =
        get_arg("--end-block")
            .or_else(|| {
                env::var("END_BLOCK").ok()
            })
            .unwrap_or_else(|| {
                "0".to_string()
            })
            .parse()?;

    // --------------------------------------------------------
    // Batch size
    // --------------------------------------------------------

    let batch_size: u64 =
        get_arg("--batch-size")
            .or_else(|| {
                env::var("BATCH_SIZE").ok()
            })
            .unwrap_or_else(|| {
                DEFAULT_BATCH_SIZE.to_string()
            })
            .parse()?;

    // --------------------------------------------------------
    // Concurrency
    // --------------------------------------------------------

    let concurrency: usize =
        get_arg("--concurrency")
            .or_else(|| {
                env::var("CONCURRENCY").ok()
            })
            .unwrap_or_else(|| {
                DEFAULT_CONCURRENCY.to_string()
            })
            .parse()?;

    // --------------------------------------------------------
    // Validation
    // --------------------------------------------------------

    if start_block > end_block {
        return Err(
            "start_block cannot be greater than end_block"
                .into(),
        );
    }

    if batch_size == 0 {
        return Err(
            "batch_size must be greater than 0"
                .into(),
        );
    }

    if concurrency == 0 {
        return Err(
            "concurrency must be greater than 0"
                .into(),
        );
    }

    println!(
        "Chain       : {}",
        chain
    );

    println!(
        "Start Block : {}",
        start_block
    );

    println!(
        "End Block   : {}",
        end_block
    );

    println!(
        "Batch Size  : {}",
        batch_size
    );

    println!(
        "Concurrency : {}",
        concurrency
    );

    println!();

    // --------------------------------------------------------
    // RPCs
    // --------------------------------------------------------

    let rpcs =
        rpc_list(&chain);

    println!(
        "RPC providers configured: {}",
        rpcs.len()
    );

    for (i, rpc) in
        rpcs.iter().enumerate()
    {
        println!(
            "  RPC #{}: {}",
            i + 1,
            rpc
        );
    }

    println!();

    // --------------------------------------------------------
    // HTTP client
    // --------------------------------------------------------

    let client =
        Client::builder()
            .connect_timeout(
                Duration::from_secs(15)
            )
            .timeout(
                Duration::from_secs(60)
            )
            .pool_idle_timeout(
                Duration::from_secs(30)
            )
            .pool_max_idle_per_host(8)
            .user_agent(
                "evm-blockwise-extractor/2.2"
            )
            .build()?;

    let client =
        Arc::new(client);

    // ========================================================
    // CHECK LATEST BLOCK
    // ========================================================

    let latest_block =
        get_latest_block(
            &client,
            &rpcs,
        )
        .await?;

    println!();

    println!(
        "Latest chain block: {}",
        latest_block
    );

    if end_block > latest_block {
        return Err(
            format!(
                "Requested end block {} is greater than latest chain block {}",
                end_block,
                latest_block
            )
            .into(),
        );
    }

    // ========================================================
    // RELEASE TAG
    // ========================================================

    let release_tag =
        env::var("RELEASE_TAG")
            .ok();

    if let Some(tag) =
        &release_tag
    {
        println!(
            "GitHub Release: {}",
            tag
        );
    } else {
        println!(
            "GitHub Release: not configured"
        );
    }

    // ========================================================
    // PART LOOP
    // ========================================================

    let mut part_start =
        start_block;

    let mut part_num: u32 =
        1;

    while part_start <= end_block {
        let part_end =
            part_start
                .saturating_add(
                    PART_SIZE - 1
                )
                .min(end_block);

        println!();

        println!(
            "======================================================="
        );

        println!(
            "PART {:03}",
            part_num
        );

        println!(
            "Blocks: {} -> {}",
            part_start,
            part_end
        );

        println!(
            "======================================================="
        );

        // ----------------------------------------------------
        // Create batches
        // ----------------------------------------------------

        let mut batches:
            Vec<Vec<u64>> =
            Vec::new();

        let mut current =
            part_start;

        while current <= part_end {
            let batch_end =
                current
                    .saturating_add(
                        batch_size - 1
                    )
                    .min(part_end);

            let batch:
                Vec<u64> =
                (current..=batch_end)
                    .collect();

            batches.push(batch);

            if batch_end == u64::MAX {
                break;
            }

            current =
                batch_end + 1;
        }

        let total_batches =
            batches.len();

        println!(
            "Total batches: {}",
            total_batches
        );

        println!();

        // ----------------------------------------------------
        // Shared data
        // ----------------------------------------------------

        let shared_client =
            Arc::clone(&client);

        let shared_rpcs =
            Arc::new(rpcs.clone());

        // ----------------------------------------------------
        // Process batches
        // ----------------------------------------------------

        let mut processing_stream =
            stream::iter(batches)
                .map(|batch| {
                    let client =
                        Arc::clone(
                            &shared_client
                        );

                    let rpcs =
                        Arc::clone(
                            &shared_rpcs
                        );

                    async move {
                        let first =
                            *batch
                                .first()
                                .unwrap();

                        let last =
                            *batch
                                .last()
                                .unwrap();

                        let result =
                            fetch_batch_with_retry(
                                &client,
                                &rpcs,
                                batch,
                            )
                            .await;

                        (
                            first,
                            last,
                            result,
                        )
                    }
                })
                .buffer_unordered(
                    concurrency
                );

        let mut unique_addresses:
            HashSet<String> =
            HashSet::new();

        let mut processed_batches:
            usize = 0;

        let mut total_transactions:
            u64 = 0;

        let mut total_addresses_seen:
            u64 = 0;

        // ----------------------------------------------------
        // Receive completed batches
        // ----------------------------------------------------

        while let Some(
            (
                first,
                last,
                result,
            )
        ) =
            processing_stream.next().await
        {
            match result {
                Ok(addresses) => {
                    total_addresses_seen +=
                        addresses.len()
                            as u64;

                    total_transactions +=
                        (addresses.len()
                            / 2)
                            as u64;

                    for address
                        in addresses
                    {
                        unique_addresses
                            .insert(address);
                    }
                }

                Err(error) => {
                    return Err(
                        format!(
                            "PERMANENT EXTRACTION ERROR: batch {}-{} failed: {}",
                            first,
                            last,
                            error
                        )
                        .into(),
                    );
                }
            }

            processed_batches += 1;

            if processed_batches
                % 100
                == 0
                || processed_batches
                    == total_batches
            {
                let percentage =
                    (
                        processed_batches
                            as f64
                        / total_batches
                            as f64
                    )
                        * 100.0;

                let blocks_processed =
                    (
                        processed_batches
                            as u64
                        * batch_size
                    )
                    .min(
                        part_end
                            - part_start
                            + 1
                    );

                println!(
                    "Progress: {}/{} ({:.2}%) | Blocks: {} | Transactions approx: {} | Addresses seen: {} | Unique addresses: {}",
                    processed_batches,
                    total_batches,
                    percentage,
                    blocks_processed,
                    total_transactions,
                    total_addresses_seen,
                    unique_addresses.len()
                );
            }
        }

        // ====================================================
        // WRITE PART
        // ====================================================

        let file_name =
            write_addresses_file(
                &chain,
                part_num,
                part_start,
                part_end,
                &unique_addresses,
            )?;

        // ====================================================
        // UPLOAD
        // ====================================================

        if let Some(tag) =
            release_tag.as_deref()
        {
            let uploaded =
                upload_to_release(
                    tag,
                    &file_name,
                );

            if !uploaded {
                return Err(
                    format!(
                        "Could not upload {} to GitHub Release",
                        file_name
                    )
                    .into(),
                );
            }
        }

        // ====================================================
        // NEXT PART
        // ====================================================

        if part_end == u64::MAX {
            break;
        }

        part_start =
            part_end + 1;

        part_num += 1;

        println!();
        println!(
            "Moving to next part..."
        );
    }

    // ========================================================
    // COMPLETE
    // ========================================================

    println!();

    println!(
        "======================================================="
    );

    println!(
        "ALL BLOCKS EXTRACTED SUCCESSFULLY"
    );

    println!(
        "======================================================="
    );

    println!(
        "Chain: {}",
        chain
    );

    println!(
        "Blocks: {} -> {}",
        start_block,
        end_block
    );

    println!(
        "======================================================="
    );

    Ok(())
}
