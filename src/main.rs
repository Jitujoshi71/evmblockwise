use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================
// CONFIGURATION
// ============================================================

const BLOCKS_PER_PART: u64 = 1_000_000;

// RPC batch size
const BLOCKS_PER_BATCH: u64 = 5;

// Concurrent RPC jobs
const CONCURRENCY: usize = 4;

// Retry same RPC
const RPC_RETRIES: usize = 2;

// Number of complete retry rounds for missing blocks
const FULL_RETRY_ROUNDS: usize = 8;

// HTTP timeout
const RPC_TIMEOUT_SECONDS: u64 = 45;

// Delay between retry attempts
const RETRY_DELAY_MS: u64 = 500;

// ============================================================
// RPC LIST
// ============================================================

fn get_rpc_list(chain: &str) -> Vec<String> {
    let mut rpcs = Vec::new();

    // User supplied RPC gets first priority.
    if let Ok(custom) = env::var("RPC_URL") {
        let custom = custom.trim();

        if !custom.is_empty() {
            rpcs.push(custom.to_string());
        }
    }

    let defaults: Vec<&str> = match chain {
        "bnb" => vec![
            "https://bsc-dataseed.binance.org/",
            "https://bsc-dataseed1.defibit.io/",
            "https://bsc-dataseed1.ninicoin.io/",
            "https://bsc-dataseed2.binance.org/",
            "https://bsc-dataseed3.binance.org/",
            "https://rpc.ankr.com/bsc",
            "https://1rpc.io/bnb",
        ],

        "ethereum" => vec![
            "https://eth.llamarpc.com",
            "https://cloudflare-eth.com",
            "https://rpc.ankr.com/eth",
            "https://1rpc.io/eth",
        ],

        "polygon" => vec![
            "https://polygon-rpc.com",
            "https://rpc.ankr.com/polygon",
            "https://1rpc.io/matic",
        ],

        "arbitrum" => vec![
            "https://arb1.arbitrum.io/rpc",
            "https://rpc.ankr.com/arbitrum",
            "https://1rpc.io/arb",
        ],

        "base" => vec![
            "https://mainnet.base.org",
            "https://base.llamarpc.com",
            "https://1rpc.io/base",
        ],

        "optimism" => vec![
            "https://mainnet.optimism.io",
            "https://optimism.llamarpc.com",
            "https://1rpc.io/op",
        ],

        "avalanche_c" => vec![
            "https://api.avax.network/ext/bc/C/rpc",
            "https://rpc.ankr.com/avalanche",
            "https://1rpc.io/avax/c",
        ],

        _ => vec![],
    };

    for rpc in defaults {
        if !rpcs.iter().any(|x| x == rpc) {
            rpcs.push(rpc.to_string());
        }
    }

    rpcs
}

// ============================================================
// HEX BLOCK NUMBER
// ============================================================

fn hex_to_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(
        value.trim_start_matches("0x"),
        16,
    )
    .ok()
}

// ============================================================
// SEND SINGLE RPC REQUEST
// ============================================================

async fn send_rpc(
    client: &reqwest::Client,
    rpc: &str,
    payload: &Value,
) -> Result<Value, String> {
    let response = client
        .post(rpc)
        .json(payload)
        .send()
        .await
        .map_err(|e| format!("request error: {}", e))?;

    let status = response.status();

    if !status.is_success() {
        return Err(format!(
            "HTTP {}",
            status
        ));
    }

    let body = response
        .text()
        .await
        .map_err(|e| format!("read error: {}", e))?;

    if body.trim().is_empty() {
        return Err(
            "empty response".to_string()
        );
    }

    serde_json::from_str::<Value>(&body)
        .map_err(|e| {
            format!(
                "invalid JSON: {}",
                e
            )
        })
}

// ============================================================
// GET LATEST BLOCK
// ============================================================

async fn get_latest_block(
    client: &reqwest::Client,
    rpcs: &[String],
) -> Result<u64, String> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    });

    let mut last_error =
        String::new();

    for (index, rpc) in rpcs.iter().enumerate() {
        for attempt in 1..=RPC_RETRIES {
            match send_rpc(
                client,
                rpc,
                &payload,
            )
            .await
            {
                Ok(value) => {
                    if let Some(result) =
                        value
                            .get("result")
                            .and_then(|v| v.as_str())
                    {
                        if let Some(block) =
                            hex_to_u64(result)
                        {
                            println!(
                                "RPC #{} OK: {}",
                                index + 1,
                                rpc
                            );

                            return Ok(block);
                        }
                    }

                    last_error =
                        format!(
                            "{} returned invalid blockNumber",
                            rpc
                        );
                }

                Err(error) => {
                    last_error =
                        format!(
                            "{} -> {}",
                            rpc,
                            error
                        );
                }
            }

            if attempt < RPC_RETRIES {
                sleep(
                    Duration::from_millis(
                        RETRY_DELAY_MS
                            * attempt as u64
                    )
                )
                .await;
            }
        }

        eprintln!(
            "Latest block RPC #{} failed: {}",
            index + 1,
            last_error
        );
    }

    Err(format!(
        "All RPCs failed: {}",
        last_error
    ))
}

// ============================================================
// EXTRACT ADDRESSES FROM BLOCK
// ============================================================

fn extract_addresses(
    block: &Value,
) -> Vec<String> {
    let mut addresses =
        Vec::new();

    let transactions =
        match block
            .get("transactions")
            .and_then(|v| v.as_array())
        {
            Some(value) => value,
            None => return addresses,
        };

    for tx in transactions {
        if let Some(from) =
            tx.get("from")
                .and_then(|v| v.as_str())
        {
            addresses.push(
                from.to_ascii_lowercase()
            );
        }

        if let Some(to) =
            tx.get("to")
                .and_then(|v| v.as_str())
        {
            addresses.push(
                to.to_ascii_lowercase()
            );
        }
    }

    addresses
}

// ============================================================
// PARSE BATCH RESPONSE
//
// IMPORTANT:
// Partial responses are accepted.
// Missing block numbers are returned separately.
// ============================================================

fn parse_batch_response(
    response: Value,
    requested_blocks: &[u64],
) -> Result<
    (
        HashMap<u64, Vec<String>>,
        Vec<u64>,
    ),
    String,
> {
    let mut found:
        HashMap<u64, Vec<String>> =
        HashMap::new();

    // RPC normally returns an array.
    let array =
        match response.as_array() {
            Some(array) => array,

            None => {
                // Some RPCs return a single JSON error object.
                if response.get("error").is_some() {
                    let message =
                        response
                            .get("error")
                            .and_then(|v| {
                                v.get("message")
                            })
                            .and_then(|v| {
                                v.as_str()
                            })
                            .unwrap_or(
                                "RPC returned error object"
                            );

                    return Err(
                        format!(
                            "RPC error: {}",
                            message
                        )
                    );
                }

                return Err(
                    "RPC returned non-array response"
                        .to_string()
                );
            }
        };

    for item in array {
        // Ignore individual JSON-RPC errors.
        if item.get("error").is_some() {
            continue;
        }

        let result =
            match item.get("result") {
                Some(value)
                    if !value.is_null() =>
                {
                    value
                }

                _ => continue,
            };

        let block_number =
            result
                .get("number")
                .and_then(|v| v.as_str())
                .and_then(hex_to_u64);

        let block_number =
            match block_number {
                Some(number) => number,
                None => continue,
            };

        let addresses =
            extract_addresses(result);

        found.insert(
            block_number,
            addresses,
        );
    }

    let missing:
        Vec<u64> =
        requested_blocks
            .iter()
            .copied()
            .filter(
                |block| {
                    !found.contains_key(block)
                }
            )
            .collect();

    Ok((
        found,
        missing,
    ))
}

// ============================================================
// FETCH BATCH FROM ONE RPC
// ============================================================

async fn fetch_batch_from_rpc(
    client: &reqwest::Client,
    rpc: &str,
    blocks: &[u64],
) -> Result<
    (
        HashMap<u64, Vec<String>>,
        Vec<u64>,
    ),
    String,
> {
    let payload:
        Vec<Value> =
        blocks
            .iter()
            .map(|block| {
                json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [
                        format!("0x{:x}", block),
                        true
                    ],
                    "id": block
                })
            })
            .collect();

    let response =
        client
            .post(rpc)
            .json(&payload)
            .send()
            .await
            .map_err(
                |e| {
                    format!(
                        "request error: {}",
                        e
                    )
                }
            )?;

    let status =
        response.status();

    if !status.is_success() {
        return Err(
            format!(
                "HTTP {}",
                status
            )
        );
    }

    let body =
        response
            .text()
            .await
            .map_err(
                |e| {
                    format!(
                        "read error: {}",
                        e
                    )
                }
            )?;

    if body.trim().is_empty() {
        return Err(
            "empty response".to_string()
        );
    }

    let value:
        Value =
        serde_json::from_str(
            &body
        )
        .map_err(
            |e| {
                format!(
                    "invalid JSON: {}",
                    e
                )
            }
        )?;

    parse_batch_response(
        value,
        blocks,
    )
}

// ============================================================
// FETCH BATCH WITH MULTIPLE RPC FAILOVER
// ============================================================

async fn fetch_batch_resilient(
    client: &reqwest::Client,
    rpcs: &[String],
    blocks: Vec<u64>,
) -> Result<
    Vec<String>,
    String,
> {
    let mut remaining =
        blocks;

    let mut all_addresses:
        Vec<String> =
        Vec::new();

    // --------------------------------------------------------
    // Retry rounds
    // --------------------------------------------------------

    for round in 1..=FULL_RETRY_ROUNDS {
        if remaining.is_empty() {
            return Ok(
                all_addresses
            );
        }

        let mut round_remaining =
            remaining.clone();

        println!(
            "Batch retry round {}/{} | Missing blocks: {}",
            round,
            FULL_RETRY_ROUNDS,
            round_remaining.len()
        );

        // ----------------------------------------------------
        // Try every RPC
        // ----------------------------------------------------

        for (
            rpc_index,
            rpc,
        ) in rpcs.iter().enumerate()
        {
            if round_remaining.is_empty() {
                break;
            }

            let current =
                round_remaining.clone();

            match fetch_batch_from_rpc(
                client,
                rpc,
                &current,
            )
            .await
            {
                Ok((
                    found,
                    missing,
                )) => {
                    let found_count =
                        found.len();

                    for (
                        _block,
                        addresses,
                    ) in found
                    {
                        all_addresses
                            .extend(
                                addresses
                            );
                    }

                    round_remaining =
                        missing;

                    println!(
                        "RPC #{} recovered {}/{} blocks | Remaining: {}",
                        rpc_index + 1,
                        found_count,
                        current.len(),
                        round_remaining.len()
                    );
                }

                Err(error) => {
                    eprintln!(
                        "RPC #{} failed for batch {}-{}: {}",
                        rpc_index + 1,
                        current
                            .first()
                            .unwrap_or(&0),
                        current
                            .last()
                            .unwrap_or(&0),
                        error
                    );
                }
            }
        }

        remaining =
            round_remaining;

        if !remaining.is_empty() {
            sleep(
                Duration::from_millis(
                    RETRY_DELAY_MS
                        * round as u64
                )
            )
            .await;
        }
    }

    // --------------------------------------------------------
    // FINAL INDIVIDUAL BLOCK RECOVERY
    // --------------------------------------------------------

    if !remaining.is_empty() {
        println!(
            "Batch still has {} missing blocks. Starting individual recovery.",
            remaining.len()
        );
    }

    for block in remaining {
        match fetch_single_block_resilient(
            client,
            rpcs,
            block,
        )
        .await
        {
            Ok(addresses) => {
                all_addresses
                    .extend(
                        addresses
                    );
            }

            Err(error) => {
                return Err(
                    format!(
                        "Block {} could not be recovered: {}",
                        block,
                        error
                    )
                );
            }
        }
    }

    Ok(all_addresses)
}

// ============================================================
// SINGLE BLOCK
// ============================================================

async fn fetch_single_block_resilient(
    client: &reqwest::Client,
    rpcs: &[String],
    block: u64,
) -> Result<
    Vec<String>,
    String,
> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [
            format!("0x{:x}", block),
            true
        ],
        "id": block
    });

    let mut last_error =
        String::new();

    for round in 1..=FULL_RETRY_ROUNDS {
        for (
            rpc_index,
            rpc,
        ) in rpcs.iter().enumerate()
        {
            for attempt in 1..=RPC_RETRIES {
                match send_rpc(
                    client,
                    rpc,
                    &payload,
                )
                .await
                {
                    Ok(value) => {
                        // JSON-RPC error
                        if let Some(error) =
                            value.get("error")
                        {
                            let message =
                                error
                                    .get("message")
                                    .and_then(
                                        |v| {
                                            v.as_str()
                                        }
                                    )
                                    .unwrap_or(
                                        "RPC error"
                                    );

                            last_error =
                                message.to_string();

                            break;
                        }

                        if let Some(result) =
                            value.get("result")
                        {
                            if !result.is_null() {
                                let addresses =
                                    extract_addresses(
                                        result
                                    );

                                println!(
                                    "Block {} recovered using RPC #{}",
                                    block,
                                    rpc_index + 1
                                );

                                return Ok(
                                    addresses
                                );
                            }
                        }

                        last_error =
                            "invalid block result"
                                .to_string();
                    }

                    Err(error) => {
                        last_error =
                            error;
                    }
                }

                if attempt < RPC_RETRIES {
                    sleep(
                        Duration::from_millis(
                            RETRY_DELAY_MS
                                * attempt as u64
                        )
                    )
                    .await;
                }
            }

            eprintln!(
                "Block {} RPC #{} failed: {}",
                block,
                rpc_index + 1,
                last_error
            );
        }

        sleep(
            Duration::from_millis(
                RETRY_DELAY_MS
                    * round as u64
            )
        )
        .await;
    }

    Err(
        format!(
            "all RPCs failed after {} rounds: {}",
            FULL_RETRY_ROUNDS,
            last_error
        )
    )
}

// ============================================================
// CHECKPOINT
// ============================================================

const CHECKPOINT_FILE:
    &str =
    "output/checkpoint.txt";

fn save_checkpoint(
    block: u64,
) -> Result<
    (),
    Box<dyn std::error::Error>,
> {
    let tmp =
        "output/checkpoint.tmp";

    fs::write(
        tmp,
        block.to_string(),
    )?;

    fs::rename(
        tmp,
        CHECKPOINT_FILE,
    )?;

    Ok(())
}

fn load_checkpoint()
    -> Option<u64>
{
    if !Path::new(
        CHECKPOINT_FILE
    )
    .exists()
    {
        return None;
    }

    fs::read_to_string(
        CHECKPOINT_FILE
    )
    .ok()?
    .trim()
    .parse::<u64>()
    .ok()
}

// ============================================================
// PART NUMBER
// ============================================================

fn get_next_part(
    chain: &str,
) -> u32 {
    let mut part =
        1u32;

    loop {
        let filename =
            format!(
                "output/{}_addresses_part_{:03}.csv.gz",
                chain,
                part
            );

        if !Path::new(
            &filename
        )
        .exists()
        {
            return part;
        }

        part += 1;
    }
}

// ============================================================
// WRITE CSV.GZ
// ============================================================

fn write_gzip_csv(
    chain: &str,
    part: u32,
    start_block: u64,
    end_block: u64,
    addresses: &HashSet<String>,
) -> Result<
    String,
    Box<dyn std::error::Error>,
> {
    let filename =
        format!(
            "output/{}_blocks_{}_to_{}_part_{:03}.csv.gz",
            chain,
            start_block,
            end_block,
            part
        );

    let file =
        File::create(
            &filename
        )?;

    let encoder =
        GzEncoder::new(
            file,
            Compression::default()
        );

    let mut writer =
        csv::Writer::from_writer(
            encoder
        );

    writer.write_record(
        ["address"]
    )?;

    for address in addresses {
        writer.write_record(
            [address]
        )?;
    }

    writer.flush()?;

    let encoder =
        writer
            .into_inner()
            .map_err(
                |e| e.into_error()
            )?;

    encoder.finish()?;

    Ok(filename)
}

// ============================================================
// UPLOAD RELEASE
// ============================================================

fn upload_release(
    tag: &str,
    filename: &str,
) -> Result<
    (),
    Box<dyn std::error::Error>,
> {
    println!(
        "Uploading {}...",
        filename
    );

    let status =
        std::process::Command::new(
            "gh"
        )
        .args([
            "release",
            "upload",
            tag,
            filename,
            "--clobber",
        ])
        .status()?;

    if !status.success() {
        return Err(
            format!(
                "GitHub upload failed: {}",
                filename
            )
            .into()
        );
    }

    println!(
        "Upload successful: {}",
        filename
    );

    fs::remove_file(
        filename
    )?;

    Ok(())
}

// ============================================================
// ARGUMENT
// ============================================================

fn get_arg(
    args: &[String],
    name: &str,
) -> Option<String> {
    let mut index =
        0usize;

    while index < args.len() {
        if args[index] == name {
            if index + 1 < args.len() {
                return Some(
                    args[index + 1]
                        .clone()
                );
            }
        }

        index += 1;
    }

    None
}

// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main()
    -> Result<
        (),
        Box<dyn std::error::Error>,
    >
{
    let args:
        Vec<String> =
        env::args().collect();

    let chain =
        get_arg(
            &args,
            "--chain",
        )
        .or_else(
            || {
                env::var(
                    "TARGET_CHAIN"
                )
                .ok()
            }
        )
        .unwrap_or_else(
            || "bnb".to_string()
        );

    let start_block:
        u64 =
        get_arg(
            &args,
            "--start-block",
        )
        .or_else(
            || {
                env::var(
                    "START_BLOCK"
                )
                .ok()
            }
        )
        .ok_or(
            "Missing start block"
        )?
        .parse()?;

    let end_block:
        u64 =
        get_arg(
            &args,
            "--end-block",
        )
        .or_else(
            || {
                env::var(
                    "END_BLOCK"
                )
                .ok()
            }
        )
        .ok_or(
            "Missing end block"
        )?
        .parse()?;

    if start_block > end_block {
        return Err(
            "Start block cannot be greater than end block"
                .into()
        );
    }

    // ========================================================
    // OUTPUT
    // ========================================================

    fs::create_dir_all(
        "output"
    )?;

    // ========================================================
    // RPC
    // ========================================================

    let rpc_list =
        get_rpc_list(
            &chain
        );

    if rpc_list.is_empty() {
        return Err(
            format!(
                "Unsupported chain: {}",
                chain
            )
            .into()
        );
    }

    // ========================================================
    // HTTP CLIENT
    // ========================================================

    let client =
        reqwest::Client::builder()
            .timeout(
                Duration::from_secs(
                    RPC_TIMEOUT_SECONDS
                )
            )
            .pool_max_idle_per_host(
                16
            )
            .build()?;

    let client =
        Arc::new(client);

    let rpcs =
        Arc::new(
            rpc_list
        );

    // ========================================================
    // HEADER
    // ========================================================

    println!();
    println!(
        "======================================================="
    );
    println!(
        "EVM BLOCK ADDRESS EXTRACTOR"
    );
    println!(
        "======================================================="
    );
    println!(
        "Chain          : {}",
        chain
    );
    println!(
        "Start Block    : {}",
        start_block
    );
    println!(
        "End Block      : {}",
        end_block
    );
    println!(
        "Total Blocks   : {}",
        end_block
            .saturating_sub(
                start_block
            )
            + 1
    );
    println!(
        "Batch Size     : {}",
        BLOCKS_PER_BATCH
    );
    println!(
        "Concurrency    : {}",
        CONCURRENCY
    );
    println!(
        "RPC Count      : {}",
        rpcs.len()
    );
    println!(
        "======================================================="
    );

    // ========================================================
    // RPC LIST
    // ========================================================

    for (
        index,
        rpc,
    ) in rpcs.iter().enumerate()
    {
        println!(
            "RPC #{}: {}",
            index + 1,
            rpc
        );
    }

    println!(
        "======================================================="
    );

    // ========================================================
    // CHECK CURRENT CHAIN HEIGHT
    // ========================================================

    let latest =
        get_latest_block(
            &client,
            &rpcs,
        )
        .await?;

    println!(
        "Current chain height: {}",
        latest
    );

    if end_block > latest {
        return Err(
            format!(
                "End block {} is greater than current chain height {}",
                end_block,
                latest
            )
            .into()
        );
    }

    // ========================================================
    // CHECKPOINT
    // ========================================================

    let mut current =
        match load_checkpoint() {
            Some(saved)
                if saved >= start_block
                    && saved <= end_block + 1 =>
            {
                println!(
                    "Resuming from checkpoint: {}",
                    saved
                );

                saved
            }

            _ => {
                start_block
            }
        };

    // ========================================================
    // PART NUMBER
    // ========================================================

    let mut part =
        get_next_part(
            &chain
        );

    // ========================================================
    // RELEASE TAG
    // ========================================================

    let release_tag =
        env::var(
            "RELEASE_TAG"
        )
        .ok();

    // ========================================================
    // PART LOOP
    // ========================================================

    while current <= end_block {
        let part_end =
            current
                .saturating_add(
                    BLOCKS_PER_PART - 1
                )
                .min(
                    end_block
                );

        println!();
        println!(
            "======================================================="
        );
        println!(
            "PART {:03}",
            part
        );
        println!(
            "Blocks: {} → {}",
            current,
            part_end
        );
        println!(
            "======================================================="
        );

        // ----------------------------------------------------
        // CREATE BATCHES
        // ----------------------------------------------------

        let mut batches:
            Vec<Vec<u64>> =
            Vec::new();

        let mut block =
            current;

        while block <= part_end {
            let batch_end =
                block
                    .saturating_add(
                        BLOCKS_PER_BATCH - 1
                    )
                    .min(
                        part_end
                    );

            batches.push(
                (block..=batch_end)
                    .collect()
            );

            if batch_end ==
                part_end
            {
                break;
            }

            block =
                batch_end + 1;
        }

        let total_batches =
            batches.len();

        println!(
            "Total batches: {}",
            total_batches
        );

        // ----------------------------------------------------
        // UNIQUE ADDRESS SET
        // ----------------------------------------------------

        let mut unique:
            HashSet<String> =
            HashSet::new();

        // ----------------------------------------------------
        // PARALLEL PROCESSING
        // ----------------------------------------------------

        let mut stream =
            stream::iter(
                batches
            )
            .map(
                |batch| {
                    let client =
                        Arc::clone(
                            &client
                        );

                    let rpcs =
                        Arc::clone(
                            &rpcs
                        );

                    tokio::spawn(
                        async move {
                            fetch_batch_resilient(
                                &client,
                                &rpcs,
                                batch,
                            )
                            .await
                        }
                    )
                }
            )
            .buffer_unordered(
                CONCURRENCY
            );

        let mut processed =
            0usize;

        while let Some(result) =
            stream.next().await
        {
            let addresses =
                result
                    .map_err(
                        |e| {
                            format!(
                                "Worker crashed: {}",
                                e
                            )
                        }
                    )?
                    .map_err(
                        |e| {
                            format!(
                                "Permanent extraction error: {}",
                                e
                            )
                        }
                    )?;

            for address in addresses {
                unique.insert(
                    address
                );
            }

            processed += 1;

            if processed % 500 == 0
                || processed ==
                    total_batches
            {
                let percent =
                    processed as f64
                    /
                    total_batches as f64
                    *
                    100.0;

                println!(
                    "Progress: {}/{} ({:.2}%) | Unique addresses: {}",
                    processed,
                    total_batches,
                    percent,
                    unique.len()
                );
            }
        }

        // ----------------------------------------------------
        // WRITE FILE
        // ----------------------------------------------------

        let filename =
            write_gzip_csv(
                &chain,
                part,
                current,
                part_end,
                &unique,
            )?;

        println!();
        println!(
            "======================================================="
        );
        println!(
            "PART {:03} COMPLETE",
            part
        );
        println!(
            "Blocks           : {} → {}",
            current,
            part_end
        );
        println!(
            "Unique addresses : {}",
            unique.len()
        );
        println!(
            "File             : {}",
            filename
        );
        println!(
            "======================================================="
        );

        // ----------------------------------------------------
        // UPLOAD
        // ----------------------------------------------------

        if let Some(tag) =
            release_tag.as_deref()
        {
            upload_release(
                tag,
                &filename,
            )?;
        }

        // ----------------------------------------------------
        // CHECKPOINT
        // ----------------------------------------------------

        let next =
            part_end + 1;

        save_checkpoint(
            next
        )?;

        println!(
            "Checkpoint saved: {}",
            next
        );

        if next > end_block {
            break;
        }

        current =
            next;

        part += 1;
    }

    // ========================================================
    // COMPLETE
    // ========================================================

    if Path::new(
        CHECKPOINT_FILE
    )
    .exists()
    {
        fs::remove_file(
            CHECKPOINT_FILE
        )?;
    }

    println!();
    println!(
        "======================================================="
    );
    println!(
        "EXTRACTION COMPLETE"
    );
    println!(
        "======================================================="
    );
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
        "======================================================="
    );

    Ok(())
}
