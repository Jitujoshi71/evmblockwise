use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================
// CONFIG
// ============================================================

const BLOCKS_PER_PART: u64 = 1_000_000;

// Small batch because eth_getBlockByNumber(true)
// returns complete transaction objects.
const BLOCKS_PER_BATCH: u64 = 5;

// Maximum simultaneous jobs.
const CONCURRENCY: usize = 6;

// Retry count per RPC.
const MAX_RETRIES: usize = 2;

// HTTP timeout.
const RPC_TIMEOUT_SECONDS: u64 = 45;

// ============================================================
// RPC LIST
// ============================================================

fn get_rpc_list(chain: &str) -> Vec<String> {
    let mut list = Vec::new();

    // User's secret RPC gets priority.
    if let Ok(custom) = env::var("RPC_URL") {
        let custom = custom.trim();

        if !custom.is_empty() {
            list.push(custom.to_string());
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
        if !list.iter().any(|x| x == rpc) {
            list.push(rpc.to_string());
        }
    }

    list
}

// ============================================================
// HEX TO U64
// ============================================================

fn hex_to_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(
        value.trim_start_matches("0x"),
        16,
    )
    .ok()
}

// ============================================================
// SINGLE HTTP RPC REQUEST
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
            "HTTP status {}",
            status
        ));
    }

    let body = response
        .text()
        .await
        .map_err(|e| {
            format!(
                "response read error: {}",
                e
            )
        })?;

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
// LATEST BLOCK
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
        for attempt in 1..=MAX_RETRIES {
            match send_rpc(
                client,
                rpc,
                &payload,
            )
            .await
            {
                Ok(value) => {
                    if let Some(result) =
                        value.get("result")
                            .and_then(|v| v.as_str())
                    {
                        if let Some(block) =
                            hex_to_u64(result)
                        {
                            println!(
                                "Connected RPC #{}: {}",
                                index + 1,
                                rpc
                            );

                            return Ok(block);
                        }
                    }

                    last_error =
                        format!(
                            "{} returned invalid block number",
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

            if attempt < MAX_RETRIES {
                sleep(
                    Duration::from_millis(
                        300 * attempt as u64
                    )
                )
                .await;
            }
        }

        eprintln!(
            "Latest-block RPC failed, trying next: {}",
            last_error
        );
    }

    Err(format!(
        "All RPCs failed: {}",
        last_error
    ))
}

// ============================================================
// PARSE TRANSACTIONS
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
// BATCH PARSER
// ============================================================

fn parse_batch(
    response: Vec<Value>,
    expected_blocks: &[u64],
) -> Result<Vec<String>, String> {
    let mut addresses =
        Vec::new();

    let mut received =
        HashSet::<u64>::new();

    for item in response {
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

        if let Some(number) =
            result
                .get("number")
                .and_then(|v| v.as_str())
                .and_then(hex_to_u64)
        {
            received.insert(number);
        }

        addresses.extend(
            extract_addresses(result)
        );
    }

    if received.len()
        != expected_blocks.len()
    {
        return Err(format!(
            "incomplete batch: received {} / {} blocks",
            received.len(),
            expected_blocks.len()
        ));
    }

    Ok(addresses)
}

// ============================================================
// BATCH REQUEST WITH RPC FAILOVER
// ============================================================

async fn fetch_batch(
    client: &reqwest::Client,
    rpcs: &[String],
    blocks: &[u64],
) -> Result<Vec<String>, String> {
    let payload: Vec<Value> =
        blocks
            .iter()
            .map(|block| {
                json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [
                        format!(
                            "0x{:x}",
                            block
                        ),
                        true
                    ],
                    "id": block
                })
            })
            .collect();

    let mut last_error =
        String::new();

    for (rpc_index, rpc) in
        rpcs.iter().enumerate()
    {
        for attempt in 1..=MAX_RETRIES {
            match client
                .post(rpc)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        last_error =
                            format!(
                                "HTTP {}",
                                response.status()
                            );
                    } else {
                        match response.text().await
                        {
                            Ok(body) => {
                                match serde_json::from_str::<Vec<Value>>(
                                    &body
                                ) {
                                    Ok(result) => {
                                        match parse_batch(
                                            result,
                                            blocks,
                                        ) {
                                            Ok(addresses) => {
                                                if rpc_index > 0 {
                                                    println!(
                                                        "Batch recovered using RPC #{}",
                                                        rpc_index + 1
                                                    );
                                                }

                                                return Ok(addresses);
                                            }

                                            Err(error) => {
                                                last_error =
                                                    error;
                                            }
                                        }
                                    }

                                    Err(error) => {
                                        last_error =
                                            format!(
                                                "invalid JSON: {}",
                                                error
                                            );
                                    }
                                }
                            }

                            Err(error) => {
                                last_error =
                                    format!(
                                        "read error: {}",
                                        error
                                    );
                            }
                        }
                    }
                }

                Err(error) => {
                    last_error =
                        format!(
                            "request error: {}",
                            error
                        );
                }
            }

            if attempt < MAX_RETRIES {
                sleep(
                    Duration::from_millis(
                        300 * attempt as u64
                    )
                )
                .await;
            }
        }

        eprintln!(
            "Batch {}-{} failed on RPC #{}: {}",
            blocks.first().unwrap_or(&0),
            blocks.last().unwrap_or(&0),
            rpc_index + 1,
            last_error
        );
    }

    Err(format!(
        "all RPCs failed: {}",
        last_error
    ))
}

// ============================================================
// INDIVIDUAL BLOCK FALLBACK
// ============================================================

async fn fetch_single_block(
    client: &reqwest::Client,
    rpcs: &[String],
    block: u64,
) -> Result<Vec<String>, String> {
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

    for (rpc_index, rpc) in
        rpcs.iter().enumerate()
    {
        for attempt in 1..=MAX_RETRIES {
            match send_rpc(
                client,
                rpc,
                &payload,
            )
            .await
            {
                Ok(value) => {
                    if let Some(result) =
                        value.get("result")
                    {
                        if !result.is_null() {
                            if rpc_index > 0 {
                                println!(
                                    "Block {} recovered using RPC #{}",
                                    block,
                                    rpc_index + 1
                                );
                            }

                            return Ok(
                                extract_addresses(
                                    result
                                )
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

            if attempt < MAX_RETRIES {
                sleep(
                    Duration::from_millis(
                        300 * attempt as u64
                    )
                )
                .await;
            }
        }

        eprintln!(
            "Block {} failed on RPC #{}: {}",
            block,
            rpc_index + 1,
            last_error
        );
    }

    Err(format!(
        "Block {} failed on ALL RPCs: {}",
        block,
        last_error
    ))
}

// ============================================================
// RESILIENT BATCH
// ============================================================

async fn fetch_resilient(
    client: &reqwest::Client,
    rpcs: &[String],
    blocks: Vec<u64>,
) -> Result<Vec<String>, String> {
    match fetch_batch(
        client,
        rpcs,
        &blocks,
    )
    .await
    {
        Ok(addresses) => {
            Ok(addresses)
        }

        Err(error) => {
            eprintln!(
                "Batch {}-{} failed completely: {}",
                blocks.first().unwrap_or(&0),
                blocks.last().unwrap_or(&0),
                error
            );

            eprintln!(
                "Falling back to individual blocks..."
            );

            let mut addresses =
                Vec::new();

            for block in blocks {
                let block_addresses =
                    fetch_single_block(
                        client,
                        rpcs,
                        block,
                    )
                    .await?;

                addresses.extend(
                    block_addresses
                );
            }

            Ok(addresses)
        }
    }
}

// ============================================================
// CHECKPOINT
// ============================================================

const CHECKPOINT_FILE: &str =
    "output/checkpoint.txt";

fn save_checkpoint(
    block: u64,
) -> Result<(), Box<dyn std::error::Error>> {
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

fn load_checkpoint() -> Option<u64> {
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
    let mut part = 1u32;

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
) -> Result<String, Box<dyn std::error::Error>> {
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
// UPLOAD TO GITHUB RELEASE
// ============================================================

fn upload_release(
    tag: &str,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!();
    println!(
        "Uploading {}",
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
                "GitHub release upload failed for {}",
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
// ARGUMENT PARSER
// ============================================================

fn get_arg(
    args: &[String],
    name: &str,
) -> Option<String> {
    let mut i = 0;

    while i < args.len() {
        if args[i] == name {
            if i + 1 < args.len() {
                return Some(
                    args[i + 1].clone()
                );
            }
        }

        i += 1;
    }

    None
}

// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error>>
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
            || env::var("TARGET_CHAIN").ok()
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
            || env::var("START_BLOCK").ok()
        )
        .ok_or(
            "Missing --start-block"
        )?
        .parse()?;

    let end_block:
        u64 =
        get_arg(
            &args,
            "--end-block",
        )
        .or_else(
            || env::var("END_BLOCK").ok()
        )
        .ok_or(
            "Missing --end-block"
        )?
        .parse()?;

    if start_block > end_block {
        return Err(
            "start-block cannot be greater than end-block"
                .into()
        );
    }

    // ========================================================
    // OUTPUT DIRECTORY
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
    // CLIENT
    // ========================================================

    let client =
        reqwest::Client::builder()
            .timeout(
                Duration::from_secs(
                    RPC_TIMEOUT_SECONDS
                )
            )
            .pool_max_idle_per_host(
                32
            )
            .build()?;

    let client =
        Arc::new(client);

    let rpcs =
        Arc::new(
            rpc_list
        );

    // ========================================================
    // INFO
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
        "Chain             : {}",
        chain
    );
    println!(
        "Start Block       : {}",
        start_block
    );
    println!(
        "End Block         : {}",
        end_block
    );
    println!(
        "Blocks            : {}",
        end_block
            .saturating_sub(
                start_block
            )
            + 1
    );
    println!(
        "Blocks / Batch    : {}",
        BLOCKS_PER_BATCH
    );
    println!(
        "Concurrency       : {}",
        CONCURRENCY
    );
    println!(
        "RPC Count         : {}",
        rpcs.len()
    );
    println!(
        "======================================================="
    );

    for (
        i,
        rpc,
    ) in rpcs.iter().enumerate()
    {
        println!(
            "RPC #{}: {}",
            i + 1,
            rpc
        );
    }

    println!(
        "======================================================="
    );

    // ========================================================
    // CHECK LATEST BLOCK
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

    if start_block > latest {
        return Err(
            format!(
                "Start block {} is greater than current chain height {}",
                start_block,
                latest
            )
            .into()
        );
    }

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
                    "Checkpoint found: {}",
                    saved
                );

                saved
            }

            _ => start_block,
        };

    // ========================================================
    // PART
    // ========================================================

    let mut part =
        get_next_part(
            &chain
        );

    // ========================================================
    // RELEASE
    // ========================================================

    let release_tag =
        env::var(
            "RELEASE_TAG"
        )
        .ok();

    // ========================================================
    // MAIN LOOP
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

            if batch_end == part_end {
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
        // PARALLEL BATCH PROCESSING
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
                            fetch_resilient(
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
                                "worker error: {}",
                                e
                            )
                        }
                    )?
                    .map_err(
                        |e| {
                            format!(
                                "permanent extraction error: {}",
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
                || processed == total_batches
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
            "Blocks            : {} → {}",
            current,
            part_end
        );
        println!(
            "Unique addresses  : {}",
            unique.len()
        );
        println!(
            "File              : {}",
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
            "Checkpoint: {}",
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
    // FINISH
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
