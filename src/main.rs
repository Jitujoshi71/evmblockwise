use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================
// EVM BLOCKWISE ADDRESS EXTRACTOR v2.4
// ============================================================
//
// Features:
// - EVM block extraction
// - JSON-RPC batch requests
// - Multiple RPC providers
// - RPC fallback
// - Retry rounds
// - 20,000 block rotation
// - 10 second pause after every 20,000 blocks
// - Preferred RPC rotation
// - Concurrent batches
// - CSV.GZ output
// - GitHub Release upload
//
// ============================================================


// ============================================================
// CONFIG
// ============================================================

const MAX_RETRY_ROUNDS: usize = 12;

const INITIAL_RETRY_DELAY_SECS: u64 = 10;
const MAX_RETRY_DELAY_SECS: u64 = 120;

const PART_SIZE: u64 = 1_000_000;

const DEFAULT_BATCH_SIZE: u64 = 5;
const DEFAULT_CONCURRENCY: usize = 4;

const RPC_DELAY_MS: u64 = 300;

const HTTP_TIMEOUT_SECS: u64 = 60;

const UPLOAD_RETRIES: usize = 5;

// IMPORTANT:
//
// Every 20,000 blocks:
//
// 1. Current segment finishes
// 2. Wait 10 seconds
// 3. Preferred RPC changes
//
// Example:
//
// Segment 1 -> RPC #1
// wait 10 sec
// Segment 2 -> RPC #2
// wait 10 sec
// Segment 3 -> RPC #3
//
// After last RPC:
// RPC #7 -> RPC #1
//

const RPC_ROTATE_BLOCKS: u64 = 20_000;
const RPC_ROTATE_WAIT_SECS: u64 = 10;


// ============================================================
// RPC LIST
// ============================================================

fn rpc_list(chain: &str) -> Vec<String> {
    match chain.to_lowercase().as_str() {

        // ----------------------------------------------------
        // BNB SMART CHAIN
        // ----------------------------------------------------

        "bnb" | "bsc" => vec![
            "https://bsc-dataseed.binance.org/".to_string(),
            "https://bsc-dataseed1.defibit.io/".to_string(),
            "https://bsc-dataseed1.ninicoin.io/".to_string(),
            "https://bsc-dataseed2.defibit.io/".to_string(),
            "https://bsc-dataseed2.ninicoin.io/".to_string(),
            "https://rpc.ankr.com/bsc".to_string(),
            "https://1rpc.io/bnb".to_string(),
        ],

        // ----------------------------------------------------
        // ETHEREUM
        // ----------------------------------------------------

        "ethereum" | "eth" => vec![
            "https://eth.llamarpc.com".to_string(),
            "https://cloudflare-eth.com".to_string(),
            "https://rpc.ankr.com/eth".to_string(),
            "https://1rpc.io/eth".to_string(),
        ],

        // ----------------------------------------------------
        // POLYGON
        // ----------------------------------------------------

        "polygon" | "matic" => vec![
            "https://polygon-rpc.com".to_string(),
            "https://rpc.ankr.com/polygon".to_string(),
            "https://1rpc.io/matic".to_string(),
        ],

        // ----------------------------------------------------
        // ARBITRUM
        // ----------------------------------------------------

        "arbitrum" | "arb" => vec![
            "https://arb1.arbitrum.io/rpc".to_string(),
            "https://rpc.ankr.com/arbitrum".to_string(),
            "https://1rpc.io/arb".to_string(),
        ],

        // ----------------------------------------------------
        // BASE
        // ----------------------------------------------------

        "base" => vec![
            "https://mainnet.base.org".to_string(),
            "https://base.llamarpc.com".to_string(),
            "https://1rpc.io/base".to_string(),
        ],

        // ----------------------------------------------------
        // OPTIMISM
        // ----------------------------------------------------

        "optimism" | "op" => vec![
            "https://mainnet.optimism.io".to_string(),
            "https://optimism.llamarpc.com".to_string(),
            "https://1rpc.io/op".to_string(),
        ],

        // ----------------------------------------------------
        // AVALANCHE C-CHAIN
        // ----------------------------------------------------

        "avalanche_c" | "avalanche" | "avax" => vec![
            "https://api.avax.network/ext/bc/C/rpc".to_string(),
            "https://rpc.ankr.com/avalanche".to_string(),
            "https://1rpc.io/avax/c".to_string(),
        ],

        // ----------------------------------------------------
        // DEFAULT BSC
        // ----------------------------------------------------

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

#[derive(Debug)]
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

impl std::fmt::Display for RpcError {

    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {

        write!(
            f,
            "{}",
            self.message
        )
    }
}

impl std::error::Error for RpcError {}


// ============================================================
// HEX PARSER
// ============================================================

fn parse_hex_u64(
    value: &str,
) -> Result<u64, RpcError> {

    let clean =
        value
            .trim_start_matches("0x");

    u64::from_str_radix(
        clean,
        16,
    )
    .map_err(|e| {

        RpcError::new(
            format!(
                "invalid hex number '{}': {}",
                value,
                e
            )
        )
    })
}


// ============================================================
// VALIDATE BLOCK
// ============================================================

fn validate_block(
    block: &Value,
    expected_number: u64,
) -> Result<(), RpcError> {

    let number =
        block
            .get("number")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {

                RpcError::new(
                    format!(
                        "block {} missing number",
                        expected_number
                    )
                )
            })?;

    let actual =
        parse_hex_u64(number)?;

    if actual != expected_number {

        return Err(
            RpcError::new(
                format!(
                    "block mismatch: requested {}, received {}",
                    expected_number,
                    actual
                )
            )
        );
    }

    let transactions =
        block
            .get("transactions")
            .ok_or_else(|| {

                RpcError::new(
                    format!(
                        "block {} missing transactions field",
                        expected_number
                    )
                )
            })?;

    if !transactions.is_array() {

        return Err(
            RpcError::new(
                format!(
                    "block {} transactions is not an array",
                    expected_number
                )
            )
        );
    }

    Ok(())
}


// ============================================================
// SINGLE BLOCK REQUEST
// ============================================================

async fn request_single_block(
    client: &Client,
    rpc: &str,
    block_number: u64,
) -> Result<Value, RpcError> {

    let payload =
        json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [
                format!(
                    "0x{:x}",
                    block_number
                ),
                true
            ],
            "id": block_number
        });

    let response =
        client
            .post(rpc)
            .json(&payload)
            .send()
            .await
            .map_err(|e| {

                RpcError::new(
                    format!(
                        "HTTP request error: {}",
                        e
                    )
                )
            })?;

    let status =
        response.status();

    if !status.is_success() {

        return Err(
            RpcError::new(
                format!(
                    "HTTP status {}",
                    status
                )
            )
        );
    }

    let text =
        response
            .text()
            .await
            .map_err(|e| {

                RpcError::new(
                    format!(
                        "response body error: {}",
                        e
                    )
                )
            })?;

    let value: Value =
        serde_json::from_str(
            &text
        )
        .map_err(|e| {

            RpcError::new(
                format!(
                    "invalid JSON: {} | body: {}",
                    e,
                    text.chars()
                        .take(300)
                        .collect::<String>()
                )
            )
        })?;

    if let Some(error) =
        value.get("error")
    {

        return Err(
            RpcError::new(
                format!(
                    "RPC error: {}",
                    error
                )
            )
        );
    }

    let result =
        value
            .get("result")
            .ok_or_else(|| {

                RpcError::new(
                    "RPC response missing result"
                )
            })?;

    if result.is_null() {

        return Err(
            RpcError::new(
                format!(
                    "block {} returned null result",
                    block_number
                )
            )
        );
    }

    validate_block(
        result,
        block_number,
    )?;

    Ok(
        result.clone()
    )
}


// ============================================================
// BATCH REQUEST
// ============================================================

async fn request_batch(
    client: &Client,
    rpc: &str,
    blocks: &[u64],
) -> Result<HashMap<u64, Value>, RpcError> {

    if blocks.is_empty() {
        return Ok(
            HashMap::new()
        );
    }

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
                    "id": *block
                })

            })
            .collect();

    let response =
        client
            .post(rpc)
            .json(&payload)
            .send()
            .await
            .map_err(|e| {

                RpcError::new(
                    format!(
                        "HTTP request error: {}",
                        e
                    )
                )
            })?;

    let status =
        response.status();

    if !status.is_success() {

        return Err(
            RpcError::new(
                format!(
                    "HTTP status {}",
                    status
                )
            )
        );
    }

    let text =
        response
            .text()
            .await
            .map_err(|e| {

                RpcError::new(
                    format!(
                        "response body error: {}",
                        e
                    )
                )
            })?;

    let parsed: Value =
        serde_json::from_str(
            &text
        )
        .map_err(|e| {

            RpcError::new(
                format!(
                    "invalid JSON: {} | body: {}",
                    e,
                    text.chars()
                        .take(300)
                        .collect::<String>()
                )
            )
        })?;

    let array =
        match parsed {

            Value::Array(arr) => arr,

            Value::Object(obj) => {

                if let Some(error) =
                    obj.get("error")
                {

                    return Err(
                        RpcError::new(
                            format!(
                                "RPC error: {}",
                                error
                            )
                        )
                    );
                }

                return Err(
                    RpcError::new(
                        "RPC returned object instead of batch array"
                    )
                );
            }

            _ => {

                return Err(
                    RpcError::new(
                        "RPC returned invalid structure"
                    )
                );
            }
        };

    let requested:
        HashSet<u64> =
        blocks
            .iter()
            .copied()
            .collect();

    let mut results =
        HashMap::with_capacity(
            blocks.len()
        );

    for item in array {

        if let Some(error) =
            item.get("error")
        {

            return Err(
                RpcError::new(
                    format!(
                        "RPC item error: {}",
                        error
                    )
                )
            );
        }

        let id =
            item
                .get("id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| {

                    RpcError::new(
                        "batch item missing numeric id"
                    )
                })?;

        if !requested.contains(&id) {

            return Err(
                RpcError::new(
                    format!(
                        "unexpected block id {}",
                        id
                    )
                )
            );
        }

        let result =
            item
                .get("result")
                .ok_or_else(|| {

                    RpcError::new(
                        format!(
                            "block {} missing result",
                            id
                        )
                    )
                })?;

        if result.is_null() {

            return Err(
                RpcError::new(
                    format!(
                        "block {} returned null result",
                        id
                    )
                )
            );
        }

        validate_block(
            result,
            id,
        )?;

        results.insert(
            id,
            result.clone()
        );
    }

    if results.len()
        != blocks.len()
    {

        return Err(
            RpcError::new(
                format!(
                    "incomplete batch: received {} / {} blocks",
                    results.len(),
                    blocks.len()
                )
            )
        );
    }

    Ok(results)
}


// ============================================================
// EXTRACT ADDRESSES
// ============================================================

fn extract_block_data(
    block: &Value,
    block_number: u64,
) -> Result<(Vec<String>, usize), RpcError> {

    validate_block(
        block,
        block_number,
    )?;

    let transactions =
        block
            .get("transactions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {

                RpcError::new(
                    format!(
                        "block {} transactions array missing",
                        block_number
                    )
                )
            })?;

    let mut addresses =
        Vec::new();

    for tx in transactions {

        let object =
            match tx.as_object() {

                Some(obj) => obj,

                None => {

                    return Err(
                        RpcError::new(
                            format!(
                                "block {} returned transaction hash instead of full transaction object",
                                block_number
                            )
                        )
                    );
                }
            };

        // ----------------------------------------------------
        // FROM
        // ----------------------------------------------------

        if let Some(from) =
            object
                .get("from")
                .and_then(|v| v.as_str())
        {

            addresses.push(
                from.to_ascii_lowercase()
            );
        }

        // ----------------------------------------------------
        // TO
        //
        // Contract creation transactions can have:
        //
        // "to": null
        //
        // That is normal.
        // ----------------------------------------------------

        if let Some(to) =
            object
                .get("to")
                .and_then(|v| v.as_str())
        {

            addresses.push(
                to.to_ascii_lowercase()
            );
        }
    }

    Ok((
        addresses,
        transactions.len()
    ))
}


// ============================================================
// FETCH BATCH WITH RETRIES
// ============================================================

async fn fetch_batch_with_retry(
    client: &Client,
    rpcs: &[String],
    blocks: Vec<u64>,
    preferred_rpc: usize,
) -> Result<(Vec<String>, usize), RpcError> {

    let first =
        *blocks
            .first()
            .unwrap_or(&0);

    let last =
        *blocks
            .last()
            .unwrap_or(&0);

    if rpcs.is_empty() {

        return Err(
            RpcError::new(
                "RPC list is empty"
            )
        );
    }

    let preferred =
        preferred_rpc % rpcs.len();

    for retry_round
        in 1..=MAX_RETRY_ROUNDS
    {

        println!(
            "Trying batch {}-{} | retry round {}/{} | preferred RPC #{}",
            first,
            last,
            retry_round,
            MAX_RETRY_ROUNDS,
            preferred + 1
        );

        // ----------------------------------------------------
        // Try preferred RPC first.
        // Then every other RPC.
        // ----------------------------------------------------

        for offset
            in 0..rpcs.len()
        {

            let rpc_index =
                (preferred + offset)
                    % rpcs.len();

            let rpc =
                &rpcs[rpc_index];

            match request_batch(
                client,
                rpc,
                &blocks,
            )
            .await
            {

                Ok(block_map) => {

                    let mut addresses =
                        Vec::new();

                    let mut transactions =
                        0usize;

                    for block_number
                        in &blocks
                    {

                        let block =
                            block_map
                                .get(block_number)
                                .ok_or_else(|| {

                                    RpcError::new(
                                        format!(
                                            "missing block {}",
                                            block_number
                                        )
                                    )
                                })?;

                        let (
                            block_addresses,
                            block_txs
                        ) =
                            extract_block_data(
                                block,
                                *block_number,
                            )?;

                        transactions +=
                            block_txs;

                        addresses.extend(
                            block_addresses
                        );
                    }

                    println!(
                        "Batch {}-{} recovered using RPC #{} (attempt {}) | Transactions: {} | Addresses: {}",
                        first,
                        last,
                        rpc_index + 1,
                        retry_round,
                        transactions,
                        addresses.len()
                    );

                    return Ok((
                        addresses,
                        transactions
                    ));
                }

                Err(error) => {

                    println!(
                        "Batch {}-{} failed on RPC #{}: {}",
                        first,
                        last,
                        rpc_index + 1,
                        error
                    );
                }
            }

            sleep(
                Duration::from_millis(
                    RPC_DELAY_MS
                )
            )
            .await;
        }

        // ----------------------------------------------------
        // ALL RPCs failed
        // ----------------------------------------------------

        if retry_round
            < MAX_RETRY_ROUNDS
        {

            let multiplier =
                2u64.saturating_pow(
                    (
                        (retry_round - 1)
                            .min(4)
                    ) as u32
                );

            let delay =
                (
                    INITIAL_RETRY_DELAY_SECS
                        * multiplier
                )
                .min(
                    MAX_RETRY_DELAY_SECS
                );

            println!(
                "ALL RPCs failed for batch {}-{}.",
                first,
                last
            );

            println!(
                "Waiting {} seconds before retry round {}/{}...",
                delay,
                retry_round + 1,
                MAX_RETRY_ROUNDS
            );

            sleep(
                Duration::from_secs(
                    delay
                )
            )
            .await;
        }
    }

    // ========================================================
    // LAST RESORT
    //
    // Individual block requests.
    // ========================================================

    println!();
    println!(
        "Batch {}-{} exhausted batch retries.",
        first,
        last
    );

    println!(
        "Falling back to individual block requests..."
    );

    let mut addresses =
        Vec::new();

    let mut total_transactions =
        0usize;

    for block_number
        in &blocks
    {

        let mut recovered =
            false;

        for retry_round
            in 1..=MAX_RETRY_ROUNDS
        {

            for offset
                in 0..rpcs.len()
            {

                let rpc_index =
                    (
                        preferred
                            + offset
                    )
                    % rpcs.len();

                let rpc =
                    &rpcs[rpc_index];

                match request_single_block(
                    client,
                    rpc,
                    *block_number,
                )
                .await
                {

                    Ok(block) => {

                        match extract_block_data(
                            &block,
                            *block_number,
                        )
                        {

                            Ok((
                                block_addresses,
                                tx_count
                            )) => {

                                println!(
                                    "Block {} recovered using RPC #{} (attempt {}) | Transactions: {} | Addresses: {}",
                                    block_number,
                                    rpc_index + 1,
                                    retry_round,
                                    tx_count,
                                    block_addresses.len()
                                );

                                total_transactions +=
                                    tx_count;

                                addresses.extend(
                                    block_addresses
                                );

                                recovered =
                                    true;

                                break;
                            }

                            Err(error) => {

                                println!(
                                    "Block {} invalid on RPC #{}: {}",
                                    block_number,
                                    rpc_index + 1,
                                    error
                                );
                            }
                        }
                    }

                    Err(error) => {

                        println!(
                            "Block {} failed on RPC #{}: {}",
                            block_number,
                            rpc_index + 1,
                            error
                        );
                    }
                }

                sleep(
                    Duration::from_millis(
                        RPC_DELAY_MS
                    )
                )
                .await;
            }

            if recovered {
                break;
            }

            if retry_round
                < MAX_RETRY_ROUNDS
            {

                let multiplier =
                    2u64.saturating_pow(
                        (
                            (retry_round - 1)
                                .min(4)
                        ) as u32
                    );

                let delay =
                    (
                        INITIAL_RETRY_DELAY_SECS
                            * multiplier
                    )
                    .min(
                        MAX_RETRY_DELAY_SECS
                    );

                println!(
                    "Block {} still failed. Waiting {} seconds before retry round {}/{}...",
                    block_number,
                    delay,
                    retry_round + 1,
                    MAX_RETRY_ROUNDS
                );

                sleep(
                    Duration::from_secs(
                        delay
                    )
                )
                .await;
            }
        }

        if !recovered {

            return Err(
                RpcError::new(
                    format!(
                        "Block {} failed on ALL RPCs after {} retry rounds",
                        block_number,
                        MAX_RETRY_ROUNDS
                    )
                )
            );
        }
    }

    Ok((
        addresses,
        total_transactions
    ))
}


// ============================================================
// WRITE CSV.GZ
// ============================================================

fn write_addresses_file(
    chain: &str,
    part_num: u32,
    start_block: u64,
    end_block: u64,
    addresses: &HashSet<String>,
) -> Result<String, Box<dyn std::error::Error>> {

    fs::create_dir_all(
        "output"
    )?;

    let file_name =
        format!(
            "output/{}_blocks_{}_to_{}_part_{:03}.csv.gz",
            chain,
            start_block,
            end_block,
            part_num
        );

    let file =
        File::create(
            &file_name
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

    writer.write_record([
        "address"
    ])?;

    let mut sorted:
        Vec<&String> =
        addresses
            .iter()
            .collect();

    sorted.sort_unstable();

    for address
        in sorted
    {

        writer.write_record([
            address
        ])?;
    }

    writer.flush()?;

    println!();
    println!(
        "=============================================="
    );
    println!(
        "PART COMPLETED"
    );
    println!(
        "=============================================="
    );

    println!(
        "Part           : {}",
        part_num
    );

    println!(
        "Blocks         : {} -> {}",
        start_block,
        end_block
    );

    println!(
        "Unique Address : {}",
        addresses.len()
    );

    println!(
        "File           : {}",
        file_name
    );

    println!(
        "=============================================="
    );

    Ok(
        file_name
    )
}


// ============================================================
// GET LATEST BLOCK
// ============================================================

async fn get_latest_block(
    client: &Client,
    rpcs: &[String],
) -> Result<u64, RpcError> {

    loop {

        for (index, rpc)
            in rpcs
                .iter()
                .enumerate()
        {

            println!(
                "Checking RPC #{} for latest block...",
                index + 1
            );

            let response =
                client
                    .post(rpc)
                    .json(&json!({
                        "jsonrpc": "2.0",
                        "method": "eth_blockNumber",
                        "params": [],
                        "id": 1
                    }))
                    .send()
                    .await;

            let response =
                match response {

                    Ok(r) => r,

                    Err(error) => {

                        println!(
                            "RPC #{} connection failed: {}",
                            index + 1,
                            error
                        );

                        continue;
                    }
                };

            if !response
                .status()
                .is_success()
            {

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

                    Err(error) => {

                        println!(
                            "RPC #{} invalid JSON: {}",
                            index + 1,
                            error
                        );

                        continue;
                    }
                };

            if let Some(result) =
                value
                    .get("result")
                    .and_then(|v| v.as_str())
            {

                if let Ok(block) =
                    parse_hex_u64(
                        result
                    )
                {

                    println!(
                        "Connected successfully to RPC #{} | Latest block: {}",
                        index + 1,
                        block
                    );

                    return Ok(
                        block
                    );
                }
            }

            println!(
                "RPC #{} did not return valid block height.",
                index + 1
            );
        }

        println!();
        println!(
            "All RPCs failed while checking latest block."
        );

        println!(
            "Waiting 30 seconds before trying again..."
        );

        sleep(
            Duration::from_secs(
                30
            )
        )
        .await;
    }
}


// ============================================================
// COMMAND LINE ARGUMENT
// ============================================================

fn get_arg(
    name: &str,
) -> Option<String> {

    let args:
        Vec<String> =
        env::args()
            .collect();

    for i in 0..args.len() {

        if args[i] == name
            && i + 1 < args.len()
        {

            return Some(
                args[i + 1].clone()
            );
        }
    }

    None
}


// ============================================================
// GITHUB RELEASE UPLOAD
// ============================================================

fn upload_to_release(
    tag: &str,
    file_name: &str,
) -> bool {

    println!();
    println!(
        "=============================================="
    );
    println!(
        "UPLOADING FILE"
    );
    println!(
        "=============================================="
    );

    println!(
        "File    : {}",
        file_name
    );

    println!(
        "Release : {}",
        tag
    );

    for attempt
        in 1..=UPLOAD_RETRIES
    {

        println!(
            "Upload attempt {}/{}...",
            attempt,
            UPLOAD_RETRIES
        );

        let status =
            Command::new("gh")
                .args([
                    "release",
                    "upload",
                    tag,
                    file_name,
                    "--clobber",
                ])
                .status();

        match status {

            Ok(s)
                if s.success() =>
            {

                println!(
                    "Successfully uploaded: {}",
                    file_name
                );

                if let Err(error) =
                    fs::remove_file(
                        file_name
                    )
                {

                    eprintln!(
                        "Warning: could not remove {}: {}",
                        file_name,
                        error
                    );
                }

                return true;
            }

            _ => {

                eprintln!(
                    "Upload failed on attempt {}",
                    attempt
                );

                if attempt
                    < UPLOAD_RETRIES
                {

                    std::thread::sleep(
                        Duration::from_secs(
                            5
                        )
                    );
                }
            }
        }
    }

    eprintln!(
        "FAILED to upload {} after {} attempts.",
        file_name,
        UPLOAD_RETRIES
    );

    false
}


// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main()
    -> Result<
        (),
        Box<dyn std::error::Error>
    >
{

    println!();
    println!(
        "======================================================="
    );
    println!(
        "        EVM BLOCKWISE ADDRESS EXTRACTOR v2.4"
    );
    println!(
        "======================================================="
    );
    println!();

    // ========================================================
    // CHAIN
    // ========================================================

    let chain =
        get_arg(
            "--chain"
        )
        .or_else(|| {
            env::var(
                "CHAIN"
            ).ok()
        })
        .unwrap_or_else(|| {
            "bnb".to_string()
        });

    // ========================================================
    // START BLOCK
    // ========================================================

    let start_block:
        u64 =
        get_arg(
            "--start-block"
        )
        .or_else(|| {
            env::var(
                "START_BLOCK"
            ).ok()
        })
        .unwrap_or_else(|| {
            "0".to_string()
        })
        .parse()?;

    // ========================================================
    // END BLOCK
    // ========================================================

    let end_block:
        u64 =
        get_arg(
            "--end-block"
        )
        .or_else(|| {
            env::var(
                "END_BLOCK"
            ).ok()
        })
        .unwrap_or_else(|| {
            "0".to_string()
        })
        .parse()?;

    // ========================================================
    // BATCH SIZE
    // ========================================================

    let batch_size:
        u64 =
        get_arg(
            "--batch-size"
        )
        .or_else(|| {
            env::var(
                "BATCH_SIZE"
            ).ok()
        })
        .unwrap_or_else(|| {
            DEFAULT_BATCH_SIZE
                .to_string()
        })
        .parse()?;

    // ========================================================
    // CONCURRENCY
    // ========================================================

    let concurrency:
        usize =
        get_arg(
            "--concurrency"
        )
        .or_else(|| {
            env::var(
                "CONCURRENCY"
            ).ok()
        })
        .unwrap_or_else(|| {
            DEFAULT_CONCURRENCY
                .to_string()
        })
        .parse()?;

    // ========================================================
    // VALIDATION
    // ========================================================

    if start_block
        > end_block
    {

        return Err(
            "start block cannot be greater than end block"
                .into()
        );
    }

    if batch_size == 0 {

        return Err(
            "batch size must be greater than zero"
                .into()
        );
    }

    if concurrency == 0 {

        return Err(
            "concurrency must be greater than zero"
                .into()
        );
    }

    // ========================================================
    // PRINT CONFIG
    // ========================================================

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

    println!(
        "RPC rotate  : every {} blocks",
        RPC_ROTATE_BLOCKS
    );

    println!(
        "Rotate wait : {} seconds",
        RPC_ROTATE_WAIT_SECS
    );

    println!();

    // ========================================================
    // RPC LIST
    // ========================================================

    let rpcs =
        rpc_list(
            &chain
        );

    println!(
        "RPC providers configured: {}",
        rpcs.len()
    );

    for (i, rpc)
        in rpcs
            .iter()
            .enumerate()
    {

        println!(
            "  RPC #{}: {}",
            i + 1,
            rpc
        );
    }

    println!();

    // ========================================================
    // HTTP CLIENT
    // ========================================================

    let client =
        Client::builder()
            .connect_timeout(
                Duration::from_secs(
                    15
                )
            )
            .timeout(
                Duration::from_secs(
                    HTTP_TIMEOUT_SECS
                )
            )
            .pool_idle_timeout(
                Duration::from_secs(
                    30
                )
            )
            .pool_max_idle_per_host(
                8
            )
            .user_agent(
                "evm-blockwise-extractor/2.4"
            )
            .build()?;

    let client =
        Arc::new(
            client
        );

    // ========================================================
    // LATEST BLOCK
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

    if end_block
        > latest_block
    {

        return Err(
            format!(
                "Requested end block {} is greater than latest chain block {}",
                end_block,
                latest_block
            )
            .into()
        );
    }

    // ========================================================
    // RELEASE TAG
    // ========================================================

    let release_tag =
        env::var(
            "RELEASE_TAG"
        ).ok();

    match &release_tag {

        Some(tag) => {

            println!(
                "GitHub Release: {}",
                tag
            );
        }

        None => {

            println!(
                "GitHub Release: not configured"
            );
        }
    }

    // ========================================================
    // PART LOOP
    // ========================================================

    let mut part_start =
        start_block;

    let mut part_num:
        u32 =
        1;

    while part_start
        <= end_block
    {

        let part_end =
            part_start
                .saturating_add(
                    PART_SIZE - 1
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

        // ====================================================
        // UNIQUE ADDRESSES
        // ====================================================

        let mut unique_addresses:
            HashSet<String> =
            HashSet::new();

        let mut part_total_blocks:
            u64 =
            0;

        let mut part_total_transactions:
            u64 =
            0;

        let mut part_total_addresses:
            u64 =
            0;

        // ====================================================
        // 20K SEGMENT LOOP
        // ====================================================

        let mut segment_start =
            part_start;

        let mut segment_number:
            u64 =
            0;

        while segment_start
            <= part_end
        {

            // ------------------------------------------------
            // Calculate segment end.
            //
            // Exactly 20,000 blocks maximum.
            // ------------------------------------------------

            let segment_end =
                segment_start
                    .saturating_add(
                        RPC_ROTATE_BLOCKS - 1
                    )
                    .min(
                        part_end
                    );

            // ------------------------------------------------
            // Preferred RPC
            //
            // segment_number 0 -> RPC #1
            // segment_number 1 -> RPC #2
            // ...
            // segment_number 6 -> RPC #7
            // segment_number 7 -> RPC #1
            // ------------------------------------------------

            let preferred_rpc =
                (
                    segment_number
                        as usize
                )
                % rpcs.len();

            println!();
            println!(
                "-------------------------------------------------------"
            );

            println!(
                "20K SEGMENT #{}",
                segment_number + 1
            );

            println!(
                "Blocks: {} -> {}",
                segment_start,
                segment_end
            );

            println!(
                "Preferred RPC: #{}",
                preferred_rpc + 1
            );

            println!(
                "RPC: {}",
                rpcs[preferred_rpc]
            );

            println!(
                "-------------------------------------------------------"
            );

            // =================================================
            // CREATE BATCHES
            // =================================================

            let mut batches:
                Vec<Vec<u64>> =
                Vec::new();

            let mut current =
                segment_start;

            while current
                <= segment_end
            {

                let batch_end =
                    current
                        .saturating_add(
                            batch_size - 1
                        )
                        .min(
                            segment_end
                        );

                batches.push(
                    (
                        current
                            ..=batch_end
                    )
                    .collect()
                );

                if batch_end
                    == u64::MAX
                {
                    break;
                }

                current =
                    batch_end + 1;
            }

            let total_batches =
                batches.len();

            println!(
                "Total batches in segment: {}",
                total_batches
            );

            println!();

            // =================================================
            // SHARED RPC DATA
            // =================================================

            let shared_client =
                Arc::clone(
                    &client
                );

            let shared_rpcs =
                Arc::new(
                    rpcs.clone()
                );

            // =================================================
            // CONCURRENT BATCH STREAM
            // =================================================

            let mut batch_stream =
                stream::iter(
                    batches
                )
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
                                preferred_rpc,
                            )
                            .await;

                        (
                            first,
                            last,
                            result
                        )
                    }
                })
                .buffer_unordered(
                    concurrency
                );

            // =================================================
            // SEGMENT COUNTERS
            // =================================================

            let mut processed_batches:
                usize =
                0;

            let mut segment_blocks:
                u64 =
                0;

            // =================================================
            // PROCESS BATCHES
            // =================================================

            while let Some(
                (
                    first,
                    last,
                    result
                )
            ) =
                batch_stream
                    .next()
                    .await
            {

                match result {

                    Ok((
                        addresses,
                        tx_count
                    )) => {

                        part_total_transactions +=
                            tx_count as u64;

                        part_total_addresses +=
                            addresses.len() as u64;

                        for address
                            in addresses
                        {

                            unique_addresses
                                .insert(
                                    address
                                );
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
                            .into()
                        );
                    }
                }

                processed_batches +=
                    1;

                let current_blocks =
                    last
                        .saturating_sub(
                            first
                        )
                        + 1;

                segment_blocks +=
                    current_blocks;

                part_total_blocks +=
                    current_blocks;

                // ------------------------------------------------
                // Progress
                // ------------------------------------------------

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

                    println!(
                        "Progress: {}/{} ({:.2}%) | Segment blocks: {} | Part blocks: {} | Transactions: {} | Addresses seen: {} | Unique addresses: {}",
                        processed_batches,
                        total_batches,
                        percentage,
                        segment_blocks,
                        part_total_blocks,
                        part_total_transactions,
                        part_total_addresses,
                        unique_addresses.len()
                    );
                }
            }

            // =================================================
            // 20K BOUNDARY
            // =================================================

            if segment_end
                < part_end
            {

                println!();
                println!(
                    "======================================================="
                );

                println!(
                    "20,000 BLOCK SEGMENT COMPLETED"
                );

                println!(
                    "Blocks: {} -> {}",
                    segment_start,
                    segment_end
                );

                println!(
                    "Blocks processed in this segment: {}",
                    segment_blocks
                );

                println!(
                    "Current unique addresses: {}",
                    unique_addresses.len()
                );

                println!(
                    "======================================================="
                );

                // ------------------------------------------------
                // WAIT 10 SEC
                // ------------------------------------------------

                println!();
                println!(
                    "WAITING {} SECONDS...",
                    RPC_ROTATE_WAIT_SECS
                );

                sleep(
                    Duration::from_secs(
                        RPC_ROTATE_WAIT_SECS
                    )
                )
                .await;

                // ------------------------------------------------
                // CHANGE RPC
                // ------------------------------------------------

                segment_number =
                    segment_number
                        .saturating_add(
                            1
                        );

                let next_rpc =
                    (
                        segment_number
                            as usize
                    )
                    % rpcs.len();

                println!();
                println!(
                    "======================================================="
                );

                println!(
                    "RPC ROTATION"
                );

                println!(
                    "Old preferred RPC: #{}",
                    preferred_rpc + 1
                );

                println!(
                    "New preferred RPC: #{}",
                    next_rpc + 1
                );

                println!(
                    "New RPC: {}",
                    rpcs[next_rpc]
                );

                println!(
                    "======================================================="
                );
            }

            // ------------------------------------------------
            // Move to next segment
            // ------------------------------------------------

            if segment_end
                == u64::MAX
            {
                break;
            }

            segment_start =
                segment_end + 1;
        }

        // ====================================================
        // PART SUMMARY
        // ====================================================

        println!();
        println!(
            "======================================================="
        );

        println!(
            "PART SUMMARY"
        );

        println!(
            "======================================================="
        );

        println!(
            "Part                : {}",
            part_num
        );

        println!(
            "Blocks              : {} -> {}",
            part_start,
            part_end
        );

        println!(
            "Total blocks        : {}",
            part_total_blocks
        );

        println!(
            "Transactions        : {}",
            part_total_transactions
        );

        println!(
            "Addresses seen      : {}",
            part_total_addresses
        );

        println!(
            "Unique addresses    : {}",
            unique_addresses.len()
        );

        println!(
            "======================================================="
        );

        // ====================================================
        // WRITE CSV.GZ
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
            release_tag
                .as_deref()
        {

            if !upload_to_release(
                tag,
                &file_name,
            ) {

                return Err(
                    format!(
                        "Could not upload {} to GitHub Release",
                        file_name
                    )
                    .into()
                );
            }
        }

        // ====================================================
        // NEXT PART
        // ====================================================

        println!();
        println!(
            "Moving to next part..."
        );

        if part_end
            == u64::MAX
        {
            break;
        }

        part_start =
            part_end + 1;

        part_num +=
            1;
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
