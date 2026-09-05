use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================
// CONFIGURATION
// ============================================================

// RPC batch size.
// Small rakha hai taaki public RPCs par rate-limit kam aaye.
const BATCH_SIZE: u64 = 5;

// Ek time par kitne batches parallel chalenge.
// Public RPC ke liye 6 relatively safe hai.
const MAX_CONCURRENCY: usize = 6;

// Ek retry round me har RPC ko kitni baar try karna hai.
const RPC_RETRIES_PER_ROUND: usize = 2;

// Agar saare RPC fail ho jayein to maximum kitne retry rounds.
const MAX_BATCH_RETRY_ROUNDS: usize = 12;

// First retry delay.
const RETRY_DELAY_SECS: u64 = 15;

// Maximum wait between retry rounds.
const MAX_RETRY_DELAY_SECS: u64 = 120;

// Individual block ke liye retry rounds.
const INDIVIDUAL_RETRY_ROUNDS: usize = 8;


// ============================================================
// GITHUB RELEASE UPLOAD
// ============================================================

fn upload_to_release(tag: &str, file_name: &str) {
    println!(
        "Uploading {} to GitHub Release '{}'...",
        file_name, tag
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
            println!("Successfully uploaded: {}", file_name);

            let _ = fs::remove_file(file_name);
        }

        _ => {
            eprintln!(
                "Failed to upload {}. Keeping local file.",
                file_name
            );
        }
    }
}


// ============================================================
// RPC LIST
// ============================================================

fn rpc_urls(chain: &str) -> Vec<String> {
    match chain {

        // ----------------------------------------------------
        // BNB SMART CHAIN
        // ----------------------------------------------------
        "bnb" => vec![
            "https://bsc-dataseed.binance.org/".into(),
            "https://bsc-dataseed1.defibit.io/".into(),
            "https://bsc-dataseed1.ninicoin.io/".into(),
            "https://rpc.ankr.com/bsc".into(),
            "https://1rpc.io/bnb".into(),
            "https://bsc-rpc.publicnode.com".into(),
            "https://bsc.meowrpc.com".into(),
        ],

        // ----------------------------------------------------
        // ETHEREUM
        // ----------------------------------------------------
        "ethereum" => vec![
            "https://eth.llamarpc.com".into(),
            "https://cloudflare-eth.com".into(),
            "https://rpc.ankr.com/eth".into(),
            "https://1rpc.io/eth".into(),
            "https://ethereum-rpc.publicnode.com".into(),
        ],

        // ----------------------------------------------------
        // POLYGON
        // ----------------------------------------------------
        "polygon" => vec![
            "https://polygon-rpc.com".into(),
            "https://rpc.ankr.com/polygon".into(),
            "https://1rpc.io/matic".into(),
            "https://polygon-bor-rpc.publicnode.com".into(),
        ],

        // ----------------------------------------------------
        // ARBITRUM
        // ----------------------------------------------------
        "arbitrum" => vec![
            "https://arb1.arbitrum.io/rpc".into(),
            "https://rpc.ankr.com/arbitrum".into(),
            "https://1rpc.io/arb".into(),
            "https://arbitrum-one-rpc.publicnode.com".into(),
        ],

        // ----------------------------------------------------
        // BASE
        // ----------------------------------------------------
        "base" => vec![
            "https://mainnet.base.org".into(),
            "https://base.llamarpc.com".into(),
            "https://1rpc.io/base".into(),
            "https://base-rpc.publicnode.com".into(),
        ],

        // ----------------------------------------------------
        // OPTIMISM
        // ----------------------------------------------------
        "optimism" => vec![
            "https://mainnet.optimism.io".into(),
            "https://optimism.llamarpc.com".into(),
            "https://1rpc.io/op".into(),
            "https://optimism-rpc.publicnode.com".into(),
        ],

        // ----------------------------------------------------
        // AVALANCHE C-CHAIN
        // ----------------------------------------------------
        "avalanche_c" => vec![
            "https://api.avax.network/ext/bc/C/rpc".into(),
            "https://rpc.ankr.com/avalanche".into(),
            "https://1rpc.io/avax/c".into(),
            "https://avalanche-c-chain-rpc.publicnode.com".into(),
        ],

        // ----------------------------------------------------
        // FALLBACK
        // ----------------------------------------------------
        _ => vec![
            "https://bsc-dataseed.binance.org/".into(),
        ],
    }
}


// ============================================================
// TRANSACTION → ADDRESS EXTRACTION
// ============================================================

fn extract_addresses_from_tx(
    tx: &Value,
    set: &mut HashSet<String>,
) {
    if let Some(from) = tx
        .get("from")
        .and_then(Value::as_str)
    {
        set.insert(from.to_lowercase());
    }

    if let Some(to) = tx
        .get("to")
        .and_then(Value::as_str)
    {
        set.insert(to.to_lowercase());
    }
}


// ============================================================
// BLOCK RESULT PROCESSOR
// ============================================================

fn process_block_result(
    result: &Value,
    set: &mut HashSet<String>,
) -> Result<(), String> {

    if result.is_null() {
        return Err(
            "null block result".to_string()
        );
    }

    let txs = result
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            "invalid block result: transactions missing"
                .to_string()
        })?;

    for tx in txs {
        extract_addresses_from_tx(
            tx,
            set,
        );
    }

    Ok(())
}


// ============================================================
// GENERIC JSON-RPC REQUEST
// ============================================================

async fn send_json(
    client: &reqwest::Client,
    rpc: &str,
    payload: &Value,
) -> Result<Value, String> {

    let response = client
        .post(rpc)
        .header(
            "content-type",
            "application/json",
        )
        .header(
            "accept",
            "application/json",
        )
        .json(payload)
        .send()
        .await
        .map_err(|e| {
            format!("network error: {}", e)
        })?;

    let status = response.status();

    let body = response
        .text()
        .await
        .map_err(|e| {
            format!(
                "response body error: {}",
                e
            )
        })?;

    if !status.is_success() {

        let short_body = body
            .chars()
            .take(300)
            .collect::<String>();

        return Err(format!(
            "HTTP {} {}",
            status,
            short_body
        ));
    }

    serde_json::from_str::<Value>(&body)
        .map_err(|e| {

            let short_body = body
                .chars()
                .take(300)
                .collect::<String>();

            format!(
                "invalid JSON: {} | body: {}",
                e,
                short_body
            )
        })
}


// ============================================================
// ONE RPC BATCH ATTEMPT
// ============================================================

async fn fetch_batch_once(
    client: &reqwest::Client,
    rpc: &str,
    blocks: &[u64],
) -> Result<Vec<(u64, Value)>, String> {

    let payload: Vec<Value> = blocks
        .iter()
        .map(|&b| {

            json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [
                    format!("0x{:x}", b),
                    true
                ],
                "id": b
            })

        })
        .collect();

    let response = send_json(
        client,
        rpc,
        &Value::Array(payload),
    )
    .await?;

    // RPC response batch hona chahiye.
    // Agar map/error response aaye to yahin fail hoga
    // aur next RPC try hoga.
    let arr = response
        .as_array()
        .ok_or_else(|| {
            format!(
                "RPC returned non-array JSON: {}",
                response
            )
        })?;

    let wanted: HashSet<u64> =
        blocks.iter().copied().collect();

    let mut results =
        HashMap::<u64, Value>::new();

    let mut errors = Vec::new();

    for item in arr {

        if let Some(id) =
            item.get("id")
                .and_then(Value::as_u64)
        {

            // Normal successful response.
            if let Some(result) =
                item.get("result")
            {
                if wanted.contains(&id) {
                    results.insert(
                        id,
                        result.clone(),
                    );
                }
            }

            // RPC error response.
            else if let Some(error) =
                item.get("error")
            {
                errors.push(format!(
                    "block {} RPC error: {}",
                    id,
                    error
                ));
            }
        }
    }

    // Incomplete response.
    if results.len() != blocks.len() {

        if errors.is_empty() {

            return Err(format!(
                "incomplete batch: received {} / {} blocks",
                results.len(),
                blocks.len()
            ));

        } else {

            return Err(format!(
                "incomplete batch: received {} / {} blocks; {}",
                results.len(),
                blocks.len(),
                errors.join(" | ")
            ));
        }
    }

    let mut ordered =
        Vec::with_capacity(blocks.len());

    for &b in blocks {

        let result = results
            .remove(&b)
            .ok_or_else(|| {
                format!(
                    "missing block {} in RPC response",
                    b
                )
            })?;

        ordered.push((b, result));
    }

    Ok(ordered)
}


// ============================================================
// INDIVIDUAL BLOCK RETRY
// ============================================================

async fn fetch_individual_block(
    client: &reqwest::Client,
    rpc_urls: &[String],
    block: u64,
) -> Result<Value, String> {

    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [
            format!("0x{:x}", block),
            true
        ],
        "id": block
    });

    for round in 1..=INDIVIDUAL_RETRY_ROUNDS {

        println!(
            "Individual block {} | retry round {}/{}",
            block,
            round,
            INDIVIDUAL_RETRY_ROUNDS
        );

        // Try every RPC.
        for (idx, rpc) in
            rpc_urls.iter().enumerate()
        {

            match send_json(
                client,
                rpc,
                &payload,
            )
            .await
            {
                Ok(value) => {

                    // Explicit JSON-RPC error.
                    if let Some(error) =
                        value.get("error")
                    {
                        println!(
                            "Block {} failed on RPC #{}: RPC error {}",
                            block,
                            idx + 1,
                            error
                        );

                        continue;
                    }

                    // Successful block.
                    if let Some(result) =
                        value.get("result")
                    {
                        if !result.is_null() {

                            println!(
                                "Block {} recovered using RPC #{}",
                                block,
                                idx + 1
                            );

                            return Ok(
                                result.clone()
                            );
                        }
                    }

                    println!(
                        "Block {} failed on RPC #{}: invalid/null block result",
                        block,
                        idx + 1
                    );
                }

                Err(err) => {

                    println!(
                        "Block {} failed on RPC #{}: {}",
                        block,
                        idx + 1,
                        err
                    );
                }
            }
        }

        // Sab RPC fail ho gaye.
        // Immediately exit nahi karna.
        // Break lekar same block dobara.
        if round < INDIVIDUAL_RETRY_ROUNDS {

            let delay =
                (
                    RETRY_DELAY_SECS
                        * 2u64.pow(
                            (round - 1)
                                .min(3)
                                as u32
                        )
                )
                .min(
                    MAX_RETRY_DELAY_SECS
                );

            println!(
                "Block {}: ALL RPCs failed. Sleeping {} seconds before retry...",
                block,
                delay
            );

            sleep(
                Duration::from_secs(delay)
            )
            .await;
        }
    }

    Err(format!(
        "Block {} failed after all RPC retry rounds",
        block
    ))
}


// ============================================================
// BATCH RETRY ENGINE
// ============================================================

async fn fetch_batch_with_retry(
    client: &reqwest::Client,
    rpc_urls: &[String],
    blocks: Vec<u64>,
) -> Result<Vec<Value>, String> {

    for round in
        1..=MAX_BATCH_RETRY_ROUNDS
    {

        println!(
            "Trying batch {}-{} | retry round {}/{}",
            blocks[0],
            blocks[blocks.len() - 1],
            round,
            MAX_BATCH_RETRY_ROUNDS
        );

        // ----------------------------------------------------
        // TRY EVERY RPC
        // ----------------------------------------------------

        for (idx, rpc) in
            rpc_urls.iter().enumerate()
        {

            // Har RPC ko 2 attempts.
            for attempt in
                1..=RPC_RETRIES_PER_ROUND
            {

                match fetch_batch_once(
                    client,
                    rpc,
                    &blocks,
                )
                .await
                {

                    // SUCCESS
                    Ok(results) => {

                        // Validate every block.
                        let mut local =
                            HashSet::new();

                        for (
                            _block,
                            result
                        ) in &results
                        {

                            process_block_result(
                                result,
                                &mut local,
                            )?;
                        }

                        println!(
                            "Batch {}-{} recovered using RPC #{} (attempt {}) | Addresses in batch: {}",
                            blocks[0],
                            blocks[blocks.len() - 1],
                            idx + 1,
                            attempt,
                            local.len()
                        );

                        let output =
                            results
                                .into_iter()
                                .map(
                                    |(_, result)|
                                        result
                                )
                                .collect();

                        return Ok(output);
                    }

                    // FAILURE
                    Err(err) => {

                        println!(
                            "Batch {}-{} failed on RPC #{} attempt {}: {}",
                            blocks[0],
                            blocks[blocks.len() - 1],
                            idx + 1,
                            attempt,
                            err
                        );
                    }
                }

                // Small gap before same RPC attempt.
                sleep(
                    Duration::from_secs(2)
                )
                .await;
            }
        }

        // ----------------------------------------------------
        // ALL RPC FAILED
        // ----------------------------------------------------

        if round <
            MAX_BATCH_RETRY_ROUNDS
        {

            // 15 sec
            // 30 sec
            // 60 sec
            // 120 sec
            // 120 sec...
            let delay =
                (
                    RETRY_DELAY_SECS
                        * 2u64.pow(
                            (round - 1)
                                .min(3)
                                as u32
                        )
                )
                .min(
                    MAX_RETRY_DELAY_SECS
                );

            println!();
            println!(
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            );
            println!(
                "Batch {}-{}: ALL RPCs FAILED",
                blocks[0],
                blocks[blocks.len() - 1]
            );
            println!(
                "Taking {} seconds BREAK...",
                delay
            );
            println!(
                "After break SAME batch will be retried."
            );
            println!(
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            );
            println!();

            sleep(
                Duration::from_secs(delay)
            )
            .await;
        }
    }

    // --------------------------------------------------------
    // BATCH RETRIES EXHAUSTED
    // --------------------------------------------------------

    println!();
    println!(
        "Batch {}-{} batch retries exhausted.",
        blocks[0],
        blocks[blocks.len() - 1]
    );

    println!(
        "Falling back to INDIVIDUAL BLOCK retries..."
    );

    let mut results =
        Vec::with_capacity(
            blocks.len()
        );

    for block in blocks {

        let result =
            fetch_individual_block(
                client,
                rpc_urls,
                block,
            )
            .await?;

        results.push(result);
    }

    Ok(results)
}


// ============================================================
// GET LATEST BLOCK
// ============================================================

async fn get_latest_block(
    client: &reqwest::Client,
    rpc_urls: &[String],
) -> Result<u64, String> {

    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    });

    for round in 1..=10 {

        for (idx, rpc) in
            rpc_urls.iter().enumerate()
        {

            match send_json(
                client,
                rpc,
                &payload,
            )
            .await
            {

                Ok(value) => {

                    if let Some(hex) =
                        value
                            .get("result")
                            .and_then(Value::as_str)
                    {

                        let block =
                            u64::from_str_radix(
                                hex.trim_start_matches(
                                    "0x"
                                ),
                                16,
                            )
                            .map_err(
                                |e|
                                    e.to_string()
                            )?;

                        println!(
                            "Connected to RPC #{}: {} | Height: {}",
                            idx + 1,
                            rpc,
                            block
                        );

                        return Ok(block);
                    }
                }

                Err(err) => {

                    println!(
                        "Latest block failed on RPC #{}: {}",
                        idx + 1,
                        err
                    );
                }
            }
        }

        let delay =
            (
                10u64
                    * 2u64.pow(
                        (round - 1)
                            .min(3)
                            as u32
                    )
            )
            .min(120);

        println!(
            "All latest-block RPCs failed. Sleeping {} seconds...",
            delay
        );

        sleep(
            Duration::from_secs(delay)
        )
        .await;
    }

    Err(
        "Unable to obtain latest block from any RPC"
            .to_string()
    )
}


// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error>>
{
    // --------------------------------------------------------
    // ENVIRONMENT
    // --------------------------------------------------------

    let release_tag =
        env::var("RELEASE_TAG").ok();

    let chain =
        env::var("TARGET_CHAIN")
            .unwrap_or_else(
                |_| "bnb".to_string()
            );

    let start_block: u64 =
        env::var("START_BLOCK")
            .unwrap_or_else(
                |_| "0".to_string()
            )
            .parse()?;

    let requested_end_block:
        Option<u64> =
        env::var("END_BLOCK")
            .ok()
            .and_then(|v| {

                if v.trim().is_empty() {
                    None
                } else {
                    v.parse().ok()
                }
            });


    // --------------------------------------------------------
    // RPC + HTTP CLIENT
    // --------------------------------------------------------

    let rpc_list =
        Arc::new(
            rpc_urls(&chain)
        );

    let client =
        Arc::new(
            reqwest::Client::builder()
                .timeout(
                    Duration::from_secs(45)
                )
                .pool_max_idle_per_host(2)
                .build()?
        );


    // --------------------------------------------------------
    // START
    // --------------------------------------------------------

    println!(
        "==============================================="
    );

    println!(
        "EVM BLOCK-WISE ADDRESS EXTRACTOR"
    );

    println!(
        "Chain: {}",
        chain.to_uppercase()
    );

    println!(
        "Start block: {}",
        start_block
    );

    println!(
        "==============================================="
    );


    // --------------------------------------------------------
    // LATEST BLOCK
    // --------------------------------------------------------

    let latest =
        get_latest_block(
            &client,
            &rpc_list,
        )
        .await?;


    let end_block =
        requested_end_block
            .unwrap_or(latest)
            .min(latest);


    if start_block > end_block {

        return Err(
            format!(
                "START_BLOCK {} is greater than END_BLOCK {}",
                start_block,
                end_block
            )
            .into()
        );
    }


    // --------------------------------------------------------
    // CONFIG DISPLAY
    // --------------------------------------------------------

    println!(
        "Actual range: {} -> {}",
        start_block,
        end_block
    );

    println!(
        "RPC count: {}",
        rpc_list.len()
    );

    println!(
        "Batch size: {} blocks",
        BATCH_SIZE
    );

    println!(
        "Concurrency: {}",
        MAX_CONCURRENCY
    );

    println!(
        "Failed batches will PAUSE and RETRY."
    );

    println!(
        "==============================================="
    );


    // --------------------------------------------------------
    // OUTPUT DIRECTORY
    // --------------------------------------------------------

    fs::create_dir_all(
        "output"
    )?;


    // --------------------------------------------------------
    // CALCULATE TOTAL BATCHES
    // --------------------------------------------------------

    let total_blocks =
        end_block
            - start_block
            + 1;

    let total_batches =
        (
            total_blocks
                + BATCH_SIZE
                - 1
        )
        / BATCH_SIZE;


    println!(
        "Total blocks: {}",
        total_blocks
    );

    println!(
        "Total batches: {}",
        total_batches
    );


    // --------------------------------------------------------
    // BUILD BATCH LIST
    // --------------------------------------------------------

    let batches:
        Vec<Vec<u64>> =
        (0..total_batches)
            .map(|i| {

                let first =
                    start_block
                        + i * BATCH_SIZE;

                let last =
                    (
                        first
                            + BATCH_SIZE
                            - 1
                    )
                    .min(end_block);

                (first..=last)
                    .collect::<Vec<u64>>()
            })
            .collect();


    // --------------------------------------------------------
    // GLOBAL UNIQUE ADDRESS SET
    // --------------------------------------------------------

    let mut unique_set:
        HashSet<String> =
        HashSet::new();


    let mut completed_batches:
        u64 = 0;


    // --------------------------------------------------------
    // PARALLEL STREAM
    // --------------------------------------------------------

    let mut stream =
        stream::iter(batches)
            .map(|blocks| {

                let client =
                    Arc::clone(&client);

                let rpcs =
                    Arc::clone(&rpc_list);

                async move {

                    let first =
                        blocks[0];

                    let last =
                        blocks[
                            blocks.len()
                                - 1
                        ];


                    // Main retry engine.
                    let results =
                        fetch_batch_with_retry(
                            &client,
                            &rpcs,
                            blocks,
                        )
                        .await?;


                    // Extract addresses.
                    let mut local =
                        HashSet::new();

                    for result in
                        results
                    {
                        process_block_result(
                            &result,
                            &mut local,
                        )?;
                    }


                    Ok::<
                        (
                            u64,
                            u64,
                            HashSet<String>
                        ),
                        String
                    >(
                        (
                            first,
                            last,
                            local
                        )
                    )
                }
            })
            .buffer_unordered(
                MAX_CONCURRENCY
            );


    // --------------------------------------------------------
    // RECEIVE COMPLETED BATCHES
    // --------------------------------------------------------

    while let Some(result) =
        stream.next().await
    {

        match result {

            Ok((
                first,
                last,
                addresses,
            )) => {

                unique_set.extend(
                    addresses
                );

                completed_batches += 1;


                let processed_blocks =
                    (
                        completed_batches
                            * BATCH_SIZE
                    )
                    .min(total_blocks);


                let percent =
                    processed_blocks
                        as f64
                        * 100.0
                        / total_blocks
                            as f64;


                println!(
                    "Progress: {}/{} ({:.2}%) | Last batch: {}-{} | Unique addresses: {}",
                    completed_batches,
                    total_batches,
                    percent,
                    first,
                    last,
                    unique_set.len()
                );
            }


            Err(err) => {

                return Err(
                    format!(
                        "PERMANENT EXTRACTION ERROR: {}",
                        err
                    )
                    .into()
                );
            }
        }
    }


    // --------------------------------------------------------
    // WRITE FINAL CSV.GZ
    // --------------------------------------------------------

    let file_name =
        format!(
            "output/{}_addresses_{}_to_{}.csv.gz",
            chain,
            start_block,
            end_block
        );


    let file =
        File::create(
            &file_name
        )?;


    let encoder =
        GzEncoder::new(
            file,
            Compression::default(),
        );


    let mut writer =
        csv::Writer::from_writer(
            encoder
        );


    writer.write_record([
        "address"
    ])?;


    // Sort addresses for deterministic output.
    let mut addresses:
        Vec<String> =
        unique_set
            .into_iter()
            .collect();


    addresses.sort_unstable();


    for address in
        addresses
    {
        writer.write_record([
            address
        ])?;
    }


    writer.flush()?;


    // --------------------------------------------------------
    // COMPLETE
    // --------------------------------------------------------

    println!(
        "==============================================="
    );

    println!(
        "EXTRACTION COMPLETE"
    );

    println!(
        "File: {}",
        file_name
    );

    println!(
        "==============================================="
    );


    // --------------------------------------------------------
    // GITHUB RELEASE
    // --------------------------------------------------------

    if let Some(tag) =
        release_tag.as_deref()
    {
        upload_to_release(
            tag,
            &file_name
        );
    }


    Ok(())
}
