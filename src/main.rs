mod firehose;
mod parquer_writer;

use anyhow::{Context, Result};
use atrium_api::com::atproto::sync::subscribe_repos::CommitData;
use clap::Parser;
use firehose::{FirehoseMessage, decode_body, decode_header, split_frame};
use futures::StreamExt;
use parquer_writer::{BlockWriter, CommitWriter};
use std::time::Duration;
use tokio::time::sleep;
use tokio::{spawn, sync::mpsc};
use tokio_tungstenite::connect_async;

async fn run_firehose(
    commit_tx: mpsc::Sender<CommitData>,
    block_tx: mpsc::Sender<CommitData>,
    cursor: Option<i64>,
) {
    let mut current_cursor = cursor;
    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 10;
    const BASE_DELAY_MS: u64 = 1000; // 1 second
    loop {
        let url = firehose_url(current_cursor); // Start from a specific sequence number
        println!("Connecting to firehose at URL: {}", url);
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                            if let Ok(FirehoseMessage::Commit(commit)) =
                                decode_raw_firehose_message(&data)
                            {
                                current_cursor = Some(commit.seq);
                                if let Err(e) = commit_tx.send(*commit.clone()).await {
                                    eprintln!("Error sending commit to commit writer: {:?}", e);
                                }
                                if let Err(e) = block_tx.send(*commit).await {
                                    eprintln!("Error sending commit to block writer: {:?}", e);
                                }
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                            println!("WebSocket closed by server.");
                            break;
                        }
                        Err(e) => {
                            eprintln!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {} // Ignore other message types (e.g., Text, Ping, Pong)
                    }
                }
            }
            Err(e) => {
                eprintln!("Connection failed: {}. Retrying...", e);
            }
        }

        if retry_count >= MAX_RETRIES {
            eprintln!("Max retries exceeded. Giving up.");
            break;
        }

        // Exponential backoff: 1s, 2s, 4s, 8s, etc.
        let delay_ms = BASE_DELAY_MS * 2u64.pow(retry_count);
        eprintln!("Reconnecting in {}ms...", delay_ms);
        sleep(Duration::from_millis(delay_ms)).await;
        retry_count += 1;
    }
}

fn span_block_writer(
    mut commit_rx: mpsc::Receiver<CommitData>,
    batch_size: usize,
    output_dir: &str,
    facets: bool,
) {
    let mut block_writer = BlockWriter::new(batch_size, output_dir, facets);
    spawn(async move {
        while let Some(commit) = commit_rx.recv().await {
            if let Err(e) = block_writer.add_commit(commit) {
                eprintln!("Error: {}", e);
            }
        }
    });
}

fn span_commit_writer(
    mut commit_rx: mpsc::Receiver<CommitData>,
    batch_size: usize,
    output_dir: &str,
) {
    let mut commit_writer = CommitWriter::new(batch_size, output_dir);
    spawn(async move {
        while let Some(commit) = commit_rx.recv().await {
            if let Err(e) = commit_writer.add_commit(commit) {
                eprintln!("Error: {}", e);
            }
        }
    });
}

///
/// Decodes a raw Bluesky CBOR-encoded Firehose message into a structured FirehoseMessage object.
///
fn decode_raw_firehose_message(raw_bytes: &[u8]) -> anyhow::Result<FirehoseMessage> {
    let (header, body) = split_frame(raw_bytes)?;
    let (_, event_type) =
        decode_header(header).context("Failed to decode firehose message header")?;
    decode_body(event_type, body).context("Failed to decode firehose message body")
}

fn firehose_url(cursor: Option<i64>) -> String {
    // Bluesky firehose WebSocket endpoint
    const FIREHOSE_URL: &str = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
    match cursor {
        Some(seq) => format!("{FIREHOSE_URL}?cursor={seq}"),
        None => FIREHOSE_URL.to_string(),
    }
}

/// bsky-firehose: A Rust client for the Bluesky firehose stream
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Output directory for the flushed frames
    #[arg(short, long, default_value_t = String::from("./output"))]
    output_dir: String,

    /// Maximum number of frames to keep in memory before flushing to disk
    #[arg(short, long, default_value_t = 1000)]
    batch_size: usize,

    /// Optional starting sequence number to resume from
    #[arg(short, long)]
    cursor: Option<i64>,

    /// filter out facets blocks
    #[arg(short, long, default_value_t = false)]
    facets: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Create channels for communication
    let (commit_tx, commit_rx) = mpsc::channel(args.batch_size * 2);
    span_commit_writer(commit_rx, args.batch_size, args.output_dir.as_ref());
    let (block_tx, block_rx) = mpsc::channel(args.batch_size * 2);
    span_block_writer(
        block_rx,
        args.batch_size,
        args.output_dir.as_ref(),
        args.facets,
    );
    run_firehose(commit_tx, block_tx, args.cursor).await;
    Ok(())
}
