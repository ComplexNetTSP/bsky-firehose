mod firehose;
mod parquer_writer;

use anyhow::{Context, Result};
use atrium_api::com::atproto::sync::subscribe_repos::CommitData;
use clap::Parser;
use firehose::{FirehoseMessage, decode_body, decode_header, split_frame};
use futures::StreamExt;
use log::{error, info};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};
use parquer_writer::{BlockWriter, CommitWriter};
use std::time::Duration;
use tokio::{spawn, sync::mpsc, time::sleep};
use tokio_tungstenite::connect_async;

fn setup_logger(logfile: &str) -> Result<()> {
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}\n")))
        .build(logfile)?;

    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(
            Root::builder()
                .appender("logfile")
                .build(log::LevelFilter::Info),
        )?;

    log4rs::init_config(config)?;
    Ok(())
}

async fn run_firehose(
    commit_tx: mpsc::Sender<CommitData>,
    block_tx: mpsc::Sender<CommitData>,
    cursor: Option<i64>,
    max_retries: u32,
) {
    let mut current_cursor = cursor;
    let mut retry_count = 0;
    const BASE_DELAY_MS: u64 = 1000; // 1 second
    loop {
        let url = firehose_url(current_cursor);
        info!(
            "Attempting to connect to firehose (attempt {}/{}, cursor: {:?}) at URL: {}",
            retry_count + 1,
            max_retries,
            current_cursor,
            url
        );
        // connect to bluesky firehose endpoint
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                let (_, mut read) = ws_stream.split();

                // receive message and parse them
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                            if let Ok(FirehoseMessage::Commit(commit)) =
                                decode_raw_firehose_message(&data)
                            {
                                current_cursor = Some(commit.seq);
                                if let Err(e) = commit_tx.send(*commit.clone()).await {
                                    error!(
                                        "Error sending commit (seq: {}) to commit writer: {:?}",
                                        commit.seq, e
                                    );
                                }
                                if let Err(e) = block_tx.send(*commit.clone()).await {
                                    error!(
                                        "Error sending commit (seq: {}) to block writer: {:?}",
                                        commit.seq, e
                                    );
                                }
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                            error!(
                                "WebSocket closed by server (cursor: {:?}). Reconnecting...",
                                current_cursor
                            );
                            break;
                        }
                        Err(e) => {
                            error!(
                                "WebSocket error (cursor: {:?}, attempt {}/{}): {}",
                                current_cursor,
                                retry_count + 1,
                                max_retries,
                                e
                            );
                            break;
                        }
                        _ => {} // Ignore other message types (e.g., Text, Ping, Pong)
                    }
                }
            }
            Err(e) => {
                error!(
                    "Connection failed (cursor: {:?}, attempt {}/{}): {}. Retrying...",
                    current_cursor,
                    retry_count + 1,
                    max_retries,
                    e
                );
            }
        }

        // retry until MAX_RETRIES
        if retry_count >= max_retries {
            error!(
                "Max retries ({}) exceeded. Giving up. Last cursor: {:?}",
                max_retries, current_cursor
            );
            break;
        }

        // Exponential backoff: 1s, 2s, 4s, 8s, etc.
        let delay_ms = BASE_DELAY_MS * 2u64.pow(retry_count);
        info!(
            "Reconnecting to firehose (attempt {}/{}, cursor: {:?}) in {}ms...",
            retry_count + 1,
            max_retries,
            current_cursor,
            delay_ms
        );
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

    /// Log file
    #[arg(short, long, default_value_t = String::from("bsky.log"))]
    logfile: String,

    /// Maximum number of retry to connect the bluesky firehose
    #[arg(short, long, default_value_t = 100)]
    max_retries: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    // initiliaze logger
    setup_logger(&args.logfile).context("Unable to setup logger")?;

    // setup commit writer
    let (commit_tx, commit_rx) = mpsc::channel(args.batch_size * 2);
    span_commit_writer(commit_rx, args.batch_size, args.output_dir.as_ref());

    // setup block writer
    let (block_tx, block_rx) = mpsc::channel(args.batch_size * 2);
    span_block_writer(
        block_rx,
        args.batch_size,
        args.output_dir.as_ref(),
        args.facets,
    );

    // stream firehose
    run_firehose(commit_tx, block_tx, args.cursor, args.max_retries).await;
    Ok(())
}
