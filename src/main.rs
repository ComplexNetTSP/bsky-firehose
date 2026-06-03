mod firehose;
mod parquer_writer;

use anyhow::{Context, Result};
use clap::Parser;
use firehose::{FirehoseMessage, decode_body, decode_header, split_frame};
use futures::StreamExt;
use parquer_writer::CommitWriter;
use tokio_tungstenite::connect_async;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut commit_writer = CommitWriter::new(args.batch_size, args.output_dir);
    let url = firehose_url(args.cursor); // Start from a specific sequence number
    println!("Connecting to firehose at URL: {}", url);
    let (ws_stream, _) = connect_async(&url).await?;
    let (_, mut read) = ws_stream.split();
    while let Some(msg) = read.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                if let Ok(FirehoseMessage::Commit(commit)) = decode_raw_firehose_message(&data)
                    && let Err(e) = commit_writer.add_commit(*commit)
                {
                    eprintln!("Error writing commit to Parquet: {}", e);
                }
            }
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                println!("WebSocket closed by server.")
            }
            Err(e) => eprintln!("WebSocket error: {}", e),
            _ => {} // Ignore other message types (e.g., Text, Ping, Pong)
        }
    }
    Ok(())
}
