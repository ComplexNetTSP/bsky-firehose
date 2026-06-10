# Bluesky Firehose Consumer

[![GitHub Release](https://img.shields.io/github/v/release/ComplexNetTSP/bsky-firehose)](https://github.com/ComplexNetTSP/bsky-firehose/releases)

A Rust application that connects to the Bluesky AT Protocol firehose and writes commit and block data to Parquet files.

**Author**: Vincent Gauthier <vincent.gauthier@telecom-sudparis.eu>

## Features

- Real-time consumption of Bluesky's WebSocket firehose
- CBOR message decoding for firehose events
- Automatic reconnection with exponential backoff (1s, 2s, 4s...)
- Dual output: separate Parquet files for commits and blocks
- Batch writing with configurable batch size
- Timestamped hierarchical output:
  ```
  output/
  ├── commits/year=YYYY/month=MM/day=DD/commit_TIMESTAMP.parquet
  └── blocks/year=YYYY/month=MM/day=DD/block_TIMESTAMP.parquet
  ```

## Prerequisites

- Rust 1.92.0 or later
- Cargo

## Installation

### From Source
```bash
git clone https://github.com/ComplexNetTSP/bsky-firehose.git
cd bsky-firehose
cargo build --release
```

## Usage
```bash
./target/release/bsky-firehose --output-dir ./data --batch-size 1000 --cursor 12345
```

## Options
| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output-dir` | `./output` | Base directory for Parquet files |
| `-b, --batch-size` | `1000` | Number of records per file |
| `-c, --cursor` | `None` | Start from specific sequence number |
| `-f, --facets` | `false` | Filter out facets blocks |
| `-l, --logfile` | `bsky.log` | Log file path for application logs |

## Output Format

### Commit Files
Each commit file contains repository state updates with full record changes.

### Block Files
Each block file contains individual content blocks (posts, likes, etc.) with:
- Block CID
- Repository DID
- Block type (e.g., `app.bsky.feed.post`)
- Raw JSON representation

Files are organized in partitioned directories by date for efficient querying.

## Reconnection
The client automatically handles disconnections with:
- Exponential backoff (1s, 2s, 4s, 8s...)
- Maximum 10 retry attempts
- Automatic cursor maintenance for resuming

## Architecture
```
WebSocket → CBOR Decode → Message Parse → [Async Channels] →
  ├─> Commit Writer (Spawning Task)
  └─> Block Writer (Spawning Task)
```

## Dependencies
- `atrium-api` - Bluesky AT Protocol types
- `tokio-tungstenite` - Async WebSocket client
- `arrow` + `parquet` - Parquet file serialization
- `ipld-core` + `rs-car-sync` - CAR file decoding
- `serde-ipld-dagcbor` - CBOR decoding

## License
MIT
