# Bluesky Firehose Consumer

[![GitHub Release](https://img.shields.io/github/v/release/ComplexNetTSP/bsky-firehose)](https://github.com/ComplexNetTSP/bsky-firehose/releases)
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/ComplexNetTSP/bsky-firehose/build.yaml)](https://github.com/ComplexNetTSP/bsky-firehose/actions)

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
| `-m, --max_retries` | 100 | maxium number of retries attemps |

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
- Maximum retry attempts (defined in parameters)
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

## Changelog

### v0.1.7
- 🔧 **WebSocket Error Handling**: Enhanced error handling for WebSocket connections with specific handling for TLS, HTTP, and Protocol errors
- 🐞 **Flush Method**: Fixed flush method parameter type in CommitWriter for better memory management

### v0.1.6
- 🐞 **Clippy Fixes**: Fixed clippy linting errors in commit and block writers
- 🐞 **Network Error Handling**: Fixed panic when network errors occur during firehose connection
- 📁 **File Naming**: Updated file naming structure to use datetime of the last commit received for more accurate file organization
- 🔧 **Error Improvements**: Enhanced error handling and robustness in main module

### v0.1.5
- ✨ **Datetime Parsing**: Added proper RFC 3339 datetime parsing from commit timestamps in both CommitWriter and BlockWriter modules to reference the parquet file base on the datetime of the last commit received
- 🐞 **Error Handling**: Improved error handling and logging in main in order to account for network IO errors


### v0.1.4
- ✨ **Database Integration**: Added Turso SQLite database module for cursor persistence
  - New `db` module with connection management (`setup.rs`) and operations (`ops.rs`)
  - Stores and retrieves the last processed sequence number to resume from the correct position
- 🔧 **New CLI Option**: `--max-retries` (default: 100) to configure maximum reconnection attempts
- 📝 **Logging**: Added log file support via `log4rs` with configurable output path (`--logfile`)
- 🐞 **Bug Fixes & Improvements**:
  - Fixed error messages and context in firehose connection handling
  - Improved CBOR decoding robustness for firehose messages
  - Enhanced commit and block writer error handling
- 📦 **Dependencies**: Updated to Rust 1.92.0 and latest crate versions

## License
MIT
