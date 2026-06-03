# Bluesky Firehose Consumer

[![GitHub Release](https://img.shields.io/github/v/release/ComplexNetTSP/bsky-firehose)](https://github.com/ComplexNetTSP/bsky-firehose/releases)

A Rust application that connects to the Bluesky AT Protocol firehose and writes commit data to Parquet files.

**Author**: Vincent Gauthier <vincent.gauthier@telecom-sudparis.eu>

## Features

- Real-time consumption of Bluesky's WebSocket firehose
- CBOR message decoding for all firehose event types
- Efficient Parquet serialization using Apache Arrow
- Batch writing with configurable batch size
- Timestamped output files: `commit_YYYY_MM_DD_HH_MIN_SEC.parquet`

## Prerequisites

- Rust 1.92.0 or later
- Cargo

## Installation

### From Source

```bash
git clone https://github.com/vgauthier/bsky-shrike.git
cd bsky-shrike
cargo build --release
```

### Pre-built Binaries

Download from [Releases](https://github.com/vgauthier/bsky-shrike/releases):

- Linux (x86_64)
- Windows (x86_64)
- macOS (x86_64, aarch64)

## Usage

```bash
./bsky-firehose --output-dir ./data --batch-size 1000
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--output-dir` | `output` | Directory for Parquet files |
| `--batch-size` | `1000` | Number of commits per file |
| `--cursor` | `None` | Start from specific sequence number |

## Output

Commit data is written to Parquet files with the naming pattern:
```
output/commit_2024_01_15_14_30_00.parquet
```

Each file contains up to `--batch-size` commit records with full repository state changes.

## Firehose Event Types

| Type | Description |
|------|-------------|
| `Commit` | Repository state updates with record changes |
| `Sync` | Recover from broken streams, update repo to new state |
| `Identity` | Account identity changes (handle, signing key, PDS) |
| `Account` | Account status changes (active/inactive) |
| `Info` | Server info messages |

## Architecture

```
WebSocket -> CBOR Decode -> FirehoseMessage -> Commit Filter -> Parquet Writer
```

## Dependencies

- `atrium-api` - Bluesky AT Protocol types
- `tokio-tungstenite` - Async WebSocket client
- `arrow` + `parquet` - Parquet file serialization
- `serde-ipld-dagcbor` - CBOR decoding

## License

MIT
