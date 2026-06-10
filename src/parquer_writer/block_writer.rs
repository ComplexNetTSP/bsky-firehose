use anyhow::Result;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use atrium_api::com::atproto::sync::subscribe_repos::CommitData;
use base64::Engine;
use chrono::Datelike;
use ipld_core::cid::Cid;
use ipld_core::ipld::Ipld;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rs_car_sync::CarReader;
use serde_ipld_dagcbor::from_slice;
use std::io::Cursor;
use std::{fs::File, path::Path, sync::Arc};
/// Buffers blocks from CommitData and writes to Parquet files in batches
pub struct BlockWriter {
    buffer: Vec<BlockRecord>,
    batch_size: usize,
    file_path: String,
    facets: bool,
}

/// Extracted block data ready for Arrow array construction
struct ExtractedBlockData {
    // main message repo information
    commit_cid: Vec<String>,
    repo: Vec<String>,
    seq: Vec<i64>,
    cids: Vec<String>,
    block_types: Vec<String>,
    raw_json: Vec<String>,
}

/// Single block record with CID and IPLD data
pub struct BlockRecord {
    pub commit_cid: String,
    pub repo: String,
    pub seq: i64,
    pub cid: String,
    pub block_type: String,
    pub ipld: Ipld,
}

impl BlockWriter {
    /// Create a new BlockWriter with specified batch size and output directory
    pub fn new(batch_size: usize, file_path: &str, facets: bool) -> Self {
        Self {
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            file_path: file_path.to_owned(),
            facets,
        }
    }

    /// Add blocks from a CommitData by decoding its CAR file
    pub fn add_commit(&mut self, commit: CommitData) -> Result<()> {
        let blocks = decode_blocks(&commit)?;
        for (cid, ipld, block_type) in blocks {
            if block_type == "facets" && self.facets {
                continue;
            }
            self.buffer.push(BlockRecord {
                commit_cid: commit.commit.0.to_string(),
                repo: commit.repo.to_string(),
                seq: commit.seq,
                cid: cid.to_string(),
                block_type,
                ipld,
            });
        }

        if self.buffer.len() >= self.batch_size {
            let blocks = self.buffer.drain(..).collect::<Vec<_>>();
            self.flush(blocks)?;
        }
        Ok(())
    }

    /// Flush buffered blocks to Parquet file
    fn flush(&self, blocks: Vec<BlockRecord>) -> Result<()> {
        let dt = chrono::Local::now();
        let file_path = format!(
            "{}/{}/year={}/month={}/day={}/block_{}.parquet",
            self.file_path,
            "blocks",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.format("%Y_%m_%d_%H_%M_%S")
        );
        self.serialize_blocks_to_parquet(blocks, &file_path)?;
        Ok(())
    }

    /// Define Parquet schema for blocks
    fn build_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("cid", DataType::Utf8, false),
            Field::new("repo", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
            Field::new("cid", DataType::Utf8, false),
            Field::new("block_type", DataType::Utf8, false),
            Field::new("raw_json", DataType::Utf8, false),
        ]))
    }

    /// Write RecordBatch to Parquet file
    fn write_to_parquet(batch: &RecordBatch, schema: Arc<Schema>, file_path: &str) -> Result<()> {
        if let Some(parent) = Path::new(file_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(file_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_data_page_size_limit(1024 * 1024)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }

    /// Extract data from block records for Arrow arrays
    fn extract_data(blocks: Vec<BlockRecord>) -> ExtractedBlockData {
        let mut commit_cid = Vec::with_capacity(blocks.len());
        let mut repo = Vec::with_capacity(blocks.len());
        let mut seq = Vec::with_capacity(blocks.len());
        let mut cids = Vec::with_capacity(blocks.len());
        let mut block_types = Vec::with_capacity(blocks.len());
        let mut raw_json = Vec::with_capacity(blocks.len());

        for block in blocks {
            commit_cid.push(block.commit_cid);
            repo.push(block.repo);
            seq.push(block.seq);
            cids.push(block.cid);
            block_types.push(block.block_type);
            raw_json.push(ipld_to_json(block.ipld).to_string());
        }

        ExtractedBlockData {
            commit_cid,
            repo,
            seq,
            cids,
            block_types,
            raw_json,
        }
    }

    /// Create Arrow arrays from extracted data
    fn create_arrays(data: ExtractedBlockData) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(data.commit_cid)),
            Arc::new(StringArray::from(data.repo)),
            Arc::new(Int64Array::from(data.seq)),
            Arc::new(StringArray::from(data.cids)),
            Arc::new(StringArray::from(data.block_types)),
            Arc::new(StringArray::from(data.raw_json)),
        ]
    }

    /// Full pipeline: extract data, create arrays, write to Parquet
    fn serialize_blocks_to_parquet(&self, blocks: Vec<BlockRecord>, file_path: &str) -> Result<()> {
        let block_len = blocks.len();
        let schema = Self::build_schema();
        let data = Self::extract_data(blocks);
        let arrays = Self::create_arrays(data);
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Self::write_to_parquet(&batch, schema, file_path)?;
        info!(
            "Block writer wrote {} blocks to {} (facets_filter: {})",
            block_len, file_path, self.facets
        );
        Ok(())
    }
}

/// Decode blocks from CommitData's CAR file
fn decode_blocks(commit: &CommitData) -> Result<Vec<(Cid, Ipld, String)>> {
    let mut cursor = Cursor::new(&commit.blocks);
    let car_reader = CarReader::new(&mut cursor, true)?;
    let mut blocks = Vec::new();

    for item in car_reader {
        let (cid, block_data) = item?;
        let ipld: Ipld = from_slice(&block_data)?;

        // Extract block type from IPLD map
        let block_type = if let Ipld::Map(ref map) = ipld {
            if let Some(Ipld::String(t)) = map.get("$type") {
                t.clone()
            } else {
                "facets".to_string()
            }
        } else {
            "unknown".to_string()
        };

        blocks.push((cid, ipld, block_type));
    }
    Ok(blocks)
}

/// Convert IPLD to JSON for storage
fn ipld_to_json(ipld: Ipld) -> serde_json::Value {
    match ipld {
        Ipld::Null => serde_json::Value::Null,
        Ipld::Bool(b) => serde_json::Value::Bool(b),
        Ipld::Integer(i) => serde_json::json!(i),
        Ipld::Float(f) => serde_json::json!(f),
        Ipld::String(s) => serde_json::Value::String(s),
        // Encode bytes as base64 string since JSON has no byte type
        Ipld::Bytes(b) => {
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(&b))
        }
        Ipld::List(list) => serde_json::Value::Array(list.into_iter().map(ipld_to_json).collect()),
        Ipld::Map(map) => {
            serde_json::Value::Object(map.into_iter().map(|(k, v)| (k, ipld_to_json(v))).collect())
        }
        // Encode CID links as their string representation
        Ipld::Link(cid) => serde_json::Value::String(cid.to_string()),
    }
}
