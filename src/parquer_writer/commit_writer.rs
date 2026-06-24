use crate::db::ops::db_insert_cursor;
use anyhow::Result;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, ListBuilder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use atrium_api::com::atproto::sync::subscribe_repos::CommitData;
use chrono::Datelike;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::{fs::File, path::Path, sync::Arc};
use turso::Connection;

pub struct CommitWriter {
    buffer: Vec<CommitData>,
    batch_size: usize,
    file_path: String,
    db_conn: Connection,
}

/// Extracted data from CommitData ready for Arrow array construction
struct ExtractedCommitData {
    cids: Vec<String>,
    repos: Vec<String>,
    revs: Vec<String>,
    seqs: Vec<i64>,
    prev_cid: Vec<Option<String>>,
    times: Vec<String>,
    too_bigs: Vec<bool>,
    ops_counts: Vec<i64>,
    ops: Vec<String>,
    blob_cids: Vec<Option<Vec<String>>>,
}

// / Impl for CommitWriter - buffers AT Protocol commit data and writes to Parquet files in batches.
// /
// / Features:
// / - Accumulates commits in memory until batch_size is reached
// / - Automatically flushes to timestamped Parquet files
// / - Converts CommitData to Arrow arrays for efficient parquet serialization
impl CommitWriter {
    /// ==== Constructor ====
    pub fn new(batch_size: usize, file_path: &str, db_conn: Connection) -> Self {
        Self {
            buffer: Vec::<CommitData>::with_capacity(batch_size),
            batch_size,
            file_path: file_path.to_owned(),
            db_conn,
        }
    }

    pub async fn add_commit(&mut self, commit: CommitData) -> Result<()> {
        let cursor = commit.seq;
        // push commit to buffer
        self.buffer.push(commit);
        // flush the buffer to disk if full
        if self.buffer.len() >= self.batch_size {
            // store the last cursor received
            db_insert_cursor(&self.db_conn, cursor).await?;
            let commits = self.buffer.drain(..).collect::<Vec<_>>();
            self.flush(&commits)?;
        }
        Ok(())
    }

    /// ==== Batching ====
    fn flush(&self, commits: &[CommitData]) -> Result<()> {
        let dt = chrono::Local::now();
        let file_path = format!(
            "{}/{}/year={}/month={}/day={}/commit_{}.parquet",
            self.file_path,
            "commit",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.format("%Y_%m_%d_%H_%M_%S")
        );
        Self::serialize_commits_to_parquet(commits.to_vec(), &file_path)?;
        Ok(())
    }

    /// ==== Schema Definition ====
    fn build_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("commit_cid", DataType::Utf8, false),
            Field::new("repo", DataType::Utf8, false),
            Field::new("rev", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
            Field::new("prev_cid", DataType::Utf8, true),
            Field::new("time", DataType::Utf8, false),
            Field::new("too_big", DataType::Boolean, false),
            Field::new("ops_count", DataType::Int64, false),
            Field::new("ops", DataType::Utf8, false),
            Field::new(
                "blob_cids",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]))
    }

    /// ==== Parquet Serialization ====
    fn write_to_parquet(batch: &RecordBatch, schema: Arc<Schema>, file_path: &str) -> Result<()> {
        // Ensure output directory exists
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

    /// ==== Data Extraction ====
    fn extract_data(commits: &[CommitData]) -> ExtractedCommitData {
        let mut cids = Vec::with_capacity(commits.len());
        let mut repos = Vec::with_capacity(commits.len());
        let mut revs = Vec::with_capacity(commits.len());
        let mut seqs = Vec::with_capacity(commits.len());
        let mut prev_cid = Vec::with_capacity(commits.len());
        let mut times = Vec::with_capacity(commits.len());
        let mut too_bigs = Vec::with_capacity(commits.len());
        let mut ops_counts = Vec::with_capacity(commits.len());
        let mut ops = Vec::with_capacity(commits.len());
        let mut blob_cids = Vec::with_capacity(commits.len());

        for c in commits {
            cids.push(c.commit.0.to_string());
            repos.push(c.repo.to_string());
            revs.push(c.rev.to_string());
            seqs.push(c.seq);
            prev_cid.push(c.prev_data.as_ref().map(|cid| cid.0.to_string()));
            times.push(c.time.as_str().to_owned());
            too_bigs.push(c.too_big);
            ops_counts.push(c.ops.len() as i64);
            let ops_json: Vec<_> = c
                .ops
                .iter()
                .map(|op| {
                    serde_json::json!({
                        "action": op.action,
                        "cid": op.cid.as_ref().map(|cid| cid.0.to_string()),
                        "path": op.path,
                        "record_type": op.path.split("/").next()
                    })
                })
                .collect();
            ops.push(serde_json::to_string(&ops_json).unwrap_or_default());

            blob_cids.push(Some(
                c.blobs.iter().map(|b| b.0.to_string()).collect::<Vec<_>>(),
            ));
        }

        ExtractedCommitData {
            cids,
            repos,
            revs,
            seqs,
            prev_cid,
            times,
            too_bigs,
            ops_counts,
            ops,
            blob_cids,
        }
    }

    /// ==== Arrow Array Construction ====
    fn create_arrays(data: ExtractedCommitData) -> Vec<ArrayRef> {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for item in data.blob_cids {
            if let Some(list) = item {
                for s in list {
                    builder.values().append_option(Some(s));
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        vec![
            Arc::new(StringArray::from(data.cids)),
            Arc::new(StringArray::from(data.repos)),
            Arc::new(StringArray::from(data.revs)),
            Arc::new(Int64Array::from(data.seqs)),
            Arc::new(StringArray::from(data.prev_cid)),
            Arc::new(StringArray::from(data.times)),
            Arc::new(BooleanArray::from(data.too_bigs)),
            Arc::new(Int64Array::from(data.ops_counts)),
            Arc::new(StringArray::from(data.ops)),
            Arc::new(builder.finish()),
        ]
    }

    fn serialize_commits_to_parquet(commits: Vec<CommitData>, file_path: &str) -> Result<()> {
        let schema = Self::build_schema();
        let data = Self::extract_data(&commits);
        let arrays = Self::create_arrays(data);
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Self::write_to_parquet(&batch, schema, file_path)?;
        info!(
            "Commit writer wrote {} commits to {}",
            commits.len(),
            file_path
        );
        Ok(())
    }
}
