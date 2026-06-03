use anyhow::{Context, Result};
use atrium_api::com::atproto::sync::subscribe_repos::{
    AccountData, CommitData, IdentityData, InfoData, SyncData,
};
use serde::Deserialize;
use serde_ipld_dagcbor::{from_reader, from_slice};
use std::io::Cursor;

#[derive(Debug, Deserialize)]
pub struct FirehoseMessageHeader {
    pub op: i32,
    #[serde(rename = "t")]
    pub event_type: String,
}

/// Supported event types
#[derive(Debug, Clone, Copy)]
pub enum FirehoseMessageHeaderEventType {
    Commit,
    Sync,
    Identity,
    Account,
    Info,
}

/// Represents a decoded firehose message
#[allow(dead_code)]
#[derive(Debug)]
pub enum FirehoseMessage {
    Commit(Box<CommitData>),
    Sync(Box<SyncData>),
    Identity(Box<IdentityData>),
    Account(Box<AccountData>),
    Info(Box<InfoData>),
}

impl FirehoseMessageHeaderEventType {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "#commit" => Some(FirehoseMessageHeaderEventType::Commit),
            "#sync" => Some(FirehoseMessageHeaderEventType::Sync),
            "#identity" => Some(FirehoseMessageHeaderEventType::Identity),
            "#account" => Some(FirehoseMessageHeaderEventType::Account),
            "#info" => Some(FirehoseMessageHeaderEventType::Info),
            _ => None,
        }
    }
}

/// Decodes the message body based on event type
pub fn decode_body(
    event_type: FirehoseMessageHeaderEventType,
    body: &[u8],
) -> Result<FirehoseMessage> {
    match event_type {
        FirehoseMessageHeaderEventType::Commit => {
            let commit = from_slice::<CommitData>(body)
                .context("Failed to deserialize commit from firehose message")?;
            Ok(FirehoseMessage::Commit(Box::new(commit)))
        }
        FirehoseMessageHeaderEventType::Sync => {
            let sync = from_slice::<SyncData>(body)
                .context("Failed to deserialize sync from firehose message")?;
            Ok(FirehoseMessage::Sync(Box::new(sync)))
        }
        FirehoseMessageHeaderEventType::Identity => {
            let identity = from_slice::<IdentityData>(body)
                .context("Failed to deserialize identity from firehose message")?;
            Ok(FirehoseMessage::Identity(Box::new(identity)))
        }
        FirehoseMessageHeaderEventType::Account => {
            let account = from_slice::<AccountData>(body)
                .context("Failed to deserialize account from firehose message")?;
            Ok(FirehoseMessage::Account(Box::new(account)))
        }
        FirehoseMessageHeaderEventType::Info => {
            let info = from_slice::<InfoData>(body)
                .context("Failed to deserialize info from firehose message")?;
            Ok(FirehoseMessage::Info(Box::new(info)))
        }
    }
}

///
/// Supported event types (event_type in header):
/// - #commit Repository state update with record changes (creates/updates/deletes)
/// - #sync Recover from broken streams or data loss; updates repo to new state without detailed ops
/// - #identity Account identity change (handle, signing key, PDS endpoint)
/// - #account Account status change on a host (active/inactive)
/// - #info Server info messages
///
pub fn decode_header(
    header_bytes: &[u8],
) -> anyhow::Result<(FirehoseMessageHeader, FirehoseMessageHeaderEventType)> {
    let header: FirehoseMessageHeader = from_reader(Cursor::new(header_bytes))?;
    if header.op != 1 {
        anyhow::bail!("Unsupported operation code: {}", header.op);
    }
    let event_type = FirehoseMessageHeaderEventType::from_str(&header.event_type)
        .ok_or_else(|| anyhow::anyhow!("Unsupported event type: {}", header.event_type))?;
    Ok((header, event_type))
}

/// Splits a CBOR-encoded frame into (header, body) by decoding the header
/// and using the cursor position after a TrailingData error to find the split point.
/// Returns the byte slices for header and body, or an error if the frame is malformed.
pub fn split_frame(data: &[u8]) -> anyhow::Result<(&[u8], &[u8])> {
    use ipld_core::ipld::Ipld;
    let mut cursor = Cursor::new(data);
    // Attempt to decode the header (IPLD data)
    match serde_ipld_dagcbor::from_reader::<Ipld, _>(&mut cursor) {
        Err(serde_ipld_dagcbor::DecodeError::TrailingData) => {
            // ✅ Expected! Header decoded, cursor is at the split point
            Ok(data.split_at(cursor.position() as usize))
        }
        Ok(_) => anyhow::bail!("Frame has no body"),
        Err(e) => anyhow::bail!("Invalid frame: {}", e),
    }
}
