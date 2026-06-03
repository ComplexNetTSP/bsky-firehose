use ipld_core::ipld::Ipld;
use rs_car_sync::CarReader;
use serde_ipld_dagcbor::from_slice;
use std::io::Cursor;
fn decode_blocks(commit: &CommitData) -> anyhow::Result<()> {
    let mut cursor = Cursor::new(&commit.blocks);
    let mut car_reader = CarReader::new(&mut cursor, true)?;
    // Access the CAR roots (commit CIDs)
    let roots = car_reader.header.roots.clone();
    //println!("Roots: {:?}", roots);
    while let Some(item) = car_reader.next() {
        let (_, block) = item.map_err(|e| anyhow::anyhow!("error {}", e))?;
        let ipld: Ipld = from_slice(&block).map_err(|e| anyhow::anyhow!("error {}", e))?;
        if let Ipld::Map(ref map) = ipld {
            if let Some(Ipld::String(block_type)) = map.get("$type") {
                match block_type.as_str() {
                    "app.bsky.feed.post" => {
                        eprintln!("post: {:?}", map);
                    }
                    "app.bsky.feed.like" => {
                        eprintln!("like: {:?}", map);
                    }
                    "app.bsky.feed.repost" => {
                        eprintln!("like: {:?}", map);
                    }
                    _ => {
                        eprintln!("block_type: {}", block_type)
                    }
                }
            }
        }
        // Then convert Ipld → JSON, handling byte arrays
        //let json = ipld_to_json(ipld);
        //eprint!("{:?}", json);
    }
    Ok(())
}

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
