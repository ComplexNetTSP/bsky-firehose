use crate::db::setup::get_conn;
use anyhow::{Context, Result, anyhow};
use std::path::Path;
use turso::Connection;
pub async fn db_insert_cursor(conn: &Connection, cursor: i64) -> Result<()> {
    // Create a table
    conn.execute(
        "INSERT INTO bluesky_firehose_cursor (cursor) VALUES (?1)",
        [cursor],
    )
    .await
    .map_err(|e| anyhow!(e))?;
    Ok(())
}

pub async fn get_last_cursor(conn: &Connection) -> Result<i64> {
    let mut rows = conn
        .query(
            "SELECT cursor FROM bluesky_firehose_cursor ORDER BY rowid DESC LIMIT 1",
            (),
        )
        .await
        .context("Error in query when fetching Cursor")?;

    if let Some(row) = rows
        .next()
        .await
        .context("Unable to fetch row from database")?
    {
        let cursor: i64 = row.get(0)?;
        Ok(cursor)
    } else {
        Err(anyhow::anyhow!("No cursor found in database"))
    }
}

pub async fn check_for_last_cursor(db_path: &str) -> Option<i64> {
    if !Path::new(db_path).exists() {
        return None;
    }
    if let Ok(conn) = get_conn(db_path).await {
        match get_last_cursor(&conn).await {
            Ok(cursor) => return Some(cursor),
            Err(_) => return None,
        }
    };
    None
}
