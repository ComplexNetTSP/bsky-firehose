use anyhow::{Context, Result};
use turso::{Builder, Connection};

pub async fn get_conn(db_path: &str) -> Result<Connection> {
    // Test if path exist not create it
    let db = Builder::new_local(db_path)
        .build()
        .await
        .context("Enable to open local database file")?;
    let conn = db
        .connect()
        .context("Enable to connect to the local database")?;
    // create table if it doesn't exist
    create_table(&conn).await?;
    Ok(conn)
}

async fn create_table(conn: &Connection) -> Result<()> {
    // Create a table
    conn.execute(
        r#"CREATE TABLE IF NOT EXISTS bluesky_firehose_cursor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cursor INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"#,
        (),
    )
    .await
    .context("Unable to create table bluesky_firehose_cursor")?;

    Ok(())
}
