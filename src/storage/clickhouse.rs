use clickhouse::{Client, Row};
use crate::models::log_entry::{LogEntry, LogLevel};
use chrono::{Utc, TimeZone};
use serde::{Serialize, Deserialize};
use tracing::info;

#[derive(Serialize, Deserialize, Row, Debug)]
struct InsertLogRow {
    source: String,
    level: String,
    message: String,
    // Store timestamps as i32 (seconds since epoch) for ClickHouse DateTime.
    client_timestamp: i32,
    server_timestamp: i32,
    trace_id: Option<String>,
    span_id: Option<String>,
    service: Option<String>,
    metadata: Option<String>,
    log_type: Option<String>,
}

impl LogEntry {
    /// Converts this log entry into a row for ClickHouse.
    /// Expects `client_timestamp` is in milliseconds and `server_timestamp`
    /// is provided (or defaults to the current time), and then converts them to seconds.
    fn to_insert_row(&self) -> InsertLogRow {
        // Convert millisecond timestamps to seconds.
        let client_sec = (self.client_timestamp / 1000) as i32;
        let server_ms = self.server_timestamp.unwrap_or_else(|| LogEntry::current_timestamp());
        let server_sec = (server_ms / 1000) as i32;

        InsertLogRow {
            source: self.source.clone(),
            level: match self.level {
                LogLevel::INFO  => "INFO",
                LogLevel::WARN  => "WARN",
                LogLevel::ERROR => "ERROR",
                LogLevel::DEBUG => "DEBUG",
            }
            .to_string(),
            message: self.message.clone(),
            client_timestamp: client_sec,
            server_timestamp: server_sec,
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            service: self.service.clone(),
            metadata: self.metadata.as_ref().map(|m| serde_json::to_string(m).unwrap_or_default()),
            log_type: self.log_type.clone(),
        }
    }
}

pub struct LogStorage {
    pub client: Client,
}

impl LogStorage {
    /// Initializes a new ClickHouse client.
    pub fn new(url: &str, user: &str, password: &str) -> Self {
        info!("Initializing ClickHouse connection to: {}", url);

        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_compression(clickhouse::Compression::Lz4)
            .with_database("default");

        info!("ClickHouse client configured");
        Self { client }
    }

    /// Creates the logs table if it doesn't exist.
    pub async fn init_table(&self) -> Result<(), clickhouse::error::Error> {
        info!("Creating logs table if it doesn't exist");
        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS logs (
                    source String,
                    level String,
                    message String,
                    client_timestamp DateTime,
                    server_timestamp DateTime,
                    trace_id Nullable(String),
                    span_id Nullable(String),
                    service Nullable(String),
                    metadata Nullable(String),
                    log_type Nullable(String)
                ) ENGINE = MergeTree() ORDER BY server_timestamp"
            )
            .execute()
            .await?;
        info!("Logs table ready");
        Ok(())
    }

    /// Inserts a single log entry into ClickHouse with additional debug logging.
    pub async fn insert_log(&self, log: &LogEntry) -> Result<(), clickhouse::error::Error> {
        let row = log.to_insert_row();
        println!("DEBUG: Inserting row: {:?}", row);
        // Use fully qualified table name if needed (e.g., "default.logs")
        let mut insert = self.client.insert("default.logs")?;
        println!("DEBUG: Writing row data to ClickHouse...");
        insert.write(&row).await?;
        println!("DEBUG: Finished writing row data. Flushing insert...");
        let end_result = insert.end().await;
        match &end_result {
            Ok(_) => println!("DEBUG: Insert flushed successfully."),
            Err(e) => println!("DEBUG: Insert flush failed: {:?}", e),
        }
        end_result?;
        Ok(())
    }

    /// Batch inserts multiple log entries into ClickHouse.
    pub async fn insert_logs(&self, logs: &[LogEntry]) -> Result<(), clickhouse::error::Error> {
        if logs.is_empty() {
            return Ok(());
        }

        info!("Batch inserting {} logs", logs.len());
        let rows: Vec<InsertLogRow> = logs.iter().map(LogEntry::to_insert_row).collect();
        let mut insert = self.client.insert("default.logs")?;

        for row in rows {
            insert.write(&row).await?;
        }

        insert.end().await?;
        info!("Successfully inserted {} logs", logs.len());
        Ok(())
    }

    /// Queries logs by a time range.
    pub async fn query_logs_by_timerange(
        &self,
        start_time: String,
        end_time: String,
        limit: u32
    ) -> Result<Vec<InsertLogRow>, clickhouse::error::Error> {
        let query = format!(
            "SELECT 
                source, level, message, client_timestamp, server_timestamp, 
                trace_id, span_id, service, metadata, log_type
             FROM logs 
             WHERE server_timestamp BETWEEN '{}' AND '{}' 
             ORDER BY server_timestamp DESC 
             LIMIT {}",
            start_time, end_time, limit
        );

        let result = self.client.query(&query).fetch_all::<InsertLogRow>().await?;
        Ok(result)
    }
}
