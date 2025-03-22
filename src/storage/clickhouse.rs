use clickhouse::{Client, Row};
use crate::models::log_entry::{LogEntry, LogLevel};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Serialize, Deserialize};
use tracing::info;

#[derive(Serialize, Deserialize, Row)]
struct InsertLogRow {
    source: String,
    level: String,
    message: String,
    timestamp: NaiveDateTime,
    trace_id: Option<String>,
    span_id: Option<String>,
    service: Option<String>,
    metadata: Option<String>,
    log_type: Option<String>,
}

impl LogEntry {
    fn to_insert_row(&self) -> InsertLogRow {
        // Use the non-deprecated DateTime::from_timestamp method
        let dt = DateTime::<Utc>::from_timestamp((self.timestamp / 1000) as i64, 0)
            .unwrap_or(DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        
        InsertLogRow {
            source: self.source.clone(),
            level: match self.level {
                LogLevel::INFO => "INFO",
                LogLevel::WARN => "WARN",
                LogLevel::ERROR => "ERROR",
                LogLevel::DEBUG => "DEBUG",
            }
            .to_string(),
            message: self.message.clone(),
            timestamp: dt.naive_utc(),
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            service: self.service.clone(),
            metadata: self
                .metadata
                .as_ref()
                .map(|m| serde_json::to_string(m).unwrap_or_default()),
            log_type: self.log_type.clone(),
        }
    }
}

pub struct LogStorage {
    pub client: Client,
}

impl LogStorage {
    pub fn new(url: &str, user: &str, password: &str) -> Self {
        info!("Initializing ClickHouse connection to: {}", url);
        
        // The clickhouse client does not have a with_secure method
        // Instead, use https in the URL to enable TLS
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_compression(clickhouse::Compression::Lz4)
            .with_database("default");
        
        info!("ClickHouse client configured");
        Self { client }
    }

    pub async fn init_table(&self) -> Result<(), clickhouse::error::Error> {
        info!("Creating logs table if it doesn't exist");
        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS logs (
                    source String,
                    level String,
                    message String,
                    timestamp DateTime,
                    trace_id Nullable(String),
                    span_id Nullable(String),
                    service Nullable(String),
                    metadata Nullable(String),
                    log_type Nullable(String)
                ) ENGINE = MergeTree() ORDER BY timestamp"
            )
            .execute()
            .await?;
        info!("Logs table ready");
        Ok(())
    }

    pub async fn insert_log(&self, log: &LogEntry) -> Result<(), clickhouse::error::Error> {
        let row = log.to_insert_row();
        let mut insert = self.client.insert("logs")?;
        insert.write(&row).await?;
        insert.end().await?; // Explicitly end and flush the insert operation
        Ok(())
    }

    pub async fn insert_logs(&self, logs: &[LogEntry]) -> Result<(), clickhouse::error::Error> {
        if logs.is_empty() {
            return Ok(());
        }
    
        info!("Batch inserting {} logs", logs.len());
        let rows: Vec<InsertLogRow> = logs.iter().map(LogEntry::to_insert_row).collect();
        let mut insert = self.client.insert("logs")?;
    
        for row in rows {
            insert.write(&row).await?;
        }
    
        // Explicitly end and flush the insert operation
        insert.end().await?;
        info!("Successfully inserted {} logs", logs.len());
        Ok(())
    }
    
    // Modified query method to return a custom struct that we can deserialize
    pub async fn query_logs_by_timerange(
        &self,
        start_time: NaiveDateTime,
        end_time: NaiveDateTime,
        limit: u32
    ) -> Result<Vec<InsertLogRow>, clickhouse::error::Error> {
        let query = format!(
            "SELECT 
                source, level, message, timestamp, 
                trace_id, span_id, service, metadata, log_type
             FROM logs 
             WHERE timestamp BETWEEN '{}' AND '{}' 
             ORDER BY timestamp DESC 
             LIMIT {}",
            start_time, end_time, limit
        );
        
        let result = self.client.query(&query).fetch_all::<InsertLogRow>().await?;
        Ok(result)
    }
}