use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LogLevel {
    INFO,
    WARN,
    ERROR,
    DEBUG,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub source: String,
    pub level: LogLevel,
    pub message: String,
    pub client_timestamp: i64,
    #[serde(default)] // This will default to None if the field is missing.
    pub server_timestamp: Option<i64>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub service: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    pub log_type: Option<String>,
}

impl LogEntry {
    /// Returns the current timestamp in milliseconds.
    pub fn current_timestamp() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }
}
