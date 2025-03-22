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
    pub timestamp: i64,
    // New fields for enrichment:
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub service: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    pub log_type: Option<String>, // Changed from Option<_> to Option<String>
}

impl LogEntry {
    pub fn current_timestamp() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }
}
