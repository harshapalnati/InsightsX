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
    pub timestamp: i64, // ✅ Change from `Option<Timestamp>` to `i64`
    pub metadata: Option<HashMap<String, String>>,
}

// ✅ Helper function to get current timestamp
impl LogEntry {
    pub fn current_timestamp() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }
}
