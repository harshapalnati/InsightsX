use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum LogLevel {
    INFO,
    WARN,
    ERROR,
    DEBUG,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub source: String,
    pub level: LogLevel,
    pub message: String,
    pub timestamp: i64,  // Unix timestamp (milliseconds)
    pub metadata: Option<std::collections::HashMap<String, String>>,
}
