// src/processor.rs

use tokio::sync::mpsc;
use crate::models::log_entry::LogEntry;
use tracing::info;

pub async fn process_logs(mut receiver: mpsc::Receiver<LogEntry>) {
    let mut batch = Vec::new();
    loop {
        tokio::select! {
            Some(log) = receiver.recv() => {
                batch.push(log);
                if batch.len() >= 100 {
                    info!("Processing batch of {} logs", batch.len());
                    // Here you would forward the batch to Kafka or write to storage.
                    batch.clear();
                }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)), if !batch.is_empty() => {
                info!("Time-based flush: processing {} logs", batch.len());
                // Process remaining logs in the batch.
                batch.clear();
            }
        }
    }
}
