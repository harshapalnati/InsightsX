// src/kafka/consumer.rs

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::time::Duration;
use rmp_serde::from_slice;
use crate::models::log_entry::{LogEntry, LogLevel};
use chrono::Utc;

pub struct KafkaConsumer {
    consumer: Consumer,
}

impl KafkaConsumer {
    pub fn new(brokers: Vec<String>, topic: &str) -> Self {
        println!("🔄 Initializing Kafka Consumer for topic: {}", topic);

        let consumer = Consumer::from_hosts(brokers)
            .with_topic(topic.to_string())
            .with_group("insightx-group".to_owned())
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(Some(GroupOffsetStorage::Kafka))
            .with_fetch_max_wait_time(Duration::from_millis(200))
            .with_fetch_min_bytes(1)
            .create()
            .expect("🔥 Consumer creation failed! Check Kafka connection.");

        println!("✅ Kafka Consumer initialized successfully!");
        Self { consumer }
    }

    /// Continuously polls Kafka and processes each message using the provided handler.
    /// The handler receives a reference to a `LogEntry`.
    pub fn consume<F>(&mut self, mut handler: F)
    where
        F: FnMut(&LogEntry),
    {
        println!("🚀 Kafka Consumer started listening...");

        loop {
            match self.consumer.poll() {
                Ok(message_sets) => {
                    // Iterate over each message set in the batch.
                    for ms in message_sets.iter() {
                        for m in ms.messages() {
                            // Attempt to deserialize the message.
                            let log_entry: LogEntry = from_slice(m.value).unwrap_or_else(|err| {
                                eprintln!("❌ Failed to deserialize message: {}. Using fallback.", err);
                                LogEntry {
                                    source: "Unknown".to_string(),
                                    level: LogLevel::ERROR,
                                    message: "Corrupted log entry".to_string(),
                                    timestamp: Utc::now().timestamp_millis(),
                                    trace_id: None,
                                    span_id: None,
                                    service: None,
                                    metadata: None,
                                }
                            });
                            // Process the log entry using the provided handler.
                            handler(&log_entry);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Error polling messages from Kafka: {}", e);
                    // Sleep briefly to avoid busy-looping on errors.
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
}
