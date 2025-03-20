use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::time::Duration;
use rmp_serde::from_slice;
use crate::models::log_entry::{LogEntry, LogLevel};

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
            .with_fetch_max_wait_time(Duration::from_millis(500))
            .with_fetch_min_bytes(1)
            .create()
            .expect("🔥 Consumer creation failed! Check Kafka connection.");

        println!("✅ Kafka Consumer initialized successfully!");
        Self { consumer }
    }

    pub fn consume<F>(&mut self, mut handler: F)
    where
        F: FnMut(&LogEntry),
    {
        println!("🚀 Kafka Consumer started listening...");

        loop {
            match self.consumer.poll() {
                Ok(message_sets) => {
                    for ms in message_sets.iter() {
                        for m in ms.messages() {
                            let log_entry: LogEntry = from_slice(m.value).unwrap_or_else(|_| {
                                println!("❌ Failed to deserialize message!");
                                LogEntry {
                                    source: "Unknown".to_string(),
                                    level: LogLevel::ERROR,
                                    message: "Corrupted log entry".to_string(),
                                    timestamp: chrono::Utc::now().timestamp_millis(), // ✅ Fix: Use Unix timestamp
                                    metadata: None,
                                }
                            });

                            println!("✅ Received Kafka message: {:?}", log_entry);
                            handler(&log_entry);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Error polling messages from Kafka: {}", e);
                }
            }
        }
    }
}
