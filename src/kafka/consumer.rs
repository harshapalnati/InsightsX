use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::time::Duration;
use rmp_serde::from_slice;
use crate::models::log_entry::{LogEntry, LogLevel};
use rayon::prelude::*;

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
            .with_fetch_max_wait_time(Duration::from_millis(200)) // You might reduce this for more responsiveness
            .with_fetch_min_bytes(1)
            .create()
            .expect("🔥 Consumer creation failed! Check Kafka connection.");

        println!("✅ Kafka Consumer initialized successfully!");
        Self { consumer }
    }

    // Changed the handler to be Fn instead of FnMut to ease parallelism
    pub fn consume<F>(&mut self, handler: F)
    where
        F: Fn(&LogEntry) + Sync + Send,
    {
        println!("🚀 Kafka Consumer started listening...");

        loop {
            match self.consumer.poll() {
                Ok(message_sets) => {
                    // Use the iterator provided by message_sets (if available)
                    let message_sets_vec: Vec<_> = message_sets.iter().collect();
                    message_sets_vec.into_par_iter().for_each(|ms| {
                        for m in ms.messages() {
                            let log_entry: LogEntry = from_slice(m.value).unwrap_or_else(|_| {
                                LogEntry {
                                    source: "Unknown".to_string(),
                                    level: LogLevel::ERROR,
                                    message: "Corrupted log entry".to_string(),
                                    timestamp: chrono::Utc::now().timestamp_millis(),
                                    metadata: None,
                                }
                            });
                            handler(&log_entry);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("❌ Error polling messages from Kafka: {}", e);
                }
            }
            
        }
    }
}
