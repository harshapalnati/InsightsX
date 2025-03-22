use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::time::Duration;
use rmp_serde::from_slice;
use crate::models::log_entry::{LogEntry, LogLevel};
use chrono::Utc;
use std::sync::Arc;
use crate::storage::clickhouse::LogStorage;

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
                    for ms in message_sets.iter() {
                        for m in ms.messages() {
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
                                    log_type: None,
                                }
                            });
                            handler(&log_entry);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Error polling messages from Kafka: {}", e);
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
}

/// Spawns a Kafka consumer in a blocking task that reads logs and writes them to ClickHouse.
/// The function now takes owned `String` values for brokers and topic.
pub async fn start_kafka_consumer(brokers: String, topic: String, storage: Arc<LogStorage>) {
    tokio::task::spawn_blocking(move || {
        let mut consumer = KafkaConsumer::new(vec![brokers], &topic);
        consumer.consume(|log_entry| {
            let storage_clone = storage.clone();
            let log_entry = log_entry.clone(); // Clone for moving into async task
            tokio::spawn(async move {
                if let Err(e) = storage_clone.insert_log(&log_entry).await {
                    eprintln!("Failed to insert log into ClickHouse: {:?}", e);
                }
            });
        });
    })
    .await
    .expect("Kafka consumer task panicked");
}
