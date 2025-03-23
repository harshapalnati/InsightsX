use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::time::Duration;
use rmp_serde::from_slice;
use crate::models::log_entry::{LogEntry, LogLevel};
use std::sync::Arc;
use crate::storage::clickhouse::LogStorage;

/// A Kafka consumer wrapper with built-in retry logic for consumer creation.
pub struct KafkaConsumer {
    consumer: Consumer,
}

impl KafkaConsumer {
    /// Creates a new KafkaConsumer, retrying until successful.
    pub fn new_with_retry(brokers: Vec<String>, topic: &str) -> Self {
        let mut attempts = 0;
        loop {
            attempts += 1;
            println!(
                "🔄 Attempting to initialize Kafka Consumer for topic: {} (attempt {})",
                topic, attempts
            );
            match Consumer::from_hosts(brokers.clone())
                .with_topic(topic.to_string())
                .with_group("insightx-group".to_owned())
                .with_fallback_offset(FetchOffset::Earliest)
                .with_offset_storage(Some(GroupOffsetStorage::Kafka))
                .with_fetch_max_wait_time(Duration::from_millis(200))
                .with_fetch_min_bytes(1)
                .create()
            {
                Ok(consumer) => {
                    println!(
                        "✅ Kafka Consumer initialized successfully after {} attempt(s)!",
                        attempts
                    );
                    return Self { consumer };
                }
                Err(e) => {
                    eprintln!(
                        "🔥 Consumer creation failed: {}. Retrying in 2 seconds...",
                        e
                    );
                    std::thread::sleep(Duration::from_secs(2));
                }
            }
        }
    }

    /// Continuously polls Kafka and calls the provided handler on each deserialized LogEntry.
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
                                eprintln!(
                                    "❌ Failed to deserialize message: {}. Using fallback.",
                                    err
                                );
                                LogEntry {
                                    source: "Unknown".to_string(),
                                    level: LogLevel::ERROR,
                                    message: "Corrupted log entry".to_string(),
                                    trace_id: None,
                                    span_id: None,
                                    service: None,
                                    metadata: None,
                                    log_type: None,
                                    client_timestamp: chrono::Utc::now().timestamp_millis(),
                                    server_timestamp: Some(chrono::Utc::now().timestamp_millis()),
                                }
                            });
                            handler(&log_entry);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Error polling messages from Kafka: {}. Retrying...", e);
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
}

/// Spawns the Kafka consumer in a blocking task, processing each log entry by inserting it into ClickHouse.
pub async fn start_kafka_consumer(brokers: String, topic: String, storage: Arc<LogStorage>) {
    tokio::task::spawn_blocking(move || {
        let mut consumer = KafkaConsumer::new_with_retry(vec![brokers], &topic);
        consumer.consume(|log_entry| {
            let storage_clone = storage.clone();
            let log_entry = log_entry.clone(); // Clone the log for the async task
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
