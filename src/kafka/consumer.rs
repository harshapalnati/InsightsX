// src/kafka/consumer.rs
use std::sync::Arc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use tokio_stream::StreamExt;
use rmp_serde::from_slice;
use crate::models::log_entry::{LogEntry, LogLevel};
use crate::storage::clickhouse::LogStorage;

pub async fn start_kafka_consumer(brokers: &str, topic: &str, storage: Arc<LogStorage>) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "insightx-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe to topic");

    let mut message_stream = consumer.stream();
    let mut buffer = Vec::new();
    let batch_size = 500;

    println!("🚀 Kafka Consumer started for topic: {}", topic);

    while let Some(result) = message_stream.next().await {
        match result {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    let log: LogEntry = from_slice(payload).unwrap_or_else(|_| LogEntry {
                        source: "Unknown".into(),
                        level: LogLevel::ERROR,
                        message: "Corrupted log".into(),
                        trace_id: None,
                        span_id: None,
                        service: None,
                        metadata: None,
                        log_type: None,
                        client_timestamp: chrono::Utc::now().timestamp_millis(),
                        server_timestamp: Some(chrono::Utc::now().timestamp_millis()),
                    });
                    println!(
                        "{} ✅ Received log from Kafka: {}",
                        chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f"),
                        log.message
                    );
                    buffer.push(log);

                    if buffer.len() >= batch_size {
                        let batch = std::mem::take(&mut buffer);
                        let storage_clone = storage.clone();
                        tokio::spawn(async move {
                            if let Err(e) = storage_clone.insert_logs(&batch).await {
                                eprintln!("❌ Failed to insert batch into ClickHouse: {:?}", e);
                            }
                        });
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Kafka error: {}", e);
            }
        }
    }
}
