// src/kafka/producer.rs
use std::time::Duration;
use std::sync::Arc;
use std::collections::VecDeque;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
    buffer: Arc<Mutex<VecDeque<Vec<u8>>>>,
    batch_size: usize,
    flush_interval: Duration,
}

impl KafkaProducer {
    pub fn new(brokers: &str, topic: &str, batch_size: usize, flush_interval: Duration) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("acks", "1")
            .set("compression.type", "lz4")
            .create()
            .expect("Failed to create Kafka producer");

        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let producer_clone = producer.clone();
        let buffer_clone = buffer.clone();
        let topic_string = topic.to_string();

        tokio::spawn(Self::flush_loop(
            producer_clone,
            topic_string,
            buffer_clone,
            batch_size,
            flush_interval,
        ));

        Self {
            producer,
            topic: topic.to_string(),
            buffer,
            batch_size,
            flush_interval,
        }
    }

    pub async fn send(&self, payload: Vec<u8>) {
        let mut buffer = self.buffer.lock().await;
        buffer.push_back(payload);
    }

    pub async fn flush(&self) {
        // Simulate flush (no-op for now)
        sleep(Duration::from_millis(200)).await;
    }

    async fn flush_loop(
        producer: FutureProducer,
        topic: String,
        buffer: Arc<Mutex<VecDeque<Vec<u8>>>>,
        batch_size: usize,
        flush_interval: Duration,
    ) {
        let mut last_flush = Instant::now();
        loop {
            sleep(Duration::from_millis(100)).await;

            let mut batch = Vec::new();
            {
                let mut buf = buffer.lock().await;
                while batch.len() < batch_size && !buf.is_empty() {
                    if let Some(payload) = buf.pop_front() {
                        batch.push(payload);
                    }
                }
            }

            if !batch.is_empty()
                && (batch.len() >= batch_size || last_flush.elapsed() >= flush_interval)
            {
                for payload in batch {
                    let record = FutureRecord::to(&topic)
                        .payload(&payload)
                        .key(&());

                    let _ = producer.send(record, Duration::from_secs(1)).await;
                }
                last_flush = Instant::now();
            }
        }
    }
}
