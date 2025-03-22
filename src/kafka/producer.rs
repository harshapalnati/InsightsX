use kafka::producer::{Producer, Record, RequiredAcks};
use std::collections::VecDeque;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use tokio::time::sleep;

pub struct KafkaProducer {
    producer: Arc<Mutex<Producer>>, // Thread-safe shared producer
    topic: String,
    buffer: VecDeque<Vec<u8>>,
    batch_size: usize,
}

impl KafkaProducer {
    pub fn new(brokers: &str, topic: &str, batch_size: usize) -> Self {
        let producer = Producer::from_hosts(vec![brokers.to_string()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .expect("Kafka Producer creation error");

        Self {
            producer: Arc::new(Mutex::new(producer)),
            topic: topic.to_owned(),
            buffer: VecDeque::new(),
            batch_size,
        }
    }

    /// Asynchronously adds the payload to the buffer and flushes if the batch size is reached.
    pub async fn send(&mut self, payload: &[u8]) {
        self.buffer.push_back(payload.to_vec());
        if self.buffer.len() >= self.batch_size {
            self.flush().await;
        }
    }

    /// Asynchronously flushes the buffer by sending all messages with retry logic.
    pub async fn flush(&mut self) {
        while let Some(payload) = self.buffer.pop_front() {
            let producer = self.producer.clone();
            let topic = self.topic.clone();
            Self::send_with_retry(producer, &topic, &payload, 3).await;
        }
    }

    /// Async retry logic: attempts to send the payload; if it fails, waits and retries.
    async fn send_with_retry(producer: Arc<Mutex<Producer>>, topic: &str, payload: &[u8], retries: u8) {
        for attempt in 0..=retries {
            {
                let mut producer_lock = producer.lock().unwrap();
                match producer_lock.send(&Record {
                    topic,
                    partition: -1,
                    key: (),
                    value: payload,
                }) {
                    Ok(_) => return,
                    Err(e) => {
                        println!("❌ Kafka send failed (attempt {}/{}): {}", attempt, retries, e);
                    }
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        println!("❌ All Kafka send attempts failed, log dropped!");
    }
}

impl Clone for KafkaProducer {
    fn clone(&self) -> Self {
        Self {
            producer: Arc::clone(&self.producer),
            topic: self.topic.clone(),
            buffer: VecDeque::new(),
            batch_size: self.batch_size,
        }
    }
}
