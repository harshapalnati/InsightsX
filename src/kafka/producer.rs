use kafka::producer::{Producer, Record, RequiredAcks};
use std::collections::VecDeque;
use std::time::Duration;
use std::thread::sleep;
use std::sync::{Arc, Mutex};

pub struct KafkaProducer {
    producer: Arc<Mutex<Producer>>,  // ✅ Use Arc<Mutex<Producer>> for thread safety
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
            producer: Arc::new(Mutex::new(producer)), // ✅ Wrap Producer in Arc<Mutex<>>
            topic: topic.to_owned(),
            buffer: VecDeque::new(),
            batch_size,
        }
    }

    pub fn send(&mut self, payload: &[u8]) {
        let compressed_payload = Self::compress_data(payload);
        self.buffer.push_back(compressed_payload);

        if self.buffer.len() >= self.batch_size {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        while let Some(payload) = self.buffer.pop_front() {
            let producer = self.producer.clone(); // ✅ Clone Arc pointer
            let topic = self.topic.clone();
            Self::send_with_retry(producer, &topic, &payload, 3);
        }
    }

    fn send_with_retry(producer: Arc<Mutex<Producer>>, topic: &str, payload: &[u8], retries: u8) {
        for attempt in 0..=retries {
            let mut producer = producer.lock().unwrap(); // ✅ Lock the Mutex to get producer
            match producer.send(&Record {
                topic,
                partition: -1,
                key: (),
                value: payload,
            }) {
                Ok(_) => return,
                Err(e) => {
                    println!("❌ Kafka send failed (attempt {}/{}): {}", attempt, retries, e);
                    sleep(Duration::from_millis(500));
                }
            }
        }
        println!("❌ All Kafka send attempts failed, log dropped!");
    }

    fn compress_data(data: &[u8]) -> Vec<u8> {
        let mut encoder = snap::raw::Encoder::new();
        encoder.compress_vec(data).expect("Compression failed")
    }
}

// ✅ Manually Implement Clone
impl Clone for KafkaProducer {
    fn clone(&self) -> Self {
        Self {
            producer: Arc::clone(&self.producer),  // ✅ Clone Arc pointer
            topic: self.topic.clone(),
            buffer: VecDeque::new(),
            batch_size: self.batch_size,
        }
    }
}
