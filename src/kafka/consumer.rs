use kafka::consumer::{Consumer, FetchOffset};

pub struct KafkaConsumer {
    consumer: Consumer,
}

impl KafkaConsumer {
    pub fn new(brokers: Vec<String>, topic: &str) -> Self {
        let consumer = Consumer::from_hosts(brokers)
            .with_topic(topic.to_string())
            .with_fallback_offset(FetchOffset::Earliest)
            .create()
            .expect("Consumer creation failed");

        Self { consumer }
    }

    pub fn consume<F>(&mut self, mut handler: F)
    where
        F: FnMut(&[u8]),
    {
        loop {
            for ms in self.consumer.poll().unwrap().iter() {
                for m in ms.messages() {
                    let payload = std::str::from_utf8(m.value).unwrap_or("[Invalid UTF-8]");
                    println!("✅ Received Kafka message: {}", payload);
                    handler(m.value);
                }
                self.consumer.consume_messageset(ms).unwrap();
            }
            self.consumer.commit_consumed().unwrap();
        }
    }
}
