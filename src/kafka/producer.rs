use kafka::producer::{Producer, Record, RequiredAcks};

pub struct KafkaProducer {
    producer: Producer,
    topic: String,
}

impl KafkaProducer {
    pub fn new(brokers: &str, topic: &str) -> Self {
        let producer = Producer::from_hosts(vec![brokers.to_string()])
            .with_ack_timeout(std::time::Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .expect("Kafka Producer creation error");

        Self {
            producer,
            topic: topic.to_owned(),
        }
    }

    pub fn send(&mut self, payload: &[u8]) -> Result<(), kafka::Error> {
        self.producer.send(&Record {
            topic: &self.topic,
            partition: -1,
            key: (),
            value: payload,
        })
    }
}
