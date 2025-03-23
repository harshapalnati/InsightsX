use insightsx::config::AppConfig;
use insightsx::kafka::consumer::KafkaConsumer;
use chrono::Local;
use serde_json;

fn main() {
    let config = AppConfig::from_env();

    let brokers = vec![config.kafka_brokers.clone()];
    let topic = config.kafka_topic.clone();

    // Use the retry-enabled constructor instead of `new`
    let mut kafka_consumer = KafkaConsumer::new_with_retry(brokers, &topic);

    kafka_consumer.consume(|msg| {
        let payload = serde_json::to_string(msg)
            .unwrap_or_else(|_| "[Serialization Error]".to_string());
        println!("{} ✅ Received Kafka message: {}", Local::now().format("%Y-%m-%d %H:%M:%S%.6f"), payload);
    });
}
