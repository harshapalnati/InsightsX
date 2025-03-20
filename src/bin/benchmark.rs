use insightsx::config::AppConfig;
use insightsx::kafka::producer::KafkaProducer;
use std::sync::{Arc, Mutex}; // ✅ Use Arc<Mutex<T>> for shared mutability
use std::time::Instant;
use rayon::prelude::*; // ✅ Use parallel execution

fn main() {
    let config = AppConfig::from_env();
    let producer = Arc::new(Mutex::new(KafkaProducer::new(
        &config.kafka_brokers,
        &config.kafka_topic,
        10,
    )));

    let num_messages = 1_000_000;
    let start_time = Instant::now();

    (0..num_messages).into_par_iter().for_each(|i| {
        let message = format!("Benchmark log {}", i);
        if let Ok(mut producer) = producer.lock() {
            let _ = producer.send(message.as_bytes());
        }
    });

    let duration = start_time.elapsed();
    println!(
        "✅ Sent {} messages in {:.2?} seconds",
        num_messages, duration
    );
}
