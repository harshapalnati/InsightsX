pub mod kafka;
pub mod config;
pub mod grpc;
pub mod models;

use tokio::signal;
use tracing::{info, error};
use tokio::task;
use crate::kafka::consumer::KafkaConsumer;

#[tokio::main]
async fn main() {
    let config = config::AppConfig::from_env();

    // ✅ Use tracing for better logs
    info!("🚀 InsightsX gRPC server running at: {}", config.grpc_address);

    // ✅ Start gRPC server asynchronously
    let grpc_task = tokio::spawn(async move {
        if let Err(e) = grpc::server::start_grpc_server(&config.grpc_address).await {
            error!("❌ Failed to start gRPC server: {}", e);
        }
    });

    // ✅ Start Kafka Consumer in a separate task
    let kafka_task = task::spawn_blocking(move || {
        let mut kafka_consumer = KafkaConsumer::new(vec![config.kafka_brokers.clone()], &config.kafka_topic);
        kafka_consumer.consume(|msg| {
            info!("✅ Consumed message: {:?}", msg);
        });
    });

    // ✅ Graceful shutdown handling
    tokio::select! {
        _ = grpc_task => error!("⚠️ gRPC server exited unexpectedly"),
        _ = kafka_task => error!("⚠️ Kafka consumer exited unexpectedly"),
        _ = signal::ctrl_c() => {
            info!("🛑 Shutdown signal received. Exiting...");
        }
    }
}
