mod grpc;
mod models;
mod config;
mod kafka;

#[tokio::main]
async fn main() {
    let config = config::AppConfig::from_env();
    println!("InsightsX gRPC server running at: {}", config.grpc_address);

    grpc::server::start_grpc_server(&config.grpc_address)
        .await
        .expect("Failed to start gRPC server");



        let config = config::AppConfig::from_env();

    // Spawn gRPC server clearly as separate task
    tokio::spawn(async move {
        grpc::server::start_grpc_server(&config.grpc_address)
            .await
            .expect("Failed to start gRPC server");
    });

    // Kafka Consumer clearly defined
    let mut kafka_consumer = kafka::consumer::KafkaConsumer::new(
        vec![config.kafka_brokers.clone()],
        &config.kafka_topic, // <-- fixed clearly
    );

    kafka_consumer.consume(|msg| {
        println!("Consumed message: {:?}", msg);
    });
}
