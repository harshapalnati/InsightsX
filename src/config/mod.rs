

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub grpc_address: String,
}
impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            kafka_brokers: std::env::var("KAFKA_BROKERS").unwrap_or("kafka:9092".into()),
            kafka_topic: std::env::var("KAFKA_TOPIC").unwrap_or("insightx-logs".into()),
            grpc_address: std::env::var("GRPC_ADDRESS").unwrap_or("0.0.0.0:50051".into()),
        }
    }
}
