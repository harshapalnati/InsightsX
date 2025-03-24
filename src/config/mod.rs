

use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub grpc_address: String,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_password: String,
}
impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            kafka_brokers: std::env::var("KAFKA_BROKERS").unwrap_or("kafka:9092".into()),
            kafka_topic: std::env::var("KAFKA_TOPIC").unwrap_or("insightx-logs".into()),
            grpc_address: std::env::var("GRPC_ADDRESS").unwrap_or("0.0.0.0:50051".into()),
            clickhouse_url: env::var("CLICKHOUSE_URL").unwrap_or("https://i0zbyfvwa7.us-east-1.aws.clickhouse.cloud:8443".into()),
            clickhouse_database: env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "logs".to_string()),
            clickhouse_password: env::var("CLICKHOUSE_PASSWORD").unwrap_or("yq3ELB.ONGWE_".into()),
        }
    }
}
