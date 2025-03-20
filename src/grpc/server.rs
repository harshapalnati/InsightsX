use tonic::{transport::Server, Request, Response, Status, Streaming};
use std::net::SocketAddr;
use tracing::{info, error};
use crate::kafka::producer::KafkaProducer;
use rmp_serde::{to_vec, from_slice};
use snap::raw::Encoder;
use crate::models::log_entry::{LogEntry, LogLevel};
use chrono::{TimeZone, Utc};


pub mod logs {
    tonic::include_proto!("logs");
}

use logs::{Ack, LogEntry as GrpcLogEntry};
use logs::log_service_server::{LogService, LogServiceServer};

#[derive(Debug, Default)]
pub struct MyLogService;


#[tonic::async_trait]
impl LogService for MyLogService {
    async fn stream_logs(
        &self,
        request: Request<Streaming<GrpcLogEntry>>,
    ) -> Result<Response<Ack>, Status> {
        let config = crate::config::AppConfig::from_env();
        let mut kafka_producer = KafkaProducer::new(&config.kafka_brokers, &config.kafka_topic, 10);

        let mut stream = request.into_inner();
        let mut batch = Vec::new();

        while let Some(grpc_log) = stream.message().await? {
            let log_entry = LogEntry {
                source: grpc_log.source,
                level: match grpc_log.level.as_str() {
                    "INFO" => LogLevel::INFO,
                    "WARN" => LogLevel::WARN,
                    "ERROR" => LogLevel::ERROR,
                    "DEBUG" => LogLevel::DEBUG,
                    _ => LogLevel::INFO, // Default
                },
                message: grpc_log.message,
                timestamp: grpc_log.timestamp,  // ✅ FIXED: Use prost Timestamp directly
                metadata: None,
            };

            let serialized_log = rmp_serde::to_vec(&log_entry)
                .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?;

            let compressed_log = compress_data(&serialized_log);
            batch.push(compressed_log);

            if batch.len() >= 10 {
                kafka_producer.flush();
            }
        }

        kafka_producer.flush();
        Ok(Response::new(Ack { success: true, message: "Logs sent successfully".into() }))
    }
}

fn compress_data(data: &[u8]) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.compress_vec(data).expect("Compression failed")
}

/// Starts the gRPC server on a given address
pub async fn start_grpc_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let socket_addr: SocketAddr = addr.parse().expect("❌ Invalid address format");

    info!("🚀 Starting gRPC server on {}", socket_addr);

    let service = LogServiceServer::new(MyLogService::default());

    Server::builder()
        .add_service(service)
        .serve(socket_addr)
        .await
        .map_err(|e| {
            error!("❌ Failed to start gRPC server: {}", e);
            Box::new(e) as Box<dyn std::error::Error>
        })
}
