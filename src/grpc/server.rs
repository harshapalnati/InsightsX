use tonic::{transport::Server, Request, Response, Status, Streaming};
use std::net::SocketAddr;
use tracing::{info, error, warn};
use tokio_stream::StreamExt; // For handling gRPC streaming
use crate::kafka::producer::KafkaProducer;
use serde_json;
use prost::Message;  // <-- add this import

// Import generated gRPC code from logs.proto
pub mod logs {
    tonic::include_proto!("logs");
}

use logs::log_service_server::{LogService, LogServiceServer};
use logs::{Ack, LogEntry};

/// Implementation of the gRPC LogService
#[derive(Debug, Default)]
pub struct MyLogService;





#[tonic::async_trait]
impl LogService for MyLogService {
    async fn stream_logs(
        &self,
        request: Request<Streaming<LogEntry>>,
    ) -> Result<Response<Ack>, Status> {
        let config = crate::config::AppConfig::from_env();
        let mut kafka_producer = KafkaProducer::new(&config.kafka_brokers, &config.kafka_topic);

        let mut stream = request.into_inner();

        while let Some(log) = stream.message().await? {
            let mut buf = Vec::new();

            // Clearly serialize the entry with Prost
            log.encode(&mut buf).map_err(|e| {
                Status::internal(format!("Protobuf serialization error: {}", e))
            })?;

            kafka_producer.send(&buf).map_err(|e| {
                Status::internal(format!("Kafka sending error: {}", e))
            })?;
        }

        Ok(Response::new(Ack {
            success: true,
            message: "Logs successfully sent to Kafka".into(),
        }))
    }
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
