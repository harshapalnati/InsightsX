use tonic::{transport::Server, Request, Response, Status, Streaming};
use std::net::SocketAddr;
use tracing::{info, error, warn};
use tokio_stream::StreamExt; // For handling gRPC streaming

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
        println!("📥 Received gRPC request for log streaming."); // ✅ Now visible in Docker

        let mut stream = request.into_inner();
        
        while let Some(log) = stream.next().await {
            match log {
                Ok(entry) => {
                    println!("✅ Received log: {:?}", entry); // ✅ Now visible in Docker
                }
                Err(e) => println!("⚠️ Error receiving log: {}", e),
            }
        }

        let response = Ack {
            success: true,
            message: "✅ Logs received successfully".to_string(),
        };

        println!("🚀 Sending response: {:?}", response); // ✅ Now visible in Docker

        Ok(Response::new(response))
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
