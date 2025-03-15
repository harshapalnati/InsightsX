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
}
