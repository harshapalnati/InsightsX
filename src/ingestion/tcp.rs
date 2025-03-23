use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tracing::{info, error};
use std::sync::Arc;
use crate::models::log_entry::LogEntry;
use serde_json;

pub async fn start_tcp_server(addr: &str, sender: Arc<mpsc::Sender<LogEntry>>) {
    let listener = TcpListener::bind(addr).await.expect("Failed to bind TCP server");
    info!("TCP Log Server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("New TCP connection from {}", addr);
                let sender_clone = sender.clone();
                tokio::spawn(async move {
                    handle_tcp_connection(socket, sender_clone).await;
                });
            }
            Err(err) => error!("TCP connection error: {}", err),
        }
    }
}

async fn handle_tcp_connection(socket: tokio::net::TcpStream, sender: Arc<mpsc::Sender<LogEntry>>) {
    let reader = BufReader::new(socket);
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        match serde_json::from_str::<LogEntry>(&line) {
            Ok(log) => {
                if sender.send(log).await.is_err() {
                    error!("TCP log queue is full, dropping log");
                }
            }
            Err(e) => error!("Failed to parse TCP log: {} | line: {}", e, line),
        }
    }
}
