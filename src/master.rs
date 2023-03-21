use bytes::Bytes;
use clap::Parser;
use core::panic;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tracing::{event, Level};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Network port to use
    #[arg(value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,
}

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let addr = format!("127.0.0.1:{0}", args.port);
    event!(Level::INFO, "Starting master service on address: {addr}");
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => {
            event!(Level::DEBUG, "{}", format!("Bind {:?}", listener));
            listener
        }
        Err(e) => {
            event!(Level::ERROR, "Failed to bind: {}", e);
            panic!("Failed to bind");
        }
    };

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _addr) = match listener.accept().await {
            Ok((socket, addr)) => {
                event!(
                    Level::DEBUG,
                    "{}",
                    format!("Connection accepted: {:?} {:?}", socket, addr)
                );
                (socket, addr)
            }
            Err(e) => {
                event!(Level::ERROR, "Failed to accept connection: {}", e);
                continue;
            }
        };

        let db_clone = db.clone();
        tokio::spawn(async move {
            handle_connection(socket, db_clone).await;
        });
    }
}

async fn handle_connection(mut socket: TcpStream, _db: Db) {
    event!(
        Level::DEBUG,
        "{}",
        format!("Handling connection: {:?}", socket)
    );

    socket.write(b"hello from server\n").await.unwrap();
}
