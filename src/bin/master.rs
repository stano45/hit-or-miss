use clap::Parser;
use core::panic;
use hash_ring::HashRing;
use hitormiss::parser::{parse_request, Error, ParsedRequest};
use std::fmt;
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
#[derive(Debug, Clone)]
struct Partition {
    conn: Arc<Mutex<TcpStream>>,
}

impl fmt::Display for Partition {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.conn.lock().unwrap().peer_addr().unwrap())
    }
}

type Ring = Arc<Mutex<HashRing<Partition>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let addr = "127.0.0.1:6969";
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

    // how many times one node is replicated on the ring
    let num_replicas = 10;
    // set this to define some initial nodes (maybe when the drops and restarts or something idk)
    let initial_nodes: Vec<Partition> = Vec::new();
    // consistent hashing node ring
    let ring: Ring = Arc::new(Mutex::new(HashRing::new(initial_nodes, num_replicas)));

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

        let ring_clone: Ring = ring.clone();
        tokio::spawn(async move {
            handle_connection(socket, ring_clone).await;
        });
    }
}

async fn handle_connection(socket: TcpStream, ring: Ring) {
    event!(
        Level::DEBUG,
        "{}",
        format!("Handling connection: {:?}", socket)
    );

    let mut buf = [0; 4096];
    let request: Result<ParsedRequest, Error> = match socket.try_read(&mut buf) {
        Ok(_) => match parse_request(buf.to_vec()) {
            Ok(parsed_request) => {
                event!(Level::DEBUG, "Parsed request: {:?}", parsed_request);
                match parsed_request.cmd.as_str() {
                    "NTF" => _handle_notify(socket, ring).await,
                    "GET" => _handle_get(socket, std::str::from_utf8(&buf).unwrap(), ring).await,
                    "SET" => _handle_set(socket, std::str::from_utf8(&buf).unwrap(), ring).await,
                    "DEL" => _handle_delete(socket, std::str::from_utf8(&buf).unwrap(), ring).await,
                    &_ => todo!(),
                }
                Ok(parsed_request)
            }
            Err(e) => {
                handle_error(socket, &e, ring).await;
                Err(e)
            }
        },
        Err(e) => {
            println!("error: {e}");
            Err(Error {
                code: 2, // Or another appropriate error code
                msg: format!("Socket read error: {}", e),
            })
        }
    };
    event!(Level::DEBUG, "Request parsed: {:?}", request);
}

async fn _handle_get(mut socket: TcpStream, buf: &str, _ring: Ring) {
    socket.write_all(b"Here is the data\n").await.unwrap();
    socket.write_all(buf.as_bytes()).await.unwrap();
}

async fn _handle_set(mut socket: TcpStream, buf: &str, _ring: Ring) {
    socket.write_all(buf.as_bytes()).await.unwrap();
}

async fn _handle_delete(mut socket: TcpStream, buf: &str, _ring: Ring) {
    socket.write_all(b"Deleted the data\n").await.unwrap();
    socket.write_all(buf.as_bytes()).await.unwrap();
}

async fn _handle_notify(mut socket: TcpStream, ring: Ring) {
    let partition_addr = socket.peer_addr().unwrap();
    event!(Level::DEBUG, "NTF from partition: {:?}", partition_addr,);
    socket.write_all(b"ACK\n").await.unwrap();

    ring.lock().unwrap().add_node(&Partition {
        conn: Arc::new(Mutex::new(socket)),
    });
    event!(
        Level::DEBUG,
        "Partition {:?} successfully added to ring",
        partition_addr,
    );
}

async fn handle_error(mut socket: TcpStream, err: &Error, _ring: Ring) {
    let mut message = b"Error: ".to_vec();
    message.extend_from_slice(err.msg.as_bytes());
    socket.write_all(&message).await.unwrap();
}
