use core::panic;
use hash_ring::HashRing;
use hitormiss::parser::{
    build_error_response, parse_request, CommandType, Error, ErrorCode, ParsedRequest,
};
use std::fmt;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{event, Level};

#[derive(Debug, Clone)]
struct Partition {
    addr: String,
    conn: Arc<Mutex<TcpStream>>,
}

impl fmt::Display for Partition {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.addr)
    }
}

type Ring = Arc<Mutex<HashRing<Partition>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    const MASTER_ADDR: &str = "127.0.0.1:6969";
    event!(
        Level::INFO,
        "Starting master service on address: {MASTER_ADDR}"
    );
    let listener = match TcpListener::bind(MASTER_ADDR).await {
        Ok(listener) => {
            event!(Level::DEBUG, "{}", format!("Bind {:?}", listener));
            listener
        }
        Err(e) => {
            event!(Level::ERROR, "Failed to bind: {}", e);
            panic!("Failed to bind");
        }
    };

    // # of replicas per partition
    let num_replicas = 1;
    // set this to define some initial nodes
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
            match handle_connection(socket, ring_clone).await {
                Ok(_) => {}
                Err(e) => {
                    event!(Level::DEBUG, "Failed to handle connection: {}", e);
                }
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream, ring: Ring) -> Result<(), Error> {
    event!(
        Level::DEBUG,
        "{}",
        format!("Handling connection: {:?}", socket)
    );

    let mut buf = [0; 4096];
    match socket.try_read(&mut buf) {
        Ok(_) => match parse_request(buf.to_vec()) {
            Ok(parsed_request) => {
                event!(Level::DEBUG, "Parsed request: {:?}", parsed_request);
                match parsed_request.cmd {
                    CommandType::Get | CommandType::Set | CommandType::Delete => {
                        forward_to_partition(socket, parsed_request, ring).await;
                        Ok(())
                    }
                    CommandType::Notify => {
                        handle_notify(socket, ring).await;
                        Ok(())
                    }
                    CommandType::ListPartitions => {
                        handle_list(socket, ring).await;
                        Ok(())
                    }
                    CommandType::LSD => {
                        unimplemented!("Send LSD to all partition and aggregate response");
                    }
                    _ => {
                        socket
                            .write_all(&build_error_response(&Error::from_code(
                                ErrorCode::UnsupportedCommandMaster,
                            )))
                            .await
                            .unwrap();
                        Ok(())
                    }
                }
            }
            Err(e) => {
                socket.write_all(&build_error_response(&e)).await.unwrap();
                Err(e)
            }
        },
        Err(e) => {
            event!(
                Level::ERROR,
                "Failed to read from socket: {:?} {:?}",
                socket,
                e
            );
            Err(Error::from_code(ErrorCode::FailedSocketRead))
        }
    }
}

async fn forward_to_partition(mut client_socket: TcpStream, request: ParsedRequest, ring: Ring) {
    let responsible_partition: Option<Partition> = if let Some(key) = request.key {
        ring.lock().await.get_node(key).map(Clone::clone)
    } else {
        None
    };
    match responsible_partition {
        Some(partition) => {
            let mut partition_socket = partition.conn.lock().await;
            partition_socket
                .write_all(request.original_rq.as_bytes())
                .await
                .unwrap();
            event!(
                Level::DEBUG,
                "Forwarded request to partition {:?}: {}",
                partition.addr,
                request.original_rq
            );
            let mut buf = vec![0; 4096];
            let read_amount = partition_socket.read(&mut buf).await.unwrap();
            event!(
                Level::DEBUG,
                "Got response from partition: {:?}: {}",
                partition.addr,
                String::from_utf8(buf[..read_amount].to_vec()).unwrap()
            );
            client_socket.write_all(&buf[..read_amount]).await.unwrap();
        }
        None => {
            client_socket
                .write_all(&build_error_response(&Error::from_code(
                    ErrorCode::NoPartition,
                )))
                .await
                .unwrap();
        }
    }
}

async fn handle_list(mut _socket: TcpStream, _ring: Ring) {
    // let mut partitions = ring.lock().unwrap();
    // let mut response = b"".to_vec();
    // for partition in partitions {
    //     response.extend_from_slice(partition.to_string().as_bytes());
    //     response.extend_from_slice(b"");
    // }
    // client_socket.write_all(&response).await.unwrap();
    // return;
}

async fn handle_notify(mut socket: TcpStream, ring: Ring) {
    let partition_addr = socket.peer_addr().unwrap();
    event!(Level::DEBUG, "NTF from partition: {:?}", partition_addr,);
    socket.write_all(b"ACK\n").await.unwrap();

    ring.lock().await.add_node(&Partition {
        addr: partition_addr.to_string(),
        conn: Arc::new(Mutex::new(socket)),
    });
    event!(
        Level::DEBUG,
        "Partition {:?} successfully added to ring",
        partition_addr,
    );
}
