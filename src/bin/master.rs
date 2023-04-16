use chrono::{DateTime, Utc};
use core::panic;
use hash_ring::HashRing;
use hitormiss::error::{Error, ErrorCode};
use hitormiss::parser::{
    build_ack_response, build_error_response, build_lsp_response, build_miss_response,
    build_ok_response, parse_request, CommandType, ParsedRequest,
};
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{event, Level};
use uuid::Uuid;

#[derive(Clone)]
struct Partition {
    id: Uuid,
    addr: String,
    conn: Arc<Mutex<TcpStream>>,
    time_joined: SystemTime,
}

impl Partition {
    fn new(addr: String, conn: Arc<Mutex<TcpStream>>) -> Self {
        Self {
            id: Uuid::new_v4(),
            addr,
            conn,
            time_joined: SystemTime::now(),
        }
    }
}

impl fmt::Debug for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let datetime: DateTime<Utc> = self.time_joined.into();
        let timestamp_formatted = datetime.format("%Y-%m-%d %H:%M:%S%.3f");
        f.debug_struct("Partition")
            .field("id", &self.id)
            .field("addr", &self.addr)
            .field("time_joined", &timestamp_formatted.to_string())
            .finish()
    }
}

impl PartialEq for Partition {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Eq for Partition {}

impl Hash for Partition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.addr)
    }
}

type Ring = Arc<Mutex<HashRing<Partition>>>;
type PartitionSet = Arc<Mutex<HashSet<Partition>>>;

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
    let partition_set: PartitionSet = Arc::new(Mutex::new(HashSet::new()));

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
        let partition_set_clone: PartitionSet = partition_set.clone();

        tokio::spawn(async move {
            match handle_connection(socket, ring_clone, partition_set_clone).await {
                Ok(_) => {}
                Err(e) => {
                    event!(Level::DEBUG, "Failed to handle connection: {}", e);
                }
            }
        });
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    ring: Ring,
    partition_set: PartitionSet,
) -> Result<(), Error> {
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
                    CommandType::Get
                    | CommandType::Set
                    | CommandType::Delete
                    | CommandType::Lsd => {
                        forward_to_partition(socket, &parsed_request, ring, partition_set).await;
                        Ok(())
                    }
                    CommandType::Notify => {
                        handle_notify(socket, ring, partition_set).await;
                        Ok(())
                    }
                    CommandType::ListPartitions => {
                        handle_list(socket, partition_set).await;
                        Ok(())
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

async fn handle_failed_forward(mut client_socket: TcpStream, request: &ParsedRequest) {
    match request.cmd {
        CommandType::Get => {
            if let Some(key) = &request.key {
                client_socket
                    .write_all(&build_miss_response(key.as_str()))
                    .await
                    .unwrap();
            } else {
                client_socket
                    .write_all(&build_error_response(&Error::from_code(ErrorCode::Unknown)))
                    .await
                    .unwrap();
            }
        }
        CommandType::Set | CommandType::Delete => {
            client_socket.write_all(&build_ok_response()).await.unwrap();
        }
        _ => {}
    }
}

async fn unregister_partition(partition: &Partition, ring: Ring, partition_set: PartitionSet) {
    ring.lock().await.remove_node(partition);
    partition_set.lock().await.remove(partition);
}

async fn forward_to_partition(
    mut client_socket: TcpStream,
    request: &ParsedRequest,
    ring: Ring,
    partition_set: PartitionSet,
) {
    let responsible_partition: Option<Partition> = if let Some(key) = &request.key {
        ring.lock()
            .await
            .get_node(key.to_string())
            .map(Clone::clone)
    } else {
        None
    };
    match responsible_partition {
        Some(partition) => {
            let mut partition_socket = partition.conn.lock().await;
            if let Err(e) = partition_socket
                .write_all(request.original_rq.as_bytes())
                .await
            {
                event!(
                    Level::ERROR,
                    "Failed to write to partition: {:?} {:?}",
                    partition.addr,
                    e
                );
                unregister_partition(&partition, ring, partition_set).await;
                handle_failed_forward(client_socket, request).await;
                return;
            }
            event!(
                Level::DEBUG,
                "Forwarded request to partition {:?}: {}",
                partition.addr,
                request.original_rq
            );
            let mut buf = vec![0; 4096];
            let read_amount = match partition_socket.read(&mut buf).await {
                Ok(amount) => amount,
                Err(error) => {
                    event!(
                        Level::ERROR,
                        "Failed to read from partition: {:?}, error: {:?}",
                        partition.addr,
                        error
                    );
                    unregister_partition(&partition, ring, partition_set).await;
                    handle_failed_forward(client_socket, request).await;
                    return;
                }
            };

            if read_amount == 0 {
                event!(
                    Level::ERROR,
                    "Zero bytes read from partition: {:?}",
                    partition.addr
                );
                unregister_partition(&partition, ring, partition_set).await;
                handle_failed_forward(client_socket, request).await;
                return;
            }
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
                    ErrorCode::NoPartitionsInRing,
                )))
                .await
                .unwrap();
        }
    }
}

async fn handle_list(mut socket: TcpStream, partition_set: PartitionSet) {
    let partitions = partition_set
        .lock()
        .await
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let partitions_str = format!("{:?}", partitions);
    socket
        .write_all(&build_lsp_response(partitions_str))
        .await
        .unwrap();
}

async fn handle_notify(mut socket: TcpStream, ring: Ring, partition_set: PartitionSet) {
    let partition_addr = socket.peer_addr().unwrap();
    event!(Level::DEBUG, "NTF from partition: {:?}", partition_addr,);
    socket.write_all(&build_ack_response()).await.unwrap();

    let partition = Partition::new(partition_addr.to_string(), Arc::new(Mutex::new(socket)));

    ring.lock().await.add_node(&partition);
    partition_set.lock().await.insert(partition.clone());

    event!(Level::DEBUG, "{:?} successfully added to ring", partition,);
}
