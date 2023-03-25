use bytes::Bytes;
use clap::Parser;
use core::panic;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tracing::{event, Level};

static ERR_INVALID_REQUEST_CMD: &[u8; 34] = b"Invalid request: command not found";
static _ERR_INVALID_REQUEST_ARG: &[u8; 42] = b"Invalid request: invalid command arguments";
static _ERR_INVALID_REQUEST_FLAG: &[u8; 32] = b"Invalid request: flags not found";
static ERR_INVALID_REQUEST_FORMAT: &[u8; 39] = b"Invalid request: invalid UTF-8 sequence";

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

async fn handle_connection(socket: TcpStream, _db: Db) {
    event!(
        Level::DEBUG,
        "{}",
        format!("Handling connection: {:?}", socket)
    );

    let mut buf = [0; 4096];
    match socket.try_read(&mut buf) {
        Ok(_) => {
            let v = buf.to_vec();
            let str_buf = match std::str::from_utf8(&v) {
                Ok(v) => {
                    event!(
                        Level::DEBUG,
                        "Successfully parsed utf8 request from {:?}: {}",
                        socket.peer_addr().unwrap(),
                        v,
                    );
                    v
                }
                Err(e) => {
                    event!(Level::DEBUG, "Erorr parsing request {:?}: {}", v, e);
                    handle_error(socket, ERR_INVALID_REQUEST_FORMAT, _db).await;
                    return;
                }
            };

            // Get indices of multi-byte characters (without this, this string would panic: ˚å)
            let start = str_buf.char_indices().nth(0).map(|(i, _)| i).unwrap_or(0);
            let end = str_buf.char_indices().nth(3).map(|(i, _)| i).unwrap_or(0);
            match &str_buf[start..end] {
                "GET" => {
                    handle_get(socket, str_buf, _db).await;
                }
                "SET" => {
                    handle_set(socket, str_buf, _db).await;
                }
                "DEL" => {
                    handle_delete(socket, str_buf, _db).await;
                }
                _ => {
                    handle_error(socket, ERR_INVALID_REQUEST_CMD, _db).await;
                }
            }
        }
        Err(e) => {
            //TODO: I guess we should handle this error lol
            println!("error: {e}");
        }
    }
}

async fn handle_get(mut socket: TcpStream, buf: &str, _db: Db) {
    socket.write_all(b"Here is the data\n").await.unwrap();
    socket.write_all(buf.as_bytes()).await.unwrap();
}

async fn handle_set(mut socket: TcpStream, buf: &str, _db: Db) {
    socket
        .write_all(
            b"[Intro: 2Pac]
                    (Sucka-ass)
                    I ain't got no mothafuckin' friends
                    That's why I fucked yo' bitch, you fat mothafucka!
                    (Take money) West Side, Bad Boy killas
                    (Take money) (You know) You know who the realest is
                    (Take money) Niggas, we bring it too
                    That's a'ight, haha
                    (Take money) Haha\n",
        )
        .await
        .unwrap();
    socket.write_all(buf.as_bytes()).await.unwrap();
}

async fn handle_delete(mut socket: TcpStream, buf: &str, _db: Db) {
    socket.write_all(b"Deleted the data\n").await.unwrap();
    socket.write_all(buf.as_bytes()).await.unwrap();
}

async fn handle_error(mut socket: TcpStream, err_msg: &[u8], _db: Db) {
    let mut message = b"Error: ".to_vec();
    message.extend_from_slice(err_msg);
    socket.write_all(&message).await.unwrap();
}
