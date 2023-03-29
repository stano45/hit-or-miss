extern crate lru;

use clap::Parser;
use core::panic;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::str;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{event, Level};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,
}

type Cache = Arc<Mutex<LruCache<String, String>>>;

#[tokio::main]
pub async fn main() {
    let args = Cli::parse();
    let addr = format!("localhost:{0}", args.port);
    let addr_clone = addr.clone();
    let _master = String::from("localhost:6969");
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    event!(Level::INFO, "Starting partition on address: {addr}");

    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => {
            event!(
                Level::INFO,
                "Connection established on address: {addr_clone}"
            );
            listener
        }
        Err(_) => {
            panic!("Failed to bind!");
        }
    };

    let mut stream = TcpStream::connect("127.0.0.1:6969").await.unwrap();
    // send data to the connected port
    stream.write_all(b"NTF: I'm alive").await.unwrap();

    let cache = Arc::new(Mutex::new(LruCache::<String, String>::new(
        NonZeroUsize::new(2).unwrap(),
    )));

    loop {
        let (stream, _addr) = match listener.accept().await {
            Ok((socket, addr)) => {
                event!(
                    Level::INFO,
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
        let cache_clone = Arc::clone(&cache);
        tokio::spawn(async move {
            stream.readable().await.unwrap();
            handle_connection(stream, cache_clone).await;
        });
    }
}

//master node port: 6969

async fn handle_connection(mut stream: TcpStream, cache: Cache) {
    let mut buf = [0; 4096];
    match stream.try_read(&mut buf) {
        Ok(_) => {
            let v = buf.to_vec();
            let str_buf = match str::from_utf8(&v) {
                Ok(v) => {
                    event!(Level::INFO, "Successfully parsed message {}", v);
                    v
                }
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };
            match &str_buf[0..3] {
                "GET" => {
                    let key = &str_buf[4..8];
                    println!("Key: {}", key);
                    let value_string = cache.lock().unwrap().get(key).unwrap().to_owned();
                    let value = &value_string[..];
                    println!("Value should be an error: {}", value);
                    stream.write_all(b"Doener mit Dativ\n").await.unwrap();
                    stream.write_all(value.as_bytes()).await.unwrap();
                }
                "SET" => {
                    let key = &str_buf[5..8];
                    let value = &str_buf[9..12];
                    println!("Key: {}, Value: {}", key, value);
                    cache
                        .lock()
                        .unwrap()
                        .push(key.to_string(), value.to_string());
                    let value_string = cache.lock().unwrap().get(key).unwrap().to_owned();
                    println!("Value in cache: {}", value_string);
                    stream.write_all(b"Here is the data\n").await.unwrap();
                    stream.write_all(str_buf.as_bytes()).await.unwrap();
                }
                "DEL" => {
                    stream.write_all(b"Deleted the data\n").await.unwrap();
                    stream.write_all(str_buf.as_bytes()).await.unwrap();
                }
                _ => {
                    stream
                        .write_all(b"There was an error with the request\n")
                        .await
                        .unwrap();
                    stream.write_all(str_buf.as_bytes()).await.unwrap();
                }
            }
        }
        Err(e) => {
            panic!("error: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_if_value_was_added_to_cache() {
        let cache = Arc::new(Mutex::new(LruCache::<String, String>::new(
            NonZeroUsize::new(2).unwrap(),
        )));
        cache
            .lock()
            .unwrap()
            .put(String::from("Name"), String::from("Fjoni"));
        assert_eq!(*cache.lock().unwrap().get("Name").unwrap(), "Fjoni");
    }
}
