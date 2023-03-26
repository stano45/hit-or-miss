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

#[tokio::main]
pub async fn main() {
    let args = Cli::parse();
    let mut _cache = Arc::new(Mutex::new(LruCache::<String, String>::new(
        NonZeroUsize::new(2).unwrap(),
    )));
    let addr = format!("localhost:{0}", args.port);
    let addr_clone = addr.clone();
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => {
            event!(
                Level::DEBUG,
                "Connection established on address: {addr_clone}"
            );
            listener
        }
        Err(_) => {
            panic!("Failed to bind!");
        }
    };

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            stream.readable().await.unwrap();
            handle_connection(stream).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 4096];
    match stream.try_read(&mut buf) {
        Ok(_) => {
            let v = buf.to_vec();
            let str_buf = match str::from_utf8(&v) {
                Ok(v) => {
                    event!(Level::DEBUG, "Successfully parsed message {}", v);
                    v
                }
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };
            match &str_buf[0..3] {
                "SET" => {
                    stream.write_all(b"Doener mit Dativ").await.unwrap();
                    stream.write_all(str_buf.as_bytes()).await.unwrap();
                }
                "GET" => {
                    println!("Inside get");
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
