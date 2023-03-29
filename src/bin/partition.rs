extern crate lru;

use clap::Parser;
use core::panic;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::str;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
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
    //send NTF to master, wait for OK
    notify_master().await;

    
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
        let cache_clone = cache.clone();
        tokio::spawn(async move {
            stream.readable().await.unwrap();
            handle_connection(stream, cache_clone).await;
        });
    }
}

async fn notify_master() {
    let master_addr = String::from("127.0.0.1:6969");
    let mut stream = TcpStream::connect(&master_addr).await.unwrap();
    // send data to master
    stream.write_all(b"NTF").await.unwrap();
   
    let mut buf = [0; 4096];
    let x = match stream.read(&mut buf).await {
        Ok(_) => {
            let v = buf.to_vec();
            let str_buf = match std::str::from_utf8(&v) {
                Ok(v) => {
                    event!(
                        Level::DEBUG,
                        "Successfully parsed utf8 request from {:?}: {}",
                        stream.peer_addr().unwrap(),
                        v,
                    );
                    v.to_string()
                }
                Err(e) => {
                    panic!("{}",e.to_string())     
                }
            };
            Ok(str_buf)
        }
        Err(e) => {
            Err(e)
        }
    };

    match x {
        Ok(str_buf) => {
            // Get indices of multi-byte characters (without this, this string would panic: ˚å)
            let start = str_buf.char_indices().next().map(|(i, _)| i).unwrap_or(0);
            let end = str_buf.char_indices().nth(3).map(|(i, _)| i).unwrap_or(0);
            match &str_buf[start..end] {
                "ACK" => {
                    event!(
                        Level::DEBUG,
                        "Received ACK from master: {:?}",
                        stream.peer_addr().unwrap(),
                    );
                }
                _ => {
                    panic!("Master responded with: {}, should be: ACK. Panicking!", str_buf);
                }
            }
        }
        Err(e) => {
            //TODO: I guess we should handle this error lol
            println!("error: {e}");
        }
    }
}

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