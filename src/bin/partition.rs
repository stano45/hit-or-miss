extern crate lru;

use clap::Parser;
use core::panic;
use hitormiss::parser::{
    build_hit_response, build_miss_response, build_ok_response, Error, ErrorCode,
};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::str;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{event, Level};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let master_addr = String::from("127.0.0.1:6969");
    let mut stream = TcpStream::connect(&master_addr).await.unwrap();

    let mut cache = LruCache::<String, String>::new(NonZeroUsize::new(2).unwrap());

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
                    panic!("In {}", e.to_string())
                }
            };
            Ok(str_buf)
        }
        Err(e) => Err(e),
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
                    panic!(
                        "Master responded with: {}, should be: ACK. Panicking!",
                        str_buf
                    );
                }
            }
        }
        Err(e) => {
            //TODO: I guess we should handle this error lol
            println!("error: {e}");
        }
    }

    loop {
        match stream.read(&mut buf).await {
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
                        let key: &str = iterate_until_newline_character(str_buf, 4);
                        match cache.get(key) {
                            Some(value) => {
                                stream
                                    .write_all(&build_hit_response(key, value))
                                    .await
                                    .unwrap();
                            }
                            None => {
                                stream.write_all(&build_miss_response(key)).await.unwrap();
                            }
                        };
                    }
                    "SET" => {
                        let key = iterate_until_whitespace(str_buf, 4);
                        let value = iterate_until_null_character(str_buf, 8);
                        cache.put(key.to_string(), value.to_string());
                        stream.write_all(&build_ok_response()).await.unwrap();
                    }
                    "DEL" => {
                        let key: &str = iterate_until_newline_character(str_buf, 4);
                        cache.pop(key);
                        stream.write_all(&build_ok_response()).await.unwrap();
                    }
                    _ => {
                        send_error(&mut stream, &Error::from_code(ErrorCode::InvalidRequestCmd))
                            .await;
                    }
                }
            }
            Err(e) => {
                panic!("error: {e}");
            }
        }
    }
}

fn iterate_until_whitespace(s: &str, start_index: usize) -> &str {
    let end_index = s[start_index..]
        .find(char::is_whitespace)
        .map_or(s.len(), |i| start_index + i);

    &s[start_index..end_index]
}

fn iterate_until_null_character(input: &str, start_index: usize) -> &str {
    let end_index = input[start_index..]
        .find('\0')
        .map_or(input.len(), |i| start_index + i);
    &input[start_index..end_index]
}

fn iterate_until_newline_character(input: &str, start_index: usize) -> &str {
    let end_index = input[start_index..]
        .find('\n')
        .map_or(input.len(), |i| start_index + i);
    &input[start_index..end_index]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_if_value_was_added_to_cache() {
        let mut cache = LruCache::<String, String>::new(NonZeroUsize::new(2).unwrap());
        cache.put(String::from("Name"), String::from("Fjoni"));
        assert_eq!(*cache.get("Name").unwrap(), "Fjoni");
    }
}

async fn send_error(socket: &mut TcpStream, err: &Error) {
    let mut message = b"Error: ".to_vec();
    message.extend_from_slice(err.msg.as_bytes());
    socket.write_all(&message).await.unwrap();
}
