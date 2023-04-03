use core::panic;
use hitormiss::error::{Error, ErrorCode};
use hitormiss::parser::{
    build_error_response, build_hit_response, build_lsd_response, build_miss_response,
    build_notify_request, build_ok_response, parse_request, CommandType,
};
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{event, Level};

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let master_addr = String::from("127.0.0.1:6969");
    let mut stream = TcpStream::connect(&master_addr).await.unwrap();

    notify_master(&mut stream).await;

    let mut cache = LruCache::<String, String>::new(NonZeroUsize::new(2).unwrap());

    loop {
        let mut buf = [0; 4096];
        match stream.read(&mut buf).await {
            Ok(_) => {
                let v = buf.to_vec();
                let parsed_request = match parse_request(v) {
                    Ok(parsed_request) => {
                        event!(
                            Level::INFO,
                            "Successfully parsed message {}",
                            parsed_request.original_rq
                        );
                        parsed_request
                    }
                    Err(e) => {
                        stream.write_all(&build_error_response(&e)).await.unwrap();
                        continue;
                    }
                };

                match parsed_request.cmd {
                    CommandType::Get => {
                        if let Some(key) = parsed_request.key {
                            match cache.get(&key) {
                                Some(value) => {
                                    stream
                                        .write_all(&build_hit_response(&key, value))
                                        .await
                                        .unwrap();
                                }
                                None => {
                                    stream.write_all(&build_miss_response(&key)).await.unwrap();
                                }
                            }
                        } else {
                            stream
                                .write_all(&build_error_response(&Error::from_code(
                                    ErrorCode::NotEnoughArgs,
                                )))
                                .await
                                .unwrap();
                        }
                    }
                    CommandType::Lsd => {
                        stream.write_all(&build_lsd_response(&cache)).await.unwrap();
                    }
                    CommandType::Set => {
                        if let (Some(key), Some(value)) = (parsed_request.key, parsed_request.value)
                        {
                            cache.put(key, value);
                            stream.write_all(&build_ok_response()).await.unwrap();
                        } else {
                            stream
                                .write_all(&build_error_response(&Error::from_code(
                                    ErrorCode::NotEnoughArgs,
                                )))
                                .await
                                .unwrap();
                        }
                    }
                    CommandType::Delete => {
                        if let Some(key) = parsed_request.key {
                            cache.pop(&key);
                            stream.write_all(&build_ok_response()).await.unwrap();
                        } else {
                            stream
                                .write_all(&build_error_response(&Error::from_code(
                                    ErrorCode::NotEnoughArgs,
                                )))
                                .await
                                .unwrap();
                        }
                    }
                    _ => {
                        stream
                            .write_all(&build_error_response(&Error::from_code(
                                ErrorCode::InvalidRequestCmd,
                            )))
                            .await
                            .unwrap();
                    }
                }
            }
            Err(e) => {
                panic!("error: {e}");
            }
        }
    }
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

async fn notify_master(stream: &mut TcpStream) {
    stream.write_all(&build_notify_request()).await.unwrap();

    let mut buf = [0; 4096];
    match stream.read(&mut buf).await {
        Ok(_) => match parse_request(buf.to_vec()) {
            Ok(parsed_request) => {
                event!(Level::DEBUG, "Parsed notify response: {:?}", parsed_request);
                match parsed_request.cmd {
                    CommandType::Ack => {
                        event!(
                            Level::INFO,
                            "Successfully connected to master. Listening for commands."
                        );
                    }
                    _ => {
                        event!(
                            Level::ERROR,
                            "Failed to connect to master. Received unexpected response: {:?}",
                            parsed_request
                        );
                        panic!("Failed to connect to master");
                    }
                }
            }
            Err(e) => {
                event!(
                    Level::ERROR,
                    "Failed to connect to master. Couldn't parse response: {:?}",
                    e
                );
                panic!("Failed to connect to master");
            }
        },
        Err(e) => {
            event!(
                Level::ERROR,
                "Failed to read from socket: {:?} {:?}",
                stream,
                e
            );
            panic!("Failed to connect to master");
        }
    }
}
