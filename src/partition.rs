use bincode::deserialize;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use std::str;
use std::collections::VecDeque;

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd)]
pub struct EntryVal {
    pub value: String,
    next: Option<Box<EntryVal>>,
    prev: Option<Box<EntryVal>>,
}

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

impl EntryVal {
    pub fn new(value: String) -> Self {
        EntryVal { 
            next: None,
            prev: None,
            value 
        }
    }
    pub fn update_value(&mut self, new_value: String) {
        self.value = new_value;
    }
}

pub struct Cache{
    pub map: Db,
    pub capacity: u32,
    pub queue: VecDeque<EntryVal>,
}

impl Cache {
    pub fn new(map: Db, capacity: u32, queue: VecDeque<EntryVal>) -> Self {
        Self { map, capacity, queue}
    }

    pub fn put(&mut self, key: String, new_entry_value: String) {
        let entry = EntryVal::new(new_entry_value);
        let encoded = Bytes::from(bincode::serialize(&entry).unwrap());
        let mut db = self.map.lock().unwrap();
        db.insert(key, encoded);
    }

    pub fn get(&mut self, key: String) -> Option<EntryVal> {
        let db = self.map.lock().unwrap();
        if db.contains_key(&key) {
            let entry_as_bytes: &Bytes = db.get(&key).unwrap();
            let entry: EntryVal = deserialize(&&entry_as_bytes[..]).unwrap();
            Some(entry)
        } else {
            println!("No value found!");
            None
        }
    }
}

#[tokio::main]
pub async fn connect() {
    let port = 7878;
    let addr = format!("localhost:{port}");
    let addr_clone = addr.clone();
    // let db = Arc::new(Mutex::new(HashMap::new()));
    // let queue: VecDeque<EntryVal> = VecDeque::new();
    // let mut cache = Cache::new(db, 20, queue);
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => {
            println!("Connection established on address: {addr_clone}");
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
            let _ = match str::from_utf8(&v) {
                Ok(v) => {
                    println!("String: {}", v);
                    v
                }
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };
            let s = str::from_utf8(&buf.to_vec()).unwrap().to_string();
            let cmd = &s[0..3];
            match cmd {
                "SET" => {
                    stream.write_all(b"OK\n").await.unwrap();
                }
                "GET" => {
                    stream.write_all(b"Here is the data\n").await.unwrap();
                }
                "DELETE" => {
                    stream.write_all(b"Deleted the data\n").await.unwrap();
                }
                _=> {
                    stream.write_all(b"There was an error with the request\n").await.unwrap();
                }
            }
        }
        Err(e) => {
            println!("error: {e}");
        }
    }
    stream.write_all(b"Hello from partition\n").await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_if_value_was_added_to_cache() {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let queue: VecDeque<EntryVal> = VecDeque::new();
        let cache = &mut Cache::new(db, 20, queue);
        cache.put(String::from("Key"), String::from("Value"));
        let res = cache.get(String::from("Key")).unwrap();
        assert_eq!(&res.value, "Value");
    }
}
