use crate::error::Error;
use crate::error::ErrorCode;
use lru::LruCache;
use std::str;

// Add CommandType enum
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandType {
    Get,
    Delete,
    Set,
    Notify,
    ListPartitions,
    Lsd,
    Hit,
    Miss,
    Ack,
    Ok,
    Error,
}
#[derive(Debug, Clone)]
pub struct ParsedRequest {
    pub cmd: CommandType,
    pub key: Option<String>,
    pub value: Option<String>,
    pub error: Option<Error>,
    pub original_rq: String,
}
pub fn parse_request(mut message: Vec<u8>) -> Result<ParsedRequest, Error> {
    if let Some(pos) = message.iter().position(|&c| c == b'\0') {
        message.truncate(pos);
    }

    let buf =
        std::str::from_utf8(&message).map_err(|_| Error::from_code(ErrorCode::InvalidSequence))?;
    let mut parts: Vec<&str> = buf.splitn(3, char::is_whitespace).collect();
    if parts.last() == Some(&"") || parts.last() == Some(&"\n") || parts.last() == Some(&"\0") {
        parts.pop();
    }

    let cmd = extract_cmd(&parts)?;

    let key = match cmd {
        CommandType::Get | CommandType::Set | CommandType::Delete | CommandType::Lsd => {
            extract_key(&parts)
        }
        _ => Ok(None),
    }?;

    let value = match cmd {
        CommandType::Set => extract_value(&parts),
        _ => Ok(None),
    }?;

    let error = match cmd {
        CommandType::Error => extract_error(&parts),
        _ => Ok(None),
    }?;

    Ok(ParsedRequest {
        cmd,
        key,
        value,
        error,
        original_rq: buf.to_string(),
    })
}

fn extract_error(parts: &Vec<&str>) -> Result<Option<Error>, Error> {
    if parts.len() < 3 {
        Err(Error::from_code(ErrorCode::NotEnoughArgs))
    } else {
        Ok(Some(Error::from_u8(parts[1].parse().unwrap())))
    }
}

fn extract_cmd(parts: &Vec<&str>) -> Result<CommandType, Error> {
    if parts.is_empty() {
        Err(Error::from_code(ErrorCode::NotEnoughArgs))
    } else {
        match parts[0] {
            "GET" => Ok(CommandType::Get),
            "DEL" => Ok(CommandType::Delete),
            "SET" => Ok(CommandType::Set),
            "NTF" => Ok(CommandType::Notify),
            "LSP" => Ok(CommandType::ListPartitions),
            "LSD" => Ok(CommandType::Lsd),
            "HIT" => Ok(CommandType::Hit),
            "MSS" => Ok(CommandType::Miss),
            "ACK" => Ok(CommandType::Ack),
            "OK" => Ok(CommandType::Ok),
            "ERR" => Ok(CommandType::Error),
            _ => Err(Error::from_code(ErrorCode::InvalidRequestCmd)),
        }
    }
}

fn extract_key(parts: &Vec<&str>) -> Result<Option<String>, Error> {
    if parts.len() < 2 {
        Err(Error::from_code(ErrorCode::NotEnoughArgs))
    } else if parts[0].to_uppercase() == "GET" && parts.len() >= 3 {
        let key = format!("{} {}", parts[1], parts[2]).replace("\\0", "\u{0000}");
        Ok(Some(key))
    } else {
        Ok(Some(parts[1].to_string()))
    }
}

fn extract_value(parts: &Vec<&str>) -> Result<Option<String>, Error> {
    if parts.len() < 3 {
        Err(Error::from_code(ErrorCode::NotEnoughArgs))
    } else {
        Ok(Some(parts[2].to_string()))
    }
}

pub fn build_hit_response(key: &str, value: &str) -> Vec<u8> {
    format!("HIT {} {}\0", key, value).into_bytes()
}

pub fn build_lsd_response(cache: &LruCache<String, String>) -> Vec<u8> {
    let mut s: String = "".to_owned();
    for (key, val) in cache.iter() {
        s.push_str(&format!("Key: {}, Value: {} \n", key, val).to_owned());
    }
    s.into_bytes()
}

pub fn build_lsp_response(partitions_str: String) -> Vec<u8> {
    format!("LSP {}\0", partitions_str).into_bytes()
}

pub fn build_miss_response(key: &str) -> Vec<u8> {
    format!("MSS {}\0", key).into_bytes()
}

pub fn build_ok_response() -> Vec<u8> {
    "OK\0".to_string().into_bytes()
}

pub fn build_error_response(err: &Error) -> Vec<u8> {
    format!("ERR {} {}\0", err.code.to_u8(), err.msg).into_bytes()
}

pub fn build_notify_request() -> Vec<u8> {
    "NTF\0".to_string().into_bytes()
}

pub fn build_ack_response() -> Vec<u8> {
    "ACK\0".to_string().into_bytes()
}
