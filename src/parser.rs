use std::fmt;
use std::str;

pub static ERR_UNKNOWN_COMMAND: &str = "Invalid request: command not found";
pub static ERR_NOT_ENOUGH_ARGS: &str = "Invalid request: not enough arguments";
pub static ERR_INVALID_ARGS: &str = "Invalid request: invalid command arguments";
pub static ERR_INVALID_SEQUENCE: &str = "Invalid request: invalid UTF-8 sequence";
pub static ERR_SOCKET_READ: &str = "Internal error: could not read from socket";
pub static ERR_UNKNOWN: &str = "Invalid request: invalid UTF-8 sequence";
pub static ERR_NO_PARTITION: &str = "Internal error: no partition found";
pub static ERR_UNSUPPORTED_MASTER: &str =
    "Internal error: this command is not supported on this node (master)";
pub static ERR_UNSUPPORTED_PARTITION: &str =
    "Internal error: this command is not supported on this node (partition)";

#[derive(Debug, Clone)]
pub struct Error {
    pub code: ErrorCode,
    pub msg: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidRequestCmd = 1,
    NotEnoughArgs = 2,
    InvalidRequestArg = 3,
    InvalidSequence = 4,
    FailedSocketRead = 5,
    NoPartition = 6,
    UnsupportedCommandMaster = 7,
    UnsupportedCommandPartition = 8,
    Unknown = 9,
}

impl ErrorCode {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => ErrorCode::InvalidRequestCmd,
            2 => ErrorCode::NotEnoughArgs,
            3 => ErrorCode::InvalidRequestArg,
            4 => ErrorCode::InvalidSequence,
            5 => ErrorCode::FailedSocketRead,
            6 => ErrorCode::NoPartition,
            7 => ErrorCode::UnsupportedCommandMaster,
            8 => ErrorCode::UnsupportedCommandPartition,
            _ => ErrorCode::Unknown,
        }
    }
    pub fn to_u8(&self) -> u8 {
        match self {
            ErrorCode::InvalidRequestCmd => 1,
            ErrorCode::NotEnoughArgs => 2,
            ErrorCode::InvalidRequestArg => 3,
            ErrorCode::InvalidSequence => 4,
            ErrorCode::FailedSocketRead => 5,
            ErrorCode::NoPartition => 6,
            ErrorCode::UnsupportedCommandMaster => 7,
            ErrorCode::UnsupportedCommandPartition => 8,
            ErrorCode::Unknown => 9,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error {
    pub fn from_code(code: ErrorCode) -> Self {
        let msg = match code {
            ErrorCode::InvalidRequestCmd => ERR_UNKNOWN_COMMAND.to_string(),
            ErrorCode::NotEnoughArgs => ERR_NOT_ENOUGH_ARGS.to_string(),
            ErrorCode::InvalidRequestArg => ERR_INVALID_ARGS.to_string(),
            ErrorCode::InvalidSequence => ERR_INVALID_SEQUENCE.to_string(),
            ErrorCode::FailedSocketRead => ERR_SOCKET_READ.to_string(),
            ErrorCode::NoPartition => ERR_NO_PARTITION.to_string(),
            ErrorCode::UnsupportedCommandMaster => ERR_UNSUPPORTED_MASTER.to_string(),
            ErrorCode::UnsupportedCommandPartition => ERR_UNSUPPORTED_PARTITION.to_string(),
            ErrorCode::Unknown => ERR_UNKNOWN.to_string(),
        };

        Error { code, msg }
    }
    pub fn from_u8(value: u8) -> Self {
        Self::from_code(ErrorCode::from_u8(value))
    }
}

// Add CommandType enum
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandType {
    Get,
    Delete,
    Set,
    Notify,
    ListPartitions,
    LSD,
    Hit,
    Miss,
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
        CommandType::Get | CommandType::Set | CommandType::Delete => extract_key(&parts),
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
            "LSD" => Ok(CommandType::LSD),
            "HIT" => Ok(CommandType::Hit),
            "MSS" => Ok(CommandType::Miss),
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

pub fn build_miss_response(key: &str) -> Vec<u8> {
    format!("MSS {}\0", key).into_bytes()
}

pub fn build_ok_response() -> Vec<u8> {
    "OK\0".to_string().into_bytes()
}

pub fn build_error_response(err: &Error) -> Vec<u8> {
    format!("ERR {} {}\0", err.code.to_u8(), err.msg).into_bytes()
}
