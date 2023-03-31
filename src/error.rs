use std::fmt;

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
