use std::fmt;

pub static ERR_INVALID_REQUEST_CMD: &str = "Invalid request: command not found";
pub static ERR_NOT_ENOUGH_ARGS: &str = "Invalid request: not enough arguments";
pub static _ERR_INVALID_REQUEST_ARG: &[u8; 42] = b"Invalid request: invalid command arguments";
pub static _ERR_INVALID_REQUEST_FLAG: &[u8; 32] = b"Invalid request: flags not found";
pub static _ERR_INVALID_REQUEST_FORMAT: &[u8; 39] = b"Invalid request: invalid UTF-8 sequence";

#[derive(Debug, Clone)]
pub struct Error {
    pub code: u8,
    pub msg: String,
}
impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Error {}: {}", self.code, self.msg)
    }
}

#[derive(Debug, Clone)]
pub struct ParsedRequest {
    pub cmd: String,
    pub key: Option<String>,
    pub value: Option<String>,
    pub error: Option<Error>,
}
pub fn parse_request(mut message: Vec<u8>) -> Result<ParsedRequest, Error> {
    if let Some(pos) = message.iter().position(|&c| c == b'\0') {
        message.truncate(pos);
    }

    let buf = std::str::from_utf8(&message).map_err(|_| Error {
        code: 1,
        msg: "XD".to_string(),
    })?;
    let parts: Vec<&str> = buf.splitn(3, char::is_whitespace).collect();

    let cmd = extract_cmd(&parts)?;

    let key = match cmd.as_str() {
        "GET" | "SET" | "DEL" => extract_key(&parts),
        _ => Ok(None),
    }?;

    let value = match cmd.as_str() {
        "SET" => extract_value(&parts),
        _ => Ok(None),
    }?;

    let error = match cmd.as_str() {
        "ERR" => extract_error(&parts),
        _ => Ok(None),
    }?;
    Ok(ParsedRequest {
        cmd,
        key,
        value,
        error,
    })
}

fn extract_error(parts: &Vec<&str>) -> Result<Option<Error>, Error> {
    if parts.len() < 3 {
        Err(Error {
            code: 1,
            msg: ERR_NOT_ENOUGH_ARGS.to_string(),
        })
    } else {
        Ok(Some(Error {
            code: parts[1].parse().unwrap(),
            msg: parts[2].to_string(),
        }))
    }
}

fn extract_cmd(parts: &Vec<&str>) -> Result<String, Error> {
    if parts.is_empty() {
        Err(Error {
            code: 1,
            msg: ERR_INVALID_REQUEST_CMD.to_string(),
        })
    } else {
        match parts[0] {
            "GET" | "DEL" | "SET" | "NTF" | "LSP" | "LSD" | "ERR" => Ok(parts[0].to_string()),
            _ => Err(Error {
                code: 1,
                msg: ERR_INVALID_REQUEST_CMD.to_string(),
            }),
        }
    }
}

fn extract_key(parts: &Vec<&str>) -> Result<Option<String>, Error> {
    if parts.len() < 2 {
        Err(Error {
            code: 1,
            msg: ERR_NOT_ENOUGH_ARGS.to_string(),
        })
    } else if parts[0].to_uppercase() == "GET" && parts.len() >= 3 {
        Ok(Some(format!("{} {}", parts[1], parts[2])))
    } else {
        Ok(Some(parts[1].to_string()))
    }
}

fn extract_value(parts: &Vec<&str>) -> Result<Option<String>, Error> {
    if parts.len() < 3 {
        Err(Error {
            code: 1,
            msg: ERR_NOT_ENOUGH_ARGS.to_string(),
        })
    } else {
        Ok(Some(parts[2].to_string()))
    }
}
