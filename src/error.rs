use std::{error::Error, fmt::Display};

/// Just an error message from Bufcli
#[derive(Debug)]
pub struct BufcliError {
    msg: &'static str,
}

impl Display for BufcliError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.msg)
    }
}

impl Error for BufcliError {}

impl BufcliError {
    pub fn new(msg: &'static str) -> Self {
        BufcliError { msg }
    }
}
