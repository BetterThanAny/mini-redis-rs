pub mod encoder;
pub mod parser;

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    NullArray,
    Array(Vec<Frame>),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("incomplete frame")]
    Incomplete,
    #[error("protocol error: {0}")]
    Protocol(String),
}
