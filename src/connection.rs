use crate::resp::{encoder, parser, Frame};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    stream: TcpStream,
    buf: BytesMut,
    out: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
            out: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        loop {
            if let Some(frame) = parser::parse(&mut self.buf)? {
                return Ok(Some(frame));
            }
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(anyhow::anyhow!("connection closed mid-frame"));
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> anyhow::Result<()> {
        self.out.clear();
        encoder::encode(frame, &mut self.out);
        self.stream.write_all(&self.out).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
