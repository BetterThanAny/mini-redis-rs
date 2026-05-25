use crate::resp::{encoder, parser, Frame};
use bytes::BytesMut;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

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
            if self.buf.len() > crate::limits::MAX_BUFFERED_FRAME_BYTES {
                anyhow::bail!(
                    "client frame exceeds buffered limit of {} bytes",
                    crate::limits::MAX_BUFFERED_FRAME_BYTES
                );
            }
            let n = timeout(READ_IDLE_TIMEOUT, self.stream.read_buf(&mut self.buf))
                .await
                .map_err(|_| anyhow::anyhow!("connection read idle timeout"))??;
            if n == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(anyhow::anyhow!("connection closed mid-frame"));
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> anyhow::Result<()> {
        let encoded_len = encoder::encoded_len(frame)
            .ok_or_else(|| anyhow::anyhow!("server response length overflow"))?;
        if encoded_len > crate::limits::MAX_RESPONSE_FRAME_BYTES {
            anyhow::bail!(
                "server response exceeds output limit of {} bytes",
                crate::limits::MAX_RESPONSE_FRAME_BYTES
            );
        }
        self.out.clear();
        encoder::encode(frame, &mut self.out);
        timeout(WRITE_TIMEOUT, self.stream.write_all(&self.out))
            .await
            .map_err(|_| anyhow::anyhow!("connection write timeout"))??;
        timeout(WRITE_TIMEOUT, self.stream.flush())
            .await
            .map_err(|_| anyhow::anyhow!("connection flush timeout"))??;
        Ok(())
    }
}
