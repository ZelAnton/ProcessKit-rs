//! Shared Windows adapter for a channel-backed asynchronous byte reader.
//!
//! The synchronous Windows pipe bridges use a dedicated thread to read their
//! handle and send fixed-size chunks to the async side. This adapter owns the
//! receiving half and presents those chunks as an [`AsyncRead`], keeping the
//! stream contract in one place for both merged pipes and ConPTY output.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;

/// A message produced by a synchronous Windows reader bridge.
#[derive(Debug)]
pub(crate) enum ReaderMessage {
    /// Bytes read from the bridged handle.
    Chunk(Vec<u8>),
    /// A read failure that must be returned to the async caller unchanged.
    Error(io::Error),
}

/// Adapt a bounded mpsc receiver of reader messages to [`AsyncRead`].
///
/// The receiver is intentionally stored directly. `poll_read` has exclusive
/// access to the adapter, and Tokio's receiver is safe to keep in the boxed
/// `Send + Sync` output reader without serializing every poll through a mutex.
/// A chunk is retained until all bytes have been copied, so a small caller
/// buffer cannot lose the remainder before the next channel message arrives.
#[derive(Debug)]
pub(crate) struct ChannelReader {
    rx: mpsc::Receiver<ReaderMessage>,
    leftover: Vec<u8>,
    pos: usize,
}

impl ChannelReader {
    /// Create a reader over the bridge's message receiver.
    pub(crate) fn new(rx: mpsc::Receiver<ReaderMessage>) -> Self {
        Self {
            rx,
            leftover: Vec::new(),
            pos: 0,
        }
    }

    /// Close the receiving side before an owning bridge is cancelled.
    ///
    /// Closing first wakes a bridge thread blocked in `blocking_send`; dropping
    /// the reader also closes the channel, but an outer owner such as the
    /// merge-pipe bridge needs this ordering before it interrupts and waits for
    /// that thread.
    pub(crate) fn close(&mut self) {
        self.rx.close();
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if this.pos < this.leftover.len() {
            let start = this.pos;
            let n = (this.leftover.len() - start).min(buf.remaining());
            buf.put_slice(&this.leftover[start..start + n]);
            this.pos += n;
            return Poll::Ready(Ok(()));
        }

        loop {
            match this.rx.poll_recv(cx) {
                Poll::Ready(Some(ReaderMessage::Chunk(chunk))) => {
                    // Empty chunks are not EOF: only a closed receiver is EOF.
                    // Bridges normally never produce one, but skipping it keeps
                    // the adapter's AsyncRead contract sound for all messages.
                    if chunk.is_empty() {
                        continue;
                    }
                    let n = chunk.len().min(buf.remaining());
                    buf.put_slice(&chunk[..n]);
                    if n < chunk.len() {
                        this.leftover = chunk;
                        this.pos = n;
                    } else {
                        this.leftover.clear();
                        this.pos = 0;
                    }
                    return Poll::Ready(Ok(()));
                }
                // A genuine read error reaches the caller unchanged — never
                // reported as a short read or as EOF.
                Poll::Ready(Some(ReaderMessage::Error(error))) => {
                    return Poll::Ready(Err(error));
                }
                // All bridge senders have gone away: clean end of stream.
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChannelReader, ReaderMessage};
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn preserves_chunk_remainders_and_fifo_order() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(ReaderMessage::Chunk(b"abcdef".to_vec()))
            .await
            .expect("queue first chunk");
        tx.send(ReaderMessage::Chunk(b"ghi".to_vec()))
            .await
            .expect("queue second chunk");
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut buf = [0u8; 2];
        assert_eq!(reader.read(&mut buf).await.expect("read prefix"), 2);
        assert_eq!(&buf, b"ab");
        assert_eq!(reader.read(&mut buf).await.expect("read middle"), 2);
        assert_eq!(&buf, b"cd");
        assert_eq!(reader.read(&mut buf).await.expect("read remainder"), 2);
        assert_eq!(&buf, b"ef");
        assert_eq!(reader.read(&mut buf).await.expect("read next chunk"), 2);
        assert_eq!(&buf, b"gh");
        assert_eq!(reader.read(&mut buf).await.expect("read final byte"), 1);
        assert_eq!(&buf[..1], b"i");
        assert_eq!(reader.read(&mut buf).await.expect("read EOF"), 0);
    }

    #[tokio::test]
    async fn returns_prefix_then_the_original_read_error() {
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(ReaderMessage::Chunk(b"prefix".to_vec()))
            .await
            .expect("queue prefix");
        tx.send(ReaderMessage::Error(std::io::Error::from_raw_os_error(5)))
            .await
            .expect("queue read failure");
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut captured = Vec::new();
        let error = reader
            .read_to_end(&mut captured)
            .await
            .expect_err("a bridge failure must not become EOF");
        assert_eq!(captured, b"prefix");
        assert_eq!(error.raw_os_error(), Some(5));
    }

    #[tokio::test]
    async fn a_closed_channel_is_clean_eof() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        let mut reader = ChannelReader::new(rx);
        let mut buf = [0u8; 1];
        assert_eq!(reader.read(&mut buf).await.expect("read EOF"), 0);
    }

    #[tokio::test]
    async fn dropping_reader_releases_a_blocked_sender() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(ReaderMessage::Chunk(vec![1]))
            .await
            .expect("fill queue");
        let sender = std::thread::spawn(move || {
            tx.blocking_send(ReaderMessage::Chunk(vec![2]))
                .expect_err("a dropped reader must reject blocked sends");
        });

        drop(ChannelReader::new(rx));
        sender.join().expect("blocked sender thread");
    }

    #[test]
    fn reader_keeps_the_output_reader_auto_traits() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ChannelReader>();
    }
}
