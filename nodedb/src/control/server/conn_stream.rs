//! Polymorphic TCP/TLS connection stream for server sessions.
//!
//! Wraps plain TCP and TLS-encrypted streams behind a single type
//! implementing `AsyncRead + AsyncWrite`.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// A connection stream — either plain TCP or TLS-wrapped.
pub enum ConnStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl ConnStream {
    pub fn plain(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }

    pub fn tls(stream: tokio_rustls::server::TlsStream<TcpStream>) -> Self {
        Self::Tls(Box::new(stream))
    }
}

impl AsyncRead for ConnStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ConnStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}
