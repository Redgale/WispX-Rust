use std::{
	io,
	marker::PhantomData,
	pin::Pin,
	task::{ready, Context, Poll},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use pin_project::pin_project;

use crate::{packet::CloseReason, ws::Payload, WispError, DATA_HEADROOM};

use super::{MuxStream, MuxStreamRead, MuxStreamWrite, StreamRef};

fn to_io(err: WispError) -> io::Error {
	match err {
		WispError::StreamAlreadyClosed => io::Error::new(io::ErrorKind::BrokenPipe, err),
		err => io::Error::other(err),
	}
}

/// [`AsyncRead`] + [`AsyncBufRead`] over a multiplexor stream.
pub struct MuxStreamAsyncRead<W> {
	inner: StreamRef,
	chunk: Payload,
	_phantom: PhantomData<fn() -> W>,
}

impl<W> MuxStreamAsyncRead<W> {
	pub(crate) fn new(inner: MuxStreamRead<W>) -> Self {
		Self {
			inner: inner.inner,
			chunk: Bytes::new(),
			_phantom: PhantomData,
		}
	}

	/// Get the stream's close reason, if it was closed.
	pub fn get_close_reason(&self) -> Option<CloseReason> {
		self.inner.close_reason()
	}
}

impl<W> AsyncBufRead for MuxStreamAsyncRead<W> {
	fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
		let this = self.get_mut();
		while this.chunk.is_empty() {
			match ready!(this.inner.poll_recv(cx)) {
				Some(chunk) => this.chunk = chunk,
				None => break,
			}
		}
		Poll::Ready(Ok(&this.chunk))
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		self.get_mut().chunk.advance(amt);
	}
}

impl<W> AsyncRead for MuxStreamAsyncRead<W> {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		if buf.is_empty() {
			return Poll::Ready(Ok(0));
		}
		let chunk = ready!(self.as_mut().poll_fill_buf(cx))?;
		let len = chunk.len().min(buf.len());
		buf[..len].copy_from_slice(&chunk[..len]);
		self.consume(len);
		Poll::Ready(Ok(len))
	}

	fn poll_read_vectored(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &mut [io::IoSliceMut<'_>],
	) -> Poll<io::Result<usize>> {
		let mut chunk = ready!(self.as_mut().poll_fill_buf(cx))?;
		let mut total = 0;
		for buf in bufs {
			if chunk.is_empty() {
				break;
			}
			let len = chunk.len().min(buf.len());
			buf[..len].copy_from_slice(&chunk[..len]);
			chunk = &chunk[len..];
			total += len;
		}
		self.consume(total);
		Poll::Ready(Ok(total))
	}
}

#[cfg(feature = "embedded-io-async")]
const _: () = {
	use embedded_io_async::ErrorType;

	impl<W> ErrorType for MuxStreamAsyncRead<W> {
		type Error = std::io::Error;
	}
	impl<W> embedded_io_async::Read for MuxStreamAsyncRead<W> {
		async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
			use futures::AsyncReadExt;

			AsyncReadExt::read(self, buf).await
		}
	}
};

/// [`AsyncWrite`] over a multiplexor stream.
///
/// Every write is copied into one frame. Closing half-closes the stream (WispX) or closes it
/// (v1/v2).
pub struct MuxStreamAsyncWrite<W> {
	inner: StreamRef,
	_phantom: PhantomData<fn() -> W>,
}

impl<W> MuxStreamAsyncWrite<W> {
	pub(crate) fn new(inner: MuxStreamWrite<W>) -> Self {
		Self {
			inner: inner.inner,
			_phantom: PhantomData,
		}
	}

	/// Get the stream's close reason, if it was closed.
	pub fn get_close_reason(&self) -> Option<CloseReason> {
		self.inner.close_reason()
	}

	/// Copy up to `len` bytes (via `copy`) into a frame buffer with headroom, so this copy is
	/// the only one: the writer sends the buffer as-is.
	fn poll_write_with(
		&mut self,
		cx: &mut Context<'_>,
		len: usize,
		copy: impl FnOnce(&mut BytesMut, usize),
	) -> Poll<io::Result<usize>> {
		if len == 0 {
			return Poll::Ready(Ok(0));
		}
		let mux = &self.inner.mux;
		let stream = &self.inner.stream;
		ready!(mux.poll_send_ready(stream, cx)).map_err(to_io)?;

		let len = len.min(mux.send_limit(stream));
		let mut buf = BytesMut::with_capacity(DATA_HEADROOM + len);
		buf.put_bytes(0, DATA_HEADROOM);
		copy(&mut buf, len);
		let rest = mux
			.send_data_with_headroom(stream, buf)
			.map_err(to_io)?;
		debug_assert!(rest.is_empty());
		Poll::Ready(Ok(len))
	}
}

impl<W> AsyncWrite for MuxStreamAsyncWrite<W> {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		self.get_mut()
			.poll_write_with(cx, buf.len(), |out, len| out.extend_from_slice(&buf[..len]))
	}

	fn poll_write_vectored(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &[io::IoSlice<'_>],
	) -> Poll<io::Result<usize>> {
		let total = bufs.iter().map(|x| x.len()).sum();
		self.get_mut().poll_write_with(cx, total, |out, len| {
			let mut remaining = len;
			for buf in bufs {
				if remaining == 0 {
					break;
				}
				let take = buf.len().min(remaining);
				out.extend_from_slice(&buf[..take]);
				remaining -= take;
			}
		})
	}

	fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
		// writes go straight to the multiplexor's queue
		Poll::Ready(Ok(()))
	}

	fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
		Poll::Ready(self.inner.finish().map_err(to_io))
	}
}

#[cfg(feature = "embedded-io-async")]
const _: () = {
	use embedded_io_async::ErrorType;

	impl<W> ErrorType for MuxStreamAsyncWrite<W> {
		type Error = std::io::Error;
	}
	impl<W> embedded_io_async::Write for MuxStreamAsyncWrite<W> {
		async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
			use futures::AsyncWriteExt;

			AsyncWriteExt::write(self, buf).await
		}
	}
};

/// [`AsyncRead`] + [`AsyncBufRead`] + [`AsyncWrite`] over a multiplexor stream.
#[pin_project]
pub struct MuxStreamAsyncRW<W> {
	#[pin]
	read: MuxStreamAsyncRead<W>,
	#[pin]
	write: MuxStreamAsyncWrite<W>,
}

impl<W> MuxStreamAsyncRW<W> {
	pub(crate) fn new(old: MuxStream<W>) -> Self {
		let (read, write) = old.into_split();
		Self {
			read: MuxStreamAsyncRead::new(read),
			write: MuxStreamAsyncWrite::new(write),
		}
	}

	pub fn into_split(self) -> (MuxStreamAsyncRead<W>, MuxStreamAsyncWrite<W>) {
		(self.read, self.write)
	}

	/// Get the stream's close reason, if it was closed.
	pub fn get_close_reason(&self) -> Option<CloseReason> {
		self.write.get_close_reason()
	}
}

impl<W> AsyncRead for MuxStreamAsyncRW<W> {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		self.project().read.poll_read(cx, buf)
	}

	fn poll_read_vectored(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &mut [io::IoSliceMut<'_>],
	) -> Poll<io::Result<usize>> {
		self.project().read.poll_read_vectored(cx, bufs)
	}
}

impl<W> AsyncBufRead for MuxStreamAsyncRW<W> {
	fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
		self.project().read.poll_fill_buf(cx)
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		self.project().read.consume(amt);
	}
}

impl<W> AsyncWrite for MuxStreamAsyncRW<W> {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		self.project().write.poll_write(cx, buf)
	}

	fn poll_write_vectored(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &[io::IoSlice<'_>],
	) -> Poll<io::Result<usize>> {
		self.project().write.poll_write_vectored(cx, bufs)
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.project().write.poll_flush(cx)
	}

	fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.project().write.poll_close(cx)
	}
}

#[cfg(feature = "embedded-io-async")]
const _: () = {
	use embedded_io_async::ErrorType;

	impl<W> ErrorType for MuxStreamAsyncRW<W> {
		type Error = std::io::Error;
	}
	impl<W> embedded_io_async::Read for MuxStreamAsyncRW<W> {
		async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
			use futures::AsyncReadExt;

			AsyncReadExt::read(self, buf).await
		}
	}
	impl<W> embedded_io_async::Write for MuxStreamAsyncRW<W> {
		async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
			use futures::AsyncWriteExt;

			AsyncWriteExt::write(self, buf).await
		}
	}
};
