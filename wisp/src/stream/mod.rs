use std::{
	future::poll_fn,
	marker::PhantomData,
	pin::Pin,
	sync::Arc,
	task::{ready, Context, Poll},
};

use futures::{Sink, Stream};

use crate::{
	mux::shared::{MuxShared, StreamShared},
	packet::{CloseReason, StreamType},
	ws::Payload,
	WispError,
};

mod compat;
mod handles;
pub use compat::*;
pub use handles::*;

/// A counted reference to a stream. The stream is closed when the last one is dropped.
pub(crate) struct StreamRef {
	pub mux: Arc<MuxShared>,
	pub stream: Arc<StreamShared>,
}

impl StreamRef {
	pub fn new(mux: Arc<MuxShared>, stream: Arc<StreamShared>) -> Self {
		mux.acquire_stream_handle(&stream);
		Self { mux, stream }
	}

	pub fn id(&self) -> u32 {
		self.stream.id
	}

	pub fn close_reason(&self) -> Option<CloseReason> {
		self.mux.close_reason(&self.stream)
	}

	pub fn close(&self, reason: CloseReason) -> Result<(), WispError> {
		self.mux.close_stream(&self.stream, reason)
	}

	pub fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<Payload>> {
		self.mux.poll_recv(&self.stream, cx)
	}

	/// Send everything in `pending`, waiting for credit as needed.
	pub fn poll_send_all(
		&self,
		cx: &mut Context<'_>,
		pending: &mut Option<Payload>,
	) -> Poll<Result<(), WispError>> {
		while pending.is_some() {
			if let Err(err) = ready!(self.mux.poll_send_ready(&self.stream, cx)) {
				return Poll::Ready(Err(err));
			}
			let Some(data) = pending.take() else {
				break;
			};
			match self.mux.send_data(&self.stream, data) {
				Ok(rest) if rest.is_empty() => {}
				Ok(rest) => *pending = Some(rest),
				Err(err) => return Poll::Ready(Err(err)),
			}
		}
		Poll::Ready(Ok(()))
	}

	pub fn finish(&self) -> Result<(), WispError> {
		match self.mux.finish_stream(&self.stream) {
			// shutting down an already closed stream is not an error
			Err(WispError::StreamAlreadyClosed) => Ok(()),
			x => x,
		}
	}
}

impl Clone for StreamRef {
	fn clone(&self) -> Self {
		Self::new(self.mux.clone(), self.stream.clone())
	}
}

impl Drop for StreamRef {
	fn drop(&mut self) {
		self.mux.release_stream_handle(&self.stream);
	}
}

/// Read half of a multiplexor stream.
pub struct MuxStreamRead<W> {
	pub(crate) inner: StreamRef,
	_phantom: PhantomData<fn() -> W>,
}

impl<W> MuxStreamRead<W> {
	pub(crate) fn new(inner: StreamRef) -> Self {
		Self {
			inner,
			_phantom: PhantomData,
		}
	}

	pub fn get_stream_id(&self) -> u32 {
		self.inner.id()
	}

	pub fn get_stream_type(&self) -> StreamType {
		self.inner.stream.stream_type
	}

	/// The reason the stream was closed, if it was.
	pub fn get_close_reason(&self) -> Option<CloseReason> {
		self.inner.close_reason()
	}

	pub fn into_async_read(self) -> MuxStreamAsyncRead<W> {
		MuxStreamAsyncRead::new(self)
	}
}

impl<W> Stream for MuxStreamRead<W> {
	type Item = Result<Payload, WispError>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.inner.poll_recv(cx).map(|x| x.map(Ok))
	}
}

/// Write half of a multiplexor stream.
pub struct MuxStreamWrite<W> {
	pub(crate) inner: StreamRef,
	pending: Option<Payload>,
	_phantom: PhantomData<fn() -> W>,
}

impl<W> MuxStreamWrite<W> {
	pub(crate) fn new(inner: StreamRef) -> Self {
		Self {
			inner,
			pending: None,
			_phantom: PhantomData,
		}
	}

	pub fn get_stream_id(&self) -> u32 {
		self.inner.id()
	}

	pub fn get_stream_type(&self) -> StreamType {
		self.inner.stream.stream_type
	}

	/// The reason the stream was closed, if it was.
	pub fn get_close_reason(&self) -> Option<CloseReason> {
		self.inner.close_reason()
	}

	pub fn get_close_handle(&self) -> MuxStreamCloser<W> {
		MuxStreamCloser::new(self.inner.clone())
	}

	pub fn get_protocol_extension_stream(&self) -> MuxProtocolExtensionStream<W> {
		MuxProtocolExtensionStream::new(self.inner.clone())
	}

	/// Close the stream. You will no longer be able to write or read after this has been called.
	pub async fn close(&self, reason: CloseReason) -> Result<(), WispError> {
		self.inner.close(reason)
	}

	/// Half-close the stream: send any buffered data, then tell the peer no more data is coming.
	/// Reading continues to work. On Wisp v1/v2, which have no half-close, this closes the stream.
	pub async fn finish(&mut self) -> Result<(), WispError> {
		poll_fn(|cx| self.inner.poll_send_all(cx, &mut self.pending)).await?;
		self.inner.finish()
	}

	pub fn into_async_write(self) -> MuxStreamAsyncWrite<W> {
		MuxStreamAsyncWrite::new(self)
	}
}

impl<W> Sink<Payload> for MuxStreamWrite<W> {
	type Error = WispError;

	fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		let this = self.get_mut();
		ready!(this.inner.poll_send_all(cx, &mut this.pending))?;
		// report a closed stream before the caller hands us data
		this.inner.mux.poll_send_ready(&this.inner.stream, cx)
	}

	fn start_send(self: Pin<&mut Self>, item: Payload) -> Result<(), Self::Error> {
		let this = self.get_mut();
		debug_assert!(this.pending.is_none());
		if !item.is_empty() {
			// send what we can right away, so `feed` doesn't leave data sitting here
			let rest = this.inner.mux.send_data(&this.inner.stream, item)?;
			if !rest.is_empty() {
				this.pending = Some(rest);
			}
		}
		Ok(())
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		let this = self.get_mut();
		this.inner.poll_send_all(cx, &mut this.pending)
	}

	/// Half-closes the stream (WispX) or closes it (v1/v2) after sending buffered data.
	fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		let this = self.get_mut();
		match ready!(this.inner.poll_send_all(cx, &mut this.pending)) {
			Ok(()) | Err(WispError::StreamAlreadyClosed) => {}
			Err(err) => return Poll::Ready(Err(err)),
		}
		Poll::Ready(this.inner.finish())
	}
}

/// A multiplexor stream.
pub struct MuxStream<W> {
	read: MuxStreamRead<W>,
	write: MuxStreamWrite<W>,
}

impl<W> MuxStream<W> {
	pub(crate) fn new(mux: Arc<MuxShared>, stream: Arc<StreamShared>) -> Self {
		let inner = StreamRef::new(mux, stream);
		Self {
			read: MuxStreamRead::new(inner.clone()),
			write: MuxStreamWrite::new(inner),
		}
	}

	pub fn get_stream_id(&self) -> u32 {
		self.read.get_stream_id()
	}

	pub fn get_stream_type(&self) -> StreamType {
		self.read.get_stream_type()
	}

	/// The reason the stream was closed, if it was.
	pub fn get_close_reason(&self) -> Option<CloseReason> {
		self.read.get_close_reason()
	}

	pub fn get_close_handle(&self) -> MuxStreamCloser<W> {
		self.write.get_close_handle()
	}

	pub fn get_protocol_extension_stream(&self) -> MuxProtocolExtensionStream<W> {
		self.write.get_protocol_extension_stream()
	}

	/// Close the stream. You will no longer be able to write or read after this has been called.
	pub async fn close(&self, reason: CloseReason) -> Result<(), WispError> {
		self.write.close(reason).await
	}

	pub fn into_async_rw(self) -> MuxStreamAsyncRW<W> {
		MuxStreamAsyncRW::new(self)
	}

	pub fn into_split(self) -> (MuxStreamRead<W>, MuxStreamWrite<W>) {
		(self.read, self.write)
	}
}

impl<W> Stream for MuxStream<W> {
	type Item = <MuxStreamRead<W> as Stream>::Item;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		Pin::new(&mut self.read).poll_next(cx)
	}
}

impl<W> Sink<Payload> for MuxStream<W> {
	type Error = <MuxStreamWrite<W> as Sink<Payload>>::Error;

	fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Pin::new(&mut self.write).poll_ready(cx)
	}

	fn start_send(mut self: Pin<&mut Self>, item: Payload) -> Result<(), Self::Error> {
		Pin::new(&mut self.write).start_send(item)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Pin::new(&mut self.write).poll_flush(cx)
	}

	fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Pin::new(&mut self.write).poll_close(cx)
	}
}
