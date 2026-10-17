use std::{future::poll_fn, marker::PhantomData};

use bytes::Bytes;

use crate::{packet::CloseReason, ws::PayloadRef, WispError};

use super::StreamRef;

/// Close handle for a multiplexor stream. Keeps the stream open while it exists.
pub struct MuxStreamCloser<W> {
	pub(crate) inner: StreamRef,
	_phantom: PhantomData<fn() -> W>,
}

impl<W> Clone for MuxStreamCloser<W> {
	fn clone(&self) -> Self {
		Self::new(self.inner.clone())
	}
}

impl<W> MuxStreamCloser<W> {
	pub(crate) fn new(inner: StreamRef) -> Self {
		Self {
			inner,
			_phantom: PhantomData,
		}
	}

	/// Close the stream. You will no longer be able to write or read after this has been called.
	pub async fn close(&self, reason: CloseReason) -> Result<(), WispError> {
		self.inner.close(reason)
	}

	/// Get the stream's close reason, if it was closed.
	pub fn get_close_reason(&self) -> Option<CloseReason> {
		self.inner.close_reason()
	}

	/// Wait until the stream is aborted: a CLOSE was sent or received, or the multiplexor
	/// stopped. Graceful half-closes (FIN) do not resolve this.
	pub async fn closed(&self) -> CloseReason {
		poll_fn(|cx| self.inner.mux.poll_closed(&self.inner.stream, cx)).await
	}
}

/// Stream for sending arbitrary protocol extension packets.
pub struct MuxProtocolExtensionStream<W> {
	pub(crate) inner: StreamRef,
	_phantom: PhantomData<fn() -> W>,
}

impl<W> MuxProtocolExtensionStream<W> {
	pub(crate) fn new(inner: StreamRef) -> Self {
		Self {
			inner,
			_phantom: PhantomData,
		}
	}

	/// Send a protocol extension packet with this stream's ID.
	pub async fn send(&mut self, packet_type: u8, data: PayloadRef<'_>) -> Result<(), WispError> {
		let data: Bytes = data.into_owned();
		self.inner
			.mux
			.send_raw(&self.inner.stream, packet_type, data)
	}
}
