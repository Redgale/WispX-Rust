//! The multiplexor future: one read loop and one write loop over the transport.

use std::{
	collections::VecDeque,
	future::poll_fn,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use futures::{future::Either, lock::Mutex, Sink, SinkExt, StreamExt};

use crate::{
	extensions::AnyProtocolExtension,
	frame::{
		decode_message, encode_batch, InFrame, OutFrame, Proto, TY_CLOSE, TY_CONNECT, TY_CONTINUE,
		TY_DATA, TY_FIN, TY_INFO,
	},
	packet::{CloseReason, ConnectPacket},
	ws::{Payload, TransportRead, TransportWrite},
	Role, WispError,
};

use super::shared::{MuxShared, StreamShared, WriterEvent};

/// Stop pulling more frames into one flush after this many bytes, so a busy connection still
/// flushes regularly.
const MAX_BYTES_PER_FLUSH: usize = 1024 * 1024;

/// Returns `Pending` once, so the other half of the driver gets polled.
///
/// The read and write loops share one task. Without this, a constantly readable transport keeps
/// the read loop busy until the runtime's budget runs out, and the frames it queues (credit
/// updates, replies) wait behind megabytes of reading. The write loop doesn't need it: it
/// returns `Pending` whenever the queue drains or the transport is full. (Yielding in the write
/// loop to collect bigger batches was measured: it costs 5-10% on request/response traffic.)
async fn yield_now() {
	let mut yielded = false;
	poll_fn(|cx| {
		if yielded {
			Poll::Ready(())
		} else {
			yielded = true;
			cx.waker().wake_by_ref();
			Poll::Pending
		}
	})
	.await;
}

/// Runs the connection until the transport closes, an error occurs, or a close is requested.
pub(crate) async fn run<R: TransportRead, W: TransportWrite>(
	shared: Arc<MuxShared>,
	rx: R,
	tx: Arc<Mutex<W>>,
	extensions: Vec<AnyProtocolExtension>,
	initial: Option<Bytes>,
) -> Result<(), WispError> {
	struct ShutdownGuard(Arc<MuxShared>);
	impl Drop for ShutdownGuard {
		fn drop(&mut self) {
			self.0.shutdown();
		}
	}
	let _guard = ShutdownGuard(shared.clone());

	let read = Box::pin(read_loop(shared.clone(), rx, extensions, initial));
	let write = Box::pin(write_loop(shared.clone(), tx.clone()));

	let ret = match futures::future::select(read, write).await {
		Either::Left((ret, _)) | Either::Right((ret, _)) => ret,
	};

	shared.shutdown();
	let _ = tx.lock().await.close().await;

	ret
}

async fn write_loop<W: TransportWrite>(
	shared: Arc<MuxShared>,
	tx: Arc<Mutex<W>>,
) -> Result<(), WispError> {
	let proto = shared.params.proto;
	let batch_size = shared.params.batch_size;

	let mut frames = VecDeque::new();
	let mut blocked = Vec::new();
	let mut messages = Vec::new();

	loop {
		let event = poll_fn(|cx| shared.poll_writer(cx, &mut frames, &mut blocked)).await;
		blocked.drain(..).for_each(std::task::Waker::wake);

		match event {
			WriterEvent::Frames => {
				let mut tx = tx.lock().await;
				let mut written = 0;
				loop {
					encode_batch(proto, &mut frames, batch_size, &mut messages);
					for message in messages.drain(..) {
						written += message.len();
						tx.feed(message).await?;
					}
					// pick up anything queued while we were writing, so it shares the flush
					if written >= MAX_BYTES_PER_FLUSH
						|| !shared.try_take_frames(&mut frames, &mut blocked)
					{
						break;
					}
					blocked.drain(..).for_each(std::task::Waker::wake);
				}
				tx.flush().await?;
			}
			WriterEvent::Close(reason) => {
				let mut tx = tx.lock().await;
				if let Some(reason) = reason {
					tx.feed(OutFrame::close(0, reason).encode(proto)).await?;
				}
				let _ = tx.close().await;
				return Ok(());
			}
			WriterEvent::Dead => return Ok(()),
		}
	}
}

async fn read_loop<R: TransportRead>(
	shared: Arc<MuxShared>,
	mut rx: R,
	mut extensions: Vec<AnyProtocolExtension>,
	initial: Option<Bytes>,
) -> Result<(), WispError> {
	let proto = shared.params.proto;
	let mut frames = Vec::new();
	let mut last_connect_id = 0u32;
	let mut stream_cache = None;

	let mut next = initial;
	loop {
		let message = match next.take() {
			Some(x) => x,
			None => match rx.next().await {
				Some(x) => x?,
				None => return Ok(()),
			},
		};

		decode_message(proto, message, &mut frames)?;
		for frame in frames.drain(..) {
			if handle_frame(
				&shared,
				frame,
				&mut last_connect_id,
				&mut stream_cache,
				&mut extensions,
				&mut rx,
			)
			.await?
			{
				return Ok(());
			}
		}

		// let the write loop send what we just queued (credit, replies) before reading more
		if shared.has_queued_frames() {
			yield_now().await;
		}
	}
}

/// Returns true if the connection should stop.
async fn handle_frame<R: TransportRead>(
	shared: &Arc<MuxShared>,
	frame: InFrame,
	last_connect_id: &mut u32,
	stream_cache: &mut Option<Arc<StreamShared>>,
	extensions: &mut [AnyProtocolExtension],
	rx: &mut R,
) -> Result<bool, WispError> {
	let InFrame {
		ty,
		id,
		mut payload,
	} = frame;
	let proto = shared.params.proto;

	match ty {
		TY_DATA => {
			if let Some(stream) = shared.on_data(id, payload, stream_cache) {
				// a v1/v2 client is sending faster than the stream is read: stop reading the
				// connection (TCP backpressure) until it catches up
				poll_fn(|cx| shared.poll_drained(&stream, cx)).await;
			}
		}
		TY_CONTINUE => {
			if payload.len() >= 4 {
				shared.on_credit(id, payload.get_u32_le());
			}
		}
		TY_CLOSE => {
			let reason = if payload.is_empty() {
				CloseReason::Unknown
			} else {
				CloseReason::from(payload.get_u8())
			};
			if id == 0 {
				return match reason {
					CloseReason::Voluntary | CloseReason::Unknown => Ok(true),
					reason => Err(WispError::ClosedByPeer(reason)),
				};
			}
			shared.on_close(id, reason);
		}
		TY_CONNECT if shared.params.role == Role::Server => {
			// WispX stream IDs must strictly increase, which also rules out reuse
			let id_ok = id != 0 && (proto != Proto::X || id > *last_connect_id);
			let accepted = id_ok
				&& match ConnectPacket::decode_payload(payload) {
					Ok(connect) => shared.accept_stream(id, connect),
					Err(_) => false,
				};
			if accepted {
				*last_connect_id = id;
			} else if id != 0 {
				let _ = shared.enqueue(OutFrame::close(id, CloseReason::ServerStreamInvalidInfo));
			}
		}
		TY_FIN if proto == Proto::X => shared.on_fin(id),
		TY_INFO => {}
		_ => {
			if let Some(extension) = extensions
				.iter_mut()
				.find(|x| x.get_supported_packets().contains(&ty))
			{
				let mut sink = ExtensionSink {
					shared: shared.clone(),
				};
				extension.handle_packet(ty, payload, rx, &mut sink).await?;
			}
			// unknown frame types are ignored
		}
	}

	Ok(false)
}

/// Lets protocol extensions send packets through the writer queue.
///
/// Extensions write v2-layout packets (`type`, `stream id`, `payload`). The writer re-frames
/// them for the connection's protocol.
struct ExtensionSink {
	shared: Arc<MuxShared>,
}

impl Sink<Payload> for ExtensionSink {
	type Error = WispError;

	fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn start_send(self: Pin<&mut Self>, mut item: Payload) -> Result<(), Self::Error> {
		if item.len() < 5 {
			return Err(WispError::PacketTooSmall);
		}
		let ty = item.get_u8();
		let id = item.get_u32_le();
		self.shared.enqueue(OutFrame::raw(ty, id, item))
	}

	fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}
}
