mod client;
mod driver;
pub(crate) mod shared;
mod server;

use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use futures::{lock::Mutex, SinkExt};

pub use client::ClientImpl;
pub use server::ServerImpl;

use crate::{
	extensions::{udp::UdpProtocolExtension, AnyProtocolExtension, AnyProtocolExtensionBuilder},
	frame::{OutFrame, Proto, TY_INFO, X_HEADER_LEN},
	packet::{CloseReason, ConnectPacket, InfoPacket, Packet, PacketType},
	stream::MuxStream,
	ws::{TransportRead, TransportWrite},
	Role, WispError, WISPX_MIN_MESSAGE_SIZE, WISP_VERSION,
};

use shared::{MuxParams, MuxShared, StreamShared};

pub type ServerMux<W> = Multiplexor<ServerImpl<W>, W>;
pub type ClientMux<W> = Multiplexor<ClientImpl, W>;

/// Exclusive access to the write half of the transport, from [`Multiplexor::lock_ws`].
pub type LockedWebSocketWriteGuard<W> = futures::lock::OwnedMutexGuard<W>;

/// Default initial per-stream receive window for WispX connections.
pub const DEFAULT_WISPX_STREAM_WINDOW: u32 = 256 * 1024;
/// Default maximum per-stream receive window for WispX connections. Windows grow up to this
/// while the stream's reader keeps up. 64 MiB keeps a single stream above 1 GiB/s at 50 ms RTT.
pub const DEFAULT_WISPX_MAX_STREAM_WINDOW: u32 = 64 * 1024 * 1024;
/// Default limit on window growth across all streams of one WispX connection, which bounds how
/// much unread data a peer can make us hold.
pub const DEFAULT_WISPX_MAX_CONNECTION_WINDOW: u64 = 256 * 1024 * 1024;
/// Default largest incoming websocket message for WispX connections.
pub const DEFAULT_WISPX_MAX_MESSAGE_SIZE: u32 = 1024 * 1024;
/// Default cap on coalesced outgoing messages for WispX connections.
pub const DEFAULT_WISPX_MAX_BATCH_SIZE: usize = 64 * 1024;

/// Queued bytes in the writer above which data senders wait.
const OUT_HIGH_WATERMARK: usize = 1024 * 1024;
/// Largest DATA payload a v1/v2 client sends. The reference server rejects websocket messages
/// over 64 KiB.
const V2_CLIENT_MAX_FRAME_PAYLOAD: usize = 64 * 1024 - 5;
/// Largest DATA payload a v1/v2 server sends.
const V2_SERVER_MAX_FRAME_PAYLOAD: usize = 1024 * 1024;

/// Wire protocol spoken on a connection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WispProtocolVersion {
	/// Wisp v1.
	V1,
	/// Wisp v2.
	V2,
	/// WispX (Wisp v3 draft), see `protocolX.md`.
	X,
}

impl From<Proto> for WispProtocolVersion {
	fn from(value: Proto) -> Self {
		match value {
			Proto::V1 => Self::V1,
			Proto::V2 => Self::V2,
			Proto::X => Self::X,
		}
	}
}

/// Wisp V2 middleware closure.
pub type WispV2Middleware = dyn for<'a> Fn(
		&'a mut Vec<AnyProtocolExtensionBuilder>,
	) -> Pin<Box<dyn Future<Output = Result<(), WispError>> + Sync + Send + 'a>>
	+ Send;

/// Wisp v2/WispX protocol extension settings.
pub struct WispV2Handshake {
	builders: Vec<AnyProtocolExtensionBuilder>,
	closure: Box<WispV2Middleware>,
}

impl WispV2Handshake {
	/// Create a Wisp V2 settings struct with no middleware.
	pub fn new(builders: Vec<AnyProtocolExtensionBuilder>) -> Self {
		Self {
			builders,
			closure: Box::new(|_| Box::pin(async { Ok(()) })),
		}
	}

	/// Create a Wisp V2 settings struct with some middleware.
	pub fn new_with_middleware(
		builders: Vec<AnyProtocolExtensionBuilder>,
		closure: Box<WispV2Middleware>,
	) -> Self {
		Self { builders, closure }
	}

	/// Add a Wisp V2 extension builder to the settings struct.
	pub fn add_extension(&mut self, extension: AnyProtocolExtensionBuilder) {
		self.builders.push(extension);
	}
}

/// WispX connection settings.
pub struct WispXOptions {
	/// Protocol extensions, negotiated exactly like Wisp v2's.
	pub extensions: WispV2Handshake,
	/// Credit, in bytes, granted to the peer on every new flow controlled stream.
	pub stream_window: u32,
	/// Largest receive window a stream can grow to while its reader keeps up.
	pub max_stream_window: u32,
	/// Limit on window growth summed over all streams of the connection.
	pub max_connection_window: u64,
	/// Largest websocket message we accept. Must be at least 64 KiB.
	pub max_message_size: u32,
	/// Cap on coalesced outgoing websocket messages.
	pub max_batch_size: usize,
}

impl WispXOptions {
	/// Settings with the default window and message sizes.
	pub fn new(extensions: Vec<AnyProtocolExtensionBuilder>) -> Self {
		Self::from_handshake(WispV2Handshake::new(extensions))
	}

	/// Settings with the default window and message sizes, using existing extension settings.
	pub fn from_handshake(extensions: WispV2Handshake) -> Self {
		Self {
			extensions,
			stream_window: DEFAULT_WISPX_STREAM_WINDOW,
			max_stream_window: DEFAULT_WISPX_MAX_STREAM_WINDOW,
			max_connection_window: DEFAULT_WISPX_MAX_CONNECTION_WINDOW,
			max_message_size: DEFAULT_WISPX_MAX_MESSAGE_SIZE,
			max_batch_size: DEFAULT_WISPX_MAX_BATCH_SIZE,
		}
	}
}

/// Everything the handshake decided.
struct Handshake {
	proto: Proto,
	downgraded: bool,
	extensions: Vec<AnyProtocolExtension>,
	max_frame_payload: usize,
	batch_size: usize,
	tx_window: u64,
	rx_window: u64,
	rx_max_window: u64,
	rx_max_growth: u64,
	/// A message already read from the transport that the driver must process first.
	initial: Option<Bytes>,
	/// A frame to send before anything else (the WispX client HELLO).
	first_frame: Option<OutFrame>,
}

impl Handshake {
	fn v1_v2(proto: Proto, role: Role, extensions: Vec<AnyProtocolExtension>, buffer: u32) -> Self {
		Self {
			proto,
			downgraded: false,
			extensions,
			max_frame_payload: match role {
				Role::Server => V2_SERVER_MAX_FRAME_PAYLOAD,
				Role::Client => V2_CLIENT_MAX_FRAME_PAYLOAD,
			},
			batch_size: 0,
			tx_window: buffer.into(),
			rx_window: buffer.into(),
			rx_max_window: buffer.into(),
			rx_max_growth: 0,
			initial: None,
			first_frame: None,
		}
	}

	fn x(
		extensions: Vec<AnyProtocolExtension>,
		our_batch_size: usize,
		our_stream_window: u32,
		our_max_stream_window: u32,
		our_max_connection_window: u64,
		peer_max_message_size: u32,
		peer_stream_window: u32,
	) -> Self {
		let peer_max = (peer_max_message_size as usize).max(X_HEADER_LEN + 1);
		Self {
			proto: Proto::X,
			downgraded: false,
			extensions,
			max_frame_payload: peer_max - X_HEADER_LEN,
			batch_size: our_batch_size.clamp(X_HEADER_LEN + 1, peer_max),
			tx_window: peer_stream_window.into(),
			rx_window: our_stream_window.into(),
			rx_max_window: our_max_stream_window.max(our_stream_window).into(),
			rx_max_growth: our_max_connection_window,
			initial: None,
			first_frame: None,
		}
	}
}

async fn send_info_packet<W: TransportWrite>(
	write: &mut W,
	builders: &mut [AnyProtocolExtensionBuilder],
	role: Role,
) -> Result<(), WispError> {
	let extensions = builders
		.iter_mut()
		.map(|x| x.build_to_extension(role))
		.collect::<Result<Vec<_>, _>>()?;

	write
		.send(
			InfoPacket {
				version: WISP_VERSION,
				extensions,
			}
			.encode(),
		)
		.await
}

async fn handle_handshake<R: TransportRead, W: TransportWrite>(
	read: &mut R,
	write: &mut W,
	extensions: &mut [AnyProtocolExtension],
) -> Result<(), WispError> {
	for extension in extensions {
		extension.handle_handshake(read, write).await?;
	}
	Ok(())
}

fn validate_continue_packet(packet: &Packet) -> Result<u32, WispError> {
	if packet.stream_id != 0 {
		return Err(WispError::InvalidStreamId(packet.stream_id));
	}

	let PacketType::Continue(continue_packet) = packet.packet_type else {
		return Err(WispError::InvalidPacketType(packet.packet_type.get_type()));
	};

	Ok(continue_packet.buffer_remaining)
}

fn get_supported_extensions(
	extensions: Vec<AnyProtocolExtension>,
	builders: &mut [AnyProtocolExtensionBuilder],
) -> Vec<AnyProtocolExtension> {
	let extension_ids: Vec<_> = builders.iter().map(|x| x.get_id()).collect();
	extensions
		.into_iter()
		.filter(|x| extension_ids.contains(&x.get_id()))
		.collect()
}

/// Split the first frame off a WispX message, checking that it is a HELLO on stream 0.
fn take_hello_frame(message: &mut Bytes) -> Result<Bytes, WispError> {
	if message.len() < X_HEADER_LEN {
		return Err(WispError::PacketTooSmall);
	}
	if message[0] != TY_INFO {
		return Err(WispError::InvalidPacketType(message[0]));
	}
	let id = u32::from_le_bytes([message[1], message[2], message[3], message[4]]);
	if id != 0 {
		return Err(WispError::InvalidStreamId(id));
	}
	let len = u32::from_le_bytes([message[5], message[6], message[7], message[8]]) as usize;
	if message.len() < X_HEADER_LEN + len {
		return Err(WispError::PacketTooSmall);
	}
	let mut frame = message.split_to(X_HEADER_LEN + len);
	Ok(frame.split_off(X_HEADER_LEN))
}

fn validate_x_options(options: &WispXOptions) -> Result<(), WispError> {
	if options.max_message_size < WISPX_MIN_MESSAGE_SIZE
		|| options.stream_window < WISPX_MIN_MESSAGE_SIZE
	{
		return Err(WispError::ProtocolViolation(
			"WispX max_message_size and stream_window must be at least 64 KiB",
		));
	}
	Ok(())
}

/// A Wisp multiplexor, either server or client side.
pub struct Multiplexor<M, W: TransportWrite> {
	mux: M,
	shared: Arc<MuxShared>,
	tx: Arc<Mutex<W>>,

	downgraded: bool,
	protocol: WispProtocolVersion,
	supported_extensions: Vec<AnyProtocolExtension>,
}

impl<M, W: TransportWrite> Drop for Multiplexor<M, W> {
	fn drop(&mut self) {
		self.shared.release_handle();
	}
}

impl<M, W: TransportWrite> Multiplexor<M, W> {
	fn build<R: TransportRead>(
		mux: M,
		role: Role,
		rx: R,
		tx: W,
		handshake: Handshake,
		new_streams: Option<flume::Sender<(ConnectPacket, Arc<StreamShared>)>>,
	) -> MuxResult<M, W> {
		let flow_stream_types = handshake
			.extensions
			.iter()
			.flat_map(|x| x.get_congestion_stream_types())
			.copied()
			.collect();

		let params = MuxParams {
			proto: handshake.proto,
			role,
			max_frame_payload: handshake.max_frame_payload,
			batch_size: handshake.batch_size,
			tx_window: handshake.tx_window,
			rx_window: handshake.rx_window,
			rx_max_window: handshake.rx_max_window,
			rx_max_growth: handshake.rx_max_growth,
			out_high_watermark: OUT_HIGH_WATERMARK,
			flow_stream_types,
		};
		let shared = Arc::new(MuxShared::new(params, new_streams));
		if let Some(frame) = handshake.first_frame {
			// the queue is fresh, this can't fail
			let _ = shared.enqueue(frame);
		}

		let tx = Arc::new(Mutex::new(tx));
		let fut = Box::pin(driver::run(
			shared.clone(),
			rx,
			tx.clone(),
			handshake.extensions.clone(),
			handshake.initial,
		));

		MuxResult(
			Self {
				mux,
				shared,
				tx,
				downgraded: handshake.downgraded,
				protocol: handshake.proto.into(),
				supported_extensions: handshake.extensions,
			},
			fut,
		)
	}

	/// Whether the connection was downgraded to an older protocol than requested.
	pub fn was_downgraded(&self) -> bool {
		self.downgraded
	}

	/// The wire protocol this connection speaks.
	pub fn protocol(&self) -> WispProtocolVersion {
		self.protocol
	}

	/// Get a shared reference to the extensions that are supported by both sides.
	pub fn get_extensions(&self) -> &[AnyProtocolExtension] {
		&self.supported_extensions
	}

	/// Get a mutable reference to the extensions that are supported by both sides.
	pub fn get_extensions_mut(&mut self) -> &mut [AnyProtocolExtension] {
		&mut self.supported_extensions
	}

	/// Get a `Vec` of all extension IDs that are supported by both sides.
	pub fn get_extension_ids(&self) -> Vec<u8> {
		self.supported_extensions
			.iter()
			.map(|x| x.get_id())
			.collect()
	}

	/// Get exclusive access to the write half of the transport, for example to send a
	/// websocket ping. The multiplexor's writer waits while the guard is held.
	pub async fn lock_ws(&self) -> Result<LockedWebSocketWriteGuard<W>, WispError> {
		if self.shared.is_dead() {
			Err(WispError::WsImplSocketClosed)
		} else {
			Ok(self.tx.clone().lock_owned().await)
		}
	}

	fn close_internal(&self, reason: Option<CloseReason>) -> Result<(), WispError> {
		if self.shared.is_dead() {
			return Err(WispError::MuxMessageFailedToSend);
		}
		self.shared.request_close(reason);
		Ok(())
	}

	/// Close all streams. Queued data is still sent first.
	///
	/// Also terminates the multiplexor future.
	pub async fn close(&self) -> Result<(), WispError> {
		self.close_internal(None)
	}

	/// Close all streams and send a close reason on stream ID 0.
	///
	/// Also terminates the multiplexor future.
	pub async fn close_with_reason(&self, reason: CloseReason) -> Result<(), WispError> {
		self.close_internal(Some(reason))
	}

	fn wrap_stream(&self, stream: Arc<StreamShared>) -> MuxStream<W> {
		MuxStream::new(self.shared.clone(), stream)
	}
}

pub type MultiplexorActorFuture = Pin<Box<dyn Future<Output = Result<(), WispError>> + Send>>;

/// Result of creating a multiplexor. Helps require protocol extensions.
pub struct MuxResult<M, W>(Multiplexor<M, W>, MultiplexorActorFuture)
where
	W: TransportWrite;

impl<M, W> MuxResult<M, W>
where
	W: TransportWrite,
{
	/// Require no protocol extensions.
	pub fn with_no_required_extensions(self) -> (Multiplexor<M, W>, MultiplexorActorFuture) {
		(self.0, self.1)
	}

	/// Require protocol extensions by their ID. Will close the multiplexor connection if
	/// extensions are not supported.
	pub async fn with_required_extensions(
		self,
		extensions: &[u8],
	) -> Result<(Multiplexor<M, W>, MultiplexorActorFuture), WispError> {
		let mut unsupported_extensions = Vec::new();
		let supported_extensions = self.0.get_extensions();

		for extension in extensions {
			if !supported_extensions
				.iter()
				.any(|x| x.get_id() == *extension)
			{
				unsupported_extensions.push(*extension);
			}
		}

		if unsupported_extensions.is_empty() {
			Ok((self.0, self.1))
		} else {
			self.0
				.close_with_reason(CloseReason::ExtensionsIncompatible)
				.await?;
			self.1.await?;
			Err(WispError::ExtensionsNotSupported(unsupported_extensions))
		}
	}

	/// Shorthand for `with_required_extensions(&[UdpProtocolExtension::ID])`
	pub async fn with_udp_extension_required(
		self,
	) -> Result<(Multiplexor<M, W>, MultiplexorActorFuture), WispError> {
		self.with_required_extensions(&[UdpProtocolExtension::ID])
			.await
	}
}
