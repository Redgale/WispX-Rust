use bytes::Bytes;

use crate::{
	extensions::udp::UdpProtocolExtension,
	frame::{classify_server_first_message, OutFrame, Proto, ServerFirstMessage, TY_INFO},
	packet::{ConnectPacket, HelloPacket, InfoPacket, Packet, StreamType},
	stream::MuxStream,
	ws::{TransportRead, TransportReadExt, TransportWrite},
	Role, WispError, WISPX_VERSION,
};

use super::{
	get_supported_extensions, handle_handshake, send_info_packet, take_hello_frame,
	validate_continue_packet, validate_x_options, Handshake, Multiplexor, MuxResult,
	WispV2Handshake, WispXOptions,
};

/// Client side multiplexor state.
pub struct ClientImpl;

/// Wisp v1/v2 client handshake. `first` is the server's first message, if already read.
async fn handshake_v1_v2<R: TransportRead, W: TransportWrite>(
	rx: &mut R,
	tx: &mut W,
	wisp_v2: Option<WispV2Handshake>,
	first: Option<Bytes>,
) -> Result<Handshake, WispError> {
	let first = match first {
		Some(x) => x,
		None => rx.next_erroring().await?,
	};

	let Some(WispV2Handshake {
		mut builders,
		closure,
	}) = wisp_v2
	else {
		// user asked for a v1 client
		let buffer_size = validate_continue_packet(&Packet::decode(first)?)?;
		return Ok(Handshake::v1_v2(
			Proto::V1,
			Role::Client,
			vec![UdpProtocolExtension.into()],
			buffer_size,
		));
	};

	if first.first() == Some(&TY_INFO) {
		// v2 server
		let mut packet = first.slice(5..);
		let info = InfoPacket::decode(&mut packet, &mut builders, Role::Client)?;
		(closure)(&mut builders).await?;
		send_info_packet(tx, &mut builders, Role::Client).await?;

		let mut extensions = get_supported_extensions(info.extensions, &mut builders);
		handle_handshake(rx, tx, &mut extensions).await?;

		let buffer_size = validate_continue_packet(&Packet::decode(rx.next_erroring().await?)?)?;
		Ok(Handshake::v1_v2(
			Proto::V2,
			Role::Client,
			extensions,
			buffer_size,
		))
	} else {
		// v1 server
		let buffer_size = validate_continue_packet(&Packet::decode(first)?)?;
		let mut handshake = Handshake::v1_v2(
			Proto::V1,
			Role::Client,
			vec![UdpProtocolExtension.into()],
			buffer_size,
		);
		handshake.downgraded = true;
		Ok(handshake)
	}
}

async fn handshake_x<R: TransportRead, W: TransportWrite>(
	rx: &mut R,
	tx: &mut W,
	options: WispXOptions,
) -> Result<Handshake, WispError> {
	validate_x_options(&options)?;

	let mut first = rx.next_erroring().await?;
	match classify_server_first_message(&first) {
		ServerFirstMessage::X => {}
		ServerFirstMessage::V2 => {
			let mut handshake =
				handshake_v1_v2(rx, tx, Some(options.extensions), Some(first)).await?;
			handshake.downgraded = true;
			return Ok(handshake);
		}
		ServerFirstMessage::V1 => {
			let mut handshake = handshake_v1_v2(rx, tx, None, Some(first)).await?;
			handshake.downgraded = true;
			return Ok(handshake);
		}
	}

	let WispXOptions {
		extensions: WispV2Handshake {
			mut builders,
			closure,
		},
		stream_window,
		max_message_size,
		max_batch_size,
	} = options;

	let hello = HelloPacket::decode(take_hello_frame(&mut first)?, &mut builders, Role::Client)?;
	(closure)(&mut builders).await?;

	let our_extensions = builders
		.iter_mut()
		.map(|x| x.build_to_extension(Role::Client))
		.collect::<Result<Vec<_>, _>>()?;
	let our_hello = HelloPacket {
		version: WISPX_VERSION,
		max_message_size,
		stream_window,
		extensions: our_extensions,
	}
	.encode();

	let extensions = get_supported_extensions(hello.extensions, &mut builders);
	let mut handshake = Handshake::x(
		extensions,
		max_batch_size,
		stream_window,
		hello.max_message_size,
		hello.stream_window,
	);
	// queued instead of sent, so the writer can put the first CONNECTs in the same message
	handshake.first_frame = Some(OutFrame::raw(
		TY_INFO,
		0,
		our_hello.slice(crate::frame::X_HEADER_LEN..),
	));
	if !first.is_empty() {
		handshake.initial = Some(first);
	}
	Ok(handshake)
}

impl<W: TransportWrite> Multiplexor<ClientImpl, W> {
	/// Create a new Wisp v1/v2 client side multiplexor.
	///
	/// If `wisp_v2` is None a Wisp v1 connection is created, otherwise a Wisp v2 connection is created.
	/// **It is not guaranteed that all extensions you specify are available.** You must manually check
	/// if the extensions you need are available after the multiplexor has been created.
	#[expect(clippy::new_ret_no_self)]
	pub async fn new<R: TransportRead>(
		mut rx: R,
		mut tx: W,
		wisp_v2: Option<WispV2Handshake>,
	) -> Result<MuxResult<ClientImpl, W>, WispError> {
		let handshake = handshake_v1_v2(&mut rx, &mut tx, wisp_v2, None).await?;
		Ok(Self::build(ClientImpl, Role::Client, rx, tx, handshake, None))
	}

	/// Create a new WispX client side multiplexor. The websocket should have been opened with
	/// the [`WISPX_WS_PROTOCOL`](crate::WISPX_WS_PROTOCOL) subprotocol.
	///
	/// If the server answers with Wisp v2 or v1 instead, the connection falls back to that
	/// protocol and [`Multiplexor::was_downgraded`] returns true.
	pub async fn new_wispx<R: TransportRead>(
		mut rx: R,
		mut tx: W,
		options: WispXOptions,
	) -> Result<MuxResult<ClientImpl, W>, WispError> {
		let handshake = handshake_x(&mut rx, &mut tx, options).await?;
		Ok(Self::build(ClientImpl, Role::Client, rx, tx, handshake, None))
	}

	/// Create a new stream, multiplexed through Wisp.
	///
	/// This does not wait for the server: data written to the stream right away is sent after
	/// the CONNECT, often in the same websocket message.
	pub async fn new_stream(
		&self,
		stream_type: StreamType,
		host: String,
		port: u16,
	) -> Result<MuxStream<W>, WispError> {
		if self.shared.is_dead() {
			return Err(WispError::MuxTaskEnded);
		}

		if stream_type == StreamType::Udp
			&& !self
				.supported_extensions
				.iter()
				.any(|x| x.get_id() == UdpProtocolExtension::ID)
		{
			return Err(WispError::ExtensionsNotSupported(vec![
				UdpProtocolExtension::ID,
			]));
		}

		let stream = self.shared.open_stream(&ConnectPacket {
			stream_type,
			host,
			port,
		})?;
		Ok(self.wrap_stream(stream))
	}
}

#[cfg(feature = "embedded-nal-async")]
const _: () = {
	use crate::stream::MuxStreamAsyncRW;

	impl<W: TransportWrite> embedded_nal_async::TcpConnect for Multiplexor<ClientImpl, W> {
		type Error = std::io::Error;

		type Connection<'a>
			= MuxStreamAsyncRW<W>
		where
			Self: 'a;

		async fn connect<'a>(
			&'a self,
			remote: std::net::SocketAddr,
		) -> Result<Self::Connection<'a>, Self::Error> {
			let stream = self
				.new_stream(StreamType::Tcp, remote.ip().to_string(), remote.port())
				.await
				.map_err(std::io::Error::other)?;
			Ok(stream.into_async_rw())
		}
	}
};
