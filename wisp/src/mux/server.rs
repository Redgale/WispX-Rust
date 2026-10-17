use std::{marker::PhantomData, sync::Arc};

use futures::SinkExt;

use crate::{
	extensions::udp::UdpProtocolExtension,
	frame::{OutFrame, Proto, TY_INFO},
	packet::{CloseReason, ConnectPacket, HelloPacket, InfoPacket, Packet},
	stream::MuxStream,
	ws::{TransportRead, TransportReadExt, TransportWrite},
	Role, WispError, WISPX_VERSION,
};

use super::{
	get_supported_extensions, handle_handshake, send_info_packet, shared::StreamShared,
	take_hello_frame, validate_x_options, Handshake, Multiplexor, MuxResult, WispV2Handshake,
	WispXOptions,
};

/// Server side multiplexor state.
pub struct ServerImpl<W> {
	stream_rx: flume::Receiver<(ConnectPacket, Arc<StreamShared>)>,
	_phantom: PhantomData<fn() -> W>,
}

/// Send the CLOSE matching a failed authentication, then close the transport.
async fn handle_error<W: TransportWrite>(
	err: WispError,
	tx: &mut W,
	proto: Proto,
) -> Result<WispError, WispError> {
	let reason = match err {
		WispError::PasswordExtensionCredsInvalid => CloseReason::ExtensionsPasswordAuthFailed,
		WispError::CertAuthExtensionSigInvalid => CloseReason::ExtensionsCertAuthFailed,
		x => return Ok(x),
	};
	tx.send(OutFrame::close(0, reason).encode(proto)).await?;
	tx.close().await?;
	Ok(err)
}

async fn handshake_v1_v2<R: TransportRead, W: TransportWrite>(
	rx: &mut R,
	tx: &mut W,
	buffer_size: u32,
	wisp_v2: Option<WispV2Handshake>,
) -> Result<Handshake, WispError> {
	let continue_packet = Packet::new_continue(0, buffer_size).encode();

	let Some(WispV2Handshake {
		mut builders,
		closure,
	}) = wisp_v2
	else {
		// user asked for a v1 server
		tx.send(continue_packet).await?;
		return Ok(Handshake::v1_v2(
			Proto::V1,
			Role::Server,
			vec![UdpProtocolExtension.into()],
			buffer_size,
		));
	};

	send_info_packet(tx, &mut builders, Role::Server).await?;
	(closure)(&mut builders).await?;

	let first = rx.next_erroring().await?;
	if first.first() == Some(&TY_INFO) {
		let mut packet = first.slice(5..);
		let info = InfoPacket::decode(&mut packet, &mut builders, Role::Server)?;
		let mut extensions = get_supported_extensions(info.extensions, &mut builders);
		handle_handshake(rx, tx, &mut extensions).await?;
		tx.send(continue_packet).await?;

		Ok(Handshake::v1_v2(
			Proto::V2,
			Role::Server,
			extensions,
			buffer_size,
		))
	} else {
		// a v1 client: its first packet is a normal one, hand it to the driver
		let mut handshake = Handshake::v1_v2(
			Proto::V1,
			Role::Server,
			vec![UdpProtocolExtension.into()],
			buffer_size,
		);
		handshake.downgraded = true;
		handshake.initial = Some(first);
		Ok(handshake)
	}
}

async fn handshake_x<R: TransportRead, W: TransportWrite>(
	rx: &mut R,
	tx: &mut W,
	options: WispXOptions,
) -> Result<Handshake, WispError> {
	validate_x_options(&options)?;
	let WispXOptions {
		extensions: WispV2Handshake {
			mut builders,
			closure,
		},
		stream_window,
		max_stream_window,
		max_connection_window,
		max_message_size,
		max_batch_size,
	} = options;

	let extensions = builders
		.iter_mut()
		.map(|x| x.build_to_extension(Role::Server))
		.collect::<Result<Vec<_>, _>>()?;
	tx.send(
		HelloPacket {
			version: WISPX_VERSION,
			max_message_size,
			stream_window,
			extensions,
		}
		.encode(),
	)
	.await?;

	(closure)(&mut builders).await?;

	// the client HELLO comes first; frames after it in the same message are for the driver
	let mut first = rx.next_erroring().await?;
	let hello = HelloPacket::decode(take_hello_frame(&mut first)?, &mut builders, Role::Server)?;
	let extensions = get_supported_extensions(hello.extensions, &mut builders);

	let mut handshake = Handshake::x(
		extensions,
		max_batch_size,
		stream_window,
		max_stream_window,
		max_connection_window,
		hello.max_message_size,
		hello.stream_window,
	);
	if !first.is_empty() {
		handshake.initial = Some(first);
	}
	Ok(handshake)
}

impl<W: TransportWrite> Multiplexor<ServerImpl<W>, W> {
	/// Create a new Wisp v1/v2 server-side multiplexor.
	///
	/// If `wisp_v2` is None a Wisp v1 connection is created, otherwise a Wisp v2 connection is created.
	/// **It is not guaranteed that all extensions you specify are available.** You must manually check
	/// if the extensions you need are available after the multiplexor has been created.
	#[expect(clippy::new_ret_no_self)]
	pub async fn new<R: TransportRead>(
		mut rx: R,
		mut tx: W,
		buffer_size: u32,
		wisp_v2: Option<WispV2Handshake>,
	) -> Result<MuxResult<ServerImpl<W>, W>, WispError> {
		match handshake_v1_v2(&mut rx, &mut tx, buffer_size, wisp_v2).await {
			Ok(handshake) => Ok(Self::finish(rx, tx, handshake)),
			Err(err) => Err(handle_error(err, &mut tx, Proto::V2).await?),
		}
	}

	/// Create a new WispX server-side multiplexor. Use this when the client selected the
	/// [`WISPX_WS_PROTOCOL`](crate::WISPX_WS_PROTOCOL) websocket subprotocol.
	///
	/// **It is not guaranteed that all extensions you specify are available.** You must manually check
	/// if the extensions you need are available after the multiplexor has been created.
	pub async fn new_wispx<R: TransportRead>(
		mut rx: R,
		mut tx: W,
		options: WispXOptions,
	) -> Result<MuxResult<ServerImpl<W>, W>, WispError> {
		match handshake_x(&mut rx, &mut tx, options).await {
			Ok(handshake) => Ok(Self::finish(rx, tx, handshake)),
			Err(err) => Err(handle_error(err, &mut tx, Proto::X).await?),
		}
	}

	fn finish<R: TransportRead>(rx: R, tx: W, handshake: Handshake) -> MuxResult<ServerImpl<W>, W> {
		let (stream_tx, stream_rx) = flume::unbounded();
		Self::build(
			ServerImpl {
				stream_rx,
				_phantom: PhantomData,
			},
			Role::Server,
			rx,
			tx,
			handshake,
			Some(stream_tx),
		)
	}

	/// Wait for a stream to be created.
	pub async fn wait_for_stream(&self) -> Option<(ConnectPacket, MuxStream<W>)> {
		let (connect, stream) = self.mux.stream_rx.recv_async().await.ok()?;
		Some((connect, self.wrap_stream(stream)))
	}
}

