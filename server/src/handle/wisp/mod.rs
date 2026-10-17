#[cfg(feature = "twisp")]
pub mod twisp;
pub mod utils;
pub mod wispnet;

use std::{
	collections::VecDeque,
	io::IoSlice,
	sync::Arc,
	time::{Duration, Instant},
};

use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use cfg_if::cfg_if;
use event_listener::Event;
use futures_util::{future::Either, FutureExt, SinkExt, StreamExt};
use log::{debug, trace};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	select,
	task::JoinSet,
	time::interval,
};
use uuid::Uuid;
use wisp_mux::{
	packet::{CloseReason, ConnectPacket},
	stream::MuxStream,
	ServerMux, WispProtocolVersion, WispXOptions, DEFAULT_WISPX_MAX_BATCH_SIZE,
};
use wispnet::route_wispnet;

use crate::{
	route::{WispClientProtocol, WispResult, WispStreamWrite, WispWsStreamWrite},
	stream::{ClientStream, ResolvedPacket},
	CLIENTS, CONFIG,
};

/// Relay a Wisp stream and a TCP socket until both directions are done.
///
/// Origin to client: reads go straight into a `BytesMut` whose frozen slices are queued on the
/// multiplexor, so the data is copied once (into the outgoing websocket message). On WispX each
/// direction finishes independently (FIN / `shutdown`). Wisp v1/v2 can't half-close, so the
/// first direction to finish ends the stream, as before.
async fn copy_tcp(
	mux: MuxStream<WispStreamWrite>,
	tcp: TcpStream,
	half_close: bool,
	#[cfg(feature = "speed-limit")] read_limit: async_speed_limit::Limiter<
		async_speed_limit::clock::StandardClock,
	>,
	#[cfg(feature = "speed-limit")] write_limit: async_speed_limit::Limiter<
		async_speed_limit::clock::StandardClock,
	>,
) -> std::io::Result<()> {
	let (muxrx, mut muxtx) = mux.into_split();
	let (tcprx, tcptx) = tcp.into_split();

	#[cfg(feature = "speed-limit")]
	let mut tcprx = read_limit.limit(tcprx);
	#[cfg(not(feature = "speed-limit"))]
	let mut tcprx = tcprx;
	#[cfg(feature = "speed-limit")]
	let mut tcptx = write_limit.limit(tcptx);
	#[cfg(not(feature = "speed-limit"))]
	let mut tcptx = tcptx;

	let upload = async {
		// everything already queued goes out in one vectored write
		let mut muxrx = muxrx.ready_chunks(64);
		while let Some(chunks) = muxrx.next().await {
			let mut chunks = chunks
				.into_iter()
				.collect::<Result<VecDeque<Bytes>, _>>()
				.map_err(std::io::Error::other)?;
			while !chunks.is_empty() {
				let slices: Vec<_> = chunks.iter().map(|x| IoSlice::new(x)).collect();
				let mut written = tcptx.write_vectored(&slices).await?;
				if written == 0 {
					return Err(std::io::ErrorKind::WriteZero.into());
				}
				while written > 0 {
					let front = chunks.front_mut().expect("wrote more than was queued");
					if written >= front.len() {
						written -= front.len();
						chunks.pop_front();
					} else {
						front.advance(written);
						written = 0;
					}
				}
			}
		}
		tcptx.shutdown().await
	};

	let download = async {
		let read_size = CONFIG.stream.buffer_size.max(4096);
		let mut buf = BytesMut::new();
		let mut last_send = None;
		loop {
			// reclaims the old allocation once the multiplexor is done with the previous chunk
			buf.reserve(read_size);
			if tcprx.read_buf(&mut buf).await? == 0 {
				break;
			}
			muxtx
				.send(buf.split().freeze())
				.await
				.map_err(std::io::Error::other)?;
			last_send = Some(Instant::now());
		}
		// see `WispConfig::v2_close_delay_ms`: only data sent shortly before is at risk
		if let (false, Some(last_send)) = (half_close, last_send) {
			let delay = Duration::from_millis(CONFIG.wisp.v2_close_delay_ms);
			tokio::time::sleep(delay.saturating_sub(last_send.elapsed())).await;
		}
		muxtx.finish().await.map_err(std::io::Error::other)
	};

	if half_close {
		futures_util::try_join!(upload, download)?;
	} else {
		select! {
			x = upload => x?,
			x = download => x?,
		}
	}

	Ok(())
}

async fn resolve_stream(
	connect: ConnectPacket,
	muxstream: &MuxStream<WispStreamWrite>,
) -> Option<(ConnectPacket, ConnectPacket, ClientStream)> {
	let requested_stream = connect.clone();

	let Ok(resolved) = ClientStream::resolve(connect).await else {
		let _ = muxstream.close(CloseReason::ServerStreamUnreachable).await;
		return None;
	};
	let (stream, resolved_stream) = match resolved {
		ResolvedPacket::Valid(connect) => {
			let resolved = connect.clone();
			let Ok(stream) = ClientStream::connect(connect).await else {
				let _ = muxstream.close(CloseReason::ServerStreamUnreachable).await;
				return None;
			};
			(stream, resolved)
		}
		ResolvedPacket::ValidWispnet(server, connect) => {
			let resolved = connect.clone();
			let Ok(stream) = route_wispnet(server, connect).await else {
				let _ = muxstream.close(CloseReason::ServerStreamUnreachable).await;
				return None;
			};
			(stream, resolved)
		}
		ResolvedPacket::NoResolvedAddrs => {
			let _ = muxstream.close(CloseReason::ServerStreamUnreachable).await;
			return None;
		}
		ResolvedPacket::Blocked => {
			let _ = muxstream
				.close(CloseReason::ServerStreamBlockedAddress)
				.await;
			return None;
		}
		ResolvedPacket::Invalid => {
			let _ = muxstream.close(CloseReason::ServerStreamInvalidInfo).await;
			return None;
		}
	};

	Some((requested_stream, resolved_stream, stream))
}

async fn forward_stream(
	muxstream: MuxStream<WispStreamWrite>,
	stream: ClientStream,
	resolved_stream: ConnectPacket,
	uuid: Uuid,
	half_close: bool,
	#[cfg(feature = "twisp")] twisp_map: twisp::TwispMap,
	#[cfg(feature = "speed-limit")] read_limit: async_speed_limit::Limiter<
		async_speed_limit::clock::StandardClock,
	>,
	#[cfg(feature = "speed-limit")] write_limit: async_speed_limit::Limiter<
		async_speed_limit::clock::StandardClock,
	>,
) {
	match stream {
		ClientStream::Tcp(stream) => {
			let closer = muxstream.get_close_handle();

			let copy = copy_tcp(
				muxstream,
				stream,
				half_close,
				#[cfg(feature = "speed-limit")]
				read_limit,
				#[cfg(feature = "speed-limit")]
				write_limit,
			);
			// a CLOSE from the client (or the multiplexor going away) aborts both directions
			let ret = select! {
				x = copy => x,
				_ = closer.closed() => Ok(()),
			};

			match ret {
				Ok(()) => {
					let _ = closer.close(CloseReason::Voluntary).await;
				}
				Err(_) => {
					let _ = closer.close(CloseReason::Unexpected).await;
				}
			}
		}
		ClientStream::Udp(stream) => {
			let closer = muxstream.get_close_handle();
			let (mut read, mut write) = muxstream.into_split();

			let ret: anyhow::Result<()> = async move {
				let mut data = vec![0u8; 65507];
				loop {
					select! {
						size = stream.recv(&mut data) => {
							let size = size?;
							write.send(Bytes::copy_from_slice(&data[..size])).await?;
						}
						data = read.next() => {
							if let Some(data) = data.transpose()? {
								stream.send(&data).await?;
							} else {
								break Ok(());
							}
						}
					}
				}
			}
			.await;

			match ret {
				Ok(()) => {
					let _ = closer.close(CloseReason::Voluntary).await;
				}
				Err(_) => {
					let _ = closer.close(CloseReason::Unexpected).await;
				}
			}
		}
		#[cfg(feature = "twisp")]
		ClientStream::Pty(cmd, pty) => {
			let closer = muxstream.get_close_handle();
			let id = muxstream.get_stream_id();
			let (mut rx, mut tx) = muxstream.into_async_rw().into_split();

			match twisp::handle_twisp(id, &mut rx, &mut tx, twisp_map.clone(), pty, cmd).await {
				Ok(()) => {
					let _ = closer.close(CloseReason::Voluntary).await;
				}
				Err(_) => {
					let _ = closer.close(CloseReason::Unexpected).await;
				}
			}
		}
		ClientStream::Wispnet(stream, mux_id) => {
			Box::pin(wispnet::handle_stream(
				muxstream,
				stream,
				mux_id,
				uuid,
				resolved_stream,
				#[cfg(feature = "speed-limit")]
				read_limit,
				#[cfg(feature = "speed-limit")]
				write_limit,
			))
			.await;
		}
		ClientStream::NoResolvedAddrs => {
			let _ = muxstream.close(CloseReason::ServerStreamUnreachable).await;
		}
		ClientStream::Invalid => {
			let _ = muxstream.close(CloseReason::ServerStreamInvalidInfo).await;
		}
		ClientStream::Blocked => {
			let _ = muxstream
				.close(CloseReason::ServerStreamBlockedAddress)
				.await;
		}
	}
}

async fn handle_stream(
	connect: ConnectPacket,
	muxstream: MuxStream<WispStreamWrite>,
	id: String,
	event: Arc<Event>,
	half_close: bool,
	#[cfg(feature = "twisp")] twisp_map: twisp::TwispMap,
	#[cfg(feature = "speed-limit")] read_limit: async_speed_limit::Limiter<
		async_speed_limit::clock::StandardClock,
	>,
	#[cfg(feature = "speed-limit")] write_limit: async_speed_limit::Limiter<
		async_speed_limit::clock::StandardClock,
	>,
) {
	let uuid = Uuid::new_v4();
	debug!("[{id}] [{uuid}] stream requested: {connect:?}");

	let Some((requested_stream, resolved_stream, stream)) = select! (
		x = resolve_stream(connect, &muxstream) => x,
		() = event.listen() => None
	) else {
		debug!("[{id}] [{uuid}] stream cancelled");
		// muxstream was closed
		return;
	};

	debug!("[{id}] [{uuid}] stream resolved and connected: {resolved_stream:?}");

	if let Some(client) = CLIENTS.lock().await.get(&id) {
		client
			.0
			.lock()
			.await
			.insert(uuid, (requested_stream, resolved_stream.clone()));
	}

	let forward_fut = forward_stream(
		muxstream,
		stream,
		resolved_stream,
		uuid,
		half_close,
		#[cfg(feature = "twisp")]
		twisp_map,
		#[cfg(feature = "speed-limit")]
		read_limit,
		#[cfg(feature = "speed-limit")]
		write_limit,
	);

	select! {
		x = forward_fut => x,
		() = event.listen() => (),
	};

	debug!("[{id}] [{uuid}] stream disconnected");

	if let Some(client) = CLIENTS.lock().await.get(&id) {
		client.0.lock().await.remove(&uuid);
	}
}

pub async fn handle_wisp(
	stream: WispResult,
	protocol: WispClientProtocol,
	id: String,
) -> anyhow::Result<()> {
	let (read, write) = stream;
	cfg_if! {
		if #[cfg(feature = "twisp")] {
			let twisp_map = twisp::new_map();
			let (extensions, required_extensions, buffer_size) = CONFIG.wisp.to_opts().await?;

			let extensions = match extensions {
				Some(mut exts) => {
					exts.add_extension(twisp::new_ext(twisp_map.clone()));
					Some(exts)
				},
				None => {
					None
				}
			};
		} else {
			let (extensions, required_extensions, buffer_size) = CONFIG.wisp.to_opts().await?;
		}
	}

	#[cfg(feature = "speed-limit")]
	let read_limiter = async_speed_limit::Limiter::builder(CONFIG.wisp.read_limit)
		.refill(Duration::from_secs(1))
		.clock(async_speed_limit::clock::StandardClock)
		.build();
	#[cfg(feature = "speed-limit")]
	let write_limiter = async_speed_limit::Limiter::builder(CONFIG.wisp.write_limit)
		.refill(Duration::from_secs(1))
		.clock(async_speed_limit::clock::StandardClock)
		.build();

	let mux_result = match (protocol, extensions) {
		(WispClientProtocol::X, Some(extensions)) => {
			Box::pin(ServerMux::new_wispx(
				read,
				write,
				WispXOptions {
					extensions,
					stream_window: CONFIG.wisp.wispx_stream_window,
					max_stream_window: CONFIG.wisp.wispx_max_stream_window,
					max_message_size: u32::try_from(CONFIG.server.max_message_size)
						.unwrap_or(u32::MAX),
					max_batch_size: DEFAULT_WISPX_MAX_BATCH_SIZE,
				},
			))
			.await
		}
		(WispClientProtocol::V2, extensions) => {
			Box::pin(ServerMux::new(read, write, buffer_size, extensions)).await
		}
		(WispClientProtocol::V1 | WispClientProtocol::X, _) => {
			Box::pin(ServerMux::new(read, write, buffer_size, None)).await
		}
	};
	let (mux, fut) = Box::pin(
		mux_result
			.context("failed to create server multiplexor")?
			.with_required_extensions(&required_extensions),
	)
	.await?;
	let mux = Arc::new(mux);
	let half_close = mux.protocol() == WispProtocolVersion::X;

	debug!(
		"[{id}] client connected with protocol {:?}, extensions {:?}, downgraded {:?}",
		mux.protocol(),
		mux.get_extension_ids(),
		mux.was_downgraded()
	);

	let mut set: JoinSet<()> = JoinSet::new();
	let event: Arc<Event> = Event::new().into();

	let mux_id = id.clone();
	set.spawn(fut.map(move |x| debug!("[{mux_id}] multiplexor result: {x:?}")));

	let ping_mux = mux.clone();
	let ping_event = event.clone();
	let ping_id = id.clone();
	set.spawn(async move {
		let mut interval = interval(Duration::from_secs(30));
		let send_ping = || async {
			let mut locked = ping_mux.lock_ws().await?;
			if let Either::Left(ws) = &mut *locked {
				<WispWsStreamWrite as SinkExt<tokio_websockets::Message>>::send(
					ws,
					tokio_websockets::Message::ping(&[] as &[u8]),
				)
				.await?;
			}
			anyhow::Ok(())
		};

		while (send_ping)().await.is_ok() {
			trace!("[{ping_id}] sent ping");
			select! {
				_ = interval.tick() => (),
				() = ping_event.listen() => break,
			};
		}
	});

	while let Some((connect, stream)) = mux.wait_for_stream().await {
		set.spawn(handle_stream(
			connect,
			stream,
			id.clone(),
			event.clone(),
			half_close,
			#[cfg(feature = "twisp")]
			twisp_map.clone(),
			#[cfg(feature = "speed-limit")]
			read_limiter.clone(),
			#[cfg(feature = "speed-limit")]
			write_limiter.clone(),
		));
	}

	debug!("[{id}] shutting down wisp client");

	let _ = mux.close().await;
	event.notify(usize::MAX);

	trace!("[{id}] waiting for tasks to close");

	while set.join_next().await.is_some() {}

	debug!("[{id}] client disconnected");

	Ok(())
}
