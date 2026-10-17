//! End-to-end tests of client and server multiplexors over an in-memory transport.

use std::{
	collections::HashMap,
	sync::{
		atomic::{AtomicUsize, Ordering},
		Arc,
	},
	time::Duration,
};

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::{channel::mpsc, AsyncReadExt, AsyncWriteExt, SinkExt, StreamExt};
use tokio::time::{sleep, timeout};
use wisp_mux::{
	extensions::{
		password::{PasswordProtocolExtension, PasswordProtocolExtensionBuilder},
		udp::UdpProtocolExtensionBuilder,
		AnyProtocolExtension, AnyProtocolExtensionBuilder, ProtocolExtension,
		ProtocolExtensionBuilder,
	},
	packet::{CloseReason, StreamType},
	stream::MuxStream,
	ws::{TransportRead, TransportWrite},
	ClientMux, Role, ServerMux, WispError, WispProtocolVersion, WispV2Handshake, WispXOptions,
};

/// A connected pair of in-memory transports: (client side, server side).
#[allow(clippy::type_complexity)]
fn pipe() -> (
	(impl TransportRead, impl TransportWrite),
	(impl TransportRead, impl TransportWrite),
) {
	let (c2s_tx, c2s_rx) = mpsc::unbounded::<Bytes>();
	let (s2c_tx, s2c_rx) = mpsc::unbounded::<Bytes>();
	let err = |_| WispError::WsImplSocketClosed;
	(
		(s2c_rx.map(Ok), c2s_tx.sink_map_err(err)),
		(c2s_rx.map(Ok), s2c_tx.sink_map_err(err)),
	)
}

#[derive(Copy, Clone, Debug)]
enum Mode {
	V1,
	V2,
	X,
}

fn builders() -> Vec<AnyProtocolExtensionBuilder> {
	vec![UdpProtocolExtensionBuilder.into()]
}

fn x_options(window: u32) -> WispXOptions {
	let mut options = WispXOptions::new(builders());
	options.stream_window = window;
	options
}

async fn make_server<R: TransportRead, W: TransportWrite>(
	rx: R,
	tx: W,
	mode: Mode,
	window: u32,
) -> Result<ServerMux<W>, WispError> {
	let result = match mode {
		Mode::V1 => ServerMux::new(rx, tx, 32, None).await?,
		Mode::V2 => ServerMux::new(rx, tx, 32, Some(WispV2Handshake::new(builders()))).await?,
		Mode::X => ServerMux::new_wispx(rx, tx, x_options(window)).await?,
	};
	let (mux, fut) = result.with_no_required_extensions();
	tokio::spawn(fut);
	Ok(mux)
}

async fn make_client<R: TransportRead, W: TransportWrite>(
	rx: R,
	tx: W,
	mode: Mode,
	window: u32,
) -> Result<ClientMux<W>, WispError> {
	let result = match mode {
		Mode::V1 => ClientMux::new(rx, tx, None).await?,
		Mode::V2 => ClientMux::new(rx, tx, Some(WispV2Handshake::new(builders()))).await?,
		Mode::X => ClientMux::new_wispx(rx, tx, x_options(window)).await?,
	};
	let (mux, fut) = result.with_no_required_extensions();
	tokio::spawn(fut);
	Ok(mux)
}

/// Connect a client and server, returning both multiplexors.
async fn connect(
	client: Mode,
	server: Mode,
	window: u32,
) -> (ClientMux<impl TransportWrite>, ServerMux<impl TransportWrite>) {
	let ((crx, ctx), (srx, stx)) = pipe();
	let server = tokio::spawn(async move { make_server(srx, stx, server, window).await });
	let client = make_client(crx, ctx, client, window).await.unwrap();
	let server = server.await.unwrap().unwrap();
	(client, server)
}

/// Serve every stream by echoing it, half-closing when the client half-closes.
fn spawn_echo<W: TransportWrite>(server: ServerMux<W>) {
	tokio::spawn(async move {
		while let Some((_, stream)) = server.wait_for_stream().await {
			tokio::spawn(async move {
				let (mut rx, mut tx) = stream.into_split();
				while let Some(Ok(data)) = rx.next().await {
					if tx.send(data).await.is_err() {
						return;
					}
				}
				let _ = tx.finish().await;
			});
		}
	});
}

fn pattern(len: usize, seed: u8) -> Bytes {
	(0..len)
		.map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
		.collect::<Vec<_>>()
		.into()
}

async fn echo_roundtrip<W: TransportWrite>(stream: MuxStream<W>, total: usize, seed: u8) {
	let expected = pattern(total, seed);
	let (mut rx, mut tx) = stream.into_split();

	let to_send = expected.clone();
	let writer = tokio::spawn(async move {
		let mut sizes = [1usize, 7, 100, 1500, 16 * 1024, 70 * 1024].iter().cycle();
		let mut data = to_send;
		while !data.is_empty() {
			let len = (*sizes.next().unwrap()).min(data.len());
			tx.send(data.split_to(len)).await.unwrap();
		}
		tx
	});

	let mut received = BytesMut::new();
	while received.len() < total {
		let chunk = timeout(Duration::from_secs(10), rx.next())
			.await
			.expect("timed out waiting for echo")
			.expect("stream ended early")
			.unwrap();
		received.extend_from_slice(&chunk);
	}
	assert_eq!(received.len(), total);
	assert!(received[..] == expected[..], "echoed data differs");
	drop(writer.await.unwrap());
}

async fn echo_matrix_case(client: Mode, server: Mode, expected: WispProtocolVersion) {
	let (client, server) = connect(client, server, 64 * 1024).await;
	assert_eq!(client.protocol(), expected);
	assert_eq!(server.protocol(), expected);
	spawn_echo(server);

	let mut tasks = Vec::new();
	for i in 0..8u8 {
		let stream = client
			.new_stream(StreamType::Tcp, "example.com".into(), 80)
			.await
			.unwrap();
		tasks.push(tokio::spawn(echo_roundtrip(stream, 512 * 1024, i)));
	}
	for task in tasks {
		task.await.unwrap();
	}
}

#[tokio::test]
async fn echo_x() {
	echo_matrix_case(Mode::X, Mode::X, WispProtocolVersion::X).await;
}

#[tokio::test]
async fn echo_v2() {
	echo_matrix_case(Mode::V2, Mode::V2, WispProtocolVersion::V2).await;
}

#[tokio::test]
async fn echo_v1() {
	echo_matrix_case(Mode::V1, Mode::V1, WispProtocolVersion::V1).await;
}

#[tokio::test]
async fn x_client_falls_back_to_v2_server() {
	echo_matrix_case(Mode::X, Mode::V2, WispProtocolVersion::V2).await;
}

#[tokio::test]
async fn x_client_falls_back_to_v1_server() {
	echo_matrix_case(Mode::X, Mode::V1, WispProtocolVersion::V1).await;
}

/// A v1 client that sent a websocket subprotocol gets a v2 INFO it doesn't understand, ignores
/// it, and starts sending v1 packets. The server must downgrade and treat that packet normally.
#[tokio::test]
async fn v2_server_downgrades_on_v1_packet() {
	let ((mut crx, mut ctx), (srx, stx)) = pipe();
	let server = tokio::spawn(async move { make_server(srx, stx, Mode::V2, 0).await.unwrap() });

	let info = crx.next().await.unwrap().unwrap();
	assert_eq!(info[0], 0x05);
	// CONNECT tcp example.com:80 on stream 1
	let mut connect = vec![0x01, 1, 0, 0, 0, 0x01, 80, 0];
	connect.extend_from_slice(b"example.com");
	ctx.send(Bytes::from(connect)).await.unwrap();
	ctx.send(Bytes::from_static(&[0x02, 1, 0, 0, 0, b'h', b'i']))
		.await
		.unwrap();

	let server = server.await.unwrap();
	assert_eq!(server.protocol(), WispProtocolVersion::V1);
	assert!(server.was_downgraded());
	let (connect, mut stream) = server.wait_for_stream().await.unwrap();
	assert_eq!(connect.host, "example.com");
	assert_eq!(connect.port, 80);
	assert_eq!(&stream.next().await.unwrap().unwrap()[..], b"hi");
}

#[tokio::test]
async fn v2_client_on_v1_server() {
	echo_matrix_case(Mode::V2, Mode::V1, WispProtocolVersion::V1).await;
}

#[tokio::test]
async fn x_flow_control_limits_unread_data() {
	const WINDOW: u32 = 64 * 1024;
	let (client, server) = connect(Mode::X, Mode::X, WINDOW).await;

	let stream = client
		.new_stream(StreamType::Tcp, "a".into(), 1)
		.await
		.unwrap();
	let (_crx, mut ctx) = stream.into_split();

	let sent = Arc::new(AtomicUsize::new(0));
	let sent2 = sent.clone();
	let writer = tokio::spawn(async move {
		loop {
			let chunk = Bytes::from(vec![0u8; 4096]);
			if ctx.send(chunk).await.is_err() {
				break;
			}
			sent2.fetch_add(4096, Ordering::SeqCst);
		}
	});

	let (_, mut sstream) = server.wait_for_stream().await.unwrap();
	// the server isn't reading yet, so the client must stop at the window
	sleep(Duration::from_millis(200)).await;
	let before = sent.load(Ordering::SeqCst);
	assert!(
		before <= WINDOW as usize,
		"client sent {before} bytes with a {WINDOW} byte window"
	);
	assert!(before >= WINDOW as usize - 4096, "client sent only {before}");

	// reading frees credit and the writer continues
	let mut read = 0;
	while read < 4 * WINDOW as usize {
		read += sstream.next().await.unwrap().unwrap().len();
	}
	assert!(sent.load(Ordering::SeqCst) > before);
	writer.abort();
}

#[tokio::test]
async fn x_downstream_flow_control() {
	const WINDOW: u32 = 64 * 1024;
	let (client, server) = connect(Mode::X, Mode::X, WINDOW).await;
	let _stream = client
		.new_stream(StreamType::Tcp, "a".into(), 1)
		.await
		.unwrap();

	let (_, sstream) = server.wait_for_stream().await.unwrap();
	let (_srx, mut stx) = sstream.into_split();
	let sent = Arc::new(AtomicUsize::new(0));
	let sent2 = sent.clone();
	let writer = tokio::spawn(async move {
		while stx.send(Bytes::from(vec![1u8; 1000])).await.is_ok() {
			sent2.fetch_add(1000, Ordering::SeqCst);
		}
	});
	sleep(Duration::from_millis(200)).await;
	assert!(sent.load(Ordering::SeqCst) <= WINDOW as usize);
	writer.abort();
}

#[tokio::test]
async fn x_half_close() {
	let (client, server) = connect(Mode::X, Mode::X, 64 * 1024).await;

	let server_task = tokio::spawn(async move {
		let (_, stream) = server.wait_for_stream().await.unwrap();
		let (mut rx, mut tx) = stream.into_split();
		// read the whole request until the client half-closes
		let mut request = Vec::new();
		while let Some(chunk) = rx.next().await {
			request.extend_from_slice(&chunk.unwrap());
		}
		assert_eq!(request, b"request body");
		assert_eq!(rx.get_close_reason(), None, "FIN must not close the stream");
		// the write side still works after the client's FIN
		tx.send(Bytes::from_static(b"response")).await.unwrap();
		tx.finish().await.unwrap();
		assert_eq!(tx.get_close_reason(), Some(CloseReason::Voluntary));
		server
	});

	let stream = client
		.new_stream(StreamType::Tcp, "a".into(), 1)
		.await
		.unwrap();
	let (mut rx, mut tx) = stream.into_split();
	tx.send(Bytes::from_static(b"request body")).await.unwrap();
	tx.finish().await.unwrap();

	let mut response = Vec::new();
	while let Some(chunk) = rx.next().await {
		response.extend_from_slice(&chunk.unwrap());
	}
	assert_eq!(response, b"response");
	assert_eq!(rx.get_close_reason(), Some(CloseReason::Voluntary));
	server_task.await.unwrap();
}

async fn close_keeps_queued_data_case(client: Mode, server: Mode) {
	let (client, server) = connect(client, server, 1024 * 1024).await;
	const TOTAL: usize = 300 * 1000;

	tokio::spawn(async move {
		let (_, mut stream) = server.wait_for_stream().await.unwrap();
		for _ in 0..300 {
			stream.send(Bytes::from(vec![7u8; 1000])).await.unwrap();
		}
		stream.close(CloseReason::Voluntary).await.unwrap();
		// keep the server alive until the client is done
		sleep(Duration::from_secs(5)).await;
		drop(server);
	});

	let mut stream = client
		.new_stream(StreamType::Tcp, "a".into(), 1)
		.await
		.unwrap();
	// let everything, including the CLOSE, arrive before reading
	sleep(Duration::from_millis(200)).await;
	let mut total = 0;
	while let Some(chunk) = stream.next().await {
		total += chunk.unwrap().len();
	}
	assert_eq!(total, TOTAL, "data that arrived before CLOSE was lost");
	assert_eq!(stream.get_close_reason(), Some(CloseReason::Voluntary));
}

#[tokio::test]
async fn close_keeps_queued_data_x() {
	close_keeps_queued_data_case(Mode::X, Mode::X).await;
}

#[tokio::test]
async fn close_keeps_queued_data_v2() {
	close_keeps_queued_data_case(Mode::V2, Mode::V2).await;
}

#[tokio::test]
async fn dropping_stream_closes_it() {
	for mode in [Mode::X, Mode::V2] {
		let (client, server) = connect(mode, mode, 64 * 1024).await;
		let stream = client
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		let (_, mut sstream) = server.wait_for_stream().await.unwrap();
		drop(stream);
		let end = timeout(Duration::from_secs(5), sstream.next()).await.unwrap();
		assert!(end.is_none());
		assert_eq!(sstream.get_close_reason(), Some(CloseReason::Voluntary));
	}
}

#[tokio::test]
async fn closer_keeps_stream_open() {
	let (client, server) = connect(Mode::X, Mode::X, 64 * 1024).await;
	spawn_echo(server);
	let stream = client
		.new_stream(StreamType::Tcp, "a".into(), 1)
		.await
		.unwrap();
	let closer = stream.get_close_handle();
	drop(stream);
	// the closer still holds the stream, so an explicit reason wins
	closer.close(CloseReason::ServerStreamTimedOut).await.unwrap();
	assert_eq!(
		closer.get_close_reason(),
		Some(CloseReason::ServerStreamTimedOut)
	);
}

#[tokio::test]
async fn closed_future_resolves_on_peer_close() {
	let (client, server) = connect(Mode::X, Mode::X, 64 * 1024).await;
	let stream = client
		.new_stream(StreamType::Tcp, "a".into(), 1)
		.await
		.unwrap();
	let (_, sstream) = server.wait_for_stream().await.unwrap();
	let closer = sstream.get_close_handle();
	let waiter = tokio::spawn(async move { closer.closed().await });
	sleep(Duration::from_millis(50)).await;
	stream.close(CloseReason::Unexpected).await.unwrap();
	let reason = timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap();
	assert_eq!(reason, CloseReason::Unexpected);
}

#[tokio::test]
async fn async_rw_echo() {
	for mode in [Mode::X, Mode::V2, Mode::V1] {
		let (client, server) = connect(mode, mode, 64 * 1024).await;
		spawn_echo(server);
		let stream = client
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		let data = pattern(200 * 1024, 3);
		let (mut r, mut w) = stream.into_async_rw().into_split();
		let to_write = data.clone();
		let writer = tokio::spawn(async move {
			w.write_all(&to_write).await.unwrap();
			w
		});
		let mut out = vec![0u8; data.len()];
		r.read_exact(&mut out).await.unwrap();
		assert!(out[..] == data[..], "{mode:?}");
		writer.await.unwrap();
	}
}

#[tokio::test]
async fn udp_preserves_datagrams() {
	for mode in [Mode::X, Mode::V2] {
		let (client, server) = connect(mode, mode, 64 * 1024).await;
		spawn_echo(server);
		let mut stream = client
			.new_stream(StreamType::Udp, "a".into(), 53)
			.await
			.unwrap();
		let sizes = [1usize, 500, 1400, 9000, 60000];
		for (i, size) in sizes.iter().enumerate() {
			stream.send(pattern(*size, i as u8)).await.unwrap();
		}
		for (i, size) in sizes.iter().enumerate() {
			let datagram = stream.next().await.unwrap().unwrap();
			assert_eq!(datagram.len(), *size);
			assert!(datagram == pattern(*size, i as u8));
		}
	}
}

#[tokio::test]
async fn many_streams_x() {
	let (client, server) = connect(Mode::X, Mode::X, 256 * 1024).await;
	spawn_echo(server);
	let mut tasks = Vec::new();
	for i in 0..200u32 {
		let stream = client
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		tasks.push(tokio::spawn(echo_roundtrip(
			stream,
			8 * 1024 + i as usize,
			i as u8,
		)));
	}
	for task in tasks {
		task.await.unwrap();
	}
}

#[tokio::test]
async fn mux_close_ends_driver() {
	for mode in [Mode::X, Mode::V2] {
		let ((crx, ctx), (srx, stx)) = pipe();
		let server = tokio::spawn(async move {
			let (mux, fut) = match mode {
				Mode::X => ServerMux::new_wispx(srx, stx, x_options(64 * 1024)).await,
				_ => ServerMux::new(srx, stx, 32, Some(WispV2Handshake::new(builders()))).await,
			}
			.unwrap()
			.with_no_required_extensions();
			(mux, fut)
		});
		let client = make_client(crx, ctx, mode, 64 * 1024).await.unwrap();
		let (_server, server_fut) = server.await.unwrap();
		client
			.close_with_reason(CloseReason::Voluntary)
			.await
			.unwrap();
		timeout(Duration::from_secs(5), server_fut)
			.await
			.expect("server driver did not stop")
			.unwrap();
	}
}

#[tokio::test]
async fn password_auth() {
	for mode in [Mode::X, Mode::V2] {
		for (password, ok) in [("hunter2", true), ("wrong", false)] {
			let ((crx, ctx), (srx, stx)) = pipe();
			let server = tokio::spawn(async move {
				let mut users = HashMap::new();
				users.insert("user".to_string(), "hunter2".to_string());
				let exts: Vec<AnyProtocolExtensionBuilder> =
					vec![PasswordProtocolExtensionBuilder::new_server(users, true).into()];
				let result = match mode {
					Mode::X => ServerMux::new_wispx(srx, stx, WispXOptions::new(exts)).await,
					_ => ServerMux::new(srx, stx, 32, Some(WispV2Handshake::new(exts))).await,
				};
				match result {
					Ok(x) => x
						.with_required_extensions(&[PasswordProtocolExtension::ID])
						.await
						.map(|_| ()),
					Err(e) => Err(e),
				}
			});

			let exts: Vec<AnyProtocolExtensionBuilder> =
				vec![PasswordProtocolExtensionBuilder::new_client(Some((
					"user".into(),
					password.into(),
				)))
				.into()];
			let client = match mode {
				Mode::X => ClientMux::new_wispx(crx, ctx, WispXOptions::new(exts)).await,
				_ => ClientMux::new(crx, ctx, Some(WispV2Handshake::new(exts))).await,
			};
			// WispX sends the client HELLO from the driver, so it has to run before the
			// server can answer
			let client = client.map(|x| {
				let (mux, fut) = x.with_no_required_extensions();
				(mux, tokio::spawn(fut))
			});
			let server_result = server.await.unwrap();
			assert_eq!(server_result.is_ok(), ok, "{mode:?} {password}");
			if ok {
				let (client, _) = client.unwrap();
				assert!(client
					.get_extension_ids()
					.contains(&PasswordProtocolExtension::ID));
			} else {
				assert!(matches!(
					server_result,
					Err(WispError::PasswordExtensionCredsInvalid)
				));
				match mode {
					// v2 finds out during the handshake
					Mode::V2 | Mode::V1 => assert!(client.is_err()),
					// WispX finds out when the server's CLOSE arrives
					Mode::X => {
						let (_mux, fut) = client.unwrap();
						let result = timeout(Duration::from_secs(5), fut).await.unwrap().unwrap();
						assert!(
							matches!(
								result,
								Err(WispError::ClosedByPeer(
									CloseReason::ExtensionsPasswordAuthFailed
								))
							),
							"{result:?}"
						);
					}
				}
			}
		}
	}
}

// ---- protocol extension packets ----

#[derive(Debug, Clone)]
struct Probe(Arc<AtomicUsize>);

#[async_trait]
impl ProtocolExtension for Probe {
	fn get_id(&self) -> u8 {
		0xF0
	}
	fn get_supported_packets(&self) -> &'static [u8] {
		&[0xF0]
	}
	fn encode(&self) -> Bytes {
		Bytes::new()
	}
	async fn handle_packet(
		&mut self,
		_: u8,
		mut packet: Bytes,
		_: &mut dyn TransportRead,
		write: &mut dyn TransportWrite,
	) -> Result<(), WispError> {
		let value = packet.get_u32_le();
		self.0.fetch_add(value as usize, Ordering::SeqCst);
		if value == 1 {
			// this is a reply, don't answer it
			return Ok(());
		}
		// reply with the same packet type on stream 0
		let mut reply = BytesMut::new();
		reply.extend_from_slice(&[0xF0, 0, 0, 0, 0]);
		reply.extend_from_slice(&1u32.to_le_bytes());
		write.send(reply.freeze()).await
	}
	fn box_clone(&self) -> Box<dyn ProtocolExtension + Sync + Send> {
		Box::new(self.clone())
	}
}

struct ProbeBuilder(Arc<AtomicUsize>);
impl ProtocolExtensionBuilder for ProbeBuilder {
	fn get_id(&self) -> u8 {
		0xF0
	}
	fn build_from_bytes(&mut self, _: Bytes, _: Role) -> Result<AnyProtocolExtension, WispError> {
		Ok(Probe(self.0.clone()).into())
	}
	fn build_to_extension(&mut self, _: Role) -> Result<AnyProtocolExtension, WispError> {
		Ok(Probe(self.0.clone()).into())
	}
}

#[tokio::test]
async fn extension_packets_dont_kill_the_mux() {
	for mode in [Mode::X, Mode::V2] {
		let server_count = Arc::new(AtomicUsize::new(0));
		let client_count = Arc::new(AtomicUsize::new(0));

		let ((crx, ctx), (srx, stx)) = pipe();
		let sc = server_count.clone();
		let server = tokio::spawn(async move {
			let exts: Vec<AnyProtocolExtensionBuilder> = vec![ProbeBuilder(sc).into()];
			let (mux, fut) = match mode {
				Mode::X => ServerMux::new_wispx(srx, stx, WispXOptions::new(exts)).await,
				_ => ServerMux::new(srx, stx, 32, Some(WispV2Handshake::new(exts))).await,
			}
			.unwrap()
			.with_no_required_extensions();
			tokio::spawn(fut);
			mux
		});
		let exts: Vec<AnyProtocolExtensionBuilder> = vec![ProbeBuilder(client_count.clone()).into()];
		let (client, fut) = match mode {
			Mode::X => ClientMux::new_wispx(crx, ctx, WispXOptions::new(exts)).await,
			_ => ClientMux::new(crx, ctx, Some(WispV2Handshake::new(exts))).await,
		}
		.unwrap()
		.with_no_required_extensions();
		tokio::spawn(fut);
		let server = server.await.unwrap();
		spawn_echo(server);

		let stream = client
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		let mut ext = stream.get_protocol_extension_stream();
		ext.send(0xF0, (&5u32.to_le_bytes()[..]).into())
			.await
			.unwrap();
		ext.send(0xF0, (&6u32.to_le_bytes()[..]).into())
			.await
			.unwrap();

		// the connection keeps working afterwards
		echo_roundtrip(stream, 10_000, 1).await;
		let stream = client
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		echo_roundtrip(stream, 10_000, 2).await;

		assert_eq!(server_count.load(Ordering::SeqCst), 11, "{mode:?}");
		assert_eq!(client_count.load(Ordering::SeqCst), 2, "{mode:?}");
	}
}

// ---- multi-threaded runs of the heavy tests, to shake out races ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mt_echo_x() {
	echo_matrix_case(Mode::X, Mode::X, WispProtocolVersion::X).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mt_echo_v2() {
	echo_matrix_case(Mode::V2, Mode::V2, WispProtocolVersion::V2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mt_many_streams_x() {
	let (client, server) = connect(Mode::X, Mode::X, 64 * 1024).await;
	spawn_echo(server);
	let mut tasks = Vec::new();
	for i in 0..300u32 {
		let stream = client
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		tasks.push(tokio::spawn(echo_roundtrip(
			stream,
			100 * 1024 + i as usize,
			i as u8,
		)));
	}
	for task in tasks {
		task.await.unwrap();
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mt_close_keeps_queued_data_x() {
	close_keeps_queued_data_case(Mode::X, Mode::X).await;
}
