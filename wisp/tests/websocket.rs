//! Runs the multiplexor over real tokio-websockets streams.
#![cfg(feature = "tokio-websockets")]

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::time::timeout;
use wisp_mux::{
	packet::StreamType,
	ws::{TokioWebsocketsTransport, TransportExt},
	ClientMux, ServerMux, WispV2Handshake, WispXOptions,
};

async fn run(x: bool) {
	let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
	let server_ws = tokio_websockets::ServerBuilder::new()
		// like epoxy-server: flush once per batch instead of every 8 KiB
		.config(tokio_websockets::Config::default().flush_threshold(256 * 1024))
		.serve(server_io);
	let client_ws = tokio_websockets::ClientBuilder::new().take_over(client_io);

	tokio::spawn(async move {
		let (rx, tx) = TokioWebsocketsTransport(server_ws).split_fast();
		let result = if x {
			ServerMux::new_wispx(rx, tx, WispXOptions::new(vec![])).await
		} else {
			ServerMux::new(rx, tx, 128, Some(WispV2Handshake::new(vec![]))).await
		};
		let (mux, fut) = result.unwrap().with_no_required_extensions();
		tokio::spawn(fut);
		while let Some((_, stream)) = mux.wait_for_stream().await {
			tokio::spawn(async move {
				let (mut rx, mut tx) = stream.into_split();
				while let Some(Ok(data)) = rx.next().await {
					if tx.send(data).await.is_err() {
						break;
					}
				}
			});
		}
	});

	let (rx, tx) = TokioWebsocketsTransport(client_ws).split_fast();
	let result = if x {
		ClientMux::new_wispx(rx, tx, WispXOptions::new(vec![])).await
	} else {
		ClientMux::new(rx, tx, Some(WispV2Handshake::new(vec![]))).await
	};
	let (mux, fut) = result.unwrap().with_no_required_extensions();
	tokio::spawn(fut);

	// many streams with small messages: lots of tiny websocket messages queued per flush
	let mut tasks = Vec::new();
	for i in 0..128u32 {
		let mut stream = mux
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		tasks.push(tokio::spawn(async move {
			for j in 0..50u32 {
				let msg = Bytes::from(format!("{i}:{j}"));
				stream.send(msg.clone()).await.unwrap();
				let echo = timeout(Duration::from_secs(10), stream.next())
					.await
					.unwrap()
					.unwrap()
					.unwrap();
				assert_eq!(echo, msg);
			}
		}));
	}
	for task in tasks {
		task.await.unwrap();
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_small_streams_over_websocket_v2() {
	run(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_small_streams_over_websocket_x() {
	run(true).await;
}
