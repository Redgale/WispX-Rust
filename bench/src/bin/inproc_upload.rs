//! In-process upload check: the original or the new wisp-mux client sending to the original or
//! the new server multiplexor over an in-memory pipe. Isolates the multiplexors.
//!
//! `inproc-upload <legacy|new> [streams] [chunk] [legacy|new client]`

use std::{
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
	time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{channel::mpsc, AsyncWriteExt, SinkExt, StreamExt};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
	let args: Vec<String> = std::env::args().collect();
	let new = args.get(1).map(String::as_str) == Some("new");
	let streams: usize = args.get(2).and_then(|x| x.parse().ok()).unwrap_or(1);
	let chunk: usize = args.get(3).and_then(|x| x.parse().ok()).unwrap_or(16384);
	let new_client = args.get(4).map(String::as_str) == Some("new");

	let (c2s_tx, c2s_rx) = mpsc::channel::<Bytes>(64);
	let (s2c_tx, s2c_rx) = mpsc::channel::<Bytes>(64);
	let total = Arc::new(AtomicU64::new(0));

	// server
	let server_total = total.clone();
	tokio::spawn(async move {
		if new {
			use wisp_mux::{ServerMux, WispError, WispV2Handshake};
			let rx = c2s_rx.map(Ok::<_, WispError>);
			let tx = s2c_tx.sink_map_err(|_| WispError::WsImplSocketClosed);
			let (mux, fut) = ServerMux::new(rx, tx, 128, Some(WispV2Handshake::new(vec![])))
				.await
				.unwrap()
				.with_no_required_extensions();
			tokio::spawn(fut);
			while let Some((_, mut stream)) = mux.wait_for_stream().await {
				let total = server_total.clone();
				tokio::spawn(async move {
					while let Some(Ok(data)) = stream.next().await {
						total.fetch_add(data.len() as u64, Ordering::Relaxed);
					}
				});
			}
		} else {
			use wisp_mux_legacy::{ServerMux, WispError, WispV2Handshake};
			let rx = c2s_rx.map(Ok::<_, WispError>);
			let tx = s2c_tx.sink_map_err(|_| WispError::WsImplSocketClosed);
			let (mux, fut) = ServerMux::new(rx, tx, 128, Some(WispV2Handshake::new(vec![])))
				.await
				.unwrap()
				.with_no_required_extensions();
			tokio::spawn(fut);
			while let Some((_, mut stream)) = mux.wait_for_stream().await {
				let total = server_total.clone();
				tokio::spawn(async move {
					while let Some(Ok(data)) = stream.next().await {
						total.fetch_add(data.len() as u64, Ordering::Relaxed);
					}
				});
			}
		}
	});

	if new_client {
		use wisp_mux::{packet::StreamType, ClientMux, WispError, WispV2Handshake};
		let rx = s2c_rx.map(Ok::<_, WispError>);
		let tx = c2s_tx.sink_map_err(|_| WispError::WsImplSocketClosed);
		let (mux, fut) = ClientMux::new(rx, tx, Some(WispV2Handshake::new(vec![])))
			.await
			.unwrap()
			.with_no_required_extensions();
		tokio::spawn(fut);
		for _ in 0..streams {
			let stream = mux
				.new_stream(StreamType::Tcp, "a".into(), 1)
				.await
				.unwrap();
			tokio::spawn(async move {
				let mut w = stream.into_async_rw();
				let buf = vec![0u8; chunk];
				loop {
					w.write_all(&buf).await.unwrap();
					w.flush().await.unwrap();
				}
			});
		}
		std::mem::forget(mux);
	} else {
		use wisp_mux_legacy::{packet::StreamType, ClientMux, WispError, WispV2Handshake};
		let rx = s2c_rx.map(Ok::<_, WispError>);
		let tx = c2s_tx.sink_map_err(|_| WispError::WsImplSocketClosed);
		let (mux, fut) = ClientMux::new(rx, tx, Some(WispV2Handshake::new(vec![])))
			.await
			.unwrap()
			.with_no_required_extensions();
		tokio::spawn(fut);
		for _ in 0..streams {
			let stream = mux
				.new_stream(StreamType::Tcp, "a".into(), 1)
				.await
				.unwrap();
			tokio::spawn(async move {
				let mut w = stream.into_async_rw();
				let buf = vec![0u8; chunk];
				loop {
					w.write_all(&buf).await.unwrap();
					w.flush().await.unwrap();
				}
			});
		}
		std::mem::forget(mux);
	}

	tokio::time::sleep(Duration::from_millis(500)).await;
	let start = Instant::now();
	let before = total.load(Ordering::Relaxed);
	tokio::time::sleep(Duration::from_secs(3)).await;
	let bytes = total.load(Ordering::Relaxed) - before;
	println!(
		"{} server, {streams} streams, {chunk} B writes: {:.0} MiB/s",
		if new { "new" } else { "legacy" },
		bytes as f64 / start.elapsed().as_secs_f64() / 1048576.0
	);
	std::process::exit(0);
}
