//! In-process throughput check: client and server multiplexors over an in-memory pipe.
//!
//! `cargo run --release --example inproc -- <v2|x> [streams]`

use std::{
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
	time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{channel::mpsc, AsyncReadExt, SinkExt, StreamExt};
use wisp_mux::{
	packet::StreamType, ClientMux, ServerMux, WispError, WispV2Handshake, WispXOptions,
};

#[derive(Default)]
struct Counters {
	messages: AtomicU64,
	bytes: AtomicU64,
}

/// `WINDOW=<bytes>` sets the initial WispX window.
fn x_options() -> WispXOptions {
	let mut options = WispXOptions::new(vec![]);
	if let Some(window) = std::env::var("WINDOW").ok().and_then(|x| x.parse().ok()) {
		options.stream_window = window;
	}
	options
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
	let args: Vec<String> = std::env::args().collect();
	let x = args.get(1).map(String::as_str) == Some("x");
	let streams: usize = args.get(2).and_then(|x| x.parse().ok()).unwrap_or(1);

	let (c2s_tx, c2s_rx) = mpsc::unbounded::<Bytes>();
	let (s2c_tx, s2c_rx) = mpsc::unbounded::<Bytes>();
	let up = Arc::new(Counters::default());
	let down = Arc::new(Counters::default());
	let (up2, down2) = (up.clone(), down.clone());
	let err = |_| WispError::WsImplSocketClosed;
	let crx = s2c_rx.map(move |x: Bytes| {
		down2.messages.fetch_add(1, Ordering::Relaxed);
		down2.bytes.fetch_add(x.len() as u64, Ordering::Relaxed);
		Ok(x)
	});
	let srx = c2s_rx.map(move |x: Bytes| {
		up2.messages.fetch_add(1, Ordering::Relaxed);
		up2.bytes.fetch_add(x.len() as u64, Ordering::Relaxed);
		Ok(x)
	});
	let ctx = c2s_tx.sink_map_err(err);
	let stx = s2c_tx.sink_map_err(err);

	let server = tokio::spawn(async move {
		let result = if x {
			ServerMux::new_wispx(srx, stx, x_options()).await
		} else {
			ServerMux::new(srx, stx, 128, Some(WispV2Handshake::new(vec![]))).await
		};
		let (mux, fut) = result.unwrap().with_no_required_extensions();
		tokio::spawn(fut);
		while let Some((_, stream)) = mux.wait_for_stream().await {
			tokio::spawn(async move {
				let (_rx, mut tx) = stream.into_split();
				let chunk = Bytes::from(vec![0u8; 128 * 1024]);
				while tx.send(chunk.clone()).await.is_ok() {}
			});
		}
	});

	let result = if x {
		ClientMux::new_wispx(crx, ctx, x_options()).await
	} else {
		ClientMux::new(crx, ctx, Some(WispV2Handshake::new(vec![]))).await
	};
	let (mux, fut) = result.unwrap().with_no_required_extensions();
	tokio::spawn(fut);

	let total = Arc::new(AtomicU64::new(0));
	for _ in 0..streams {
		let stream = mux
			.new_stream(StreamType::Tcp, "a".into(), 1)
			.await
			.unwrap();
		let total = total.clone();
		tokio::spawn(async move {
			let mut rw = stream.into_async_rw();
			let mut buf = vec![0u8; 64 * 1024];
			loop {
				let n = rw.read(&mut buf).await.unwrap();
				if n == 0 {
					eprintln!("stream ended early: {:?}", rw.get_close_reason());
					return;
				}
				total.fetch_add(n as u64, Ordering::Relaxed);
			}
		});
	}

	tokio::time::sleep(Duration::from_millis(500)).await;
	let start = Instant::now();
	let (t0, dm0, um0, ub0) = (
		total.load(Ordering::Relaxed),
		down.messages.load(Ordering::Relaxed),
		up.messages.load(Ordering::Relaxed),
		up.bytes.load(Ordering::Relaxed),
	);
	tokio::time::sleep(Duration::from_secs(3)).await;
	let secs = start.elapsed().as_secs_f64();
	let bytes = total.load(Ordering::Relaxed) - t0;
	let dm = down.messages.load(Ordering::Relaxed) - dm0;
	let um = up.messages.load(Ordering::Relaxed) - um0;
	let ub = up.bytes.load(Ordering::Relaxed) - ub0;
	println!(
		"{}: {:.0} MiB/s, down msgs {:.0}/s (avg {:.0} B), up msgs {:.0}/s ({:.0} B/s)",
		if x { "x" } else { "v2" },
		bytes as f64 / secs / 1048576.0,
		dm as f64 / secs,
		bytes as f64 / dm.max(1) as f64,
		um as f64 / secs,
		ub as f64 / secs,
	);
	drop(server);
	std::process::exit(0);
}
