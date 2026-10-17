//! Wisp benchmark.
//!
//! Runs local TCP targets in-process, connects to a Wisp server with one of several client
//! implementations (including the original wisp-mux 6.0 client), and measures a workload.

use std::{
	future::Future,
	net::SocketAddr,
	pin::Pin,
	sync::{
		atomic::{AtomicBool, AtomicU64, Ordering},
		Arc, Mutex,
	},
	time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use http::{header::SEC_WEBSOCKET_PROTOCOL, HeaderValue, Uri};
use serde::Serialize;
use tokio::{
	io::{AsyncReadExt as _, AsyncWriteExt as _},
	net::{TcpListener, TcpStream},
	sync::mpsc,
	time::sleep_until,
};

#[derive(Copy, Clone, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ClientKind {
	/// Original wisp-mux 6.0 client, Wisp v1.
	LegacyV1,
	/// Original wisp-mux 6.0 client, Wisp v2.
	LegacyV2,
	/// New wisp-mux client, Wisp v1.
	V1,
	/// New wisp-mux client, Wisp v2.
	V2,
	/// New wisp-mux client, WispX.
	X,
}

#[derive(Copy, Clone, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Workload {
	/// Streams read from a target that sends as fast as it can.
	Download,
	/// Streams write to a target that discards.
	Upload,
	/// Streams send a message to an echo target and wait for it, repeatedly.
	Rr,
	/// Workers open a stream, send a request, read the whole response, repeat.
	Churn,
	/// Fresh connection each time: time to first response byte.
	Ttfb,
}

#[derive(Parser, Debug)]
struct Cli {
	/// Wisp server URL, e.g. ws://127.0.0.1:4000/
	#[arg(long)]
	url: Uri,
	#[arg(long, value_enum)]
	client: ClientKind,
	#[arg(long, value_enum)]
	workload: Workload,
	/// Wisp connections to open.
	#[arg(long, default_value_t = 1)]
	connections: usize,
	/// Streams (or churn workers) per connection.
	#[arg(long, default_value_t = 8)]
	streams: usize,
	/// Measurement time in seconds.
	#[arg(long, default_value_t = 5.0)]
	duration: f64,
	/// Warmup time in seconds before measuring.
	#[arg(long, default_value_t = 1.0)]
	warmup: f64,
	/// Upload chunk size, rr message size, churn/ttfb request size.
	#[arg(long, default_value_t = 1024)]
	size: usize,
	/// Churn/ttfb response size.
	#[arg(long, default_value_t = 16 * 1024)]
	resp_size: usize,
	/// Iterations for ttfb.
	#[arg(long, default_value_t = 20)]
	iterations: usize,
	/// Add this much one-way latency between client and server, through a local proxy.
	#[arg(long)]
	delay_ms: Option<u64>,
	/// PID of the Wisp server, to report its CPU time.
	#[arg(long)]
	server_pid: Option<u32>,
	/// Print one JSON line instead of text.
	#[arg(long)]
	json: bool,
	/// WispX client: per-stream receive window in bytes.
	#[arg(long, default_value_t = wisp_mux::DEFAULT_WISPX_STREAM_WINDOW)]
	window: u32,
}

trait Rw: AsyncRead + AsyncWrite + Send {}
impl<T: AsyncRead + AsyncWrite + Send> Rw for T {}
type BoxRw = Pin<Box<dyn Rw>>;
type OpenFuture = Pin<Box<dyn Future<Output = Result<BoxRw>> + Send>>;

/// A connected Wisp client of any implementation.
struct Client {
	open: Box<dyn Fn(String, u16) -> OpenFuture + Send + Sync>,
	protocol: String,
}

impl Client {
	async fn open(&self, addr: SocketAddr) -> Result<BoxRw> {
		(self.open)(addr.ip().to_string(), addr.port()).await
	}
}

async fn connect(url: &Uri, kind: ClientKind, window: u32) -> Result<Client> {
	let host = url.host().context("no host in url")?;
	let port = url.port_u16().unwrap_or(80);
	let tcp = TcpStream::connect((host, port)).await?;
	tcp.set_nodelay(true)?;

	let mut builder = tokio_websockets::ClientBuilder::from_uri(url.clone());
	match kind {
		ClientKind::LegacyV2 | ClientKind::V2 => {
			builder =
				builder.add_header(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("wisp-v2"))?;
		}
		ClientKind::X => {
			builder = builder.add_header(
				SEC_WEBSOCKET_PROTOCOL,
				HeaderValue::from_static(wisp_mux::WISPX_WS_PROTOCOL),
			)?;
		}
		ClientKind::LegacyV1 | ClientKind::V1 => {}
	}
	let (ws, _) = builder.connect_on(tcp).await?;

	match kind {
		ClientKind::LegacyV1 | ClientKind::LegacyV2 => {
			use wisp_mux_legacy::{
				packet::StreamType, ws::TokioWebsocketsTransport, ws::TransportExt, ClientMux,
				WispV2Handshake,
			};
			let (rx, tx) = TokioWebsocketsTransport(ws).split_fast();
			let v2 = matches!(kind, ClientKind::LegacyV2).then(|| WispV2Handshake::new(vec![]));
			let (mux, fut) = ClientMux::new(rx, tx, v2)
				.await?
				.with_no_required_extensions();
			tokio::spawn(fut);
			let protocol = format!("legacy, downgraded {}", mux.was_downgraded());
			let mux = Arc::new(mux);
			Ok(Client {
				open: Box::new(move |host, port| {
					let mux = mux.clone();
					Box::pin(async move {
						let stream = mux.new_stream(StreamType::Tcp, host, port).await?;
						Ok(Box::pin(stream.into_async_rw()) as BoxRw)
					})
				}),
				protocol,
			})
		}
		ClientKind::V1 | ClientKind::V2 | ClientKind::X => {
			use wisp_mux::{
				packet::StreamType, ws::TokioWebsocketsTransport, ws::TransportExt, ClientMux,
				WispV2Handshake, WispXOptions,
			};
			let (rx, tx) = TokioWebsocketsTransport(ws).split_fast();
			let result = match kind {
				ClientKind::X => {
					let mut options = WispXOptions::new(vec![]);
					options.stream_window = window;
					ClientMux::new_wispx(rx, tx, options).await?
				}
				ClientKind::V2 => ClientMux::new(rx, tx, Some(WispV2Handshake::new(vec![]))).await?,
				_ => ClientMux::new(rx, tx, None).await?,
			};
			let (mux, fut) = result.with_no_required_extensions();
			tokio::spawn(fut);
			let protocol = format!("{:?}, downgraded {}", mux.protocol(), mux.was_downgraded());
			let mux = Arc::new(mux);
			Ok(Client {
				open: Box::new(move |host, port| {
					let mux = mux.clone();
					Box::pin(async move {
						let stream = mux.new_stream(StreamType::Tcp, host, port).await?;
						Ok(Box::pin(stream.into_async_rw()) as BoxRw)
					})
				}),
				protocol,
			})
		}
	}
}

// ---- local TCP targets ----

async fn listen(handler: impl Fn(TcpStream) + Send + 'static) -> Result<SocketAddr> {
	let listener = TcpListener::bind("127.0.0.1:0").await?;
	let addr = listener.local_addr()?;
	tokio::spawn(async move {
		while let Ok((stream, _)) = listener.accept().await {
			let _ = stream.set_nodelay(true);
			handler(stream);
		}
	});
	Ok(addr)
}

async fn target_source() -> Result<SocketAddr> {
	listen(|mut stream| {
		tokio::spawn(async move {
			let buf = vec![0u8; 64 * 1024];
			while stream.write_all(&buf).await.is_ok() {}
		});
	})
	.await
}

/// Bytes that actually arrived at the upload target, as opposed to bytes the client wrote.
static SINK_RECEIVED: AtomicU64 = AtomicU64::new(0);

async fn target_sink() -> Result<SocketAddr> {
	listen(|mut stream| {
		tokio::spawn(async move {
			let mut buf = vec![0u8; 64 * 1024];
			while let Ok(n @ 1..) = stream.read(&mut buf).await {
				SINK_RECEIVED.fetch_add(n as u64, Ordering::Relaxed);
			}
		});
	})
	.await
}

async fn target_echo() -> Result<SocketAddr> {
	listen(|stream| {
		tokio::spawn(async move {
			let (mut r, mut w) = stream.into_split();
			let _ = tokio::io::copy(&mut r, &mut w).await;
		});
	})
	.await
}

/// Reads `req` bytes, writes `resp` bytes, then closes (or, with `keep_open`, waits for the
/// client to close).
async fn target_http(req: usize, resp: usize, keep_open: bool) -> Result<SocketAddr> {
	let response = Bytes::from(vec![b'x'; resp]);
	listen(move |mut stream| {
		let response = response.clone();
		tokio::spawn(async move {
			let mut buf = vec![0u8; req.max(1)];
			if stream.read_exact(&mut buf[..req]).await.is_ok() {
				let _ = stream.write_all(&response).await;
			}
			if keep_open {
				while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
			}
			let _ = stream.shutdown().await;
		});
	})
	.await
}

/// Forwards to `upstream`, delaying every chunk by `delay` in each direction.
async fn delay_proxy(upstream: SocketAddr, delay: Duration) -> Result<SocketAddr> {
	async fn pipe(
		mut r: tokio::net::tcp::OwnedReadHalf,
		mut w: tokio::net::tcp::OwnedWriteHalf,
		delay: Duration,
	) {
		let (tx, mut rx) = mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
		tokio::spawn(async move {
			let mut buf = vec![0u8; 256 * 1024];
			loop {
				let n = r.read(&mut buf).await.unwrap_or(0);
				let _ = tx.send((tokio::time::Instant::now() + delay, buf[..n].to_vec()));
				if n == 0 {
					break;
				}
			}
		});
		while let Some((at, data)) = rx.recv().await {
			sleep_until(at).await;
			if data.is_empty() || w.write_all(&data).await.is_err() {
				let _ = w.shutdown().await;
				break;
			}
		}
	}

	listen(move |client| {
		tokio::spawn(async move {
			let Ok(server) = TcpStream::connect(upstream).await else {
				return;
			};
			let _ = server.set_nodelay(true);
			let (cr, cw) = client.into_split();
			let (sr, sw) = server.into_split();
			tokio::spawn(pipe(cr, sw, delay));
			tokio::spawn(pipe(sr, cw, delay));
		});
	})
	.await
}

// ---- measurement helpers ----

fn process_cpu_seconds(pid: u32) -> Option<f64> {
	let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
	let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
	let utime: f64 = fields.get(11)?.parse().ok()?;
	let stime: f64 = fields.get(12)?.parse().ok()?;
	// SAFETY: sysconf has no preconditions
	#[allow(unsafe_code)]
	let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
	Some((utime + stime) / ticks)
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
	if sorted.is_empty() {
		return 0.0;
	}
	let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
	sorted[idx] as f64 / 1000.0
}

#[derive(Serialize, Default)]
struct Report {
	client: Option<ClientKind>,
	workload: Option<Workload>,
	protocol: String,
	connections: usize,
	streams: usize,
	size: usize,
	resp_size: usize,
	delay_ms: u64,
	seconds: f64,
	/// download/upload; for upload, measured where the data arrives
	mib_per_sec: Option<f64>,
	/// upload: what the client wrote, which exceeds `mib_per_sec` if data was lost
	upload_written_mib_per_sec: Option<f64>,
	/// upload: fraction of everything written that never arrived, after letting data in flight
	/// drain
	upload_lost_fraction: Option<f64>,
	/// rr round trips/s, churn streams/s
	ops_per_sec: Option<f64>,
	p50_ms: Option<f64>,
	p99_ms: Option<f64>,
	/// ttfb phases (medians)
	connect_ms: Option<f64>,
	first_byte_ms: Option<f64>,
	total_ms: Option<f64>,
	server_cpu_seconds: Option<f64>,
	/// CPU used by this process (client and local targets) while measuring.
	bench_cpu_seconds: Option<f64>,
	/// Server CPU per unit of work: per GiB moved, or per 1000 operations.
	server_cpu_ms_per_unit: Option<f64>,
	errors: u64,
	/// churn: responses that ended before all their data arrived
	truncated: u64,
}

struct Shared {
	measuring: AtomicBool,
	stop: AtomicBool,
	bytes: AtomicU64,
	ops: AtomicU64,
	errors: AtomicU64,
	/// churn: responses that ended early
	truncated: AtomicU64,
	latencies: Mutex<Vec<u64>>,
}

#[tokio::main]
async fn main() -> Result<()> {
	let cli = Cli::parse();

	let mut url = cli.url.clone();
	if let Some(delay) = cli.delay_ms {
		let host = url.host().context("no host")?;
		let upstream = tokio::net::lookup_host((host, url.port_u16().unwrap_or(80)))
			.await?
			.next()
			.context("unresolvable host")?;
		let proxy = delay_proxy(upstream, Duration::from_millis(delay)).await?;
		url = format!("ws://{proxy}{}", url.path()).parse()?;
	}

	let mut report = Report {
		client: Some(cli.client),
		workload: Some(cli.workload),
		connections: cli.connections,
		streams: cli.streams,
		size: cli.size,
		resp_size: cli.resp_size,
		delay_ms: cli.delay_ms.unwrap_or(0),
		..Default::default()
	};

	if let Workload::Ttfb = cli.workload {
		run_ttfb(&cli, &url, &mut report).await?;
	} else {
		run_load(&cli, &url, &mut report).await?;
	}

	if cli.json {
		println!("{}", serde_json::to_string(&report)?);
	} else {
		print_report(&report);
	}
	Ok(())
}

async fn run_ttfb(cli: &Cli, url: &Uri, report: &mut Report) -> Result<()> {
	// keep the target open: the original client loses data that arrives together with a CLOSE
	let target = target_http(cli.size, cli.resp_size, true).await?;
	let request = vec![b'r'; cli.size];
	let (mut connect, mut first, mut total) = (Vec::new(), Vec::new(), Vec::new());
	let cpu_before = cli.server_pid.and_then(process_cpu_seconds);

	for _ in 0..cli.iterations {
		let start = Instant::now();
		let client = connect_client(url, cli.client, cli.window).await?;
		let connected = Instant::now();
		let mut stream = client.open(target).await?;
		stream.write_all(&request).await?;
		stream.flush().await?;
		let mut byte = [0u8; 1];
		stream.read_exact(&mut byte).await?;
		let done = Instant::now();
		report.protocol.clone_from(&client.protocol);

		connect.push((connected - start).as_micros() as u64);
		first.push((done - connected).as_micros() as u64);
		total.push((done - start).as_micros() as u64);
	}

	for x in [&mut connect, &mut first, &mut total] {
		x.sort_unstable();
	}
	report.connect_ms = Some(percentile(&connect, 0.5));
	report.first_byte_ms = Some(percentile(&first, 0.5));
	report.total_ms = Some(percentile(&total, 0.5));
	report.server_cpu_seconds = cpu_before
		.zip(cli.server_pid.and_then(process_cpu_seconds))
		.map(|(a, b)| b - a);
	Ok(())
}

async fn connect_client(url: &Uri, kind: ClientKind, window: u32) -> Result<Client> {
	connect(url, kind, window).await.context("failed to connect")
}

async fn run_load(cli: &Cli, url: &Uri, report: &mut Report) -> Result<()> {
	let target = match cli.workload {
		Workload::Download => target_source().await?,
		Workload::Upload => target_sink().await?,
		Workload::Rr => target_echo().await?,
		Workload::Churn => target_http(cli.size, cli.resp_size, false).await?,
		Workload::Ttfb => unreachable!(),
	};

	let shared = Arc::new(Shared {
		measuring: AtomicBool::new(false),
		stop: AtomicBool::new(false),
		bytes: AtomicU64::new(0),
		ops: AtomicU64::new(0),
		errors: AtomicU64::new(0),
		truncated: AtomicU64::new(0),
		latencies: Mutex::new(Vec::new()),
	});

	let mut tasks = Vec::new();
	for _ in 0..cli.connections {
		let client = Arc::new(connect_client(url, cli.client, cli.window).await?);
		report.protocol.clone_from(&client.protocol);
		for _ in 0..cli.streams {
			let client = client.clone();
			let shared = shared.clone();
			let (workload, size, resp_size) = (cli.workload, cli.size, cli.resp_size);
			tasks.push(tokio::spawn(async move {
				if let Err(err) = worker(&client, target, workload, size, resp_size, &shared).await {
					if !shared.stop.load(Ordering::Relaxed) {
						eprintln!("worker error: {err:?}");
						shared.errors.fetch_add(1, Ordering::Relaxed);
					}
				}
			}));
		}
	}

	tokio::time::sleep(Duration::from_secs_f64(cli.warmup)).await;
	let bytes_before = shared.bytes.load(Ordering::Relaxed);
	let sink_before = SINK_RECEIVED.load(Ordering::Relaxed);
	let ops_before = shared.ops.load(Ordering::Relaxed);
	let truncated_before = shared.truncated.load(Ordering::Relaxed);
	let cpu_before = cli.server_pid.and_then(process_cpu_seconds);
	let self_before = process_cpu_seconds(std::process::id());
	let start = Instant::now();
	shared.measuring.store(true, Ordering::Relaxed);

	tokio::time::sleep(Duration::from_secs_f64(cli.duration)).await;

	shared.measuring.store(false, Ordering::Relaxed);
	let elapsed = start.elapsed().as_secs_f64();
	let mut bytes = shared.bytes.load(Ordering::Relaxed) - bytes_before;
	if let Workload::Upload = cli.workload {
		// count what arrived, not what was written: a server may drop data
		report.upload_written_mib_per_sec =
			Some(bytes as f64 / start.elapsed().as_secs_f64() / (1024.0 * 1024.0));
		bytes = SINK_RECEIVED.load(Ordering::Relaxed) - sink_before;
	}
	let ops = shared.ops.load(Ordering::Relaxed) - ops_before;
	let cpu = cpu_before
		.zip(cli.server_pid.and_then(process_cpu_seconds))
		.map(|(a, b)| b - a);
	report.bench_cpu_seconds = self_before
		.zip(process_cpu_seconds(std::process::id()))
		.map(|(a, b)| b - a);
	shared.stop.store(true, Ordering::Relaxed);
	if let Workload::Upload = cli.workload {
		// wait for data in flight to arrive, then compare totals
		let mut last = SINK_RECEIVED.load(Ordering::Relaxed);
		for _ in 0..30 {
			tokio::time::sleep(Duration::from_millis(300)).await;
			let now = SINK_RECEIVED.load(Ordering::Relaxed);
			if now == last {
				break;
			}
			last = now;
		}
		let written = shared.bytes.load(Ordering::Relaxed);
		report.upload_lost_fraction = Some(1.0 - last as f64 / written.max(1) as f64);
	}
	// rr/churn workers stop at the next operation and hand in their latencies
	let _ = tokio::time::timeout(Duration::from_millis(500), async {
		for task in &mut tasks {
			let _ = task.await;
		}
	})
	.await;
	for task in &tasks {
		task.abort();
	}

	report.seconds = elapsed;
	report.errors = shared.errors.load(Ordering::Relaxed);
	report.truncated = shared.truncated.load(Ordering::Relaxed) - truncated_before;
	report.server_cpu_seconds = cpu;
	match cli.workload {
		Workload::Download | Workload::Upload => {
			report.mib_per_sec = Some(bytes as f64 / elapsed / (1024.0 * 1024.0));
			report.server_cpu_ms_per_unit =
				cpu.map(|cpu| cpu * 1000.0 / (bytes as f64 / (1024.0 * 1024.0 * 1024.0)));
		}
		Workload::Rr | Workload::Churn => {
			report.ops_per_sec = Some(ops as f64 / elapsed);
			report.server_cpu_ms_per_unit = cpu.map(|cpu| cpu * 1000.0 / (ops as f64 / 1000.0));
			let mut latencies = std::mem::take(&mut *shared.latencies.lock().unwrap());
			latencies.sort_unstable();
			report.p50_ms = Some(percentile(&latencies, 0.5));
			report.p99_ms = Some(percentile(&latencies, 0.99));
		}
		Workload::Ttfb => unreachable!(),
	}
	Ok(())
}

async fn worker(
	client: &Client,
	target: SocketAddr,
	workload: Workload,
	size: usize,
	resp_size: usize,
	shared: &Shared,
) -> Result<()> {
	let mut latencies = Vec::new();
	let ret = async {
		match workload {
			Workload::Download => {
				let mut stream = client.open(target).await?;
				let mut buf = vec![0u8; 64 * 1024];
				loop {
					let n = stream.read(&mut buf).await?;
					if n == 0 {
						bail!("download stream ended");
					}
					shared.bytes.fetch_add(n as u64, Ordering::Relaxed);
				}
			}
			Workload::Upload => {
				let mut stream = client.open(target).await?;
				let buf = vec![0u8; size];
				while !shared.stop.load(Ordering::Relaxed) {
					stream.write_all(&buf).await?;
					stream.flush().await?;
					shared.bytes.fetch_add(size as u64, Ordering::Relaxed);
				}
				// keep the stream open while the rest drains to the target
				std::future::pending::<()>().await;
				Ok(())
			}
			Workload::Rr => {
				let mut stream = client.open(target).await?;
				let msg = vec![b'p'; size];
				let mut buf = vec![0u8; size];
				loop {
					let start = Instant::now();
					stream.write_all(&msg).await?;
					stream.flush().await?;
					stream.read_exact(&mut buf).await?;
					shared.ops.fetch_add(1, Ordering::Relaxed);
					if shared.measuring.load(Ordering::Relaxed) {
						latencies.push(start.elapsed().as_micros() as u64);
					}
					if shared.stop.load(Ordering::Relaxed) {
						return Ok(());
					}
					if latencies.len() >= 256 {
						shared.latencies.lock().unwrap().append(&mut latencies);
					}
				}
			}
			Workload::Churn => {
				let request = vec![b'r'; size];
				let mut buf = vec![0u8; 64 * 1024];
				loop {
					let start = Instant::now();
					let mut stream = client.open(target).await?;
					stream.write_all(&request).await?;
					stream.flush().await?;
					let mut received = 0;
					loop {
						let n = stream.read(&mut buf).await?;
						if n == 0 {
							break;
						}
						received += n;
					}
					drop(stream);
					if received != resp_size {
						// a truncated response is a failure, not a fast success
						if shared.measuring.load(Ordering::Relaxed) {
							shared.truncated.fetch_add(1, Ordering::Relaxed);
						}
						if shared.stop.load(Ordering::Relaxed) {
							return Ok(());
						}
						continue;
					}
					shared.ops.fetch_add(1, Ordering::Relaxed);
					if shared.measuring.load(Ordering::Relaxed) {
						latencies.push(start.elapsed().as_micros() as u64);
					}
					if shared.stop.load(Ordering::Relaxed) {
						return Ok(());
					}
					if latencies.len() >= 256 {
						shared.latencies.lock().unwrap().append(&mut latencies);
					}
				}
			}
			Workload::Ttfb => unreachable!(),
		}
	}
	.await;
	shared.latencies.lock().unwrap().append(&mut latencies);
	ret
}

fn print_report(r: &Report) {
	println!(
		"{:?} {:?} [{}] conns={} streams={} size={} delay={}ms",
		r.client.unwrap(),
		r.workload.unwrap(),
		r.protocol,
		r.connections,
		r.streams,
		r.size,
		r.delay_ms
	);
	if let Some(x) = r.mib_per_sec {
		println!("  throughput: {x:.1} MiB/s");
	}
	if let Some(x) = r.upload_written_mib_per_sec {
		println!("  written by client: {x:.1} MiB/s");
	}
	if let Some(x) = r.upload_lost_fraction {
		println!("  never arrived: {:.2}% of all data written", x * 100.0);
	}
	if let Some(x) = r.ops_per_sec {
		println!(
			"  rate: {x:.0} ops/s, p50 {:.2} ms, p99 {:.2} ms",
			r.p50_ms.unwrap_or(0.0),
			r.p99_ms.unwrap_or(0.0)
		);
	}
	if let Some(x) = r.total_ms {
		println!(
			"  ttfb median: connect {:.2} ms + first byte {:.2} ms = {x:.2} ms",
			r.connect_ms.unwrap_or(0.0),
			r.first_byte_ms.unwrap_or(0.0)
		);
	}
	if let Some(cpu) = r.server_cpu_seconds {
		print!("  server cpu: {cpu:.2} s");
		if let Some(per) = r.server_cpu_ms_per_unit {
			match r.workload.unwrap() {
				Workload::Download | Workload::Upload => print!(" ({per:.0} ms per GiB)"),
				_ => print!(" ({per:.1} ms per 1000 ops)"),
			}
		}
		println!();
	}
	if let Some(cpu) = r.bench_cpu_seconds {
		println!("  bench (client + targets) cpu: {cpu:.2} s");
	}
	if r.errors > 0 {
		println!("  errors: {}", r.errors);
	}
	if r.truncated > 0 {
		println!("  truncated responses: {}", r.truncated);
	}
}
