# WispX and the rewritten wisp-mux / epoxy-server

This workspace is epoxy-tls (multiplexed branch) with:

- **WispX**, a faster Wisp protocol (spec: [`../protocolX.md`](../protocolX.md)).
- **A rewritten `wisp-mux` core** that speaks Wisp v1, v2 and WispX, with the same public API.
- **epoxy-server** changes to serve WispX and to relay streams with fewer copies, syscalls and
  messages.
- **epoxy-client** (the browser library behind Scramjet's epoxy transport): a new opt-in
  `wispx` option. See [`../scramjet-integration`](../scramjet-integration/README.md).
- **`bench/`**: native and browser benchmarks, including the original wisp-mux 6.0 client
  (vendored as `wisp-mux-legacy`) and the published epoxy-tls 2.1.19 browser client.

`git log` shows every change on top of the unmodified upstream snapshot (first commit).

## Results

Measured on one 6-core desktop: server and benchmark pinned to separate cores, loopback
network (plus an artificial-latency proxy for the "50 ms RTT" rows). Each value is the median
of 3 runs. Other desktop apps were running, so expect ±10-15% noise between runs. Full tables:
[`bench/results.md`](bench/results.md) (native) and
[`bench/browser/results.md`](bench/browser/results.md) (headless Chrome).

### Server and protocol (native clients)

"Original" is the unmodified epoxy-server and wisp-mux 6.0. Throughput for uploads is measured
where the data arrives.

| workload | original server, original client (v2) | new server, original client (v2) | new server, new client, WispX |
|---|---:|---:|---:|
| download, 16 streams | 1,269 MiB/s | **4,135 MiB/s (3.3x)** | 3,036 MiB/s (2.4x) |
| download, 1 stream | 1,857 MiB/s | 2,069 MiB/s | **4,054 MiB/s (2.2x)** |
| download, 1 stream, 50 ms RTT | 1,702 MiB/s | 2,652 MiB/s | **2,630 MiB/s (1.55x)** |
| upload, 1 stream | 3,392 MiB/s, **22% of data lost** | 3,169 MiB/s | **3,853 MiB/s**, 57% less server CPU |
| upload, 1 stream, 50 ms RTT | 2,435 MiB/s, **10% lost** | 2,499 MiB/s | **2,747 MiB/s** |
| 1 KiB request/response, 64 streams | 135k/s, p99 10.0 ms | 153k/s, p99 6.6 ms | **205k/s (1.52x), p99 0.49 ms** |
| open, request, close; 32 workers | 35k/s, **258 truncated/s** | 9k/s (see below) | **74k/s (2.1x)** |
| new connection to first byte, 50 ms RTT | 159 ms | 159 ms | **107 ms (-33%)** |
| 64 B request/response, 1 stream | 46.2k/s | 44.6k/s | 44.5k/s (-4%) |

Server CPU per GiB downloaded with 16 streams: 1,140 ms (original) vs 591 ms (new server,
original client).

### Browser (Scramjet's epoxy client in headless Chrome)

Compared with today's default (original server, epoxy v1). Every one of the 61,920 requests
returned the correct body in every configuration.

| workload | new server + original epoxy | new server + new epoxy (WispX) |
|---|---:|---:|
| HTTP, 64 KiB bodies | **+26-27%** | +22% |
| HTTPS, 1 KiB bodies | +5-12% | **+13%** |
| HTTPS, 1 MiB bodies | +8-9% | +3% |
| HTTP, 1 KiB bodies | +3-9% | +0% |

In the browser, wisp-mux is about 2% of CPU. The rest is the HTTP stack, TLS in wasm and the
browser itself, so protocol work shows up mostly as fewer round trips on real networks (WispX
saves one per connection), larger transfers, and server capacity.

### Known limitations

- **Original *native* v1/v2 clients get slower short streams (9k/s above).** The original
  wisp-mux client discards data it hasn't read when a CLOSE arrives (upstream bug 2 below).
  The new server's faster, batched writes made that far more frequent, so it holds a v1/v2
  CLOSE for 2 ms after recent data. That costs about 2 ms per stream that closes right after
  its last data. It doesn't apply to browsers (the bug can't trigger there, verified with
  20,000 requests), to clients sending the new drain-on-close extension (the new wisp-mux),
  or to WispX. Set `v2_close_delay_ms = 0` to trade that protection for speed.
- **Single-stream request/response latency is ~4% worse on loopback** (about 1 µs per round
  trip): writes go through the connection's writer task instead of straight to the socket.
  That indirection is what enables batching. A direct-write fast path is possible but has to
  keep frame order exact against the writer; for 1 µs, invisible next to real network round
  trips, it isn't worth that risk.
- **WispX with many parallel streams on loopback is ~20% below v2 mode** of the new client
  (still 2.4x the original). Profiles show no hotspot: the test is bound by memory copies,
  and the difference is the cost of real flow control (senders pause when the receiver is
  behind, where v2 just keeps pushing data into the client).

## What changed and why

### Protocol (WispX)

| v1/v2 | WispX |
|---|---|
| One packet per websocket message | Many length-prefixed frames per message; the writer coalesces whatever is queued |
| v2 handshake needs a CONTINUE round trip before the first stream | No CONTINUE: the client can put CONNECT/DATA right after its HELLO |
| Flow control counts packets; CONTINUE carries an absolute value that races with packets in flight | Byte-based incremental CREDIT, so both sides agree exactly |
| No flow control from server to client | Flow control in both directions, with window auto-tuning |
| No half-close | FIN half-closes one direction |

A WispX client falls back to v2/v1 by itself when the server doesn't select the `wispx`
subprotocol, so it can be pointed at any Wisp server.

### Multiplexor core (all protocol versions)

- One read loop and one write loop per connection. Stream handles push frames into a shared
  queue. The writer drains everything queued and flushes once per batch. Upstream locked the
  websocket for every single packet, from every stream task.
- Zero-copy frames: data buffers can carry `DATA_HEADROOM` (9) scratch bytes in front, where
  the frame header is written, so large writes are sent without copying (server reads from
  the destination this way). Received data is a zero-copy slice of the websocket message.
- Client streams are announced lazily: the CONNECT is sent in the same message as the
  stream's first data (for HTTPS, the TLS ClientHello), and IDs are allocated at that point,
  so they stay in order. Streams dropped unused never reach the wire.
- WispX windows start at 256 KiB, double per turnover up to 4 MiB, then double only when the
  reader had to wait for data, up to 256 MiB per stream and 256 MiB of growth per connection.
- The read loop caches the last stream lookup and yields to the writer when it has queued
  frames (credit updates otherwise wait behind megabytes of reading).

### Server

- Selects WispX when offered, and answers with exactly one subprotocol token. Upstream echoes
  the whole header, which RFC 6455 forbids when several tokens are offered.
- TCP relay: reads go straight into reusable headroom buffers and are sent without copying.
  After each read, a non-blocking peek detects an immediate EOF, so the last data and the FIN
  (or v2 CLOSE) leave in one write. Uploads queued for a stream go out in one vectored write.
  WispX streams half-close properly (origin EOF -> FIN, client FIN -> `shutdown`).
- The websocket flush threshold is raised so batches go out in one syscall.

### Upstream bugs fixed along the way

These were all found by tests or benchmarks and have regression tests where possible:

1. **The upstream server silently drops upload data.** When a stream's 128-packet channel is
   full, `try_send` fails and the packet is discarded. Deployed clients ignore flow control
   on the path epoxy uses, so this happens whenever a client uploads faster than the
   destination accepts: up to 27% of a fast single-stream upload never arrived in the
   benchmarks. The new server never drops data. A v1/v2 client that ignores CONTINUE is
   slowed down instead (the server stops reading that connection while a stream has more
   than 4 MiB unread).
2. **The upstream client discards data that arrived before a CLOSE.** `MuxStreamRead` returns
   end-of-stream as soon as the channel is disconnected, even with data still queued, which
   truncates responses whose last data and CLOSE arrive together: hundreds to thousands of
   truncated responses per second in the benchmarks, on the unmodified server. The new client
   delivers everything that arrived. See the limitation above for how the server protects
   existing native clients.
3. **Any extension packet ends the upstream multiplexor.** An extension-handled packet makes
   the read stream return `None`, which ends the mux (breaks twisp resize and the wispnet
   probe).
4. **The upstream server never flushes CONTINUE packets.** They sit in the websocket buffer
   until something else (such as the 30 s ping) flushes it, so a client that actually waits
   for CONTINUE stalls. The new client doesn't wait for CONTINUE in v1/v2 mode, matching
   deployed clients. WispX has proper flow control.
5. **Dropped streams leak.** A stream dropped without an explicit close stayed in the stream
   map until the connection ended. The new core closes it when its last handle is dropped.
6. **tokio-websockets 0.12.0 panics** (`index out of bounds` in `chunks_vectored`) when more
   than 32 messages are queued before a flush. With `panic = "abort"` this kills the whole
   server. The workspace now requires 0.12.3, which fixes it.

Also: the `twisp` feature doesn't compile against the locked `pty-process` version. That is
unchanged upstream code and wasn't touched.

## Building

The server and benchmarks build on stable Rust. jemalloc's build fails if the target
directory path contains spaces, so use a `CARGO_TARGET_DIR` without them:

```bash
export CARGO_TARGET_DIR=/tmp/epoxy-target
cargo build --release -p epoxy-server -p wisp-bench
cargo test -p wisp-mux --all-features
```

The browser client needs nightly plus the wasm toolchain; see
[`../scramjet-integration/README.md`](../scramjet-integration/README.md).

## Server configuration

New options in the `[wisp]` section (`epoxy-server --default-config` prints them all):

| option | default | meaning |
|---|---|---|
| `wispx` | `true` | Serve WispX to clients that offer the `wispx` subprotocol (needs `wisp_v2`). |
| `wispx_stream_window` | 256 KiB | Initial upload window per stream. |
| `wispx_max_stream_window` | 256 MiB | Upload windows grow up to this while the destination keeps up. |
| `wispx_max_connection_window` | 256 MiB | Limit on window growth across one connection's streams. |
| `v2_close_delay_ms` | 2 | See the first known limitation. |

The length-delimited (non-websocket) transport has no negotiation and stays on Wisp v2.

## Using the library

```rust
// server: after a websocket upgrade where the client offered "wispx"
let (mux, fut) = ServerMux::new_wispx(rx, tx, WispXOptions::new(extensions))
	.await?
	.with_no_required_extensions();

// client: falls back to v2/v1 automatically
let (mux, fut) = ClientMux::new_wispx(rx, tx, WispXOptions::new(extensions))
	.await?
	.with_no_required_extensions();
println!("{:?}", mux.protocol()); // X, V2 or V1
```

`ServerMux::new` / `ClientMux::new` keep their v1/v2 behavior. New stream APIs:
`MuxStreamWrite::finish()` (half-close), `send_with_headroom()` / `send_last_with_headroom()`
(zero-copy sends), `MuxStreamCloser::closed()` (wait for an abort), and `get_stream_type()`.
Behavior changes: on WispX, `Sink::poll_close` / `AsyncWrite::poll_close` half-close the stream
instead of closing it; a client stream reaches the server with its first frame or first read
(`get_stream_id()` forces it).

## Running the benchmarks

```bash
python3 bench/run_matrix.py BASELINE_SERVER NEW_SERVER target/release/wisp-bench results.jsonl
python3 bench/summarize.py results.jsonl
```

`run_matrix.py` starts both servers with default configs, pins them and the benchmark to
separate halves of the CPU cores, and runs each case 3 times. The browser benchmark is
described in [`bench/browser/README.md`](bench/browser/README.md).
