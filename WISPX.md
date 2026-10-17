# WispX and the rewritten wisp-mux / epoxy-server

This workspace is epoxy-tls (multiplexed branch) with:

- **WispX**, a faster Wisp protocol (spec: [`../protocolX.md`](../protocolX.md)).
- **A rewritten `wisp-mux` core** that speaks Wisp v1, v2 and WispX, with the same public API.
- **epoxy-server** changes to serve WispX and to relay streams with fewer copies and syscalls.
- **epoxy-client** (the browser library used by Scramjet's epoxy transport): a new opt-in
  `wispx` option. See [`../scramjet-integration`](../scramjet-integration/README.md).
- **`bench/`**: a benchmark that drives the original wisp-mux 6.0 client (vendored as
  `wisp-mux-legacy`) or the new client against any server.

`git log` shows every change on top of the unmodified upstream snapshot (first commit).

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
  queue. The writer drains everything queued, encodes it into one allocation and flushes once
  per batch; v1/v2 messages are split out of that allocation without copies. Upstream locked
  the websocket for every single packet, from every stream task.
- Opening a stream is synchronous (an ID plus a queued CONNECT) instead of a round trip
  through an actor task and a oneshot channel.
- Received data is a zero-copy slice of the websocket message.

### Server

- Selects WispX when offered, and answers with exactly one subprotocol token. Upstream echoes
  the whole header, which RFC 6455 forbids when several tokens are offered.
- TCP relay: reads go straight into a reusable `BytesMut` and are queued as frozen slices.
  Upstream went through `BufReader` -> `tokio::io::copy` -> compat layers -> an `AsyncWrite`
  that copied every read into a new packet. Uploads queued for a stream go out in one vectored
  write. WispX streams half-close properly (origin EOF -> FIN, client FIN -> `shutdown`).
- The websocket flush threshold is raised so batches go out in one syscall.

### Upstream bugs fixed along the way

These were all found by tests or benchmarks and have regression tests where possible:

1. **The upstream server silently drops upload data.** When a stream's 128-packet channel is
   full, `try_send` fails and the packet is discarded. Deployed clients ignore flow control
   on the path epoxy uses, so this happens whenever a client uploads faster than the
   destination accepts: in the benchmark, **about 25% of a fast single-stream upload never
   reached the destination**. The new server never drops data. A v1/v2 client that ignores
   CONTINUE is slowed down instead (the server stops reading that connection while a stream
   has more than 4 MiB unread).
2. **The upstream client discards data that arrived before a CLOSE.** `MuxStreamRead` returns
   end-of-stream as soon as the channel is disconnected, even with data still queued, which
   truncates responses whose last data and CLOSE arrive together. In the benchmark this
   affected about 9% of short request/response streams on the unmodified server. The new
   client delivers everything that arrived. Because existing clients still have this bug, the
   new server holds a v1/v2 CLOSE for up to `v2_close_delay_ms` (20 ms) after the stream's last
   data. Without that, its faster writes made the old client's bug far more frequent.
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

## Building

The server and benchmark build on stable Rust. jemalloc's build fails if the target directory
path contains spaces, so use a `CARGO_TARGET_DIR` without them:

```bash
export CARGO_TARGET_DIR=/tmp/epoxy-target
cargo build --release -p epoxy-server -p wisp-bench
cargo test -p wisp-mux --all-features
```

The browser client needs nightly plus the wasm toolchain; see `client/README.md`.

## Server configuration

New options in the `[wisp]` section (`epoxy-server --default-config` prints them all):

| option | default | meaning |
|---|---|---|
| `wispx` | `true` | Serve WispX to clients that offer the `wispx` subprotocol (needs `wisp_v2`). |
| `wispx_stream_window` | 256 KiB | Initial upload window per stream. |
| `wispx_max_stream_window` | 16 MiB | Upload windows grow up to this while the destination keeps up. |
| `v2_close_delay_ms` | 20 | See upstream bug 2 above. |

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
`MuxStreamWrite::finish()` (half-close), `MuxStreamCloser::closed()` (wait for an abort), and
`get_stream_type()`. On WispX, `Sink::poll_close` / `AsyncWrite::poll_close` half-close the
stream instead of closing it.

## Running the benchmarks

```bash
python3 bench/run_matrix.py BASELINE_SERVER NEW_SERVER target/release/wisp-bench results.jsonl
python3 bench/summarize.py results.jsonl
```

`run_matrix.py` starts both servers with default configs, pins them and the benchmark to
separate halves of the CPU cores, and runs each case 3 times.
