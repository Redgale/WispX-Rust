# Using WispX with Scramjet

Scramjet and the Scramjet controller never speak Wisp themselves. They hand every request and
websocket to a `ProxyTransport` (`transport.request(...)` / `transport.connect(...)`). With
`@mercuryworkshop/epoxy-transport`, that transport is the epoxy-tls client (Rust compiled to
wasm), which uses `wisp-mux` to talk to the Wisp server. So WispX needs:

1. **The server:** [`server/`](../../server) in this repository. It speaks WispX to clients that ask for
   it, and keeps serving Wisp v1/v2 to every existing client.
2. **The client library:** [`client/`](../../client) in this repository, built to wasm. Its new `wispx`
   option makes it ask for WispX. On a server without WispX it falls back to v2/v1 by itself.
3. **The transport:** epoxy-transport only forwards a fixed list of options to the epoxy client.
   `epoxy-transport-wispx.patch` adds `wispx` to that list; `epoxy-transport/` is the patched
   3.0.1 source.

Scramjet and the controller need no changes.

**The short way:** steps 2 and 3 are on npm as one package, `wispx-epoxy-transport` (the patched
transport with the WispX client bundled in), so only the server needs building:

```bash
npm install wispx-epoxy-transport
```

```js
import EpoxyTransport from "wispx-epoxy-transport";
const transport = new EpoxyTransport({ wisp: "wss://your-server.example/", wisp_v2: true, wispx: true });
```

The rest of this page builds them yourself.

The single-file browser in [WispX Browser](https://github.com/Redgale/WispX-Browser) already has all of
this built in: it uses the WispX client directly (no epoxy-transport), with WispX on by default.

Step 1 on its own already helps: existing clients keep speaking v1/v2 but get the faster server
(see the benchmark results in [`WISPX.md`](../../WISPX.md)). This was tested with the published
epoxy-tls 2.1.19 client in headless Chrome: it works unchanged, 64 KiB HTTP responses were about
26% faster, and HTTPS 5-12% faster (there the browser's TLS work in wasm dominates). Other
clients (libcurl.js, wisp-js) weren't tested.

## Build the client

The epoxy client needs:

- Rust nightly with the `wasm32-unknown-unknown` target and the `rust-src` component
  (`rustup toolchain install nightly --target wasm32-unknown-unknown --component rust-src`)
- `wasm-bindgen` 0.2.100 exactly (`cargo install wasm-bindgen-cli --version 0.2.100`)
- `wasm-opt` from [binaryen](https://github.com/WebAssembly/binaryen/releases)
- `jq`
- a clang that can target wasm32, for the `ring` crate's C code. Distro clang usually works.
  The [WASI SDK](https://github.com/WebAssembly/wasi-sdk/releases) clang works too:
  `CC_wasm32_unknown_unknown=/path/to/wasi-sdk/bin/clang AR_wasm32_unknown_unknown=/path/to/wasi-sdk/bin/llvm-ar`

```bash
cd ../../client
RELEASE=1 bash build.sh   # output in pkg/; `npm pack` builds the full and minimal packages
```

This project's client was built this way (nightly 2026-10-16) and tested in headless Chrome
against both the original and the new server, in v1, v2 and WispX modes, over HTTP and HTTPS.

## Build the patched transport

```bash
cd epoxy-transport
npm install ../../../client/mercuryworkshop-epoxy-tls-*.tgz
npm install
npm run build            # see package.json; produces dist/
```

## Use it with the controller

```js
import EpoxyTransport from "@mercuryworkshop/epoxy-transport"; // the patched build

const transport = new EpoxyTransport({
	wisp: "wss://your-server.example/",
	wisp_v2: true,
	wispx: true,
});
await transport.init();

const controller = new Controller({ serviceworker, transport });
```

Only enable `wispx` for servers that answer the websocket subprotocol they are offered.
epoxy-server (upstream and this version) does. A server that ignores `Sec-WebSocket-Protocol`
makes the browser reject the websocket. The same is already true of epoxy's `wisp_v2` option.
