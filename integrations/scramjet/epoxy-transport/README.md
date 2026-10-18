# wispx-epoxy-transport

A proxy transport (for Scramjet, bare-mux and anything else that takes a `ProxyTransport`) that runs
TLS in the browser with [wispx-epoxy-tls](https://www.npmjs.com/package/wispx-epoxy-tls) and talks
[WispX](https://github.com/Redgale/WispX-Protocol) to the Wisp server, falling back to Wisp V2/V1 on servers without
it. It's [epoxy-transport](https://github.com/MercuryWorkshop/epoxy-transport) 3.0.1 with a `wispx`
option and the WispX build of the epoxy client bundled in (nothing else to install).

```sh
npm install wispx-epoxy-transport
```

## Usage

```js
import EpoxyTransport from "wispx-epoxy-transport";

const transport = new EpoxyTransport({
	wisp: "wss://your-server.example/",
	wisp_v2: true,
	wispx: true,
});
await transport.init();

// Scramjet's controller
const controller = new Controller({ serviceworker, transport });
```

There's also a non-ESM build that sets a global `EpoxyTransport` (the module; the class is its
`default`):

```html
<script src="https://unpkg.com/wispx-epoxy-transport@1/dist/index.js"></script>
<script>
	const transport = new EpoxyTransport.default({ wisp: "wss://your-server.example/", wisp_v2: true, wispx: true });
</script>
```

Only enable `wispx` for servers that answer the websocket subprotocol they're offered, such as
epoxy-server from [WispX-Rust](https://github.com/Redgale/WispX-Rust) (or upstream epoxy-server, which then speaks V2).
A server that ignores `Sec-WebSocket-Protocol` makes the browser reject the websocket; the same is
true of the `wisp_v2` option.

Options: `wisp` (server url), `wisp_v2`, `wispx`, `udp_extension_required`, `title_case_headers`,
`ws_title_case_headers`, `wisp_ws_protocols`, `redirect_limit`, `header_limit`, `buffer_size`.

## Building

```sh
# in WispX-Rust: build and pack the client first
(cd ../../../client && npm pack)
npm install --no-save ../../../client/wispx-epoxy-tls-*.tgz   # or let npm fetch it from the registry
npm run build      # dist/
```

## Copyright

Licensed under the GNU AGPL v3 ([`LICENSE`](LICENSE)). Original epoxy-transport by Mercury Workshop.
