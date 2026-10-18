# WispX-Rust

The Rust implementation of [WispX](https://github.com/Redgale/WispX-Protocol), a faster version of the Wisp protocol:
the client and server library, a WispX server, and the epoxy browser client with WispX support.
It's a fork of [epoxy-tls](https://github.com/MercuryWorkshop/epoxy-tls) (multiplexed branch),
whose history it keeps; the first commit is the unmodified upstream snapshot.

Everything here still speaks Wisp V1 and V2: existing clients keep working against the server,
and WispX clients fall back to V2/V1 on servers that don't support WispX.

| Folder | What it is |
| --- | --- |
| [`wisp/`](wisp) | `wisp-mux`, the protocol library: one multiplexor core for both the **client and the server side** of WispX, Wisp V2 and Wisp V1 |
| [`server/`](server) | `epoxy-server`, the **WispX server**, serving V1/V2 clients too |
| [`client/`](client) | the **epoxy client** (Rust compiled to WebAssembly) that browser proxies use, with a `wispx` option; npm: [`wispx-epoxy-tls`](https://www.npmjs.com/package/wispx-epoxy-tls) |
| [`integrations/scramjet/`](integrations/scramjet) | using WispX with Scramjet; its transport is on npm as [`wispx-epoxy-transport`](https://www.npmjs.com/package/wispx-epoxy-transport) |
| [`bench/`](bench) | native and headless-Chrome benchmarks against the original wisp-mux 6.0 and epoxy-tls 2.1.19 |
| [`simple-wisp-client/`](simple-wisp-client) | upstream command-line test client |

**[`WISPX.md`](WISPX.md) has the details:** benchmark results, what changed and why, the upstream
bugs fixed along the way, server configuration, the library API, and build instructions.

Related repositories:

- [WispX-Protocol](https://github.com/Redgale/WispX-Protocol) - the WispX specification (and the Wisp V1/V2 ones)
- [WispX Browser](https://github.com/Redgale/WispX-Browser) - a single-file, service-worker-less Scramjet browser built on the client here

## Quick start

```bash
# server (stable Rust); jemalloc's build fails if the target dir path contains spaces
export CARGO_TARGET_DIR=/tmp/wispx-target
cargo run --release -p epoxy-server                 # ws://127.0.0.1:4000/, WispX on
cargo run --release -p epoxy-server -- --default-config > config.toml   # every option

# library tests
cargo test -p wisp-mux --all-features
```

The browser client needs nightly Rust and the wasm toolchain; see
[`integrations/scramjet/README.md`](integrations/scramjet/README.md#build-the-client) and the
[client readme](client/README.md).

## npm packages

| Package | What it is |
| --- | --- |
| [`wispx-epoxy-tls`](https://www.npmjs.com/package/wispx-epoxy-tls) | the epoxy client from [`client/`](client) |
| [`wispx-epoxy-transport`](https://www.npmjs.com/package/wispx-epoxy-transport) | a Scramjet / bare-mux transport with the client bundled in (`npm install wispx-epoxy-transport`) |

CI builds and packs both on every push. On a push to `main` it attaches them to a "latest"
GitHub pre-release and publishes each one whose `package.json` version isn't on npm yet, so a
release is: bump `version` in `client/package.json` and/or
`integrations/scramjet/epoxy-transport/package.json`, then push. Publishing needs an npm access
token in the repository secret `NPM_TOKEN`.

## License

As upstream: `wisp-mux` ([`wisp/`](wisp/LICENSE)) is MIT, `epoxy-server` ([`server/`](server/LICENSE))
and the epoxy client ([`client/`](client/LICENSE)) are AGPL-3.0.

Original epoxy-tls and wisp-mux by [@r58Playz](https://github.com/r58Playz) and the
[Mercury Workshop](https://github.com/MercuryWorkshop) contributors.
