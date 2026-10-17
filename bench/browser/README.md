# Browser benchmark

Runs the real epoxy client (the one Scramjet's epoxy transport uses) in headless Chrome and
fetches through a Wisp server, counting requests per second and short or failed responses.

- `origin.py PORT TLS_PORT`: HTTP and HTTPS origin. `GET /?n=BYTES` answers with a
  close-delimited body (no `Content-Length`), so a truncated response is detectable. The HTTPS
  port needs `cert.pem`/`key.pem` in `$CERT_DIR` (or next to the script), for example from
  `openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 30 -subj /CN=127.0.0.1 -addext subjectAltName=IP:127.0.0.1`.
- `static.py PORT DIR`: serves the test page with the cross-origin isolation headers epoxy's
  shared wasm memory needs.
- `index.html`: the test page. `runTest({ lib, wisp, v2, wispx, total, concurrency, size, origin })`
  imports `./<lib>/full/epoxy-bundled.js`, so put epoxy builds next to it, for example
  `epoxy-orig/` (the published `@mercuryworkshop/epoxy-tls` package) and `epoxy-new/` (with
  `full/` being `client/pkg` from this workspace). It also counts websocket messages, and
  with `window.logFrames = true` lists the WispX frame types in each message.
- `run.mjs '<json array of runTest configs>'`: runs the configs, one JSON result per line.
- `frames.mjs`, `profile.mjs`: frame-type breakdown and a CPU profile of one config.
- `summary.py results.jsonl`: markdown tables.

```bash
npm install
python3 origin.py 8090 8443 &
python3 static.py 8091 site &        # site/ holds index.html, epoxy-orig/, epoxy-new/
CHROME=/path/to/chrome node run.mjs '[{"lib":"epoxy-new","wisp":"ws://127.0.0.1:4001/","v2":true,"wispx":true}]'
```

`results.jsonl` holds the run summarized in `../../WISPX.md`.
