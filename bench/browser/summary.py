import json, sys, statistics
from collections import OrderedDict
rows = OrderedDict()
for line in open(sys.argv[1]):
    if not line.startswith("{"): continue
    r = json.loads(line)
    server = "original" if r["wisp"].endswith(":4000/") else "new"
    client = "original epoxy" if r["lib"] == "epoxy-orig" else "new epoxy"
    mode = "WispX" if r["wispx"] else ("v2" if r["v2"] else "v1")
    proto = "HTTPS" if r["origin"].startswith("https") else "HTTP"
    size = r["size"]
    sz = f"{size // 1048576} MiB" if size >= 1048576 else f"{size // 1024} KiB"
    key = (f"{proto} {sz} bodies", f"{server} server + {client} ({mode})")
    rows.setdefault(key[0], OrderedDict()).setdefault(key[1], []).append(r)
for workload, configs in rows.items():
    print(f"\n### {workload} (requests/s, median of 2)\n")
    print("| setup | requests/s | vs today's default | correct responses |")
    print("|---|---:|---:|---|")
    base = None
    for setup, rs in configs.items():
        v = statistics.median(x["per_sec"] for x in rs)
        if base is None: base = v
        bad = sum(x["truncated"] + x["errors"] for x in rs)
        total = sum(x["total"] for x in rs)
        print(f"| {setup} | {v:.0f} | {v / base:.2f}x | {'all ' + str(total) if not bad else str(bad) + ' bad of ' + str(total)} |")
