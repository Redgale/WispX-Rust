#!/usr/bin/env python3
"""Run the wisp-bench matrix against a baseline and a new epoxy-server.

Usage: run_matrix.py BASELINE_SERVER NEW_SERVER WISP_BENCH OUT.jsonl [--quick]

Both servers are started with default configs (log level WARN) on ports 4000 (baseline) and
4001 (new), pinned to the first half of the physical cores; the benchmark runs on the other half.
Each case runs REPS times and every run is written to OUT.jsonl.
"""

import json
import os
import subprocess
import sys
import tempfile
import time

REPS = 3


def cpu_sets():
    """Split physical cores in half: (server cpus, bench cpus)."""
    siblings = {}
    for cpu in range(os.cpu_count()):
        path = f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
        try:
            with open(path) as f:
                siblings.setdefault(f.read().strip(), []).append(cpu)
        except OSError:
            siblings.setdefault(str(cpu), []).append(cpu)
    cores = sorted(siblings.values())
    half = len(cores) // 2
    flat = lambda xs: ",".join(str(c) for core in xs for c in core)
    return flat(cores[:half]), flat(cores[half:])


def start_server(binary, port, cpus, workdir):
    config = subprocess.run([binary, "--default-config"], capture_output=True, text=True, check=True).stdout
    config = config.replace('"127.0.0.1:4000"', f'"127.0.0.1:{port}"', 1)
    config = config.replace('log_level = "INFO"', 'log_level = "WARN"')
    path = os.path.join(workdir, f"server-{port}.toml")
    with open(path, "w") as f:
        f.write(config)
    log = open(os.path.join(workdir, f"server-{port}.log"), "w")
    proc = subprocess.Popen(["taskset", "-c", cpus, binary, path], stdout=log, stderr=log)
    time.sleep(1)
    return proc


# (label, port, client)
CONFIGS = [
    ("baseline server + original client (v1)", 4000, "legacy-v1"),
    ("baseline server + original client (v2)", 4000, "legacy-v2"),
    ("new server + original client (v1)", 4001, "legacy-v1"),
    ("new server + original client (v2)", 4001, "legacy-v2"),
    ("new server + new client (v2)", 4001, "v2"),
    ("new server + new client (WispX)", 4001, "x"),
]

# (name, args)
WORKLOADS = [
    ("download, 1 stream", ["--workload", "download", "--streams", "1"]),
    ("download, 16 streams", ["--workload", "download", "--streams", "16"]),
    ("upload 16 KiB writes, 1 stream", ["--workload", "upload", "--streams", "1", "--size", "16384"]),
    ("upload 16 KiB writes, 16 streams", ["--workload", "upload", "--streams", "16", "--size", "16384"]),
    ("request/response 1 KiB, 64 streams", ["--workload", "rr", "--streams", "64", "--size", "1024"]),
    ("request/response 64 B, 1 stream", ["--workload", "rr", "--streams", "1", "--size", "64"]),
    ("open+request+close, 32 workers", ["--workload", "churn", "--streams", "32", "--size", "1024", "--resp-size", "16384"]),
    ("open+request+close, 8 conns x 8 workers", ["--workload", "churn", "--connections", "8", "--streams", "8", "--size", "1024", "--resp-size", "16384"]),
    ("new connection to first byte, 50 ms RTT", ["--workload", "ttfb", "--delay-ms", "25", "--iterations", "15", "--size", "512", "--resp-size", "1024"]),
    ("upload 16 KiB writes, 1 stream, 50 ms RTT", ["--workload", "upload", "--streams", "1", "--size", "16384", "--delay-ms", "25", "--warmup", "2"]),
    ("download, 1 stream, 50 ms RTT", ["--workload", "download", "--streams", "1", "--delay-ms", "25", "--warmup", "2"]),
]


def main():
    baseline, new, bench, out = sys.argv[1:5]
    quick = "--quick" in sys.argv
    server_cpus, bench_cpus = cpu_sets()
    workdir = tempfile.mkdtemp(prefix="wisp-matrix-")
    servers = {
        4000: start_server(baseline, 4000, server_cpus, workdir),
        4001: start_server(new, 4001, server_cpus, workdir),
    }
    try:
        with open(out, "a") as results:
            for workload, args in WORKLOADS:
                for label, port, client in CONFIGS:
                    for rep in range(1 if quick else REPS):
                        cmd = [
                            "taskset", "-c", bench_cpus, bench,
                            "--url", f"ws://127.0.0.1:{port}/",
                            "--client", client,
                            "--duration", "1" if quick else "3",
                            "--server-pid", str(servers[port].pid),
                            "--json",
                        ] + args
                        if "--warmup" not in args:
                            cmd += ["--warmup", "0.5" if quick else "1"]
                        try:
                            run = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                            report = json.loads(run.stdout.strip().splitlines()[-1])
                        except Exception as err:  # noqa: BLE001
                            report = {"failed": str(err)}
                        report.update({"workload_name": workload, "config": label, "rep": rep})
                        results.write(json.dumps(report) + "\n")
                        results.flush()
                        print(f"{workload} | {label} | rep {rep}: "
                              f"{report.get('mib_per_sec') or report.get('ops_per_sec') or report.get('total_ms')}",
                              flush=True)
    finally:
        for proc in servers.values():
            proc.terminate()
            proc.wait()


if __name__ == "__main__":
    main()
