#!/usr/bin/env python3
"""Summarize run_matrix.py output as markdown tables (medians of the repetitions).

Usage: summarize.py RESULTS.jsonl
"""

import json
import statistics
import sys
from collections import OrderedDict

BASELINES = {
    "v1": "baseline server + original client (v1)",
    "v2": "baseline server + original client (v2)",
}


def metric(report):
    """(value, unit, higher_is_better)"""
    if report.get("mib_per_sec") is not None:
        return report["mib_per_sec"], "MiB/s", True
    if report.get("ops_per_sec") is not None:
        return report["ops_per_sec"], "ops/s", True
    if report.get("total_ms") is not None:
        return report["total_ms"], "ms", False
    return None, "", True


def main():
    rows = OrderedDict()
    for line in open(sys.argv[1]):
        report = json.loads(line)
        rows.setdefault(report["workload_name"], OrderedDict()).setdefault(report["config"], []).append(report)

    for workload, configs in rows.items():
        medians = {}
        for config, reports in configs.items():
            values = [metric(r)[0] for r in reports if metric(r)[0] is not None]
            cpu = [r["server_cpu_ms_per_unit"] for r in reports if r.get("server_cpu_ms_per_unit")]
            p50 = [r["p50_ms"] for r in reports if r.get("p50_ms") is not None]
            p99 = [r["p99_ms"] for r in reports if r.get("p99_ms") is not None]
            errors = sum(r.get("errors", 0) for r in reports) + sum(1 for r in reports if "failed" in r)
            unit, higher = next((metric(r)[1:] for r in reports if metric(r)[0] is not None), ("", True))
            medians[config] = {
                "value": statistics.median(values) if values else None,
                "cpu": statistics.median(cpu) if cpu else None,
                "p50": statistics.median(p50) if p50 else None,
                "p99": statistics.median(p99) if p99 else None,
                "errors": errors,
                "unit": unit,
                "higher": higher,
                "protocol": reports[-1].get("protocol", ""),
            }

        base = medians.get(BASELINES["v2"], {}).get("value")
        print(f"\n### {workload}\n")
        has_lat = any(m["p50"] is not None for m in medians.values())
        has_cpu = any(m["cpu"] is not None for m in medians.values())
        header = "| setup | result | vs baseline v2 |"
        sep = "|---|---:|---:|"
        if has_lat:
            header += " p50 / p99 latency |"
            sep += "---:|"
        if has_cpu:
            header += " server CPU per unit |"
            sep += "---:|"
        print(header)
        print(sep)
        for config, m in medians.items():
            if m["value"] is None:
                print(f"| {config} | failed | | |")
                continue
            value = f"{m['value']:.0f} {m['unit']}" if m["unit"] != "ms" else f"{m['value']:.1f} ms"
            if base:
                ratio = m["value"] / base if m["higher"] else base / m["value"]
                vs = f"{ratio:.2f}x"
            else:
                vs = ""
            line = f"| {config} | {value} | {vs} |"
            if has_lat:
                line += f" {m['p50']:.2f} / {m['p99']:.2f} ms |" if m["p50"] is not None else " |"
            if has_cpu:
                unit = "ms/GiB" if m["unit"] == "MiB/s" else "ms/1k ops"
                line += f" {m['cpu']:.0f} {unit} |" if m["cpu"] is not None else " |"
            if m["errors"]:
                line += f" errors: {m['errors']}"
            print(line)


if __name__ == "__main__":
    main()
