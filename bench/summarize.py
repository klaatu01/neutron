#!/usr/bin/env python3
"""Summarize bench/results.jsonl: median msgs/sec per config (run 1 is
warmup and excluded), with the neutron : pulsar-cpp ratio per matchup."""
import json
import statistics
import sys
from collections import defaultdict


def mode_of(row):
    if row["bench"] == "consumer":
        return "consumer"
    batched = row.get("batch", 0) or row.get("batching") in (True, "true")
    return "producer-batched" if batched else "producer"


def main(path):
    groups = defaultdict(list)
    for line in open(path):
        row = json.loads(line)
        if row.get("run", 0) == 1:
            continue
        groups[(mode_of(row), row["size"], row["client"])].append(row["msgs_per_sec"])

    matchups = sorted({(mode, size) for (mode, size, _) in groups})
    print(f"{'bench':<18} {'size':>6}  {'neutron msg/s':>14}  {'pulsar-cpp msg/s':>17}  {'ratio':>7}")
    for mode, size in matchups:
        neutron = statistics.median(groups.get((mode, size, "neutron"), [0]))
        cpp = statistics.median(groups.get((mode, size, "pulsar-cpp"), [0]))
        ratio = f"{neutron / cpp:.2f}x" if cpp else "-"
        print(f"{mode:<18} {size:>6}  {neutron:>14,.0f}  {cpp:>17,.0f}  {ratio:>7}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "bench/results.jsonl")
