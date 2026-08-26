#!/usr/bin/env python3
"""Summarize bench/results.jsonl: median msgs/sec per config (run 1 is
warmup and excluded), one column per client, with each client's speed
relative to neutron."""
import json
import statistics
import sys
from collections import defaultdict

CLIENTS = ["neutron", "pulsar-cpp", "pulsar-rs"]


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
    header = f"{'bench':<18} {'size':>6}"
    for client in CLIENTS:
        header += f"  {client + ' msg/s':>18}"
    header += "  " + "  ".join(f"{'n/' + c.split('-')[-1]:>7}" for c in CLIENTS[1:])
    print(header)
    for mode, size in matchups:
        medians = {
            client: statistics.median(groups[(mode, size, client)])
            for client in CLIENTS
            if (mode, size, client) in groups
        }
        line = f"{mode:<18} {size:>6}"
        for client in CLIENTS:
            value = medians.get(client)
            line += f"  {value:>18,.0f}" if value else f"  {'-':>18}"
        neutron = medians.get("neutron")
        for client in CLIENTS[1:]:
            other = medians.get(client)
            ratio = f"{neutron / other:.2f}x" if neutron and other else "-"
            line += f"  {ratio:>7}"
        print(line)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "bench/results.jsonl")
