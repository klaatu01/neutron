#!/usr/bin/env bash
# Benchmark neutron against the Apache Pulsar C++ client, both talking to
# the same in-process protocol server (examples/bench_broker). Each
# (bench, client, size) config runs RUNS times against a fresh broker;
# the first run is warmup and the summary takes the median of the rest.
#
# Prereqs:
#   cargo build --release --features bench --examples
#   g++ -O2 -std=c++17 -o bench/cpp_producer bench/bench_producer.cc -lpulsar
#   g++ -O2 -std=c++17 -o bench/cpp_consumer bench/bench_consumer.cc -lpulsar
#   (cd bench/pulsar-rs && cargo build --release)
set -euo pipefail

cd "$(dirname "$0")/.."

COUNT="${COUNT:-200000}"
# Batched publishes are ~10x faster; longer runs keep timings meaningful.
BATCHED_COUNT="${BATCHED_COUNT:-1000000}"
RUNS="${RUNS:-4}"
SIZES="${SIZES:-100 4096}"
PORT_BASE="${PORT_BASE:-16700}"
OUT="${OUT:-bench/results.jsonl}"

BROKER=./target/release/examples/bench_broker
NEUTRON_PRODUCER=./target/release/examples/bench_producer
NEUTRON_CONSUMER=./target/release/examples/bench_consumer
CPP_PRODUCER=./bench/cpp_producer
CPP_CONSUMER=./bench/cpp_consumer
RS_PRODUCER=./bench/pulsar-rs/target/release/producer
RS_CONSUMER=./bench/pulsar-rs/target/release/consumer
NEUTRON_MULTI=./target/release/examples/bench_multi
CPP_MULTI=./bench/cpp_multi
RS_MULTI=./bench/pulsar-rs/target/release/multi

: > "$OUT"
port=$PORT_BASE

run_one() { # broker_env client_cmd run_index
    local broker_env="$1" client_cmd="$2" run_index="$3"
    port=$((port + 1))
    env PORT=$port $broker_env "$BROKER" >/tmp/bench_broker.log 2>&1 &
    local broker_pid=$!
    for _ in $(seq 50); do
        grep -q READY /tmp/bench_broker.log 2>/dev/null && break
        sleep 0.1
    done
    local line
    line=$(env PORT=$port COUNT=$run_count INFLIGHT=1000 $client_cmd timeout 300 \
        $client_bin 2>/tmp/bench_client.err) || {
        echo "FAILED: $client_bin ($client_cmd)" >&2
        cat /tmp/bench_client.err >&2
        kill $broker_pid 2>/dev/null || true
        return 1
    }
    kill $broker_pid 2>/dev/null || true
    wait $broker_pid 2>/dev/null || true
    echo "${line%\}},\"run\":$run_index}" >>"$OUT"
    echo "  run $run_index: $line"
}

for size in $SIZES; do
    for mode in "producer" "producer-batched" "consumer" "multi-4x4" "multi-8x8" "multi-32x32"; do
        run_count=$COUNT
        case "$mode" in
        multi-*)
            # Mixed workloads are a concurrency test, not a bandwidth test.
            if [ "$size" != "100" ]; then continue; fi
            workers=${mode#multi-}; workers=${workers%x*}
            feed=$((run_count / workers))
            configs=(
                "neutron|$NEUTRON_MULTI|SIZE=$size PRODUCERS=$workers CONSUMERS=$workers|FEED_COUNT=$feed FEED_SIZE=$size"
                "pulsar-cpp|$CPP_MULTI|SIZE=$size PRODUCERS=$workers CONSUMERS=$workers|FEED_COUNT=$feed FEED_SIZE=$size"
                "pulsar-rs|$RS_MULTI|SIZE=$size PRODUCERS=$workers CONSUMERS=$workers|FEED_COUNT=$feed FEED_SIZE=$size"
            )
            ;;
        producer)
            configs=(
                "neutron|$NEUTRON_PRODUCER|SIZE=$size BATCH=0|"
                "pulsar-cpp|$CPP_PRODUCER|SIZE=$size BATCHING=0|"
                "pulsar-rs|$RS_PRODUCER|SIZE=$size BATCH=0|"
            )
            ;;
        producer-batched)
            # Batching is the small-message optimization; benchmark it there.
            if [ "$size" != "100" ]; then continue; fi
            run_count=$BATCHED_COUNT
            configs=(
                "neutron|$NEUTRON_PRODUCER|SIZE=$size BATCH=500|"
                "pulsar-cpp|$CPP_PRODUCER|SIZE=$size BATCHING=1|"
                "pulsar-rs|$RS_PRODUCER|SIZE=$size BATCH=500|"
            )
            ;;
        consumer)
            configs=(
                "neutron|$NEUTRON_CONSUMER|SIZE=$size|FEED_COUNT=$run_count FEED_SIZE=$size"
                "pulsar-cpp|$CPP_CONSUMER|SIZE=$size|FEED_COUNT=$run_count FEED_SIZE=$size"
                "pulsar-rs|$RS_CONSUMER|SIZE=$size|FEED_COUNT=$run_count FEED_SIZE=$size"
            )
            ;;
        esac
        for config in "${configs[@]}"; do
            IFS='|' read -r client client_bin client_env broker_env <<<"$config"
            echo "== $mode / $client / ${size}B"
            for run in $(seq "$RUNS"); do
                run_one "$broker_env" "$client_env" "$run"
            done
        done
    done
done

echo
python3 bench/summarize.py "$OUT"
