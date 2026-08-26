// Producer throughput for the Apache Pulsar C++ client, matched to
// examples/bench_producer.rs: COUNT messages of SIZE bytes, at most
// INFLIGHT unacknowledged sends (maxPendingMessages + blockIfQueueFull),
// per-message receipts, batching disabled unless BATCHING=1.
//
// Build: g++ -O2 -std=c++17 -o cpp_producer bench_producer.cc -lpulsar

#include <pulsar/Client.h>
#include <pulsar/ConsoleLoggerFactory.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>

static long env_long(const char* key, long fallback) {
    const char* value = std::getenv(key);
    return value ? std::atol(value) : fallback;
}

int main() {
    long port = env_long("PORT", 6650);
    long count = env_long("COUNT", 200000);
    long size = env_long("SIZE", 100);
    long inflight = env_long("INFLIGHT", 1000);
    bool batching = env_long("BATCHING", 0) != 0;

    pulsar::ClientConfiguration clientConf;
    clientConf.setLogger(new pulsar::ConsoleLoggerFactory(pulsar::Logger::LEVEL_WARN));

    pulsar::Client client("pulsar://127.0.0.1:" + std::to_string(port), clientConf);

    pulsar::ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(batching);
    if (batching) {
        producerConf.setBatchingMaxMessages(500);
        producerConf.setBatchingMaxPublishDelayMs(1);
        producerConf.setBatchingMaxAllowedSizeInBytes(4 * 1024 * 1024);
    }
    producerConf.setMaxPendingMessages(static_cast<int>(inflight));
    producerConf.setBlockIfQueueFull(true);

    pulsar::Producer producer;
    pulsar::Result result = client.createProducer("bench-topic", producerConf, producer);
    if (result != pulsar::ResultOk) {
        std::fprintf(stderr, "createProducer failed: %s\n", pulsar::strResult(result));
        return 1;
    }

    std::string payload(static_cast<size_t>(size), 'n');

    std::atomic<long> acked{0};
    std::atomic<long> failed{0};
    std::mutex mutex;
    std::condition_variable done;

    auto started = std::chrono::steady_clock::now();
    for (long i = 0; i < count; i++) {
        producer.sendAsync(
            pulsar::MessageBuilder().setContent(payload).build(),
            [&](pulsar::Result sendResult, const pulsar::MessageId&) {
                if (sendResult != pulsar::ResultOk) {
                    failed.fetch_add(1);
                }
                if (acked.fetch_add(1) + 1 == count) {
                    std::lock_guard<std::mutex> lock(mutex);
                    done.notify_one();
                }
            });
    }
    {
        std::unique_lock<std::mutex> lock(mutex);
        done.wait(lock, [&] { return acked.load() == count; });
    }
    auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

    if (failed.load() != 0) {
        std::fprintf(stderr, "%ld sends failed\n", failed.load());
        return 1;
    }
    std::printf(
        "{\"client\":\"pulsar-cpp\",\"bench\":\"producer\",\"count\":%ld,\"size\":%ld,"
        "\"inflight\":%ld,\"batching\":%s,\"secs\":%.4f,\"msgs_per_sec\":%.0f}\n",
        count, size, inflight, batching ? "true" : "false", elapsed, count / elapsed);

    producer.close();
    client.close();
    return 0;
}
