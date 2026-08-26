// Consumer throughput for the Apache Pulsar C++ client, matched to
// examples/bench_consumer.rs: receive COUNT messages from the
// auto-feeding bench broker, no acks, clock starting at the first
// message. receiverQueueSize(500) mirrors neutron's flow-credit profile
// (initial grant 500, re-granted in halves).
//
// Build: g++ -O2 -std=c++17 -o cpp_consumer bench_consumer.cc -lpulsar

#include <pulsar/Client.h>
#include <pulsar/ConsoleLoggerFactory.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>

static long env_long(const char* key, long fallback) {
    const char* value = std::getenv(key);
    return value ? std::atol(value) : fallback;
}

int main() {
    long port = env_long("PORT", 6650);
    long count = env_long("COUNT", 200000);
    long size = env_long("SIZE", 100);

    pulsar::ClientConfiguration clientConf;
    clientConf.setLogger(new pulsar::ConsoleLoggerFactory(pulsar::Logger::LEVEL_WARN));

    pulsar::Client client("pulsar://127.0.0.1:" + std::to_string(port), clientConf);

    pulsar::ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(pulsar::ConsumerShared);
    consumerConf.setReceiverQueueSize(500);

    pulsar::Consumer consumer;
    pulsar::Result result = client.subscribe("bench-topic", "bench-sub", consumerConf, consumer);
    if (result != pulsar::ResultOk) {
        std::fprintf(stderr, "subscribe failed: %s\n", pulsar::strResult(result));
        return 1;
    }

    pulsar::Message message;
    result = consumer.receive(message, 30000);
    if (result != pulsar::ResultOk) {
        std::fprintf(stderr, "first receive failed: %s\n", pulsar::strResult(result));
        return 1;
    }

    auto started = std::chrono::steady_clock::now();
    for (long received = 1; received < count; received++) {
        result = consumer.receive(message, 30000);
        if (result != pulsar::ResultOk) {
            std::fprintf(stderr, "receive %ld failed: %s\n", received, pulsar::strResult(result));
            return 1;
        }
    }
    auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

    std::printf(
        "{\"client\":\"pulsar-cpp\",\"bench\":\"consumer\",\"count\":%ld,\"size\":%ld,"
        "\"secs\":%.4f,\"msgs_per_sec\":%.0f}\n",
        count, size, elapsed, (count - 1) / elapsed);

    consumer.close();
    client.close();
    return 0;
}
