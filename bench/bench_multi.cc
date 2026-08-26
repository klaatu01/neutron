// Mixed workload for the Apache Pulsar C++ client, matched to
// examples/bench_multi.rs: PRODUCERS producer threads and CONSUMERS
// consumer threads over one client (one connection), COUNT messages per
// side split evenly, receipts awaited, batching disabled, no acks.
//
// Build: g++ -O2 -std=c++17 -o cpp_multi bench_multi.cc -lpulsar

#include <pulsar/Client.h>
#include <pulsar/ConsoleLoggerFactory.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

static long env_long(const char* key, long fallback) {
    const char* value = std::getenv(key);
    return value ? std::atol(value) : fallback;
}

int main() {
    long port = env_long("PORT", 6650);
    long count = env_long("COUNT", 200000);
    long size = env_long("SIZE", 100);
    long inflight = env_long("INFLIGHT", 1000);
    long producers = env_long("PRODUCERS", 4);
    long consumers = env_long("CONSUMERS", 4);

    long per_producer = count / producers;
    long per_consumer = count / consumers;
    long inflight_per = inflight / producers;
    if (inflight_per < 1) inflight_per = 1;

    pulsar::ClientConfiguration clientConf;
    clientConf.setLogger(new pulsar::ConsoleLoggerFactory(pulsar::Logger::LEVEL_WARN));
    pulsar::Client client("pulsar://127.0.0.1:" + std::to_string(port), clientConf);

    std::vector<pulsar::Producer> producerHandles(producers);
    for (long i = 0; i < producers; i++) {
        pulsar::ProducerConfiguration producerConf;
        producerConf.setBatchingEnabled(false);
        producerConf.setMaxPendingMessages(static_cast<int>(inflight_per));
        producerConf.setBlockIfQueueFull(true);
        producerConf.setProducerName("multi-producer-" + std::to_string(i));
        pulsar::Result result =
            client.createProducer("bench-topic", producerConf, producerHandles[i]);
        if (result != pulsar::ResultOk) {
            std::fprintf(stderr, "createProducer failed: %s\n", pulsar::strResult(result));
            return 1;
        }
    }

    std::vector<pulsar::Consumer> consumerHandles(consumers);
    for (long i = 0; i < consumers; i++) {
        pulsar::ConsumerConfiguration consumerConf;
        consumerConf.setConsumerType(pulsar::ConsumerShared);
        consumerConf.setReceiverQueueSize(500);
        pulsar::Result result =
            client.subscribe("bench-topic", "bench-sub-" + std::to_string(i), consumerConf,
                             consumerHandles[i]);
        if (result != pulsar::ResultOk) {
            std::fprintf(stderr, "subscribe failed: %s\n", pulsar::strResult(result));
            return 1;
        }
    }

    std::string payload(static_cast<size_t>(size), 'n');
    std::atomic<long> failed{0};

    auto started = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;

    for (long i = 0; i < producers; i++) {
        workers.emplace_back([&, i] {
            std::atomic<long> acked{0};
            std::mutex mutex;
            std::condition_variable done;
            for (long n = 0; n < per_producer; n++) {
                producerHandles[i].sendAsync(
                    pulsar::MessageBuilder().setContent(payload).build(),
                    [&](pulsar::Result sendResult, const pulsar::MessageId&) {
                        if (sendResult != pulsar::ResultOk) failed.fetch_add(1);
                        if (acked.fetch_add(1) + 1 == per_producer) {
                            std::lock_guard<std::mutex> lock(mutex);
                            done.notify_one();
                        }
                    });
            }
            std::unique_lock<std::mutex> lock(mutex);
            done.wait(lock, [&] { return acked.load() == per_producer; });
        });
    }

    for (long i = 0; i < consumers; i++) {
        workers.emplace_back([&, i] {
            pulsar::Message message;
            for (long received = 0; received < per_consumer; received++) {
                pulsar::Result result = consumerHandles[i].receive(message, 60000);
                if (result != pulsar::ResultOk) {
                    std::fprintf(stderr, "receive failed: %s\n", pulsar::strResult(result));
                    failed.fetch_add(1);
                    return;
                }
            }
        });
    }

    for (auto& worker : workers) worker.join();
    auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

    if (failed.load() != 0) {
        std::fprintf(stderr, "%ld operations failed\n", failed.load());
        return 1;
    }
    long sent = per_producer * producers;
    long received = per_consumer * consumers;
    std::printf(
        "{\"client\":\"pulsar-cpp\",\"bench\":\"multi\",\"producers\":%ld,\"consumers\":%ld,"
        "\"sent\":%ld,\"received\":%ld,\"size\":%ld,\"secs\":%.4f,\"msgs_per_sec\":%.0f}\n",
        producers, consumers, sent, received, size, elapsed, (sent + received) / elapsed);

    client.close();
    return 0;
}
