//! Mixed workload for the pulsar-rs client, matched to
//! examples/bench_multi.rs: PRODUCERS producer tasks and CONSUMERS
//! consumer tasks over one client (one connection), COUNT messages per
//! side split evenly, receipts awaited, no batching, no acks.

use futures::stream::{FuturesUnordered, StreamExt};
use futures::TryStreamExt;
use pulsar::error::{ConnectionError, ProducerError};
use pulsar::{Consumer, Error, Pulsar, SubType, TokioExecutor};

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();
    let port: u16 = env("PORT", 6650);
    let count: usize = env("COUNT", 200_000);
    let size: usize = env("SIZE", 100);
    let inflight: usize = env("INFLIGHT", 1000);
    let producers: usize = env("PRODUCERS", 4);
    let consumers: usize = env("CONSUMERS", 4);

    let per_producer = count / producers;
    let per_consumer = count / consumers;
    let inflight_per = (inflight / producers).max(1);

    let addr = format!("pulsar://127.0.0.1:{}", port);
    let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor)
        .with_outbound_channel_size(2 * inflight)
        .build()
        .await?;

    let mut producer_handles = Vec::new();
    for i in 0..producers {
        let producer = client
            .producer()
            .with_topic("bench-topic")
            .with_name(format!("multi-producer-{}", i))
            .build()
            .await?;
        producer_handles.push(producer);
    }

    let mut consumer_handles: Vec<Consumer<Vec<u8>, _>> = Vec::new();
    for i in 0..consumers {
        let consumer = client
            .consumer()
            .with_topic("bench-topic")
            .with_consumer_name(format!("multi-consumer-{}", i))
            .with_subscription(format!("bench-sub-{}", i))
            .with_subscription_type(SubType::Shared)
            .with_batch_size(500)
            .build()
            .await?;
        consumer_handles.push(consumer);
    }

    let payload = vec![0x6eu8; size];

    let started = std::time::Instant::now();
    let mut tasks = tokio::task::JoinSet::new();

    for mut producer in producer_handles {
        let payload = payload.clone();
        tasks.spawn(async move {
            let mut receipts = FuturesUnordered::new();
            for _ in 0..per_producer {
                while receipts.len() >= inflight_per {
                    let result: Result<pulsar::CommandSendReceipt, _> =
                        receipts.next().await.unwrap();
                    assert!(result.is_ok(), "send failed: {:?}", result.err());
                }
                let receipt = loop {
                    match producer.send_non_blocking(payload.clone()).await {
                        Ok(receipt) => break receipt,
                        Err(Error::Producer(ProducerError::Connection(
                            ConnectionError::SlowDown,
                        ))) => {
                            tokio::task::yield_now().await;
                        }
                        Err(e) => panic!("send failed: {:?}", e),
                    }
                };
                receipts.push(receipt);
            }
            while let Some(result) = receipts.next().await {
                assert!(result.is_ok(), "send failed: {:?}", result.err());
            }
        });
    }

    for mut consumer in consumer_handles {
        tasks.spawn(async move {
            for received in 0..per_consumer {
                let message = consumer.try_next().await.expect("receive failed");
                assert!(message.is_some(), "stream ended at {}", received);
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.expect("worker task");
    }
    let elapsed = started.elapsed().as_secs_f64();

    let sent = per_producer * producers;
    let received = per_consumer * consumers;
    println!(
        "{{\"client\":\"pulsar-rs\",\"bench\":\"multi\",\"producers\":{},\"consumers\":{},\"sent\":{},\"received\":{},\"size\":{},\"secs\":{:.4},\"msgs_per_sec\":{:.0}}}",
        producers,
        consumers,
        sent,
        received,
        size,
        elapsed,
        (sent + received) as f64 / elapsed
    );
    Ok(())
}
