//! Mixed workload: PRODUCERS producer tasks and CONSUMERS consumer tasks
//! running concurrently in one process, all multiplexed over the one
//! connection to the broker. Producers publish COUNT messages in total
//! (split evenly, receipts awaited, INFLIGHT split evenly); consumers
//! receive COUNT in total from the auto-feeding broker. The clock runs
//! from after setup until both sides finish; the reported rate counts
//! every message moved (sent + received).
//!
//!   PORT       broker port (default 6650)
//!   COUNT      total messages per side (default 200000)
//!   SIZE       payload bytes (default 100)
//!   INFLIGHT   total sends awaiting receipts (default 1000)
//!   PRODUCERS  producer tasks (default 4)
//!   CONSUMERS  consumer tasks (default 4)

use futures::StreamExt;
use neutron::{ConsumerBuilder, ProducerBuilder};

#[derive(Clone)]
struct Payload(Vec<u8>);

impl From<Payload> for Vec<u8> {
    fn from(value: Payload) -> Self {
        value.0
    }
}

#[derive(Clone)]
struct Raw(#[allow(dead_code)] Vec<u8>);

#[allow(clippy::infallible_try_from)]
impl TryFrom<Vec<u8>> for Raw {
    type Error = std::convert::Infallible;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Raw(value))
    }
}

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let port: u16 = env("PORT", 6650);
    let count: usize = env("COUNT", 200_000);
    let size: usize = env("SIZE", 100);
    let inflight: usize = env("INFLIGHT", 1000);
    let producers: usize = env("PRODUCERS", 4);
    let consumers: usize = env("CONSUMERS", 4);

    let pulsar = neutron::PulsarBuilder::new()
        .with_config(neutron::PulsarConfig {
            endpoint_url: "pulsar://127.0.0.1".to_string(),
            endpoint_port: port,
        })
        .build()
        .run();

    let per_producer = count / producers;
    let per_consumer = count / consumers;
    let inflight_per = (inflight / producers).max(1);
    let payload = Payload(vec![0x6e; size]);

    let mut producer_handles = Vec::new();
    for i in 0..producers {
        let producer = ProducerBuilder::new()
            .with_producer_name(&format!("multi-producer-{}", i))
            .with_topic("bench-topic")
            .connect(&pulsar)
            .await
            .expect("producer connect");
        producer_handles.push(producer);
    }

    let mut consumer_handles = Vec::new();
    for i in 0..consumers {
        let consumer = ConsumerBuilder::new()
            .with_topic("bench-topic")
            .with_subscription(&format!("bench-sub-{}", i))
            .with_consumer_name(&format!("multi-consumer-{}", i))
            .connect(&pulsar)
            .await
            .expect("consumer connect");
        consumer_handles.push(consumer);
    }

    let started = std::time::Instant::now();
    let mut tasks = tokio::task::JoinSet::new();

    for producer in producer_handles {
        let payload = payload.clone();
        tasks.spawn(async move {
            let failures =
                futures::stream::iter((0..per_producer).map(|_| producer.send(payload.clone())))
                    .buffer_unordered(inflight_per)
                    .fold(0usize, |failures, result| async move {
                        failures + usize::from(result.is_err())
                    })
                    .await;
            assert_eq!(failures, 0, "{} sends failed", failures);
        });
    }

    for consumer in consumer_handles {
        tasks.spawn(async move {
            for _ in 0..per_consumer {
                let message: neutron::Message<Raw> =
                    consumer.next_message().await.expect("message");
                drop(message);
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
        "{{\"client\":\"neutron\",\"bench\":\"multi\",\"producers\":{},\"consumers\":{},\"sent\":{},\"received\":{},\"size\":{},\"secs\":{:.4},\"msgs_per_sec\":{:.0}}}",
        producers,
        consumers,
        sent,
        received,
        size,
        elapsed,
        (sent + received) as f64 / elapsed
    );
}
