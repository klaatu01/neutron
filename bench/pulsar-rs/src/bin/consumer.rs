//! Consumer throughput for the pulsar-rs client, matched to
//! examples/bench_consumer.rs: receive COUNT messages from the
//! auto-feeding bench broker, no acks, clock starting at the first
//! message. Flow credit is set to 500 to mirror the other clients.
//!
//!   PORT   broker port (default 6650)
//!   COUNT  messages to receive (default 200000)
//!   SIZE   payload bytes, for the report line only (default 100)

use futures::TryStreamExt;
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};

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

    let addr = format!("pulsar://127.0.0.1:{}", port);
    let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;

    let mut consumer: Consumer<Vec<u8>, _> = client
        .consumer()
        .with_topic("bench-topic")
        .with_consumer_name("pulsar-rs-bench")
        .with_subscription("bench-sub")
        .with_subscription_type(SubType::Shared)
        .with_batch_size(500)
        .build()
        .await?;

    let first = consumer.try_next().await?.expect("first message");
    drop(first);
    let started = std::time::Instant::now();
    for received in 1..count {
        let message = consumer.try_next().await?;
        assert!(message.is_some(), "stream ended at {}", received);
    }
    let elapsed = started.elapsed().as_secs_f64();

    println!(
        "{{\"client\":\"pulsar-rs\",\"bench\":\"consumer\",\"count\":{},\"size\":{},\"secs\":{:.4},\"msgs_per_sec\":{:.0}}}",
        count,
        size,
        elapsed,
        (count - 1) as f64 / elapsed
    );
    Ok(())
}
