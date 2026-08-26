//! Producer throughput for the pulsar-rs client, matched to
//! examples/bench_producer.rs: COUNT messages of SIZE bytes with at most
//! INFLIGHT unacknowledged sends, awaiting every receipt.
//!
//!   PORT     broker port (default 6650)
//!   COUNT    messages to publish (default 200000)
//!   SIZE     payload bytes (default 100)
//!   INFLIGHT max sends awaiting receipts (default 1000)
//!   BATCH    if > 0, enable client batching with this max batch size

use futures::stream::{FuturesUnordered, StreamExt};
use pulsar::error::{ConnectionError, ProducerError};
use pulsar::{producer, Error, Pulsar, TokioExecutor};

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
    let batch: u32 = env("BATCH", 0);

    let addr = format!("pulsar://127.0.0.1:{}", port);
    // The default outbound channel holds 100 frames and rejects with
    // SlowDown beyond that; size it to the harness's in-flight cap so all
    // three clients run the same 1000-deep pipeline.
    let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor)
        .with_outbound_channel_size(2 * inflight)
        .build()
        .await?;

    let mut options = producer::ProducerOptions::default();
    if batch > 0 {
        options.batch_size = Some(batch);
    }

    let mut producer = client
        .producer()
        .with_topic("bench-topic")
        .with_name("pulsar-rs-bench")
        .with_options(options)
        .build()
        .await?;

    let payload = vec![0x6eu8; size];

    let started = std::time::Instant::now();
    let mut receipts = FuturesUnordered::new();
    let mut failures = 0usize;
    for _ in 0..count {
        while receipts.len() >= inflight {
            if let Some(result) = receipts.next().await {
                failures += usize::from(matches!(result, Err(_)));
            }
        }
        // pulsar-rs's documented backpressure: SlowDown means try again.
        let receipt = loop {
            match producer.send_non_blocking(payload.clone()).await {
                Ok(receipt) => break receipt,
                Err(Error::Producer(ProducerError::Connection(ConnectionError::SlowDown))) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(e),
            }
        };
        receipts.push(receipt);
    }
    while let Some(result) = receipts.next().await {
        failures += usize::from(matches!(result, Err(_)));
    }
    let elapsed = started.elapsed().as_secs_f64();

    assert_eq!(failures, 0, "{} sends failed", failures);
    println!(
        "{{\"client\":\"pulsar-rs\",\"bench\":\"producer\",\"count\":{},\"size\":{},\"inflight\":{},\"batch\":{},\"secs\":{:.4},\"msgs_per_sec\":{:.0}}}",
        count,
        size,
        inflight,
        batch,
        elapsed,
        count as f64 / elapsed
    );
    Ok(())
}
