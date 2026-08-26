//! Producer throughput: publish COUNT messages of SIZE bytes with at
//! most INFLIGHT unacknowledged sends, awaiting every receipt.
//!
//!   PORT     broker port (default 6650)
//!   COUNT    messages to publish (default 200000)
//!   SIZE     payload bytes (default 100)
//!   INFLIGHT max sends awaiting receipts (default 1000)
//!   BATCH    if > 0, publish in protocol batches of this many messages
//!            (in-flight cap becomes INFLIGHT / BATCH batches)

use futures::StreamExt;
use neutron::ProducerBuilder;

#[derive(Clone)]
struct Payload(Vec<u8>);

impl From<Payload> for Vec<u8> {
    fn from(value: Payload) -> Self {
        value.0
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

    let pulsar = neutron::PulsarBuilder::new()
        .with_config(neutron::PulsarConfig {
            endpoint_url: "pulsar://127.0.0.1".to_string(),
            endpoint_port: port,
        })
        .build()
        .run();

    let producer = ProducerBuilder::new()
        .with_producer_name("neutron-bench")
        .with_topic("bench-topic")
        .connect(&pulsar)
        .await
        .expect("producer connect");

    let payload = Payload(vec![0x6e; size]);
    let batch: usize = env("BATCH", 0);

    let started = std::time::Instant::now();
    let failures = if batch == 0 {
        futures::stream::iter((0..count).map(|_| producer.send(payload.clone())))
            .buffer_unordered(inflight)
            .fold(0usize, |failures, result| async move {
                failures + usize::from(result.is_err())
            })
            .await
    } else {
        let batches = count.div_ceil(batch);
        futures::stream::iter((0..batches).map(|i| {
            let batch_size = batch.min(count - i * batch);
            producer.send_batch(vec![payload.clone(); batch_size])
        }))
        .buffer_unordered((inflight / batch).max(1))
        .fold(0usize, |failures, result| async move {
            failures + usize::from(result.is_err())
        })
        .await
    };
    let elapsed = started.elapsed().as_secs_f64();

    assert_eq!(failures, 0, "{} sends failed", failures);
    println!(
        "{{\"client\":\"neutron\",\"bench\":\"producer\",\"count\":{},\"size\":{},\"inflight\":{},\"batch\":{},\"secs\":{:.4},\"msgs_per_sec\":{:.0}}}",
        count,
        size,
        inflight,
        batch,
        elapsed,
        count as f64 / elapsed
    );
}
