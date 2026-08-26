//! Consumer throughput: receive COUNT messages of SIZE bytes from an
//! auto-feeding bench broker (delivery paced by the client's own FLOW
//! credit). The clock starts at the first message, so connection and
//! subscription setup are excluded.
//!
//!   PORT   broker port (default 6650)
//!   COUNT  messages to receive (default 200000)
//!   SIZE   payload bytes, for the report line only (default 100)

use neutron::ConsumerBuilder;

#[derive(Clone)]
struct Raw(#[allow(dead_code)] Vec<u8>);

// The consumer bound is TryFrom, so an error type is required even for
// a conversion that cannot fail.
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

    let pulsar = neutron::PulsarBuilder::new()
        .with_config(neutron::PulsarConfig {
            endpoint_url: "pulsar://127.0.0.1".to_string(),
            endpoint_port: port,
        })
        .build()
        .run();

    let consumer = ConsumerBuilder::new()
        .with_topic("bench-topic")
        .with_subscription("bench-sub")
        .with_consumer_name("neutron-bench")
        .connect(&pulsar)
        .await
        .expect("consumer connect");

    let first: neutron::Message<Raw> = consumer.next_message().await.expect("first message");
    drop(first);
    let started = std::time::Instant::now();
    for _ in 1..count {
        let message: neutron::Message<Raw> = consumer.next_message().await.expect("message");
        drop(message);
    }
    let elapsed = started.elapsed().as_secs_f64();

    println!(
        "{{\"client\":\"neutron\",\"bench\":\"consumer\",\"count\":{},\"size\":{},\"secs\":{:.4},\"msgs_per_sec\":{:.0}}}",
        count,
        size,
        elapsed,
        (count - 1) as f64 / elapsed
    );
}
