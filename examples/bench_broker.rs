//! Standalone bench server: serves the crate's fake broker over TCP so
//! any Pulsar client implementation can be benchmarked against the same
//! lightweight protocol endpoint.
//!
//!   PORT        listen port (default 6650)
//!   FEED_COUNT  if set, auto-feed this many messages to consumers,
//!               paced by their FLOW credit
//!   FEED_SIZE   payload bytes for auto-fed messages (default 100)

use neutron::fake_broker::FakeBroker;

#[tokio::main]
async fn main() {
    env_logger::init();
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(6650);

    let broker = FakeBroker::start_on(port).await;
    broker.quiet();

    if let Some(count) = std::env::var("FEED_COUNT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        let size: usize = std::env::var("FEED_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(100);
        broker.auto_feed(size, count);
        println!("READY {} feeding {}x{}B", broker.port, count, size);
    } else {
        println!("READY {}", broker.port);
    }

    // Serve until killed.
    futures::future::pending::<()>().await;
}
