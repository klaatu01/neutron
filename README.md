# Neutron - **This package is currently a Work In Progress and is NOT Production Ready**

An [Apache Pulsar](https://github.com/apache/pulsar) client library, built with pure rust 🦀 and requires no C++ dependencies.

## Features:

- [x] Pure Rust, No C++ Dependencies 🦀
- [x] Consumer Client 📥
- [x] Producer Client 📤
- [x] Plugin Support 🔌
- [x] Multi/Dual Consumer & Producer Support 🤝
- [x] TLS Support via [rustls](https://github.com/rustls/rustls) 🔐
- [x] Async Resolution of Send & Acks 🪓
- [x] Batching Support 📦
- [x] Automatic Reconnection ♻️
- [x] Automatic Operation Retry 🚀

## Architecture

Neutron gives every broker connection its own actor: a reader task and a
writer task over a split frame stream. The writer drains its queue and
flushes once per batch, so syscall cost amortizes as load rises; the
reader routes each inbound frame straight to the owning consumer or
producer's bounded inbox by id. In-flight requests are correlated by the
protocol's own `request_id` (and `(producer_id, sequence_id)` for send
receipts) in a per-connection table where every entry either resolves,
times out, or is failed on teardown — nothing waits forever.

Clients hold a connection *slot* rather than the connection itself: when
a connection dies, a per-broker supervisor re-dials with jittered
backoff, swaps a fresh connection into the slot, and replays each
session (re-subscribe, re-issue flow credit, re-register producers) onto
the same inboxes clients were already reading from. Transient failures
during the gap are retried automatically; keepalive pings detect
half-open connections.

A design review of this architecture (and the dispatch-loop design it
replaced) lives in [`docs/architecture-review.html`](docs/architecture-review.html).
The gap between here and a production-mature client is written down in
[`ROADMAP.md`](ROADMAP.md).

## Installation

**Using Cargo Add**

This will install the newest version of `neutron` into your `cargo.toml`

```bash
cargo add neutron
```

**Manually**

As this is currently in prerelease you **must** use the git ssh address directly.

```toml
neutron = "0.0.2"
```

## Features

The `json` feature provides automatic de/serialization through `serde_json`.

```toml
neutron = { version = "0.0.2", features = ["json"] }
```

## Example

This is a simple example of a consumer that listens to a topic and prints the message. **with the `json` feature enabled**

```rust
use neutron::{ConsumerBuilder, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(dead_code)]
struct Data {
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let pulsar_config = neutron::PulsarConfig {
        endpoint_url: "pulsar://localhost".to_string(),
        endpoint_port: 6650,
    };

    let pulsar = neutron::PulsarBuilder::new()
        .with_config(pulsar_config)
        .build()
        .run();

    let consumer = ConsumerBuilder::new()
        .with_topic("test")
        .with_subscription("test")
        .with_consumer_name("test")
        .connect(&pulsar)
        .await?;


    loop {
        let response: Message<Data> = consumer.next_message().await?;
        log::info!("Received message: {:?}", response.payload);
        consumer.ack(&response.ack).await?;
    }
}
```
