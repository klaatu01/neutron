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
- [ ] Automatic Reconnection ♻️
- [ ] Automatic Operation Retry 🚀

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
