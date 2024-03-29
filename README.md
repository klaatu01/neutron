# Neutron - **This package is currently a Work In Progress and is NOT Production Ready**

An [Apache Pulsar](https://github.com/apache/pulsar) client library, built with pure rust 🦀 and requires no C++ dependencies.
Neutron is built with a focus on:

- Extensability through Plugins.
- Simplicity by reducing the complexity of the internal client.

## Features:

- [x] Pure Rust, No C++ Dependencies 🦀
- [x] Consumer Client 📥
- [x] Producer Client 📤
- [x] Plugin Support 🔌
- [x] Multi/Dual Consumer & Producer Support 🤝
- [x] TLS Support via [rustls](https://github.com/rustls/rustls) 🔐
- [x] Async Resolution of Send & Acks 🪓
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
neutron = { git = "git@github.com/klaatu01/neutron.git" }
```

## Features

The `json` feature provides automatic de/serialization through `serde_json`.

## Usage

There are two ways to use a Consumer or Producer client; As stream/sink or by starting it as a task.
