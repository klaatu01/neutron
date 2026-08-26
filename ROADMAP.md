# Maturity roadmap

What separates neutron today from a client you would run in production
without reading the source first. The transport core — connection actors,
request correlation, flow-credit-coupled inboxes, supervision with session
replay — is done and benchmarked; almost everything below is *protocol
surface and policy* layered on top of it, not rework underneath it.

Honesty note carried over from the benchmarks: part of neutron's current
speed edge is work it does not yet do (resend buffering, per-send timers,
per-message metadata). Each tier below flags the items that threaten the
numbers and the design that keeps them.

**Reference points:** the Java client (the feature ceiling),
`pulsar-client-cpp` 4.1, `pulsar-rs` 6.9.

---

## P0 — Can't call it a Pulsar client without these

- [ ] **Partitioned topics.** The single biggest gap: most production
  topics are partitioned, and neutron never even asks. Partition-count
  lookup (`PARTITIONED_METADATA` — the bench broker already answers it),
  a partitioned producer with routing modes (round-robin, single,
  key-hash), and a partitioned consumer that fans one `Consumer` across
  per-partition sub-consumers. The registry/slot design already supports
  many clients per process, so this is composition, not surgery.
- [ ] **Message metadata on the producer path.** `Send` carries a bare
  payload. Real messages need: partition key, ordering key, properties,
  `event_time`, and (P2) `deliver_at`/`deliver_after`. Without a key,
  Key_Shared and key-hash routing are meaningless.
- [ ] **Message metadata on the consumer path.** Expose publish time,
  key, properties, producer name, `redelivery_count`, topic on the
  delivered `Message<T>` — today the payload arrives naked.
- [ ] **Subscription options.** `SubType` is parsed but the builder
  hardcodes Shared. Expose Exclusive / Failover / Key_Shared, initial
  position (Earliest/Latest — `CommandSubscribe.initialPosition` is
  never set today), `durable`, replicate-subscription-state, and send
  the consumer name in `CommandSubscribe` (currently client-side only).
- [ ] **Broker-initiated close.** `CommandCloseProducer` /
  `CommandCloseConsumer` from the broker (topic unload, rebalance) are
  currently logged as unsupported and dropped. They must trigger the
  existing re-lookup → rebind → replay machinery. This is the other half
  of reconnection: brokers *routinely* ask healthy clients to move.
- [ ] **Graceful close APIs.** `Producer::close()` / `Consumer::close()`
  sending `CLOSE_PRODUCER`/`CLOSE_CONSUMER`, plus
  `PulsarManager::shutdown()` that drains writers and stops the actors.
  Today drop just abandons the registry entry.
- [ ] **Negative acks + ack timeout.**
  `CommandRedeliverUnacknowledgedMessages` for `nack(msg)` with a
  configurable delay, and an optional un-acked tracker that auto-requests
  redelivery. At-least-once without redelivery control is a footgun.
  *Perf note:* the tracker is a per-consumer timer wheel, not per-message
  timers — keep it off the reader's hot path.
- [ ] **Resend buffer for true reconnect-safety.** Today a send that was
  queued-but-unreceipted when the connection died is retried by the
  caller-side helper (same sequence id, bounded attempts). Mature
  behavior: the producer keeps every unreceipted message in order and
  replays them itself after the supervisor swaps the connection, before
  new sends. Sequence-id reuse (already in place) plus broker dedup makes
  this effectively-once-capable. *Perf note:* a per-producer VecDeque of
  `Bytes` handles appended at enqueue and popped at receipt — refcount
  bumps, no copies; the coalesced writer is untouched.
- [ ] **Configuration surface.** Operation timeout, keepalive interval,
  queue capacities, consumer permits (hardcoded 250), reconnect backoff
  — all constants today. `PulsarConfig`'s public fields can't grow
  compatibly; add builder-style options (`with_operation_timeout`, …)
  on `PulsarBuilder`/`ConsumerBuilder`/`ProducerBuilder`.
- [ ] **Error taxonomy.** Map wire `ServerError` codes
  (`TopicNotFound`, `AuthorizationError`, `ProducerBusy`,
  `ServiceNotReady`, `TooManyRequests`) into typed variants with correct
  `Retryability` (`ServiceNotReady`/`TooManyRequests` are transient),
  instead of `PulsarError(String)`.
- [ ] **Lookup redirects.** `LookupResponseType::Redirect` is an error
  today; follow it (bounded hops, `authoritative` flag threaded through).

## P1 — Production robustness

- [ ] **Producer send policy.** `send_timeout` (fail receipts that
  outlive it — distinct from the connection's operation timeout),
  max-pending-messages / memory limit, and an explicit
  block-vs-error-when-full choice. *Perf note:* deadlines ride the
  existing per-connection sweeper; no per-message timer allocation.
- [ ] **Automatic producer batching.** `send_batch` exists but is
  manual. Add a batching mode (max messages / max bytes / linger delay)
  inside the producer so `send()` transparently batches — this is where
  the 1.9M msg/s path becomes the default path. Include batch-level
  compression and keys.
- [ ] **Compression.** LZ4 + ZSTD first (what brokers actually run),
  zlib/Snappy after; both directions, feature-gated deps.
- [ ] **Chunking** for messages above `maxMessageSize` (negotiated from
  `CommandConnected.max_message_size` — parsed and discarded today), and
  chunk reassembly on the consumer.
- [ ] **Consumer ergonomics.** Cumulative ack as a real API (the current
  `Vec<Ack>` → cumulative mapping is accidental), batch-index acks,
  `seek` (message id and timestamp), pause/resume, receiver queue size
  configuration, `GET_LAST_MESSAGE_ID` support.
- [ ] **Connection options.** Multi-host service URLs with failover,
  `connections_per_broker > 1` (the slot already abstracts "a
  connection"; make it a small pool for workloads that outgrow one
  socket), listener names, connect timeout.
- [ ] **Auth completeness.** mTLS (client certs through rustls), a
  first-class static/supplier token plugin (today only OAuth2 client
  credentials ships), auth-refresh on `AuthChallenge` verified against a
  real broker.
- [ ] **Producer identity recovery.** Use `ProducerSuccess.
  last_sequence_id` (parsed and ignored) to resume sequences on named
  producers; producer access modes (Exclusive / WaitForExclusive).
- [ ] **Backoff polish.** Reset backoff after a stable-connection
  window; jittered lookup retries on `ServiceNotReady`.

## P2 — Ecosystem features

- [ ] **Reader API** (non-durable cursor from a start message id).
- [ ] **Multi-topic and regex subscriptions** (needs
  `GET_TOPICS_OF_NAMESPACE` + periodic re-discovery).
- [ ] **Dead-letter + retry-letter policy** on max redeliveries.
- [ ] **Delayed delivery** (`deliver_after`/`deliver_at` metadata).
- [ ] **Schema registry.** `GET_SCHEMA`/schema-in-create, with
  bytes/string/JSON first (the `json` feature becomes a schema), Avro
  later. This changes the public `DeserializeMessage`-style traits —
  design before 1.0, even if implementation lands after.
- [ ] **Key_Shared depth.** Sticky hash ranges, allow-out-of-order.
- [ ] **Interceptors & hooks.** Producer-side interceptors to match the
  existing consumer plugins; standardize both.
- [ ] **Observability.** Counters (sends, receipts, redeliveries, queue
  depths, reconnects) behind a metrics feature; the dead `telemetry`
  cfg-attrs in the codec become a real `tracing` feature.
- [ ] **End-to-end encryption** (key reader API, encrypted payload
  envelope).
- [ ] **Transactions** — last: big protocol surface (TC lookup, txn
  commands on both paths), and even the C++ client gained it late.

## P3 — Operational maturity (what makes people trust it)

- [ ] **CI against a real Pulsar.** The fake broker proves client logic;
  a `pulsar:latest` standalone in CI proves protocol truth — the full
  integration suite, plus auth and TLS paths, across the last few broker
  minors.
- [ ] **Codec hardening.** Fuzz `Codec::decode` and the batch parser
  (adversarial input is one compromised broker away); enforce
  `maxMessageSize` on inbound frames to bound allocations.
- [ ] **Soak + chaos tests.** Hours-long runs with the fake broker
  killing connections on a schedule; assert no leaks in the correlation
  table, registry, or inboxes (the invariants exist — test them over
  time).
- [ ] **Benchmark regression CI.** The harness exists (`bench/run.sh`);
  pin it to CI hardware and fail on >10% regressions, so the numbers in
  the architecture review stay true as features land.
- [ ] **API discipline to 1.0.** rustdoc on every public item,
  `#[non_exhaustive]` on error/config enums before freezing, MSRV
  policy, semver plan, examples per feature, and a CHANGELOG.
- [ ] **Docs.** A "concepts" page mapping Pulsar semantics onto
  neutron's model (slots, replay, retryability) — the architecture
  review is the seed.

---

## Keeping the numbers while landing this

The benchmark edge rests on four things: the coalesced writer, the split
reader/writer tasks, `Bytes` end to end, and the id-keyed correlation
table. The features above threaten them in three specific places:

1. **Resend buffer & send timeout** — keep both as per-producer state
   resolved at receipt time (the correlation table already sees every
   receipt); never add work inside the writer's drain loop.
2. **Per-message metadata** — build `MessageMetadata` once at `send()`
   in the caller's task, not in the writer; properties stay optional and
   unallocated when absent (pulsar-rs pays a map per message; don't).
3. **Compression/chunking** — do it in the caller's task before enqueue
   (producer) and after routing (consumer), so connection actors stay
   pure transport.

Run `bench/run.sh` before and after each P0/P1 item; the review's Fig 6
and Fig 7 are the regression baseline.
