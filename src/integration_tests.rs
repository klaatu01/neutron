//! End-to-end tests over real TCP against the in-process fake broker.
//! These exercise the whole path: dial, handshake, lookup, routing by
//! consumer/producer id, correlation by request id, and backpressure.

use std::time::Duration;

use crate::fake_broker::{BrokerEvent, FakeBroker};
use crate::{ConsumerBuilder, ProducerBuilder};

#[derive(Debug, Clone, PartialEq)]
struct Data(String);

impl TryFrom<Vec<u8>> for Data {
    type Error = crate::NeutronError;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Data(String::from_utf8(value).map_err(|_| {
            crate::NeutronError::DeserializationFailed
        })?))
    }
}

impl From<Data> for Vec<u8> {
    fn from(value: Data) -> Self {
        value.0.into_bytes()
    }
}

async fn within<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), fut)
        .await
        .expect("test step timed out")
}

#[tokio::test]
async fn consumer_subscribes_receives_and_acks() {
    let broker = FakeBroker::start().await;
    let pulsar = crate::PulsarBuilder::new()
        .with_config(broker.config())
        .build()
        .run();

    let consumer = within(
        ConsumerBuilder::new()
            .with_topic("test-topic")
            .with_subscription("test-sub")
            .with_consumer_name("consumer-a")
            .connect(&pulsar),
    )
    .await
    .unwrap();

    within(broker.expect_event(BrokerEvent::Connect)).await;
    within(broker.expect_event(BrokerEvent::Lookup {
        topic: "test-topic".into(),
    }))
    .await;
    let subscribed = within(broker.wait_for(|e| matches!(e, BrokerEvent::Subscribe { .. }))).await;
    assert_eq!(
        subscribed,
        BrokerEvent::Subscribe {
            consumer_id: consumer.consumer_id(),
            topic: "test-topic".into(),
            subscription: "test-sub".into(),
        }
    );
    // The initial flow grant, coupled to inbox capacity.
    let flowed = within(broker.wait_for(|e| matches!(e, BrokerEvent::Flow { .. }))).await;
    assert_eq!(
        flowed,
        BrokerEvent::Flow {
            consumer_id: consumer.consumer_id(),
            permits: 500
        }
    );

    broker.push_message(consumer.consumer_id(), 1, b"hello_world");
    let message: crate::Message<Data> = within(consumer.next_message()).await.unwrap();
    assert_eq!(message.payload, Data("hello_world".into()));

    within(consumer.ack(&message.ack)).await.unwrap();
    let acked = within(broker.wait_for(|e| matches!(e, BrokerEvent::Ack { .. }))).await;
    assert!(matches!(
        acked,
        BrokerEvent::Ack { consumer_id, .. } if consumer_id == consumer.consumer_id()
    ));
}

#[tokio::test]
async fn messages_route_to_the_owning_consumer() {
    let broker = FakeBroker::start().await;
    let pulsar = crate::PulsarBuilder::new()
        .with_config(broker.config())
        .build()
        .run();

    let consumer_a = within(
        ConsumerBuilder::new()
            .with_topic("topic-a")
            .with_subscription("sub")
            .with_consumer_name("a")
            .connect(&pulsar),
    )
    .await
    .unwrap();
    let consumer_b = within(
        ConsumerBuilder::new()
            .with_topic("topic-b")
            .with_subscription("sub")
            .with_consumer_name("b")
            .connect(&pulsar),
    )
    .await
    .unwrap();

    // Both consumers share one connection; delivery must split by id.
    broker.push_message(consumer_b.consumer_id(), 1, b"for-b");
    broker.push_message(consumer_a.consumer_id(), 2, b"for-a");

    let got_a: crate::Message<Data> = within(consumer_a.next_message()).await.unwrap();
    let got_b: crate::Message<Data> = within(consumer_b.next_message()).await.unwrap();
    assert_eq!(got_a.payload, Data("for-a".into()));
    assert_eq!(got_b.payload, Data("for-b".into()));
}

#[tokio::test]
async fn producer_send_resolves_its_receipt() {
    let broker = FakeBroker::start().await;
    let pulsar = crate::PulsarBuilder::new()
        .with_config(broker.config())
        .build()
        .run();

    let producer = within(
        ProducerBuilder::new()
            .with_producer_name("producer-a")
            .with_topic("topic")
            .connect(&pulsar),
    )
    .await
    .unwrap();

    within(producer.send(Data("payload".into()))).await.unwrap();
    let sent = within(broker.wait_for(|e| matches!(e, BrokerEvent::Send { .. }))).await;
    assert!(matches!(sent, BrokerEvent::Send { sequence_id: 0, .. }));
}

/// Two acks in flight at once, answered in reverse order: each waiter
/// still receives its own response. Under type-keyed correlation this
/// exact interleaving misdelivered receipts.
#[tokio::test]
async fn interleaved_acks_resolve_to_their_own_requests() {
    let broker = FakeBroker::start().await;
    let pulsar = crate::PulsarBuilder::new()
        .with_config(broker.config())
        .build()
        .run();

    let consumer_a = within(
        ConsumerBuilder::new()
            .with_topic("topic-a")
            .with_subscription("sub")
            .with_consumer_name("a")
            .connect(&pulsar),
    )
    .await
    .unwrap();
    let consumer_b = within(
        ConsumerBuilder::new()
            .with_topic("topic-b")
            .with_subscription("sub")
            .with_consumer_name("b")
            .connect(&pulsar),
    )
    .await
    .unwrap();

    broker.push_message(consumer_a.consumer_id(), 1, b"a");
    broker.push_message(consumer_b.consumer_id(), 2, b"b");
    let message_a: crate::Message<Data> = within(consumer_a.next_message()).await.unwrap();
    let message_b: crate::Message<Data> = within(consumer_b.next_message()).await.unwrap();

    broker.hold_acks();
    let consumer_a = std::sync::Arc::new(consumer_a);
    let consumer_b = std::sync::Arc::new(consumer_b);
    let ack_a = tokio::spawn({
        let consumer = consumer_a.clone();
        async move { consumer.ack(&message_a.ack).await }
    });
    let ack_b = tokio::spawn({
        let consumer = consumer_b.clone();
        async move { consumer.ack(&message_b.ack).await }
    });

    // Wait until the broker holds both acks, then answer newest-first.
    within(broker.wait_for(|e| matches!(e, BrokerEvent::Ack { .. }))).await;
    within(broker.wait_for(|e| matches!(e, BrokerEvent::Ack { .. }))).await;
    broker.release_held_acks_reversed();

    within(ack_a).await.unwrap().unwrap();
    within(ack_b).await.unwrap().unwrap();
}

/// A lookup that names another broker moves the client there: the
/// subscription must land on the owning broker's connection, not the one
/// that answered the lookup.
#[tokio::test]
async fn lookup_moves_the_client_to_the_owning_broker() {
    let owner = FakeBroker::start().await;
    let seed = FakeBroker::start().await;
    seed.advertise(&owner.service_url());

    let pulsar = crate::PulsarBuilder::new()
        .with_config(seed.config())
        .build()
        .run();

    let consumer = within(
        ConsumerBuilder::new()
            .with_topic("moved-topic")
            .with_subscription("sub")
            .with_consumer_name("mover")
            .connect(&pulsar),
    )
    .await
    .unwrap();

    // The seed broker answered the first lookup only.
    within(seed.expect_event(BrokerEvent::Connect)).await;
    within(seed.expect_event(BrokerEvent::Lookup {
        topic: "moved-topic".into(),
    }))
    .await;

    // The owner got the connection, the authoritative lookup, and the
    // subscription.
    within(owner.expect_event(BrokerEvent::Connect)).await;
    let subscribed = within(owner.wait_for(|e| matches!(e, BrokerEvent::Subscribe { .. }))).await;
    assert_eq!(
        subscribed,
        BrokerEvent::Subscribe {
            consumer_id: consumer.consumer_id(),
            topic: "moved-topic".into(),
            subscription: "sub".into(),
        }
    );

    // Delivery flows over the owner's connection.
    owner.push_message(consumer.consumer_id(), 1, b"delivered");
    let message: crate::Message<Data> = within(consumer.next_message()).await.unwrap();
    assert_eq!(message.payload, Data("delivered".into()));
}

/// The crown test of supervision: kill the connection at the socket and
/// the supervisor must reconnect, re-handshake, replay the subscription
/// and flow credit, and resume delivery into the same inbox the consumer
/// was already blocked on.
#[tokio::test]
async fn consumer_recovers_after_connection_loss() {
    let broker = FakeBroker::start().await;
    let pulsar = crate::PulsarBuilder::new()
        .with_config(broker.config())
        .build()
        .run();

    let consumer = within(
        ConsumerBuilder::new()
            .with_topic("resilient-topic")
            .with_subscription("sub")
            .with_consumer_name("survivor")
            .connect(&pulsar),
    )
    .await
    .unwrap();

    // Drain the initial session's events.
    within(broker.expect_event(BrokerEvent::Connect)).await;
    within(broker.wait_for(|e| matches!(e, BrokerEvent::Flow { .. }))).await;

    // Park the consumer on its inbox, then cut the wire.
    let consumer = std::sync::Arc::new(consumer);
    let blocked = tokio::spawn({
        let consumer = consumer.clone();
        async move {
            consumer
                .next_message()
                .await
                .map(|message: crate::Message<Data>| message.payload)
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    broker.kill_connections();

    // The supervisor re-establishes and replays the whole session.
    within(broker.expect_event(BrokerEvent::Connect)).await;
    let resubscribed =
        within(broker.wait_for(|e| matches!(e, BrokerEvent::Subscribe { .. }))).await;
    assert_eq!(
        resubscribed,
        BrokerEvent::Subscribe {
            consumer_id: consumer.consumer_id(),
            topic: "resilient-topic".into(),
            subscription: "sub".into(),
        }
    );
    let reflowed = within(broker.wait_for(|e| matches!(e, BrokerEvent::Flow { .. }))).await;
    assert_eq!(
        reflowed,
        BrokerEvent::Flow {
            consumer_id: consumer.consumer_id(),
            permits: 500
        }
    );

    // Delivery resumes into the very same blocked call.
    broker.push_message(consumer.consumer_id(), 7, b"survived");
    let payload = within(blocked).await.unwrap().unwrap();
    assert_eq!(payload, Data("survived".into()));

    // The recovered session acks end to end.
    broker.push_message(consumer.consumer_id(), 8, b"again");
    let message: crate::Message<Data> = within(consumer.next_message()).await.unwrap();
    within(consumer.ack(&message.ack)).await.unwrap();
}

/// A publish that lands in the outage window is retried against the
/// replacement connection instead of surfacing a transient error.
#[tokio::test]
async fn producer_send_retries_across_reconnect() {
    let broker = FakeBroker::start().await;
    let pulsar = crate::PulsarBuilder::new()
        .with_config(broker.config())
        .build()
        .run();

    let producer = within(
        ProducerBuilder::new()
            .with_producer_name("persistent")
            .with_topic("topic")
            .connect(&pulsar),
    )
    .await
    .unwrap();
    within(broker.wait_for(|e| matches!(e, BrokerEvent::Producer { .. }))).await;

    broker.kill_connections();
    // Publish immediately into the outage; the retry loop must carry it
    // over the reconnect (same sequence id, so broker-side dedup holds).
    within(producer.send(Data("through-the-gap".into())))
        .await
        .unwrap();

    within(broker.expect_event(BrokerEvent::Connect)).await;
    let replayed = within(broker.wait_for(|e| matches!(e, BrokerEvent::Producer { .. }))).await;
    assert!(matches!(
        replayed,
        BrokerEvent::Producer { producer_id, .. } if producer_id == producer.producer_id()
    ));
    let sent = within(broker.wait_for(|e| matches!(e, BrokerEvent::Send { .. }))).await;
    assert!(matches!(sent, BrokerEvent::Send { sequence_id: 0, .. }));
}
