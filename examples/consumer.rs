use neutron::{Message, PulsarConsumerNext};

#[derive(Debug)]
#[allow(dead_code)]
struct Data {
    id: u64,
    name: String,
}

impl TryFrom<Vec<u8>> for Data {
    type Error = Box<dyn std::error::Error>;

    fn try_from(_: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Data {
            id: 0,
            name: "test".to_string(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pulsar_config = neutron::PulsarConfig {
        endpoint_url: "127.0.0.1".to_string(),
        endpoint_port: 6650,
    };

    let consumer_config = neutron::ConsumerConfig {
        consumer_id: "test".to_string(),
        topic: "test".to_string(),
        subscription: "test".to_string(),
        consumer_name: "test".to_string(),
    };

    let client = neutron::Pulsar::new(pulsar_config);
    let consumer = neutron::Consumer::new(client, consumer_config)
        .connect()
        .await?;
    let _: Message<Data> = consumer.next().await?;
    Ok(())
}
