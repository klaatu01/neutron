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
    env_logger::init();
    let pulsar_config = neutron::PulsarConfig {
        endpoint_url: "127.0.0.1".to_string(),
        endpoint_port: 6650,
    };

    let consumer_config = neutron::ConsumerConfig {
        consumer_id: 0,
        topic: "test".to_string(),
        subscription: "test".to_string(),
        consumer_name: "test".to_string(),
    };

    let consumer = neutron::Consumer::new(pulsar_config, consumer_config)
        .connect()
        .await?;

    consumer.next().await?;
    Ok(())
}
