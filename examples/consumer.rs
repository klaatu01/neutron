use async_trait::async_trait;
use neutron::{ConsumerBuilder, ConsumerPlugin, NeutronError};

#[derive(Debug)]
#[allow(dead_code)]
struct Data {
    name: String,
}

impl TryFrom<Vec<u8>> for Data {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Data {
            name: String::from_utf8(value).unwrap(),
        })
    }
}

impl Into<Vec<u8>> for Data {
    fn into(self) -> Vec<u8> {
        self.name.as_bytes().to_vec()
    }
}

pub struct PayloadLoggerPlugin;
pub struct AutoAckPlugin;

#[async_trait]
impl ConsumerPlugin for PayloadLoggerPlugin {
    async fn on_message(
        &self,
        _: &neutron::Consumer,
        message: neutron::Message<Vec<u8>>,
    ) -> Result<(), NeutronError> {
        let data = Data::try_from(message.payload.clone())
            .map_err(|e| NeutronError::DeserializationFailed)?;
        log::info!("From plugin: {}", data.name);
        Ok(())
    }
}

#[async_trait]
impl ConsumerPlugin for AutoAckPlugin {
    async fn on_message(
        &self,
        consumer: &neutron::Consumer,
        message: neutron::Message<Vec<u8>>,
    ) -> Result<(), NeutronError> {
        consumer.ack(&message.message_id).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let pulsar_config = neutron::PulsarConfig {
        endpoint_url: "127.0.0.1".to_string(),
        endpoint_port: 6650,
    };

    let pulsar_manager = neutron::Pulsar::new(pulsar_config).run();

    let consumer = ConsumerBuilder::new()
        .with_topic("test")
        .with_subscription("test")
        .with_consumer_name("test")
        .add_plugin(PayloadLoggerPlugin)
        .add_plugin(AutoAckPlugin)
        .connect(&pulsar_manager)
        .await;

    let producer = neutron::ProducerBuilder::new()
        .with_producer_name("test")
        .with_topic("test")
        .connect(&pulsar_manager)
        .await;

    loop {
        producer
            .send(Data {
                name: "Hello, world!".to_string(),
            })
            .await?;
        let _: neutron::Message<Data> = consumer.next().await.unwrap();
    }

    Ok(())
}
