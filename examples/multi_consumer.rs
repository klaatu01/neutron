use async_trait::async_trait;
use neutron::{Client, ConsumerBuilder, ConsumerEngine, ConsumerPlugin, NeutronError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
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

impl From<Data> for Vec<u8> {
    fn from(val: Data) -> Self {
        val.name.as_bytes().to_vec()
    }
}

pub struct PayloadLoggerPlugin;
pub struct AutoAckPlugin;

#[async_trait]
impl ConsumerPlugin<Client, Data> for PayloadLoggerPlugin {
    async fn on_message(
        &mut self,
        _: &neutron::Consumer<Client, Data>,
        message: neutron::Message<Data>,
    ) -> Result<(), NeutronError> {
        log::info!("Received message: {:?}", message.payload);
        Ok(())
    }
}

#[async_trait]
impl ConsumerPlugin<Client, Data> for AutoAckPlugin {
    async fn on_message(
        &mut self,
        consumer: &neutron::Consumer<Client, Data>,
        message: neutron::Message<Data>,
    ) -> Result<(), NeutronError> {
        consumer.ack(&message.ack).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let pulsar_config = neutron::PulsarConfig {
        endpoint_url: "pulsar://127.0.0.1".to_string(),
        endpoint_port: 6650,
    };

    let pulsar = neutron::PulsarBuilder::new()
        .with_config(pulsar_config)
        .build()
        .run();

    let consumer_a = ConsumerBuilder::new()
        .with_topic("test")
        .with_subscription("test")
        .with_consumer_name("A")
        .add_plugin(AutoAckPlugin)
        .add_plugin(PayloadLoggerPlugin)
        .connect(&pulsar)
        .await?;

    let consumer_b = ConsumerBuilder::new()
        .with_topic("test")
        .with_subscription("test")
        .with_consumer_name("B")
        .add_plugin(AutoAckPlugin)
        .add_plugin(PayloadLoggerPlugin)
        .connect(&pulsar)
        .await?;

    let _handle = tokio::join!(consumer_a.consume(), consumer_b.consume());
    Ok(())
}
