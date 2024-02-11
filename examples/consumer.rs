use async_trait::async_trait;
use neutron::{ConsumerBuilder, ConsumerPlugin, NeutronError};
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

impl Into<Vec<u8>> for Data {
    fn into(self) -> Vec<u8> {
        self.name.as_bytes().to_vec()
    }
}

pub struct PayloadLoggerPlugin;
pub struct AutoAckPlugin;

// recrd the number of messages recieved per second
pub struct TransactionsPerSecondCounterPlugin {
    pub transactions: u64,
    pub start_time: Option<std::time::Instant>,
}

impl Default for TransactionsPerSecondCounterPlugin {
    fn default() -> Self {
        TransactionsPerSecondCounterPlugin {
            transactions: 0,
            start_time: None,
        }
    }
}

#[async_trait]
impl ConsumerPlugin<Data> for PayloadLoggerPlugin {
    async fn on_message(
        &mut self,
        _: &neutron::Consumer<Data>,
        message: neutron::Message<Data>,
    ) -> Result<(), NeutronError> {
        log::info!("From plugin: {}", message.payload.name);
        Ok(())
    }
}

#[async_trait]
impl ConsumerPlugin<Data> for AutoAckPlugin {
    async fn on_message(
        &mut self,
        consumer: &neutron::Consumer<Data>,
        message: neutron::Message<Data>,
    ) -> Result<(), NeutronError> {
        consumer.ack(&message.message_id).await?;
        Ok(())
    }
}

#[async_trait]
impl ConsumerPlugin<Data> for TransactionsPerSecondCounterPlugin {
    async fn on_message(
        &mut self,
        consumer: &neutron::Consumer<Data>,
        message: neutron::Message<Data>,
    ) -> Result<(), NeutronError> {
        if self.start_time.is_none() {
            self.start_time = Some(std::time::Instant::now());
        }

        self.transactions += 1;
        let elapsed = self.start_time.unwrap().elapsed();
        if elapsed.as_secs() > 0 {
            log::info!(
                "Transactions per second: {}",
                self.transactions / elapsed.as_secs()
            );
        }
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

    let pulsar = neutron::PulsarBuilder::new()
        .with_config(pulsar_config)
        .build()
        .run();

    let consumer = ConsumerBuilder::new()
        .with_topic("test")
        .with_subscription("test")
        .with_consumer_name("test")
        .add_plugin(AutoAckPlugin)
        .add_plugin(TransactionsPerSecondCounterPlugin::default())
        .connect(&pulsar)
        .await?;

    consumer.start().await?;
    Ok(())
}
