use chrono::Utc;
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

    let producer = neutron::ProducerBuilder::new()
        .with_producer_name(&nanoid::nanoid!().to_string())
        .with_topic("test")
        .connect(&pulsar)
        .await?;

    loop {
        let data = Data {
            name: Utc::now().to_rfc3339(),
        };
        if let Err(e) = producer.send(data).await {
            log::error!("Error sending message: {}", e);
            break;
        }
    }

    Ok(())
}
