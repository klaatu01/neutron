use chrono::Utc;

#[derive(Debug)]
#[allow(dead_code)]
struct Data {
    name: String,
}

impl Into<Vec<u8>> for Data {
    fn into(self) -> Vec<u8> {
        self.name.as_bytes().to_vec()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let pulsar_config = neutron::PulsarConfig {
        endpoint_url: "127.0.0.1".to_string(),
        endpoint_port: 6650,
    };

    let producer_config = neutron::ProducerConfig {
        producer_id: 0,
        topic: "test".to_string(),
        producer_name: Some("test-producer".to_string()),
    };

    let producer = neutron::Producer::new(pulsar_config, producer_config)
        .connect()
        .await?;

    log::info!("Connected to pulsar");
    loop {
        let data = Data {
            name: format!("data-{}", Utc::now().to_rfc3339()),
        };
        log::info!("Sending message.");
        producer.send(data).await?;
    }

    Ok(())
}
