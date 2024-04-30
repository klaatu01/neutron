use std::io::Write;

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

    let mut count = 0;
    let start = std::time::Instant::now();

    let mut batch = Vec::new();
    for _ in 0..1000000 {
        let data = Data {
            name: Utc::now().to_rfc3339(),
        };
        batch.push(data);
        if batch.len() == 1000 {
            log::info!("Sending batch of 1000 messages");
            if let Err(e) = producer.send_batch(batch.clone()).await {
                log::error!("Error sending message: {}", e);
                break;
            }
            if count > 0 {
                print!("\x1B[1A");
                print!("\x1B[1A");
                print!("\x1B[1A");
            }
            count += 1000;

            println!(
                "\x1B[2K{} elapsed",
                humantime::format_duration(start.elapsed())
            );
            println!("\x1B[2K{}/1000000 sent", count);
            println!(
                "\x1B[2K{} tps",
                count as f64 / start.elapsed().as_secs_f64()
            );

            // Flush stdout to ensure the output is displayed immediately.
            std::io::stdout().flush().unwrap();
            batch.clear();
        }
    }
    println!();
    Ok(())
}
