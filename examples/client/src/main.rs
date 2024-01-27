#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = neutron::PulsarConfig {
        endpoint_url: "127.0.0.1".to_string(),
        endpoint_port: 6650,
    };
    let mut client = neutron::Pulsar::new(config);
    client.connect().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    Ok(())
}
