#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = neutron::PulsarConfig {
        endpoint_url: "127.0.0.1".to_string(),
        endpoint_port: 6650,
    };
    let mut client = neutron::Pulsar::new(config);
    client.connect().await.unwrap();
    while let Ok(_) = client.next_message().await {}
    Ok(())
}
