use crate::connection::PulsarConnection;
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{Inbound, Outbound};
use crate::resolver_manager::ResolverManager;
use async_trait::async_trait;

#[derive(Clone)]
pub struct PulsarConfig {
    pub endpoint_url: String,
    pub endpoint_port: u16,
}

pub struct Pulsar {
    #[allow(dead_code)]
    pub(crate) config: PulsarConfig,
    pub(crate) connection_engine: Option<EngineConnection<Outbound, Inbound>>,
    pub(crate) resolver_manager: ResolverManager<Inbound>,
    pub(crate) client_connection: Option<EngineConnection<Inbound, Outbound>>,
    pub(crate) inbound_buffer: Vec<Inbound>,
}

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

fn is_disconnect<T>(result: &Result<T, NeutronError>) -> bool {
    match result {
        Err(e) => e.is_disconnect(),
        _ => false,
    }
}

impl Pulsar {
    pub fn new(config: PulsarConfig) -> Self {
        Self {
            config,
            connection_engine: None,
            resolver_manager: ResolverManager::new(),
            client_connection: None,
            inbound_buffer: Vec::new(),
        }
    }

    pub async fn handle_connection_inbound(
        &self,
        client_connection: &EngineConnection<Inbound, Outbound>,
        engine_connection: &EngineConnection<Outbound, Inbound>,
        inbound: &ResultInbound,
    ) -> Result<(), NeutronError> {
        match inbound {
            Err(NeutronError::Disconnected) => {
                client_connection
                    .send(Err(NeutronError::Disconnected))
                    .await
                    .unwrap();
            }
            _ => (),
        }

        Ok(())
    }

    pub async fn handle_connection_outbound(
        &self,
        outbound: &ResultOutbound,
    ) -> Result<(), NeutronError> {
        Ok(())
    }

    pub async fn handle_client_inbound(&self, inbound: &ResultInbound) -> Result<(), NeutronError> {
        Ok(())
    }

    pub async fn handle_client_outbound(
        &self,
        outbound: &ResultOutbound,
    ) -> Result<(), NeutronError> {
        Ok(())
    }

    pub async fn connect(&self) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let connection_engine =
            PulsarConnection::connect(&self.config.endpoint_url, &self.config.endpoint_port)
                .await
                .run()
                .await;
        Ok(connection_engine)
    }

    pub async fn next(&self) -> bool {
        match (&self.client_connection, &self.connection_engine) {
            (Some(client_connection), Some(connection_engine)) => {
                tokio::select! {
                    outbound = client_connection.recv() => {
                        if is_disconnect(&outbound) {
                            log::debug!("Client connection disconnected");
                            connection_engine.send(Err(NeutronError::Disconnected)).await.unwrap();
                        }
                        self.handle_client_outbound(&outbound).await.unwrap();
                    }
                    inbound = connection_engine.recv() => {
                        if is_disconnect(&inbound) {
                            log::debug!("Connection engine disconnected");
                            client_connection.send(Err(NeutronError::Disconnected)).await.unwrap();
                            return true;
                        }
                        self.handle_connection_inbound(&client_connection, &connection_engine, &inbound).await.unwrap();
                    }
                };
                ()
            }
            _ => {
                log::error!("Connection engine or client connection is None");
                return true;
            }
        }
        false
    }

    async fn start_pulsar(&mut self, client_connection: EngineConnection<Inbound, Outbound>) {
        self.client_connection = Some(client_connection);

        let inbound = self.connect().await;
        match inbound {
            Ok(inbound) => {
                self.connection_engine = Some(inbound);
            }
            Err(e) => {
                log::error!("Failed to connect to pulsar broker: {}", e);
                return;
            }
        }
        while self.next().await == false {}
    }
}

#[async_trait]
impl Engine<Inbound, Outbound> for Pulsar {
    async fn run(mut self) -> EngineConnection<Outbound, Inbound> {
        let (tx, _rx) = async_channel::unbounded::<ResultInbound>();
        let (_tx, rx) = async_channel::unbounded::<ResultOutbound>();

        let client_connection = EngineConnection::new(tx, rx);

        tokio::task::spawn(async move {
            self.start_pulsar(client_connection).await;
        });

        EngineConnection::new(_tx, _rx)
    }
}
