use crate::connection::PulsarConnection;
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{ConnectionInbound, ConnectionOutbound, EngineOutbound, Inbound, Outbound};
use crate::resolver_manager::ResolverManager;
use async_trait::async_trait;
use futures::lock::Mutex;

#[derive(Clone)]
pub struct PulsarConfig {
    pub endpoint_url: String,
    pub endpoint_port: u16,
}

pub struct Pulsar {
    #[allow(dead_code)]
    pub(crate) config: PulsarConfig,
    pub(crate) socket_connection: Option<EngineConnection<Outbound, Inbound>>,
    pub(crate) resolver_manager: ResolverManager<Inbound>,
    pub(crate) client_connection: Option<EngineConnection<Inbound, Outbound>>,
    pub(crate) inbound_buffer: Mutex<Vec<Inbound>>,
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
            socket_connection: None,
            resolver_manager: ResolverManager::new(),
            client_connection: None,
            inbound_buffer: Mutex::new(Vec::new()),
        }
    }

    pub async fn send_and_resolve(
        &self,
        socket_connection: &EngineConnection<Outbound, Inbound>,
        outbound: &ResultOutbound,
    ) -> ResultInbound {
        match outbound {
            Ok(outbound) => match self.resolver_manager.put_resolver(outbound).await {
                Some(resolver) => {
                    socket_connection
                        .send(Ok(outbound.clone()))
                        .await
                        .map_err(|_| NeutronError::ChannelTerminated)?;

                    loop {
                        tokio::select! {
                            inbound = socket_connection.recv() => {
                                match inbound {
                                    Ok(inbound) => {
                                        if self.resolver_manager.try_resolve(&inbound).await {
                                            return Ok(inbound);
                                        } else {
                                            self.inbound_buffer.lock().await.push(inbound);
                                        }
                                    }
                                    Err(e) => {
                                        return Err(e)
                                    }
                                }
                            }
                            inbound = resolver.recv() => {
                                return inbound.map_err(|_| NeutronError::OperationTimeout);
                            }
                        }
                    }
                }
                _ => Err(NeutronError::Unresolvable),
            },
            Err(e) => Err(e.clone()),
        }
    }

    pub async fn handle_socket_connection_inbound(
        &self,
        client_connection: &EngineConnection<Inbound, Outbound>,
        socket_connection: &EngineConnection<Outbound, Inbound>,
        inbound: &ResultInbound,
    ) -> Result<(), NeutronError> {
        match inbound {
            Ok(inbound_cmd) => match inbound_cmd {
                Inbound::Connection(ConnectionInbound::Ping) => {
                    socket_connection
                        .send(Ok(Outbound::Connection(ConnectionOutbound::Pong)))
                        .await
                        .unwrap();
                }
                inbound => {
                    client_connection.send(Ok(inbound.clone())).await.unwrap();
                }
            },
            Err(inbound_err) => match inbound_err {
                NeutronError::Disconnected => {
                    client_connection
                        .send(Err(NeutronError::Disconnected))
                        .await
                        .unwrap();
                }
                _ => (),
            },
        }
        Ok(())
    }

    pub async fn handle_client_connection_outbound(
        &self,
        _client_connection: &EngineConnection<Inbound, Outbound>,
        socket_connection: &EngineConnection<Outbound, Inbound>,
        outbound: &ResultOutbound,
    ) -> Result<(), NeutronError> {
        socket_connection.send(outbound.clone()).await
    }

    pub async fn connect(&self) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let connection_engine =
            PulsarConnection::connect(&self.config.endpoint_url, &self.config.endpoint_port)
                .await
                .run()
                .await;
        let _ = self
            .send_and_resolve(
                &connection_engine,
                &Ok(Outbound::Engine(crate::message::EngineOutbound::Connect {
                    auth_data: None,
                })),
            )
            .await;
        Ok(connection_engine)
    }

    pub async fn next(&self) -> bool {
        match (&self.client_connection, &self.socket_connection) {
            (Some(client_connection), Some(socket_connection)) => {
                if let Some(inbound) = self.inbound_buffer.lock().await.pop() {
                    self.handle_socket_connection_inbound(
                        &client_connection,
                        &socket_connection,
                        &Ok(inbound),
                    )
                    .await
                    .unwrap();
                    return false;
                }
                tokio::select! {
                    outbound = client_connection.recv() => {
                        if is_disconnect(&outbound) {
                            log::debug!("Client connection disconnected");
                            socket_connection.send(Err(NeutronError::Disconnected)).await.unwrap();
                        }
                        self.handle_client_connection_outbound(&client_connection, &socket_connection, &outbound).await.unwrap();
                    }
                    inbound = socket_connection.recv() => {
                        if is_disconnect(&inbound) {
                            log::debug!("Connection engine disconnected");
                            client_connection.send(Err(NeutronError::Disconnected)).await.unwrap();
                            return true;
                        }
                        self.handle_socket_connection_inbound(&client_connection, &socket_connection, &inbound).await.unwrap();
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
                self.socket_connection = Some(inbound);
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
