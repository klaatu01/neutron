use std::sync::atomic::AtomicU64;

use crate::broker_address::BrokerAddress;
use crate::client_manager::{self, ClientConnection, ClientData, ClientManager};
use crate::connection::PulsarConnection;
use crate::connection_manager::ConnectionManager;
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{self, Command, Connect, Connected};
use crate::message::{Inbound, Outbound};
use crate::AuthenticationPlugin;
use futures::lock::Mutex;

#[derive(Clone)]
pub struct PulsarConfig {
    pub endpoint_url: String,
    pub endpoint_port: u16,
}

impl PulsarConfig {
    pub fn broker_address(&self) -> BrokerAddress {
        BrokerAddress::Direct {
            url: format!("{}:{}", self.endpoint_url, self.endpoint_port),
        }
    }

    pub fn is_tls(&self) -> bool {
        self.endpoint_url.starts_with("pulsar+ssl://")
    }
}

pub struct Pulsar {
    #[allow(dead_code)]
    pub(crate) config: PulsarConfig,
    pub(crate) registration_manager_connection: Option<EngineConnection<(), ClientRegistration>>,
    pub(crate) auth_plugin: Option<Box<dyn AuthenticationPlugin + Sync + Send + 'static>>,
    pub(crate) client_manager: Mutex<ClientManager>,
    pub(crate) connection_manager: Mutex<ConnectionManager>,
}

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

enum Next {
    Inbound((BrokerAddress, ResultInbound)),
    Outbound((ClientData, ResultOutbound)),
    Registration(Result<ClientRegistration, NeutronError>),
}

impl Pulsar {
    pub fn new(config: PulsarConfig) -> Self {
        Self {
            config,
            registration_manager_connection: None,
            client_manager: Mutex::new(ClientManager::new()),
            connection_manager: Mutex::new(ConnectionManager::new()),
            auth_plugin: None,
        }
    }

    async fn get_next(&mut self) -> Result<Next, NeutronError> {
        match (
            &self.connection_manager.lock().await,
            &self.client_manager.lock().await,
            &self.registration_manager_connection,
        ) {
            (connection_manager, client_manager, Some(registration_manager_connection))
                if !client_manager.is_empty() && !connection_manager.is_empty() =>
            {
                tokio::select! {
                    outbound = client_manager.next() => {
                        Ok(Next::Outbound(outbound))
                    }
                    inbound = connection_manager.next() => {
                        Ok(Next::Inbound(inbound))
                    }
                    new_registration = registration_manager_connection.recv() => {
                        log::info!("Registration received");
                        Ok(Next::Registration(new_registration))
                    }
                }
            }
            (_, client_manager, Some(registration_manager_connection))
                if client_manager.is_empty() =>
            {
                log::info!("Awaiting Registration");
                let registration = registration_manager_connection.recv().await?;
                Ok(Next::Registration(Ok(registration)))
            }
            _ => {
                log::error!("Connection engine or client connection is None");
                Err(NeutronError::Disconnected)
            }
        }
    }

    async fn send_to_connection(
        &self,
        broker_address: &BrokerAddress,
        outbound: Outbound,
    ) -> Result<(), NeutronError> {
        let connection_lock = self.connection_manager.lock().await;
        let connection = connection_lock
            .get_connection(broker_address)
            .ok_or(NeutronError::Disconnected)?;

        match outbound {
            Outbound::Connect(connect) => {
                let (tx, rx) = futures::channel::oneshot::channel();
                connection
                    .send(Ok(Command::RequestResponse(Outbound::Connect(connect), tx)))
                    .await?;
                log::info!("Awaiting response");
                let response = rx.await.map_err(|_| NeutronError::ChannelTerminated)??;
                log::info!("Received response: {:?}", response);
                self.client_manager
                    .lock()
                    .await
                    .send(&response, broker_address)
                    .await?;
            }
            outbound => {
                connection.send(Ok(Command::Request(outbound))).await?;
            }
        }

        Ok(())
    }

    async fn handle_client_outbound(
        &self,
        client_data: &ClientData,
        outbound: &ResultOutbound,
    ) -> Result<(), NeutronError> {
        let ClientData { id, .. } = client_data;
        match outbound {
            Ok(outbound) => {
                let outbound = match outbound {
                    Outbound::Connect(connect) => {
                        let mut connect = connect.clone();

                        match connect.broker_address {
                            Some(BrokerAddress::Proxy { proxy, url }) => {
                                let proxy = proxy.clone();

                                let url = if url != "" {
                                    url.to_string()
                                } else {
                                    self.config.broker_address().base_url().to_string()
                                };

                                connect.broker_address = Some(BrokerAddress::Proxy { url, proxy });
                            }
                            None => {
                                connect.broker_address = Some(self.config.broker_address());
                            }
                            _ => (),
                        }

                        if let Some(auth_plugin) = &self.auth_plugin {
                            let auth_data = auth_plugin.auth_data().await?;
                            let auth_method_name = auth_plugin.auth_method_name();
                            connect.auth_method_name = Some(auth_method_name);
                            connect.auth_data = Some(auth_data);
                        }

                        let new_broker_address = connect.broker_address.as_ref().unwrap();

                        self.new_connection(new_broker_address).await?;

                        if new_broker_address != &self.config.broker_address() {
                            self.client_manager
                                .lock()
                                .await
                                .move_client_to_broker(*id, new_broker_address);
                        }

                        Outbound::Connect(connect)
                    }
                    _ => outbound.clone(),
                };

                let broker_address = {
                    let client_manager = self.client_manager.lock().await;
                    client_manager
                        .get_client(*id)
                        .unwrap()
                        .broker_address
                        .clone()
                };

                self.send_to_connection(&broker_address, outbound).await
            }
            Err(e) => {
                log::error!("Error in outbound: {}", e);
                Err(e.clone())
            }
        }
    }

    async fn handle_connection_inbound(
        &self,
        broker_address: &BrokerAddress,
        inbound: &ResultInbound,
    ) -> Result<(), NeutronError> {
        log::info!("Received inbound from {}", broker_address);
        match inbound {
            Ok(inbound) => {
                let inbound = match inbound {
                    Inbound::LookupTopicResponse(connected) => {
                        Inbound::LookupTopicResponse(message::LookupTopicResponse {
                            proxy: connected.proxy
                                && connected.broker_service_url != broker_address.to_string(),
                            ..connected.clone()
                        })
                    }
                    Inbound::Ping => {
                        log::info!("Received ping from {}", broker_address);
                        self.connection_manager
                            .lock()
                            .await
                            .send(Ok(Command::Request(Outbound::Pong)), broker_address)
                            .await?;
                        return Ok(());
                    }
                    _ => inbound.clone(),
                };

                let client_lock = self.client_manager.lock().await;
                client_lock.send(&inbound, broker_address).await?;
                Ok(())
            }
            Err(e) => {
                log::error!("Error in inbound: {}", e);
                Err(e.clone())
            }
        }
    }

    async fn handle_registration(
        &self,
        registration_manager_connection: &EngineConnection<(), ClientRegistration>,
        registration: ClientRegistration,
    ) {
        let ClientRegistration {
            id,
            topic,
            connection,
        } = registration;
        let client_data = ClientConnection {
            id,
            topic,
            connection,
            broker_address: self.config.broker_address(),
        };
        let mut client_manager = self.client_manager.lock().await;
        client_manager.add_client(client_data);
        registration_manager_connection.send(Ok(())).await;
    }

    async fn handle_next(&mut self, next: Next) -> Result<(), NeutronError> {
        match next {
            Next::Inbound((broker_address, inbound)) => {
                self.handle_connection_inbound(&broker_address, &inbound)
                    .await
            }
            Next::Outbound((broker_address, outbound)) => {
                self.handle_client_outbound(&broker_address, &outbound)
                    .await
            }
            Next::Registration(registration) => {
                if let Some(registration_manager_connection) = &self.registration_manager_connection
                {
                    if let Ok(registration) = registration {
                        self.handle_registration(registration_manager_connection, registration)
                            .await;
                    } else {
                        log::error!("Error in registration: {:?}", registration.err().unwrap());
                    }
                }
                Ok(())
            }
        }
    }

    async fn new_connection(&self, broker_address: &BrokerAddress) -> Result<(), NeutronError> {
        let mut connection_manager = self.connection_manager.lock().await;
        if connection_manager.get_connection(broker_address).is_some() {
            log::info!("Connection to {} already exists", broker_address);
            return Ok(());
        }

        let connection = PulsarConnection::connect(broker_address.clone())
            .await?
            .run()
            .await;

        connection_manager.add_connection(broker_address.clone(), connection);
        Ok(())
    }

    pub async fn next(&mut self) -> Result<(), NeutronError> {
        let next = self.get_next().await?;
        self.handle_next(next).await?;
        Ok(())
    }

    async fn start_pulsar(
        &mut self,
        registration_manager_connection: EngineConnection<(), ClientRegistration>,
    ) {
        self.registration_manager_connection = Some(registration_manager_connection);
        let broker_address = self.config.broker_address();
        match self.new_connection(&broker_address).await {
            Ok(_) => {
                log::info!("Connected to pulsar {}", broker_address);
            }
            Err(e) => {
                log::error!("Failed to connect to pulsar broker: {}", e);
                return;
            }
        }
        loop {
            if let Err(e) = self.next().await {
                log::error!("Error in pulsar: {}", e);
                break;
            }
        }
    }

    pub fn run(mut self) -> PulsarManager {
        let (registration_manager_connection, inner_connection) = EngineConnection::pair();

        tokio::task::spawn(async move {
            self.start_pulsar(registration_manager_connection).await;
        });

        PulsarManager::new(inner_connection)
    }
}

pub(crate) struct ClientRegistration {
    id: u64,
    topic: String,
    connection: EngineConnection<Inbound, Command<Outbound, Inbound>>,
}

impl ClientRegistration {
    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_connection(&self) -> &EngineConnection<Inbound, Command<Outbound, Inbound>> {
        &self.connection
    }
}

pub struct PulsarBuilder {
    config: Option<PulsarConfig>,
    auth_plugin: Option<Box<dyn AuthenticationPlugin + Send + Sync + 'static>>,
}

impl PulsarBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            auth_plugin: None,
        }
    }

    pub fn with_config(mut self, config: PulsarConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_auth_plugin<T>(mut self, auth_plugin: T) -> Self
    where
        T: AuthenticationPlugin + Send + Sync + 'static,
    {
        self.auth_plugin = Some(Box::new(auth_plugin));
        self
    }

    pub fn build(self) -> Pulsar {
        Pulsar {
            config: self.config.unwrap(),
            registration_manager_connection: None,
            auth_plugin: self.auth_plugin,
            client_manager: Mutex::new(ClientManager::new()),
            connection_manager: Mutex::new(ConnectionManager::new()),
        }
    }
}

pub struct PulsarManager {
    client_id_generator: AtomicU64,
    inner_connection: EngineConnection<ClientRegistration, ()>,
}

impl PulsarManager {
    pub fn new(inner_connection: EngineConnection<ClientRegistration, ()>) -> Self {
        Self {
            client_id_generator: AtomicU64::new(0),
            inner_connection,
        }
    }

    pub fn new_client_id(&self) -> u64 {
        self.client_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn register(
        &self,
        topic: String,
        client_id: u64,
    ) -> Result<EngineConnection<crate::message::Command<Outbound, Inbound>, Inbound>, NeutronError>
    {
        let (consumer_connection, connection) = EngineConnection::pair();

        self.inner_connection
            .send(Ok(ClientRegistration {
                id: client_id,
                topic: topic.clone(),
                connection: consumer_connection,
            }))
            .await
            .map_err(|_| NeutronError::ChannelTerminated)?;

        self.inner_connection.recv().await?;
        Ok(connection)
    }
}
