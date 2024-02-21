use std::sync::atomic::AtomicU64;

use crate::client_manager::ClientManager;
use crate::connection::PulsarConnection;
use crate::connection_manager::{BrokerAddress, ConnectionManager};
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{
    ConnectionInbound, ConnectionOutbound, EngineInbound, EngineOutbound, Inbound, Outbound,
};
use crate::resolver_manager::ResolverManager;
use crate::{AuthenticationPlugin, ConsumerConfig, ProducerConfig};
use futures::lock::Mutex;
use futures::FutureExt;
use oauth2::TokenIntrospectionResponse;

#[derive(Clone)]
pub struct PulsarConfig {
    pub endpoint_url: String,
    pub endpoint_port: u16,
}

impl PulsarConfig {
    pub fn broker_address(&self) -> BrokerAddress {
        format!("{}:{}", self.endpoint_url, self.endpoint_port)
    }
}

pub struct Pulsar {
    #[allow(dead_code)]
    pub(crate) config: PulsarConfig,
    pub(crate) resolver_manager: ResolverManager<Inbound>,
    pub(crate) inbound_buffer: Mutex<Vec<(BrokerAddress, Result<Inbound, NeutronError>)>>,
    pub(crate) registration_manager_connection:
        Option<EngineConnection<(), PulsarManagerRegistration>>,
    pub(crate) auth_plugin: Option<Box<dyn AuthenticationPlugin + Sync + Send + 'static>>,
    pub(crate) client_manager: Mutex<ClientManager>,
    pub(crate) connection_manager: Mutex<ConnectionManager>,
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
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            registration_manager_connection: None,
            client_manager: Mutex::new(ClientManager::new()),
            connection_manager: Mutex::new(ConnectionManager::new()),
            auth_plugin: None,
        }
    }

    pub async fn send_and_resolve(
        &self,
        broker_address: &BrokerAddress,
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
                                        }
                                        self.inbound_buffer.lock().await.push((broker_address.to_string(), Ok(inbound)));
                                    }
                                    Err(e) => {
                                        self.inbound_buffer.lock().await.push((broker_address.to_string(), Err(e)));
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

    pub(crate) async fn handle_registration(
        &self,
        registration_manager: &EngineConnection<(), PulsarManagerRegistration>,
        registration: PulsarManagerRegistration,
    ) {
        let client = match registration {
            PulsarManagerRegistration::Producer {
                producer_id,
                topic,
                connection,
            } => crate::client_manager::Client::Producer(crate::client_manager::ClientData {
                id: producer_id,
                topic,
                connection,
                broker_address: self.config.broker_address(),
            }),
            PulsarManagerRegistration::Consumer {
                consumer_id,
                topic,
                connection,
            } => crate::client_manager::Client::Consumer(crate::client_manager::ClientData {
                id: consumer_id,
                topic,
                connection,
                broker_address: self.config.broker_address(),
            }),
        };
        self.client_manager.lock().await.add_client(client);
        registration_manager.send(Ok(())).await.unwrap();
    }

    pub async fn handle_client_outbound(
        &self,
        broker_address: &str,
        outbound: &ResultOutbound,
    ) -> Result<(), NeutronError> {
        self.connection_manager
            .lock()
            .await
            .send(outbound.clone(), broker_address.to_string())
            .await
    }

    pub async fn connect(
        &self,
        broker_address: BrokerAddress,
    ) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let connection_engine = PulsarConnection::connect(broker_address.clone(), false)
            .await?
            .run()
            .await;
        let auth_data = if let Some(auth_plugin) = &self.auth_plugin {
            Some(auth_plugin.auth_data().await?)
        } else {
            None
        };
        let _ = self
            .send_and_resolve(
                &broker_address,
                &connection_engine,
                &Ok(Outbound::Engine(crate::message::EngineOutbound::Connect {
                    auth_data,
                })),
            )
            .await;
        Ok(connection_engine)
    }

    pub(crate) async fn handle_connection_inbound(
        &self,
        broker_address: &BrokerAddress,
        inbound: &ResultInbound,
    ) -> Result<(), NeutronError> {
        match inbound {
            Ok(inbound_cmd) => match inbound_cmd {
                Inbound::Connection(ConnectionInbound::Ping) => {
                    let connection_manager = self
                        .connection_manager
                        .lock()
                        .await
                        .send(
                            Ok(Outbound::Connection(ConnectionOutbound::Pong)),
                            broker_address.to_string(),
                        )
                        .await;
                }
                Inbound::Engine(EngineInbound::AuthChallenge) => {
                    if let Some(auth_plugin) = &self.auth_plugin {
                        let auth_data = auth_plugin.auth_data().await?;
                        self.connection_manager
                            .lock()
                            .await
                            .send(
                                Ok(Outbound::Engine(EngineOutbound::AuthChallenge {
                                    auth_data,
                                })),
                                broker_address.to_string(),
                            )
                            .await;
                    } else {
                        Err(NeutronError::AuthenticationFailed(
                            "No auth plugin provided, but auth challenge received".to_string(),
                        ))?;
                    }
                }
                inbound => {
                    self.client_manager
                        .lock()
                        .await
                        .send(inbound, &broker_address)
                        .await?;
                }
            },
            Err(inbound_err) => match inbound_err {
                NeutronError::Disconnected => {
                    self.client_manager
                        .lock()
                        .await
                        .send_all(&Err(NeutronError::Disconnected))
                        .await?;
                }
                _ => (),
            },
        }
        Ok(())
    }

    pub async fn next(&mut self) -> bool {
        let mut _registration: Option<Result<PulsarManagerRegistration, NeutronError>> = None;
        let mut _inbound: Option<(BrokerAddress, Result<Inbound, NeutronError>)> = None;
        let mut _outbound: Option<(BrokerAddress, Result<Outbound, NeutronError>)> = None;
        match (
            &self.connection_manager.lock().await,
            &self.client_manager.lock().await,
            &self.registration_manager_connection,
        ) {
            (connection_manager, client_manager, Some(registration_manager_connection))
                if !client_manager.is_empty() =>
            {
                if let Some(inbound) = self.inbound_buffer.lock().await.pop() {
                    _inbound = Some(inbound);
                } else {
                    tokio::select! {
                        outbound = client_manager.next_message() => {
                            _outbound = Some(outbound);
                        }
                        inbound = connection_manager.next() => {
                            _inbound = Some(inbound);
                        }
                        new_registration = registration_manager_connection.recv() => {
                            log::info!("Registration received");
                            _registration = Some(new_registration);
                        }
                    }
                }
            }
            (_, client_manager, Some(registration_manager_connection))
                if client_manager.is_empty() =>
            {
                log::info!("Awaiting Registration");
                _registration = Some(registration_manager_connection.recv().await);
                log::info!("Registration received");
            }
            _ => {
                log::error!("Connection engine or client connection is None");
                return true;
            }
        };
        if let (Some(registration_manager_connection), Some(registration)) =
            (&self.registration_manager_connection, _registration)
        {
            self.handle_registration(&registration_manager_connection, registration.unwrap())
                .await;
            return false;
        }
        if let Some((broker_address, inbound)) = _inbound {
            self.handle_connection_inbound(&broker_address, &inbound)
                .await
                .unwrap();
            return false;
        }
        if let Some((broker_address, outbound)) = _outbound {
            self.handle_client_outbound(&broker_address, &outbound)
                .await
                .unwrap();
            return false;
        }
        true
    }

    async fn start_pulsar(
        &mut self,
        registration_manager_connection: EngineConnection<(), PulsarManagerRegistration>,
    ) {
        self.registration_manager_connection = Some(registration_manager_connection);
        let connection = self.connect(self.config.broker_address()).await;
        match connection {
            Ok(connection) => self
                .connection_manager
                .lock()
                .await
                .add_connection(self.config.endpoint_url.clone(), connection),
            Err(e) => {
                log::error!("Failed to connect to pulsar broker: {}", e);
                return;
            }
        }
        while self.next().await == false {}
    }

    pub fn run(mut self) -> PulsarManager {
        let (registration_manager_connection, inner_connection) = EngineConnection::pair();

        tokio::task::spawn(async move {
            self.start_pulsar(registration_manager_connection).await;
        });

        PulsarManager::new(inner_connection)
    }
}

pub(crate) enum PulsarManagerRegistration {
    Producer {
        producer_id: u64,
        topic: String,
        connection: EngineConnection<Inbound, Outbound>,
    },
    Consumer {
        consumer_id: u64,
        topic: String,
        connection: EngineConnection<Inbound, Outbound>,
    },
}

impl PulsarManagerRegistration {
    pub fn get_id(&self) -> u64 {
        match self {
            PulsarManagerRegistration::Producer { producer_id, .. } => *producer_id,
            PulsarManagerRegistration::Consumer { consumer_id, .. } => *consumer_id,
        }
    }
    pub fn is_producer(&self) -> bool {
        match self {
            PulsarManagerRegistration::Producer { .. } => true,
            _ => false,
        }
    }
    pub fn is_consumer(&self) -> bool {
        match self {
            PulsarManagerRegistration::Consumer { .. } => true,
            _ => false,
        }
    }

    pub fn get_connection(&self) -> &EngineConnection<Inbound, Outbound> {
        match self {
            PulsarManagerRegistration::Producer { connection, .. } => connection,
            PulsarManagerRegistration::Consumer { connection, .. } => connection,
        }
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
            resolver_manager: ResolverManager::new(),
            inbound_buffer: Mutex::new(Vec::new()),
            registration_manager_connection: None,
            auth_plugin: self.auth_plugin,
            client_manager: Mutex::new(ClientManager::new()),
            connection_manager: Mutex::new(ConnectionManager::new()),
        }
    }
}

pub struct PulsarManager {
    producer_id_generator: AtomicU64,
    consumer_id_generator: AtomicU64,
    inner_connection: EngineConnection<PulsarManagerRegistration, ()>,
}

impl PulsarManager {
    pub fn new(inner_connection: EngineConnection<PulsarManagerRegistration, ()>) -> Self {
        Self {
            producer_id_generator: AtomicU64::new(0),
            consumer_id_generator: AtomicU64::new(0),
            inner_connection,
        }
    }

    pub fn consumer_id(&self) -> u64 {
        self.consumer_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub fn producer_id(&self) -> u64 {
        self.producer_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub async fn register_consumer(
        &self,
        config: &ConsumerConfig,
    ) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let (consumer_connection, connection) = EngineConnection::pair();

        self.inner_connection
            .send(Ok(PulsarManagerRegistration::Consumer {
                consumer_id: config.consumer_id,
                topic: config.topic.clone(),
                connection: consumer_connection,
            }))
            .await
            .map_err(|_| NeutronError::ChannelTerminated)?;

        self.inner_connection.recv().await?;
        Ok(connection)
    }

    pub async fn register_producer(
        &self,
        config: &ProducerConfig,
    ) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let (producer_connection, connection) = EngineConnection::pair();

        self.inner_connection
            .send(Ok(PulsarManagerRegistration::Producer {
                producer_id: config.producer_id,
                topic: config.topic.clone(),
                connection: producer_connection,
            }))
            .await
            .map_err(|_| NeutronError::ChannelTerminated)?;

        self.inner_connection.recv().await?;
        Ok(connection)
    }
}
