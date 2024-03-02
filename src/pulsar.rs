use std::sync::atomic::AtomicU64;

use crate::client_manager::ClientManager;
use crate::command_resolver::CommandResolver;
use crate::connection::PulsarConnection;
use crate::connection_manager::{BrokerAddress, ConnectionManager};
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::proto::pulsar::command_lookup_topic_response::LookupType;
use crate::message::Command;
use crate::message::{
    ConnectionInbound, ConnectionOutbound, EngineInbound, EngineOutbound, Inbound, Outbound,
};
use crate::resolver_manager::ResolverManager;
use crate::AuthenticationPlugin;
use futures::lock::Mutex;
use url::Url;

#[derive(Clone)]
pub struct PulsarConfig {
    pub endpoint_url: String,
    pub endpoint_port: u16,
}

impl PulsarConfig {
    pub fn broker_address(&self) -> BrokerAddress {
        format!("{}:{}", self.endpoint_url, self.endpoint_port)
    }

    pub fn is_tls(&self) -> bool {
        self.endpoint_url.starts_with("pulsar+ssl://")
    }
}

pub struct Pulsar {
    #[allow(dead_code)]
    pub(crate) config: PulsarConfig,
    pub(crate) inbound_buffer: Mutex<Vec<(BrokerAddress, Result<Inbound, NeutronError>)>>,
    pub(crate) registration_manager_connection: Option<EngineConnection<(), ClientRegistration>>,
    pub(crate) auth_plugin: Option<Box<dyn AuthenticationPlugin + Sync + Send + 'static>>,
    pub(crate) client_manager: Mutex<ClientManager>,
    pub(crate) connection_manager: Mutex<ConnectionManager>,
}

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

enum Next {
    Inbound((BrokerAddress, ResultInbound)),
    Outbound((BrokerAddress, ResultOutbound)),
    Registration(Result<ClientRegistration, NeutronError>),
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
        registration_manager: &EngineConnection<(), ClientRegistration>,
        registration: ClientRegistration,
    ) {
        let client = match registration {
            ClientRegistration::Producer {
                producer_id,
                topic,
                connection,
            } => crate::client_manager::Client::Producer(crate::client_manager::ClientData {
                id: producer_id,
                topic,
                connection,
                broker_address: self.config.broker_address(),
            }),
            ClientRegistration::Consumer {
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

    async fn handle_topic_lookup(
        &self,
        outbound: &ResultOutbound,
    ) -> Result<Option<()>, NeutronError> {
        if let Ok(Outbound::Client(crate::message::ClientOutbound::LookupTopic { topic, .. })) =
            outbound
        {
            let broker_address = self.config.broker_address();
            let inbound = {
                let connection = self.connection_manager.lock().await;
                let connection = connection
                    .get_connection(&broker_address)
                    .ok_or(NeutronError::Disconnected)?;
                self.send_and_resolve(&broker_address, connection, outbound)
                    .await?
            };
            if let Inbound::Client(crate::message::ClientInbound::LookupTopic {
                response,
                proxy,
                broker_service_url,
                broker_service_url_tls,
                ..
            }) = &inbound
            {
                log::debug!("Topic lookup response: {:?}", inbound);
                let url = if broker_service_url != "" {
                    broker_service_url
                } else if broker_service_url_tls != "" {
                    broker_service_url_tls
                } else {
                    return Err(NeutronError::ConnectionFailed);
                };
                let url = Url::parse(&url).map_err(|_| NeutronError::InvalidUrl)?;
                match (response, proxy) {
                    (LookupType::Connect, true) => {
                        let proxy_url = format!("{}:{}", url.host().unwrap(), url.port().unwrap());
                        let connection = self
                            .connect(broker_address.clone(), true, Some(proxy_url.clone()))
                            .await;
                        match connection {
                            Ok(connection) => {
                                let inbound = self
                                    .send_and_resolve(&broker_address, &connection, outbound)
                                    .await?;
                                self.connection_manager
                                    .lock()
                                    .await
                                    .add_connection(proxy_url.clone(), connection);
                                self.client_manager
                                    .lock()
                                    .await
                                    .update_broker_address_for_topic(topic, &proxy_url);
                                self.client_manager
                                    .lock()
                                    .await
                                    .send(&inbound, &proxy_url)
                                    .await?;
                                return Ok(Some(()));
                            }
                            Err(e) => {
                                log::error!("Failed to connect to pulsar broker: {}", e);
                            }
                        }
                    }
                    (LookupType::Connect, false) => (),
                    _ => (),
                }
                self.client_manager
                    .lock()
                    .await
                    .send(&inbound, &broker_address)
                    .await?;
                return Ok(Some(()));
            }
        };
        Ok(None)
    }

    pub async fn handle_client_outbound(
        &self,
        broker_address: &str,
        outbound: &ResultOutbound,
    ) -> Result<(), NeutronError> {
        if self.handle_topic_lookup(outbound).await?.is_some() {
            return Ok(());
        }
        self.connection_manager
            .lock()
            .await
            .send(outbound.clone(), broker_address.to_string())
            .await
    }

    pub async fn connect(
        &self,
        broker_address: BrokerAddress,
        is_tls: bool,
        proxy_url: Option<String>,
    ) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let connection_engine = PulsarConnection::connect(broker_address.clone(), is_tls)
            .await?
            .run()
            .await;
        let (auth_data, auth_method_name) = if let Some(auth_plugin) = &self.auth_plugin {
            (
                Some(auth_plugin.auth_data().await?),
                Some(auth_plugin.auth_method_name()),
            )
        } else {
            (None, None)
        };
        if let Some(url) = proxy_url.clone() {
            log::debug!("Connecting to proxy: {}", url);
        }
        let _ = self
            .send_and_resolve(
                &broker_address,
                &connection_engine,
                &Ok(Outbound::Engine(crate::message::EngineOutbound::Connect {
                    auth_data,
                    auth_method_name,
                    proxy_url: proxy_url.clone(),
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
                    self.connection_manager
                        .lock()
                        .await
                        .send(
                            Ok(Outbound::Connection(ConnectionOutbound::Pong)),
                            broker_address.to_string(),
                        )
                        .await?;
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
                            .await?;
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
            Err(inbound_err) => {
                self.client_manager
                    .lock()
                    .await
                    .send_all(&Err(inbound_err.clone()))
                    .await?
            }
        }
        Ok(())
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
                if let Some(inbound) = self.inbound_buffer.lock().await.pop() {
                    Ok(Next::Inbound(inbound))
                } else {
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
                    self.handle_registration(
                        registration_manager_connection,
                        registration.unwrap(),
                    )
                    .await;
                }
                Ok(())
            }
        }
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
        let connection = self
            .connect(self.config.broker_address(), self.config.is_tls(), None)
            .await;
        match connection {
            Ok(connection) => self
                .connection_manager
                .lock()
                .await
                .add_connection(self.config.broker_address(), connection),
            Err(e) => {
                log::error!("Failed to connect to pulsar broker: {}", e);
                return;
            }
        }
        loop {
            if let Err(e) = self.next().await {
                log::error!("Error in pulsar: {}", e);
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
