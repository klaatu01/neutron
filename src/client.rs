use crate::connection::PulsarConnection;
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{ConnectionInbound, ConnectionOutbound, EngineOutbound, Inbound, Outbound};
use crate::resolver_manager::ResolverManager;
use crate::{Consumer, ConsumerConfig, Producer};
use futures::lock::Mutex;
use futures::FutureExt;

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
    pub(crate) client_connection: Mutex<Vec<PulsarManagerRegistration>>,
    pub(crate) inbound_buffer: Mutex<Vec<Inbound>>,
    pub(crate) registration_manager_connection:
        Option<EngineConnection<(), PulsarManagerRegistration>>,
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
            client_connection: Mutex::new(Vec::new()),
            inbound_buffer: Mutex::new(Vec::new()),
            registration_manager_connection: None,
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

    pub(crate) async fn handle_socket_connection_inbound(
        &self,
        client_connections: &Vec<PulsarManagerRegistration>,
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
                    for client_connection in client_connections {
                        client_connection
                            .get_connection()
                            .send(Ok(inbound.clone()))
                            .await
                            .unwrap();
                    }
                }
            },
            Err(inbound_err) => match inbound_err {
                NeutronError::Disconnected => {
                    for client_connection in client_connections {
                        client_connection
                            .get_connection()
                            .send(Err(NeutronError::Disconnected))
                            .await
                            .unwrap();
                    }
                }
                _ => (),
            },
        }
        Ok(())
    }

    pub(crate) async fn handle_registration(
        &self,
        registration_manager: &EngineConnection<(), PulsarManagerRegistration>,
        registration: PulsarManagerRegistration,
    ) {
        self.client_connection.lock().await.push(registration);
        registration_manager.send(Ok(())).await.unwrap();
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

    pub async fn next(&mut self) -> bool {
        let mut registration: Option<Result<PulsarManagerRegistration, NeutronError>> = None;
        match (
            &self.client_connection.lock().await,
            &self.socket_connection,
            &self.registration_manager_connection,
        ) {
            (
                // where the vec is not empty
                client_connections,
                Some(socket_connection),
                Some(registration_manager_connection),
            ) if !client_connections.is_empty() => {
                if let Some(inbound) = self.inbound_buffer.lock().await.pop() {
                    self.handle_socket_connection_inbound(
                        &client_connections,
                        &socket_connection,
                        &Ok(inbound),
                    )
                    .await
                    .unwrap();
                    return false;
                }
                let client_connection_futures = futures::future::select_all(
                    client_connections
                        .iter()
                        .map(|x| async { x.get_connection().recv().await }.boxed()),
                );
                tokio::select! {
                    (outbound, index, _) = client_connection_futures => {
                        if is_disconnect(&outbound) {
                            log::debug!("Client connection disconnected");
                            socket_connection.send(Err(NeutronError::Disconnected)).await.unwrap();
                        }
                        if let Some(client_connection) = client_connections.get(index) {
                            self.handle_client_connection_outbound(&client_connection.get_connection(), &socket_connection, &outbound).await.unwrap();
                        }
                    }
                    inbound = socket_connection.recv() => {
                        if is_disconnect(&inbound) {
                            log::debug!("Connection engine disconnected");
                            //client_connection.send(Err(NeutronError::Disconnected)).await.unwrap();
                            return true;
                        }
                        self.handle_socket_connection_inbound(&client_connections, &socket_connection, &inbound).await.unwrap();
                    }
                    new_registration = registration_manager_connection.recv() => {
                        log::info!("Registration received");
                        registration = Some(new_registration);
                    }
                };
                ()
            }
            (client_connections, _, Some(registration_manager_connection))
                if client_connections.is_empty() =>
            {
                log::info!("Awaiting Registration");
                registration = Some(registration_manager_connection.recv().await);
            }
            _ => {
                log::error!("Connection engine or client connection is None");
                return true;
            }
        }
        if let (Some(registration_manager_connection), Some(registration)) =
            (&self.registration_manager_connection, registration)
        {
            self.handle_registration(&registration_manager_connection, registration.unwrap())
                .await;
        }
        false
    }

    async fn start_pulsar(
        &mut self,
        registration_manager_connection: EngineConnection<(), PulsarManagerRegistration>,
    ) {
        self.registration_manager_connection = Some(registration_manager_connection);
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

    pub fn run(mut self) -> PulsarManager {
        let (tx, _rx) = async_channel::unbounded::<Result<(), NeutronError>>();
        let (_tx, rx) =
            async_channel::unbounded::<Result<PulsarManagerRegistration, NeutronError>>();

        let registration_manager_connection = EngineConnection::new(tx, rx);

        tokio::task::spawn(async move {
            self.start_pulsar(registration_manager_connection).await;
        });

        PulsarManager {
            inner_connection: EngineConnection::new(_tx, _rx),
        }
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

pub struct PulsarManager {
    inner_connection: EngineConnection<PulsarManagerRegistration, ()>,
}

impl PulsarManager {
    pub async fn register_consumer(
        &self,
        config: &ConsumerConfig,
    ) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let (tx, _rx) = async_channel::unbounded::<ResultInbound>();
        let (_tx, rx) = async_channel::unbounded::<ResultOutbound>();
        let connection = EngineConnection::new(tx, rx);

        self.inner_connection
            .send(Ok(PulsarManagerRegistration::Consumer {
                consumer_id: config.consumer_id,
                topic: config.topic.clone(),
                connection,
            }))
            .await
            .map_err(|_| NeutronError::ChannelTerminated)?;

        self.inner_connection.recv().await?;

        Ok(EngineConnection::new(_tx, _rx))
    }

    pub async fn register_producer(
        &self,
        producer: &Producer,
    ) -> Result<EngineConnection<Outbound, Inbound>, NeutronError> {
        let (tx, _rx) = async_channel::unbounded::<ResultInbound>();
        let (_tx, rx) = async_channel::unbounded::<ResultOutbound>();
        let connection = EngineConnection::new(tx, rx);

        let config = producer.config.read().await;

        self.inner_connection
            .send(Ok(PulsarManagerRegistration::Producer {
                producer_id: config.producer_id,
                topic: config.topic.clone(),
                connection,
            }))
            .await
            .map_err(|_| NeutronError::ChannelTerminated)?;

        self.inner_connection.recv().await?;

        Ok(EngineConnection::new(_tx, _rx))
    }
}
