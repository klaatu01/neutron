use bimap::BiHashMap;
use futures::FutureExt;

use crate::{
    broker_address::BrokerAddress,
    command_resolver::CommandResolver,
    engine::EngineConnection,
    message::{Command, Inbound, Outbound},
    NeutronError,
};

#[derive(Clone)]
pub struct ClientData {
    pub id: u64,
    pub broker_address: BrokerAddress,
    pub topic: String,
}

#[derive(Debug)]
pub struct ClientConnection {
    pub id: u64,
    pub connection: EngineConnection<Inbound, Command<Outbound, Inbound>>,
    pub topic: String,
    pub broker_address: BrokerAddress,
}

impl Into<ClientData> for &ClientConnection {
    fn into(self) -> ClientData {
        ClientData {
            id: self.id,
            broker_address: self.broker_address.clone(),
            topic: self.topic.clone(),
        }
    }
}

impl ClientConnection {
    fn get_connection(&self) -> &EngineConnection<Inbound, Command<Outbound, Inbound>> {
        &self.connection
    }

    fn broker_address(&self) -> &BrokerAddress {
        &self.broker_address
    }
}

pub struct ClientManager {
    clients: Vec<ClientConnection>,
    pub(crate) command_resolver: CommandResolver<Outbound, Inbound>,
    pub(crate) request_id_map: BiHashMap<(u64, u64), u64>,
}

impl ClientManager {
    pub fn new() -> Self {
        ClientManager {
            clients: Vec::new(),
            command_resolver: CommandResolver::new(),
            request_id_map: BiHashMap::new(),
        }
    }

    pub fn add_client(&mut self, client: ClientConnection) {
        self.clients.push(client);
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub async fn next(&self) -> (ClientData, Result<Outbound, NeutronError>) {
        let (next, _, _) = futures::future::select_all(self.clients.iter().map(|client| {
            async {
                let connection = client.get_connection();
                let message = connection.recv().await;
                (client.into(), message)
            }
            .boxed()
        }))
        .await;

        let outbound = match next.1 {
            Ok(cmd) => match cmd {
                Command::RequestResponse(outbound, sender) => {
                    self.command_resolver.put(outbound.clone(), sender).await;
                    Ok(outbound)
                }
                _ => Ok(cmd.get_outbound()),
            },
            Err(err) => Err(err),
        };

        (next.0, outbound)
    }

    pub async fn send(
        &self,
        inbound: &Inbound,
        broker_address: &BrokerAddress,
    ) -> Result<(), NeutronError> {
        if self.command_resolver.try_resolve(inbound.clone()).await {
            return Ok(());
        }

        let consumer_or_producer_id = inbound.try_consumer_or_producer_id();

        let clients = self
            .clients
            .iter()
            .filter(|client| client.broker_address() == broker_address)
            .filter(|client| {
                if let Some(id) = consumer_or_producer_id {
                    client.id == id
                } else {
                    true
                }
            });

        futures::future::join_all(clients.map(|client| async {
            let connection = client.get_connection();
            connection.send(Ok(inbound.clone())).await
        }))
        .await;

        Ok(())
    }

    pub async fn send_all(
        &self,
        inbound: &Result<Inbound, NeutronError>,
    ) -> Result<(), NeutronError> {
        let clients = self.clients.iter().map(|client| {
            let connection = client.get_connection();
            connection.send(inbound.clone())
        });

        futures::future::join_all(clients).await;

        Ok(())
    }

    pub fn move_client_to_broker(&mut self, id: u64, broker_address: &BrokerAddress) {
        log::debug!("clients: {:?}", self.clients);
        for client in self.clients.iter_mut() {
            if client.id == id {
                log::debug!(
                    "old broker: {:?}, new: {:?}",
                    client.broker_address,
                    broker_address
                );
                client.broker_address = broker_address.clone();
            }
        }
        log::debug!("clients: {:?}", self.clients);
    }

    pub fn get_client(&self, id: u64) -> Option<&ClientConnection> {
        self.clients.iter().find(|client| client.id == id)
    }
}
