
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
pub(crate) struct ClientConnection {
    pub(crate) id: u64,
    pub(crate) connection: EngineConnection<Inbound, Command<Outbound, Inbound>>,
    pub(crate) topic: String,
    pub(crate) broker_address: BrokerAddress,
}

impl From<&ClientConnection> for ClientData {
    fn from(val: &ClientConnection) -> Self {
        ClientData {
            id: val.id,
            broker_address: val.broker_address.clone(),
            topic: val.topic.clone(),
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

pub(crate) struct ClientManager {
    clients: Vec<ClientConnection>,
    pub(crate) command_resolver: CommandResolver<Outbound, Inbound>,
}

impl ClientManager {
    pub(crate) fn new() -> Self {
        ClientManager {
            clients: Vec::new(),
            command_resolver: CommandResolver::new(),
        }
    }

    pub(crate) fn add_client(&mut self, client: ClientConnection) {
        self.clients.push(client);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub(crate) async fn next(&self) -> (ClientData, Result<Outbound, NeutronError>) {
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

    pub(crate) async fn send(
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

    #[allow(dead_code)]
    pub(crate) async fn send_all(
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

    pub(crate) fn move_client_to_broker(&mut self, id: u64, broker_address: &BrokerAddress) {
        for client in self.clients.iter_mut() {
            if client.id == id {
                client.broker_address = broker_address.clone();
            }
        }
    }

    pub(crate) fn get_client(&self, id: u64) -> Option<&ClientConnection> {
        self.clients.iter().find(|client| client.id == id)
    }
}
