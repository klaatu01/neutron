use bimap::BiHashMap;
use futures::FutureExt;

use crate::{
    broker_address::BrokerAddress,
    command_resolver::CommandResolver,
    engine::EngineConnection,
    message::{Command, Inbound, Outbound},
    NeutronError,
};

pub struct ClientData {
    pub id: u64,
    pub connection: EngineConnection<Inbound, Command<Outbound, Inbound>>,
    pub topic: String,
    pub broker_address: BrokerAddress,
}

impl ClientData {
    fn get_connection(&self) -> &EngineConnection<Inbound, Command<Outbound, Inbound>> {
        &self.connection
    }

    fn broker_address(&self) -> &BrokerAddress {
        &self.broker_address
    }
}

pub struct ClientManager {
    clients: Vec<ClientData>,
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

    pub fn add_client(&mut self, client: ClientData) {
        self.clients.push(client);
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub async fn next(&self) -> (BrokerAddress, Result<Outbound, NeutronError>) {
        let (next, _, _) = futures::future::select_all(self.clients.iter().map(|client| {
            async {
                let connection = client.get_connection();
                let broker_address = client.broker_address();
                let message = connection.recv().await;
                (broker_address, message)
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

        (next.0.clone(), outbound)
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

    pub fn update_broker_address_for_topic(&mut self, topic: &str, broker_address: &BrokerAddress) {
        for mut client in self.clients.iter_mut() {
            if client.topic == topic {
                println!("Updating broker address for topic: {}", topic);
                client.broker_address = broker_address.clone();
            }
        }
    }

    pub fn move_clients_to_new_broker(&mut self, from: &BrokerAddress, to: &BrokerAddress) {
        let mut clients = Vec::new();
        for mut client in self.clients.iter_mut() {
            if client.broker_address == *from {
                client.broker_address = to.clone();
            }
        }
        self.clients = clients;
    }
}
