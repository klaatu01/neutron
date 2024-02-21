use futures::FutureExt;

use crate::{
    connection_manager::BrokerAddress,
    engine::EngineConnection,
    message::{Inbound, Outbound},
    NeutronError,
};

pub struct ClientData {
    pub id: u64,
    pub connection: EngineConnection<Inbound, Outbound>,
    pub topic: String,
    pub broker_address: String,
}

pub enum Client {
    Producer(ClientData),
    Consumer(ClientData),
}

impl Client {
    fn data(&self) -> &ClientData {
        match self {
            Client::Producer(data) => data,
            Client::Consumer(data) => data,
        }
    }

    fn get_connection(&self) -> &EngineConnection<Inbound, Outbound> {
        &self.data().connection
    }

    fn broker_address(&self) -> &str {
        &self.data().broker_address
    }

    fn is_producer(&self) -> bool {
        match self {
            Client::Producer(_) => true,
            _ => false,
        }
    }

    fn is_consumer(&self) -> bool {
        match self {
            Client::Consumer(_) => true,
            _ => false,
        }
    }
}

pub struct ClientManager {
    clients: Vec<Client>,
}

impl ClientManager {
    pub fn new() -> Self {
        ClientManager {
            clients: Vec::new(),
        }
    }

    pub fn add_client(&mut self, client: Client) {
        self.clients.push(client);
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub async fn next_message(&self) -> (BrokerAddress, Result<Outbound, NeutronError>) {
        let (next, _, _) = futures::future::select_all(self.clients.iter().map(|client| {
            async {
                let connection = client.get_connection();
                let broker_address = client.broker_address().to_string();
                let message = connection.recv().await;
                (broker_address, message)
            }
            .boxed()
        }))
        .await;
        next
    }

    pub async fn send(
        &self,
        inbound: &Inbound,
        broker_address: &BrokerAddress,
    ) -> Result<(), NeutronError> {
        let clients = self
            .clients
            .iter()
            .filter(|client| client.broker_address() == broker_address)
            .filter(|client| match inbound.get_target() {
                Some(target) => match target {
                    crate::message::Target::Producer { producer_id } if client.is_producer() => {
                        client.data().id == producer_id
                    }
                    crate::message::Target::Consumer { consumer_id } if client.is_consumer() => {
                        client.data().id == consumer_id
                    }
                    _ => false,
                },
                None => true,
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
}
