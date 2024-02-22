use std::collections::HashMap;

use futures::FutureExt;

use crate::{
    engine::EngineConnection,
    message::{Inbound, Outbound},
    NeutronError,
};

pub type BrokerAddress = String;

pub struct ConnectionManager {
    connections: HashMap<BrokerAddress, EngineConnection<Outbound, Inbound>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        ConnectionManager {
            connections: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub async fn send(
        &mut self,
        message: Result<Outbound, NeutronError>,
        broker_address: BrokerAddress,
    ) -> Result<(), NeutronError> {
        let connection = self
            .connections
            .get(broker_address.as_str())
            .ok_or(NeutronError::Disconnected)?;

        connection.send(message).await
    }

    pub async fn next(&self) -> (BrokerAddress, Result<Inbound, NeutronError>) {
        let (next, _, _) = futures::future::select_all(self.connections.iter().map(
            |(broker_address, connection)| {
                async {
                    let message = connection.recv().await;
                    (broker_address.clone(), message)
                }
                .boxed()
            },
        ))
        .await;
        next
    }

    pub fn add_connection(
        &mut self,
        broker_address: BrokerAddress,
        connection: EngineConnection<Outbound, Inbound>,
    ) {
        self.connections.insert(broker_address, connection);
    }

    pub fn get_connection(
        &self,
        broker_address: &str,
    ) -> Option<&EngineConnection<Outbound, Inbound>> {
        self.connections.get(broker_address)
    }

    pub fn remove_connection(&mut self, broker_address: &str) {
        self.connections.remove(broker_address);
    }
}
