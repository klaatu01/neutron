use std::collections::HashMap;

use futures::future::FutureExt;

use crate::{
    broker_address::BrokerAddress,
    engine::EngineConnection,
    message::{Command, Inbound, Outbound},
    NeutronError,
};

pub struct ConnectionManager {
    connections: HashMap<BrokerAddress, EngineConnection<Command<Outbound, Inbound>, Inbound>>,
}

impl ConnectionManager {
    pub(crate) fn new() -> Self {
        ConnectionManager {
            connections: HashMap::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub(crate) async fn send(
        &mut self,
        message: Result<Command<Outbound, Inbound>, NeutronError>,
        broker_address: &BrokerAddress,
    ) -> Result<(), NeutronError> {
        let connection = self
            .connections
            .get(broker_address)
            .ok_or(NeutronError::Disconnected)?;

        connection.send(message).await
    }

    pub(crate) async fn next(&self) -> (BrokerAddress, Result<Inbound, NeutronError>) {
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

    pub(crate) fn add_connection(
        &mut self,
        broker_address: BrokerAddress,
        connection: EngineConnection<Command<Outbound, Inbound>, Inbound>,
    ) {
        self.connections.insert(broker_address, connection);
    }

    pub(crate) fn get_connection(
        &self,
        broker_address: &BrokerAddress,
    ) -> Option<&EngineConnection<Command<Outbound, Inbound>, Inbound>> {
        self.connections.get(broker_address)
    }
}
