use std::{
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::Future;

use crate::{
    engine::EngineConnection,
    message::{
        self, proto::pulsar::MessageIdData, AckReciept, Command, Connect, Connected, Inbound,
        LookupResponseType, LookupTopic, LookupTopicResponse, Outbound, SendReceipt, Subscribe,
        Success,
    },
    NeutronError,
};

pub(crate) struct Client {
    pub(crate) pulsar_engine_connection: EngineConnection<Command<Outbound, Inbound>, Inbound>,
    pub(crate) client_id: u64,
    pub(crate) client_name: String,
    pub(crate) request_id: AtomicU64,
    sequence_id: AtomicU64,
}

impl Client {
    pub(crate) fn new(
        pulsar_engine_connection: EngineConnection<Command<Outbound, Inbound>, Inbound>,
        client_id: u64,
        client_name: String,
    ) -> Self {
        Self {
            pulsar_engine_connection,
            client_id,
            client_name,
            request_id: AtomicU64::new(0),
            sequence_id: AtomicU64::new(0),
        }
    }

    pub(crate) async fn send_command<Request>(&self, command: Request) -> Result<(), NeutronError>
    where
        Request: Into<Outbound>,
    {
        let command = command.into();
        self.pulsar_engine_connection
            .send(Ok(Command::Request(command)))
            .await
    }

    pub(crate) async fn send_command_and_resolve<Request, Response>(
        &self,
        command: Request,
    ) -> Result<Pin<Box<dyn Future<Output = Result<Response, NeutronError>> + Send>>, NeutronError>
    where
        Request: Into<Outbound>,
        Response: TryFrom<Inbound>,
    {
        let outbound = command.into();
        let (tx, rx) = futures::channel::oneshot::channel();
        let command = Command::RequestResponse(outbound, tx);
        self.pulsar_engine_connection.send(Ok(command)).await?;

        Ok(Box::pin(async move {
            let inbound: Result<Result<Inbound, NeutronError>, _> = rx.await;
            match inbound {
                Ok(Ok(inbound)) => {
                    Response::try_from(inbound).map_err(|_| NeutronError::Unresolvable)
                }
                Ok(Err(err)) => Err(err),
                Err(_) => Err(NeutronError::ChannelTerminated),
            }
        }))
    }

    pub(crate) async fn next(&self) -> Result<Inbound, NeutronError> {
        self.pulsar_engine_connection.recv().await
    }

    pub(crate) async fn connect(&self) -> Result<Connected, NeutronError> {
        self.send_command_and_resolve(Connect {
            proxy_url: None,
            auth_data: None,
            auth_method_name: None,
        })
        .await?
        .await
    }

    pub(crate) async fn connect_to_proxy(
        &self,
        proxy_url: String,
    ) -> Result<Connected, NeutronError> {
        self.send_command_and_resolve(Connect {
            proxy_url: Some(proxy_url),
            auth_data: None,
            auth_method_name: None,
        })
        .await?
        .await
    }

    pub(crate) async fn lookup_topic(&self, topic: &str) -> Result<(), NeutronError> {
        log::info!("Looking up topic {}", topic);
        let lookup: LookupTopicResponse = self
            .send_command_and_resolve::<_, LookupTopicResponse>(LookupTopic {
                request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
                topic: topic.to_string(),
            })
            .await?
            .await?;

        match lookup.response_type {
            LookupResponseType::Connect => {
                if lookup.proxy {
                    log::info!("Proxying to {}", lookup.broker_service_url);
                    self.connect_to_proxy(lookup.broker_service_url).await?;
                    self.send_command_and_resolve::<_, LookupTopicResponse>(LookupTopic {
                        request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
                        topic: topic.to_string(),
                    })
                    .await?
                    .await?;
                }
                Ok(())
            }
            LookupResponseType::Redirect => {
                unimplemented!("Redirect is not a supported feature yet");
            }
            LookupResponseType::Failed => Err(NeutronError::ConnectionFailed),
        }
    }

    pub(crate) async fn producer(&self, topic: &str) -> Result<(), NeutronError> {
        self.send_command_and_resolve::<_, Success>(message::Producer {
            producer_id: self.client_id,
            producer_name: Some(self.client_name.clone()),
            topic: topic.to_string(),
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
        })
        .await?
        .await
        .map(|_| ())
    }

    pub(crate) async fn subscribe(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<(), NeutronError> {
        self.send_command_and_resolve::<_, Success>(Subscribe {
            topic: topic.to_string(),
            consumer_id: self.client_id,
            subscription: subscription.to_string(),
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            sub_type: message::SubType::Shared,
        })
        .await?
        .await?;
        Ok(())
    }

    pub(crate) async fn ack(&self, message_id: &MessageIdData) -> Result<(), NeutronError> {
        self.send_command_and_resolve::<_, AckReciept>(message::Ack {
            consumer_id: self.client_id,
            message_id: message_id.clone(),
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
        })
        .await?
        .await?;
        Ok(())
    }

    pub(crate) async fn send_message(
        &self,
        payload: Vec<u8>,
    ) -> Result<Pin<Box<dyn Future<Output = Result<SendReceipt, NeutronError>> + Send>>, NeutronError>
    {
        self.send_command_and_resolve(message::Send {
            producer_name: self.client_name.clone(),
            producer_id: self.client_id,
            sequence_id: self.sequence_id.fetch_add(1, Ordering::SeqCst),
            payload,
        })
        .await
    }

    pub(crate) async fn next_message(&self) -> Result<message::Message, NeutronError> {
        loop {
            let inbound = self.next().await?;
            match message::Message::try_from(inbound) {
                Ok(message) => return Ok(message),
                Err(_) => continue,
            }
        }
    }

    pub(crate) async fn flow(&self, message_permits: u32) -> Result<(), NeutronError> {
        self.send_command(message::Flow {
            message_permits,
            consumer_id: self.client_id,
        })
        .await
    }
}
