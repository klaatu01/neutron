use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{Codec, Inbound, Message, Outbound};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::codec::Framed;

type ResultInbound = Result<Inbound, NeutronError>;
type ResultOutbound = Result<Outbound, NeutronError>;

pub struct PulsarConnection {
    stream: Framed<TcpStream, Codec>,
}

impl PulsarConnection {
    pub async fn connect(host: &String, port: &u16) -> Self {
        let addr = format!("{}:{}", host, port);
        log::debug!("Opening connection to {}", addr);
        let stream = TcpStream::connect(addr)
            .await
            .map(|stream| tokio_util::codec::Framed::new(stream, Codec))
            .unwrap();
        log::debug!("Connection opened");

        Self { stream }
    }

    pub async fn start_connection(
        &mut self,
        client_connection: EngineConnection<Inbound, Outbound>,
    ) {
        loop {
            tokio::select! {
                outbound = client_connection.recv() => {
                    match outbound {
                        Ok(outbound) => {
                            log::debug!("{}", outbound.to_string());
                            let msg: Message = outbound.into();
                            let _ = self
                                .stream.send(msg).await;
                        }
                        Err(e) => {
                            log::warn!("{}", e);
                        }
                    }
                },
                message = self.stream.next() => {
                    let inbound = match message {
                        None => {
                            log::warn!("Connection closed");
                            Err(NeutronError::Disconnected)
                        }
                        Some(Ok(message)) =>
                                Inbound::try_from(&message)
                                    .map_err(|_| NeutronError::UnsupportedCommand),
                        Some(Err(e)) => {
                            log::warn!("Error: {}", e);
                            Err(NeutronError::DecodeFailed)
                        }
                    };

                    if let Err(_) = client_connection.send(inbound.clone()).await {
                        break;
                    }

                    match &inbound {
                        Ok(inbound) => {
                            log::debug!("{}", inbound.to_string());
                        }
                        Err(NeutronError::Disconnected) => {
                            log::warn!("Disconnected");
                            break;
                        }
                        Err(e) => {
                            log::warn!("Error: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Engine<Inbound, Outbound> for PulsarConnection {
    async fn run(mut self) -> EngineConnection<Outbound, Inbound> {
        let (tx, _rx) = async_channel::unbounded::<ResultInbound>();
        let (_tx, rx) = async_channel::unbounded::<ResultOutbound>();

        let client_connection = EngineConnection::new(tx, rx);

        tokio::task::spawn(async move {
            self.start_connection(client_connection).await;
        });

        EngineConnection::new(_tx, _rx)
    }
}
