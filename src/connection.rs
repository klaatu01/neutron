use crate::codec::Codec;
use crate::connection_manager::BrokerAddress;
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{Inbound, Message, Outbound};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_util::codec::Framed;

pub enum ConnectionStream {
    Tcp(Framed<TcpStream, Codec>),
    Tls(Framed<tokio_rustls::client::TlsStream<TcpStream>, Codec>),
}

pub struct PulsarConnection {
    stream: ConnectionStream,
}

impl ConnectionStream {
    pub async fn send(&mut self, message: Message) -> Result<(), NeutronError> {
        match self {
            ConnectionStream::Tcp(stream) => {
                stream.send(message).await.map_err(|e| {
                    log::warn!("Error: {}", e);
                    NeutronError::EncodeFailed
                })?;
            }
            ConnectionStream::Tls(stream) => {
                stream.send(message).await.map_err(|e| {
                    log::warn!("Error: {}", e);
                    NeutronError::EncodeFailed
                })?;
            }
        }
        Ok(())
    }

    pub async fn next(&mut self) -> Option<Result<Message, NeutronError>> {
        match self {
            ConnectionStream::Tcp(stream) => {
                let message = stream.next().await;
                match message {
                    None => None,
                    Some(Ok(message)) => Some(Ok(message)),
                    Some(Err(e)) => {
                        log::warn!("Error: {}", e);
                        Some(Err(NeutronError::DecodeFailed))
                    }
                }
            }
            ConnectionStream::Tls(stream) => {
                let message = stream.next().await;
                match message {
                    None => None,
                    Some(Ok(message)) => Some(Ok(message)),
                    Some(Err(e)) => {
                        log::warn!("Error: {}", e);
                        Some(Err(NeutronError::DecodeFailed))
                    }
                }
            }
        }
    }
}

impl PulsarConnection {
    pub async fn connect(broker_address: BrokerAddress, tls: bool) -> Result<Self, NeutronError> {
        let stream = TcpStream::connect(broker_address.clone())
            .await
            .map_err(|e| {
                log::warn!("Error: {}", e);
                NeutronError::ConnectionFailed
            })?;

        let stream = if tls {
            let mut root_cert_store = RootCertStore::empty();
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(config));
            let dns_name = ServerName::try_from(broker_address).unwrap();
            let stream = connector.connect(dns_name, stream).await;
            match stream {
                Ok(stream) => ConnectionStream::Tls(tokio_util::codec::Framed::new(stream, Codec)),
                Err(e) => {
                    log::warn!("Error: {}", e);
                    return Err(NeutronError::ConnectionFailed);
                }
            }
        } else {
            ConnectionStream::Tcp(tokio_util::codec::Framed::new(stream, Codec))
        };

        Ok(Self { stream })
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
        let (client_connection, connection) = EngineConnection::pair();
        tokio::task::spawn(async move {
            self.start_connection(client_connection).await;
        });
        connection
    }
}
