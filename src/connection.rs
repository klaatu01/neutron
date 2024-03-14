use crate::broker_address::BrokerAddress;
use crate::codec::Codec;
use crate::command_resolver::CommandResolver;
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{Command, Inbound, MessageCommand, Outbound};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_util::codec::Framed;
use url::Url;

pub enum ConnectionStream {
    Tcp(Framed<TcpStream, Codec>),
    Tls(Framed<tokio_rustls::client::TlsStream<TcpStream>, Codec>),
}

pub struct PulsarConnection {
    stream: ConnectionStream,
    command_resolver: CommandResolver<Outbound, Inbound>,
}

impl ConnectionStream {
    pub async fn send(&mut self, message: MessageCommand) -> Result<(), NeutronError> {
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

    pub async fn next(&mut self) -> Option<Result<MessageCommand, NeutronError>> {
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
    pub async fn connect(broker_address: BrokerAddress) -> Result<Self, NeutronError> {
        let broker_url =
            Url::parse(broker_address.base_url()).map_err(|_| NeutronError::InvalidUrl)?;
        log::info!("Connecting to {}", broker_url);

        let host = broker_url
            .host_str()
            .ok_or(NeutronError::InvalidUrl)?
            .to_owned();

        let port = broker_url.port().ok_or(NeutronError::InvalidUrl)?;

        let stream = TcpStream::connect(format!("{}:{}", &host, &port))
            .await
            .map_err(|e| {
                log::warn!("Error: {}", e);
                NeutronError::ConnectionFailed
            })?;

        let stream = if broker_address.is_tls() {
            log::info!("TLS enabled");
            let mut root_cert_store = RootCertStore::empty();
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(config));
            log::info!("Connecting to {}", broker_address);
            let dns_name = ServerName::try_from(host).unwrap();
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

        Ok(Self {
            stream,
            command_resolver: CommandResolver::new(),
        })
    }

    pub async fn start_connection(
        &mut self,
        client_connection: EngineConnection<Inbound, Command<Outbound, Inbound>>,
    ) {
        loop {
            tokio::select! {
                outbound = client_connection.recv() => {
                    match outbound {
                        Ok(command) => {
                            let outbound = command.get_outbound();
                            match command {
                                Command::RequestResponse(outbound, sender) => {
                                    self.command_resolver.put(outbound, sender).await;
                                },
                                _ => ()
                            }
                            log::debug!("-> {}", outbound.to_string());
                            let msg: MessageCommand = outbound.into();
                            let _ = self
                                .stream.send(msg).await;
                        }
                        Err(e) => {
                            log::warn!("{}", e);
                            if e.is_disconnect() {
                                break;
                            }
                        }
                    }
                },
                message = self.stream.next() => {
                    let inbound = match message {
                        None => {
                            log::warn!("Connection closed");
                            Err(NeutronError::Disconnected)
                        }
                        Some(Ok(message)) => {
                            let _type = message.command.type_();
                            Inbound::try_from(message)
                                .map_err(|_| {
                                    log::warn!("Unsupported command: {:?}", _type);
                                    NeutronError::UnsupportedCommand
                                })
                        },
                        Some(Err(e)) => {
                            log::warn!("Error: {}", e);
                            Err(NeutronError::DecodeFailed)
                        }
                    };

                    match &inbound {
                        Ok(inbound) => {
                            log::debug!("<- {}", inbound.to_string());
                            log::debug!("{:?}", inbound);
                        }
                        Err(NeutronError::Disconnected) => {
                            log::warn!("Disconnected");
                            break;
                        }
                        Err(e) => {
                            log::warn!("Error: {}", e);
                        }
                    }

                    if let Ok(inbound) = &inbound {
                        if self.command_resolver.try_resolve(inbound.clone()).await {
                            continue
                        }
                        if let Err(_) = client_connection.send(Ok(inbound.clone())).await {
                            break;
                        }
                    }

                }
            }
        }
    }
}

#[async_trait]
impl Engine<Inbound, Command<Outbound, Inbound>> for PulsarConnection {
    async fn run(mut self) -> EngineConnection<Command<Outbound, Inbound>, Inbound> {
        let (client_connection, connection) = EngineConnection::pair();
        tokio::task::spawn(async move {
            self.start_connection(client_connection).await;
        });
        connection
    }
}
