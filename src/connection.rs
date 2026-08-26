use crate::broker_address::BrokerAddress;
use crate::codec::Codec;
use crate::correlation::{CorrelationKey, Inflight};
use crate::engine::{Engine, EngineConnection};
use crate::error::NeutronError;
use crate::message::{Command, Inbound, MessageCommand, Outbound};
use async_trait::async_trait;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_util::codec::Framed;
use url::Url;

/// How many outbound commands may queue toward the socket before senders
/// feel backpressure.
const OUTBOUND_QUEUE_CAPACITY: usize = 256;

/// The most frames fed into the encoder per flush. Under load the writer
/// drains whatever has accumulated (up to this bound) and pays for one
/// flush, so syscall cost amortizes across the batch.
const MAX_WRITE_BATCH: usize = 128;

/// TCP and TLS streams erased behind one boxed IO type, so the connection
/// is a single `Framed` and splits into genuine sink/stream halves.
pub(crate) trait AsyncIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncIo for T {}
pub(crate) type IoStream = Box<dyn AsyncIo>;

pub struct PulsarConnection {
    framed: Framed<IoStream, Codec>,
    inflight: Arc<Inflight>,
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

        let io: IoStream = if broker_address.is_tls() {
            log::info!("TLS enabled");
            let mut root_cert_store = RootCertStore::empty();
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(config));
            let dns_name =
                ServerName::try_from(host).map_err(|_| NeutronError::InvalidUrl)?;
            match connector.connect(dns_name, stream).await {
                Ok(stream) => Box::new(stream),
                Err(e) => {
                    log::warn!("Error: {}", e);
                    return Err(NeutronError::ConnectionFailed);
                }
            }
        } else {
            Box::new(stream)
        };

        Ok(Self {
            framed: Framed::new(io, Codec),
            inflight: Inflight::new(),
        })
    }
}

/// The writer owns the sink half and is the only place that decides when
/// to flush: it drains whatever commands have queued, feeds each into the
/// encoder, and flushes once for the whole batch. Cost per message falls
/// as offered load rises.
async fn run_writer(
    mut sink: SplitSink<Framed<IoStream, Codec>, MessageCommand>,
    outbound: async_channel::Receiver<Result<Command<Outbound, Inbound>, NeutronError>>,
    inflight: Arc<Inflight>,
) {
    'connection: loop {
        let first = match outbound.recv().await {
            Ok(command) => command,
            Err(_) => break,
        };

        let mut batch = Vec::with_capacity(8);
        batch.push(first);
        while batch.len() < MAX_WRITE_BATCH {
            match outbound.try_recv() {
                Ok(command) => batch.push(command),
                Err(_) => break,
            }
        }

        let mut fed = false;
        for command in batch {
            match command {
                Ok(command) => {
                    let outbound = match command {
                        Command::Request(outbound) => outbound,
                        Command::RequestResponse(outbound, sender) => {
                            // The entry must exist before the frame is on
                            // the wire, or the response could race it.
                            match CorrelationKey::of_outbound(&outbound) {
                                Some(key) => inflight.register(key, sender),
                                None => {
                                    let _ = sender.send(Err(NeutronError::Unresolvable));
                                }
                            }
                            outbound
                        }
                    };
                    log::debug!("-> {}", outbound);
                    if let Err(e) = sink.feed(outbound.into()).await {
                        log::warn!("Write failed: {}", e);
                        break 'connection;
                    }
                    fed = true;
                }
                Err(e) => {
                    log::warn!("{}", e);
                    if e.is_disconnect() {
                        break 'connection;
                    }
                }
            }
        }

        if fed {
            if let Err(e) = sink.flush().await {
                log::warn!("Flush failed: {}", e);
                break;
            }
        }
    }
    log::debug!("Connection writer stopped");
}

#[async_trait]
impl Engine<Inbound, Command<Outbound, Inbound>> for PulsarConnection {
    async fn run(mut self) -> EngineConnection<Command<Outbound, Inbound>, Inbound> {
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_QUEUE_CAPACITY);
        let (inbound_tx, inbound_rx) = async_channel::unbounded();
        let hub_side = EngineConnection::new(outbound_tx, inbound_rx);

        self.inflight.start_sweeper();
        let (sink, stream) = self.framed.split();

        tokio::spawn(run_writer(sink, outbound_rx, self.inflight.clone()));

        let inflight = self.inflight;
        tokio::spawn(async move {
            reader_loop(stream, inbound_tx, &inflight).await;
            // No response can arrive past this point; nobody may be left
            // waiting on one.
            inflight.drain(NeutronError::Disconnected);
        });

        hub_side
    }
}

/// The reader owns the stream half: it decodes frames, completes waiting
/// requests through the in-flight table, and forwards everything else.
/// A slow flush on the writer can never delay decoding.
async fn reader_loop(
    mut stream: SplitStream<Framed<IoStream, Codec>>,
    inbound_tx: async_channel::Sender<Result<Inbound, NeutronError>>,
    inflight: &Inflight,
) {
    while let Some(next) = stream.next().await {
        let frame = match next {
            Ok(frame) => frame,
            Err(e) => {
                // After a framing error the stream alignment is unknown;
                // continuing would decode garbage.
                log::warn!("Decode failed, closing connection: {}", e);
                break;
            }
        };

        let frame_type = frame.command.type_();
        let inbound = match Inbound::try_from(frame) {
            Ok(inbound) => inbound,
            Err(_) => {
                log::warn!("Unsupported command: {:?}", frame_type);
                continue;
            }
        };

        log::debug!("<- {}", inbound);

        if inflight.try_resolve(&inbound) {
            continue;
        }
        if inbound_tx.send(Ok(inbound)).await.is_err() {
            break;
        }
    }
    log::warn!("Connection reader stopped");
}
