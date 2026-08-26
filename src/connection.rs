use std::collections::HashMap;
use std::sync::{Arc, Weak};

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_util::codec::Framed;
use url::Url;

use crate::broker_address::BrokerAddress;
use crate::codec::Codec;
use crate::correlation::{CorrelationKey, Inflight, OPERATION_TIMEOUT};
use crate::error::NeutronError;
use crate::message::{AuthChallenge, Connect, Connected, Inbound, MessageCommand, Outbound};
use crate::pulsar::PulsarInner;

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

/// Control messages for the reader task, which owns the routing map: only
/// the reader ever touches it, so routing needs no lock at all.
pub(crate) enum ReaderCtl {
    Bind {
        id: u64,
        inbox: mpsc::Sender<Inbound>,
    },
    Unbind {
        id: u64,
    },
}

/// A live connection to one broker: the address for sending frames toward
/// its writer task, binding clients into its reader's routing map, and
/// registering request/response waiters.
///
/// Cheap to clone by `Arc`; dropping every handle does not close the
/// connection (the actor tasks own the socket), but the actor stops when
/// the socket does.
pub(crate) struct ConnectionHandle {
    #[allow(dead_code)] // read by the supervisor (reconnect), which lands next
    pub(crate) broker_address: BrokerAddress,
    writer_tx: mpsc::Sender<MessageCommand>,
    reader_ctl: mpsc::Sender<ReaderCtl>,
    inflight: Arc<Inflight>,
    connected: Connected,
}

impl ConnectionHandle {
    /// Fire-and-forget: queue a command toward the socket. A full queue
    /// applies backpressure to the caller.
    pub(crate) async fn send(&self, outbound: Outbound) -> Result<(), NeutronError> {
        self.writer_tx
            .send(outbound.into())
            .await
            .map_err(|_| NeutronError::Disconnected)
    }

    /// Send a command and hand back the receiver its response will
    /// arrive on. The in-flight entry is registered before the frame is
    /// queued, so the response can never race its own table entry.
    pub(crate) async fn request_deferred(
        &self,
        outbound: Outbound,
    ) -> Result<oneshot::Receiver<Result<Inbound, NeutronError>>, NeutronError> {
        let key = CorrelationKey::of_outbound(&outbound).ok_or(NeutronError::Unresolvable)?;
        let (tx, rx) = oneshot::channel();
        self.inflight.register(key, tx);
        self.writer_tx
            .send(outbound.into())
            .await
            .map_err(|_| NeutronError::Disconnected)?;
        Ok(rx)
    }

    /// Send a command and await its response.
    #[allow(dead_code)] // used by session replay (reconnect), which lands next
    pub(crate) async fn request(&self, outbound: Outbound) -> Result<Inbound, NeutronError> {
        let rx = self.request_deferred(outbound).await?;
        rx.await.map_err(|_| NeutronError::ChannelTerminated)?
    }

    /// Route this client's inbound traffic to `inbox`.
    pub(crate) async fn bind(
        &self,
        id: u64,
        inbox: mpsc::Sender<Inbound>,
    ) -> Result<(), NeutronError> {
        self.reader_ctl
            .send(ReaderCtl::Bind { id, inbox })
            .await
            .map_err(|_| NeutronError::Disconnected)
    }

    /// Stop routing this client's inbound traffic. Best effort: a dead
    /// connection has nothing to unbind from.
    pub(crate) async fn unbind(&self, id: u64) {
        let _ = self.reader_ctl.send(ReaderCtl::Unbind { id }).await;
    }

    /// The CONNECTED response cached from this connection's handshake.
    pub(crate) fn connected(&self) -> Connected {
        self.connected.clone()
    }
}

/// A connection that has dialed but not yet completed the CONNECT
/// handshake.
pub(crate) struct PulsarConnection {
    framed: Framed<IoStream, Codec>,
}

impl PulsarConnection {
    pub(crate) async fn connect(broker_address: &BrokerAddress) -> Result<Self, NeutronError> {
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
            let dns_name = ServerName::try_from(host).map_err(|_| NeutronError::InvalidUrl)?;
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
        })
    }

    /// Perform the CONNECT handshake on the raw stream. The connection
    /// only becomes an actor — and only becomes visible to clients — once
    /// the broker has accepted it.
    pub(crate) async fn handshake(
        mut self,
        connect: Connect,
    ) -> Result<(Connected, Self), NeutronError> {
        self.framed
            .send(Outbound::Connect(connect).into())
            .await
            .map_err(|_| NeutronError::EncodeFailed)?;

        loop {
            let frame = tokio::time::timeout(OPERATION_TIMEOUT, self.framed.next())
                .await
                .map_err(|_| NeutronError::OperationTimeout)?
                .ok_or(NeutronError::Disconnected)??;

            match Inbound::try_from(frame) {
                Ok(Inbound::Connected(connected)) => return Ok((connected, self)),
                Ok(Inbound::Ping) => {
                    self.framed
                        .send(Outbound::Pong.into())
                        .await
                        .map_err(|_| NeutronError::EncodeFailed)?;
                }
                Ok(Inbound::Error(error)) => {
                    return Err(NeutronError::PulsarError(error.error));
                }
                Ok(other) => {
                    log::warn!("Unexpected frame during handshake: {}", other);
                }
                Err(_) => continue,
            }
        }
    }

    /// Split into reader and writer tasks and return the handle clients
    /// talk through. `inner` is notified when the connection dies.
    pub(crate) fn spawn(
        self,
        broker_address: BrokerAddress,
        connected: Connected,
        inner: Weak<PulsarInner>,
    ) -> Arc<ConnectionHandle> {
        let (writer_tx, writer_rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let (ctl_tx, ctl_rx) = mpsc::channel(64);
        let inflight = Inflight::new();
        inflight.start_sweeper();

        let (sink, stream) = self.framed.split();

        tokio::spawn(run_writer(sink, writer_rx));

        let handle = Arc::new(ConnectionHandle {
            broker_address: broker_address.clone(),
            writer_tx: writer_tx.clone(),
            reader_ctl: ctl_tx,
            inflight: inflight.clone(),
            connected,
        });

        tokio::spawn({
            let handle = Arc::downgrade(&handle);
            async move {
                run_reader(stream, ctl_rx, writer_tx, inflight.clone(), inner.clone()).await;
                // No response can arrive past this point; nobody may be
                // left waiting on one.
                inflight.drain(NeutronError::Disconnected);
                if let Some(inner) = inner.upgrade() {
                    inner.connection_closed(&broker_address, handle).await;
                }
            }
        });

        handle
    }
}

/// The writer owns the sink half and is the only place that decides when
/// to flush: it drains whatever frames have queued, feeds each into the
/// encoder, and flushes once for the whole batch. Cost per message falls
/// as offered load rises.
async fn run_writer(
    mut sink: SplitSink<Framed<IoStream, Codec>, MessageCommand>,
    mut rx: mpsc::Receiver<MessageCommand>,
) {
    let mut batch: Vec<MessageCommand> = Vec::with_capacity(MAX_WRITE_BATCH);
    loop {
        batch.clear();
        if rx.recv_many(&mut batch, MAX_WRITE_BATCH).await == 0 {
            break;
        }
        for frame in batch.drain(..) {
            if let Err(e) = sink.feed(frame).await {
                log::warn!("Write failed: {}", e);
                return;
            }
        }
        if let Err(e) = sink.flush().await {
            log::warn!("Flush failed: {}", e);
            return;
        }
    }
    let _ = sink.close().await;
    log::debug!("Connection writer stopped");
}

/// The reader owns the stream half and the routing map. Decoded frames
/// resolve in-flight requests or route straight to the owning client's
/// inbox by consumer/producer id — O(1), no intermediary task, no lock.
async fn run_reader(
    mut stream: SplitStream<Framed<IoStream, Codec>>,
    mut ctl_rx: mpsc::Receiver<ReaderCtl>,
    writer_tx: mpsc::Sender<MessageCommand>,
    inflight: Arc<Inflight>,
    inner: Weak<PulsarInner>,
) {
    let mut routes: HashMap<u64, mpsc::Sender<Inbound>> = HashMap::new();

    loop {
        // biased: binds enqueued before a subscribe are processed before
        // any frame that subscribe could have provoked.
        tokio::select! {
            biased;
            ctl = ctl_rx.recv() => match ctl {
                Some(ReaderCtl::Bind { id, inbox }) => {
                    routes.insert(id, inbox);
                }
                Some(ReaderCtl::Unbind { id }) => {
                    routes.remove(&id);
                }
                None => break,
            },
            next = stream.next() => {
                let Some(next) = next else {
                    log::warn!("Connection closed by peer");
                    break;
                };
                let frame = match next {
                    Ok(frame) => frame,
                    Err(e) => {
                        // After a framing error the stream alignment is
                        // unknown; continuing would decode garbage.
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
                handle_inbound(inbound, &mut routes, &writer_tx, &inflight, &inner).await;
            }
        }
    }
    log::warn!("Connection reader stopped");
}

async fn handle_inbound(
    inbound: Inbound,
    routes: &mut HashMap<u64, mpsc::Sender<Inbound>>,
    writer_tx: &mpsc::Sender<MessageCommand>,
    inflight: &Inflight,
    inner: &Weak<PulsarInner>,
) {
    match inbound {
        Inbound::Ping => {
            if writer_tx.try_send(Outbound::Pong.into()).is_err() {
                log::warn!("Outbound queue full; dropping pong");
            }
        }
        Inbound::AuthChallengeRequest(_) => {
            let Some(inner) = inner.upgrade() else { return };
            let Some(auth) = inner.auth_plugin() else {
                log::warn!("Broker sent an auth challenge but no auth plugin is configured");
                return;
            };
            // Fetching auth data can do real IO (token refresh); never
            // stall decoding on it.
            let writer_tx = writer_tx.clone();
            tokio::spawn(async move {
                match auth.auth_data().await {
                    Ok(auth_data) => {
                        let response = Outbound::AuthChallenge(AuthChallenge {
                            auth_method_name: auth.auth_method_name(),
                            auth_data,
                        });
                        let _ = writer_tx.send(response.into()).await;
                    }
                    Err(e) => log::error!("Failed to answer auth challenge: {}", e),
                }
            });
        }
        inbound => {
            if inflight.try_resolve(&inbound) {
                return;
            }
            let Some(id) = inbound.try_consumer_or_producer_id() else {
                log::debug!("Unroutable inbound: {}", inbound);
                return;
            };
            let Some(inbox) = routes.get(&id) else {
                log::debug!("Inbound for unbound client {}: {}", id, inbound);
                return;
            };
            // The inbox capacity is coupled to the flow credit granted to
            // the broker, so this try_send only fails if that accounting
            // is broken; falling back to an await keeps delivery lossless
            // at the cost of head-of-line blocking while the consumer
            // catches up.
            match inbox.try_send(inbound) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    routes.remove(&id);
                }
                Err(mpsc::error::TrySendError::Full(inbound)) => {
                    log::warn!(
                        "Inbox full for client {} — flow credit exceeds inbox capacity",
                        id
                    );
                    if inbox.send(inbound).await.is_err() {
                        routes.remove(&id);
                    }
                }
            }
        }
    }
}
