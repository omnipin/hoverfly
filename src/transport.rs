//! libp2p transport built around bee's bidirectional handshake/pricing dance.
//!
//! Each public method (`fetch_chunk`, `pushsync_chunk`, `discover_peers`)
//! builds a fresh `Swarm`, dials the peer, drives the bidirectional
//! handshake + pricing exchange, then performs its operation and drops the
//! swarm. The transport only accepts ws/wss multiaddrs.

use core::time::Duration;
use futures::StreamExt;
use libp2p::{
    identity::Keypair,
    noise,
    swarm::{dial_opts::DialOpts, NetworkBehaviour, SwarmEvent},
    yamux, Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use thiserror::Error;
use tracing::{debug, info};

use crate::dnsaddr::is_ws_multiaddr;
use crate::peers::Peer;
use crate::protocols::handshake::{self, HandshakeError};
use crate::protocols::hive;
use crate::protocols::pricing;
use crate::protocols::pushsync::{self, PushsyncReceipt};
use crate::protocols::retrieval::{self, ChunkDelivery};
use crate::signer::{SignerError, SwarmSigner};

const HANDSHAKE_PROTO: StreamProtocol = StreamProtocol::new("/swarm/handshake/14.0.0/handshake");
const PRICING_PROTO: StreamProtocol = StreamProtocol::new("/swarm/pricing/1.0.0/pricing");
const HIVE_PROTO: StreamProtocol = StreamProtocol::new("/swarm/hive/1.1.0/peers");
const RETRIEVAL_PROTO: StreamProtocol = StreamProtocol::new("/swarm/retrieval/1.4.0/retrieval");
const PUSHSYNC_PROTO: StreamProtocol = StreamProtocol::new("/swarm/pushsync/1.3.1/pushsync");

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("multiaddr is not ws/wss: {0}")]
    NotWebSocket(String),
    #[error("multiaddr missing peer id")]
    MissingPeerId,
    #[error("dial failed: {0}")]
    DialFailed(String),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("timeout")]
    Timeout,
    #[error("handshake: {0}")]
    Handshake(#[from] HandshakeError),
    #[error("hive: {0}")]
    Hive(#[from] crate::protocols::hive::HiveError),
    #[error("pricing: {0}")]
    Pricing(#[from] crate::protocols::pricing::PricingError),
    #[error("retrieval: {0}")]
    Retrieval(#[from] crate::protocols::retrieval::RetrievalError),
    #[error("pushsync: {0}")]
    Pushsync(#[from] crate::protocols::pushsync::PushsyncError),
    #[error("stream control: {0}")]
    StreamControl(String),
    #[error("framing: {0}")]
    Framing(#[from] crate::protocols::framing::FrameError),
    #[error("signer: {0}")]
    Signer(#[from] SignerError),
    #[error("network mismatch: expected {ours}, got {theirs}")]
    NetworkMismatch { ours: u64, theirs: u64 },
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub timeout: Duration,
    pub network_id: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            network_id: 1,
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub stream: libp2p_stream::Behaviour,
    pub identify: libp2p::identify::Behaviour,
}

fn behaviour(keypair: &Keypair) -> Behaviour {
    Behaviour {
        stream: libp2p_stream::Behaviour::new(),
        identify: libp2p::identify::Behaviour::new(
            libp2p::identify::Config::new("/swarm/0.1.0".to_string(), keypair.public())
                .with_agent_version(format!("isheika/{}", crate::VERSION)),
        ),
    }
}

pub struct Transport {
    keypair: Keypair,
    signer: SwarmSigner,
    config: TransportConfig,
}

impl Transport {
    pub fn new(signer: SwarmSigner, config: TransportConfig) -> Self {
        let keypair = Keypair::generate_ed25519();
        Self { keypair, signer, config }
    }

    pub const fn signer(&self) -> &SwarmSigner {
        &self.signer
    }

    pub const fn config(&self) -> &TransportConfig {
        &self.config
    }

    /// Fetch a single chunk by address.
    pub async fn fetch_chunk(
        &self,
        peer_addr: &Multiaddr,
        chunk_addr: &[u8; 32],
    ) -> Result<ChunkDelivery, TransportError> {
        let peer_id = ensure_ws(peer_addr)?;
        let mut swarm = build_swarm(self).await?;
        let mut control = swarm.behaviour().stream.new_control();
        let mut hs_in = accept(&mut control, HANDSHAKE_PROTO)?;
        let mut pr_in = accept(&mut control, PRICING_PROTO)?;
        let _hive_in = accept(&mut control, HIVE_PROTO)?;
        dial(&mut swarm, peer_id, peer_addr)?;
        let underlay = prep_connection(&mut swarm, peer_id, self.config.timeout).await?;
        do_handshake(&mut swarm, peer_id, &mut control, &mut hs_in, &underlay, &self.signer).await?;
        do_pricing(&mut swarm, peer_id, &mut control, &mut pr_in, self.config.timeout).await?;
        let mut stream = poll_until(&mut swarm, control.open_stream(peer_id, RETRIEVAL_PROTO))
            .await
            .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
        let result = poll_until(&mut swarm, retrieval::fetch(&mut stream, chunk_addr)).await?;
        Ok(result)
    }

    /// Push a single chunk to a peer.
    pub async fn pushsync_chunk(
        &self,
        peer_addr: &Multiaddr,
        chunk_addr: &[u8; 32],
        chunk_data: &[u8],
        stamp: &[u8],
    ) -> Result<PushsyncReceipt, TransportError> {
        let peer_id = ensure_ws(peer_addr)?;
        let mut swarm = build_swarm(self).await?;
        let mut control = swarm.behaviour().stream.new_control();
        let mut hs_in = accept(&mut control, HANDSHAKE_PROTO)?;
        let mut pr_in = accept(&mut control, PRICING_PROTO)?;
        let _hive_in = accept(&mut control, HIVE_PROTO)?;
        dial(&mut swarm, peer_id, peer_addr)?;
        let underlay = prep_connection(&mut swarm, peer_id, self.config.timeout).await?;
        do_handshake(&mut swarm, peer_id, &mut control, &mut hs_in, &underlay, &self.signer).await?;
        do_pricing(&mut swarm, peer_id, &mut control, &mut pr_in, self.config.timeout).await?;
        let mut stream = poll_until(&mut swarm, control.open_stream(peer_id, PUSHSYNC_PROTO))
            .await
            .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
        let result = poll_until(&mut swarm, pushsync::push(&mut stream, chunk_addr, chunk_data, stamp)).await?;
        Ok(result)
    }

    /// Discover peers from one node by listening on the hive stream.
    pub async fn discover_peers(
        &self,
        peer_addr: &Multiaddr,
        wait: Duration,
    ) -> Result<Vec<Peer>, TransportError> {
        let peer_id = ensure_ws(peer_addr)?;
        let mut swarm = build_swarm(self).await?;
        let mut control = swarm.behaviour().stream.new_control();
        let mut hs_in = accept(&mut control, HANDSHAKE_PROTO)?;
        let mut pr_in = accept(&mut control, PRICING_PROTO)?;
        let mut hive_in = accept(&mut control, HIVE_PROTO)?;
        dial(&mut swarm, peer_id, peer_addr)?;

        let underlay = prep_connection(&mut swarm, peer_id, self.config.timeout).await?;
        do_handshake(&mut swarm, peer_id, &mut control, &mut hs_in, &underlay, &self.signer).await?;
        do_pricing(&mut swarm, peer_id, &mut control, &mut pr_in, self.config.timeout).await?;

        // Wait for the first hive envelope (bee sends a single one then stops),
        // bounded by `wait`. Exit as soon as we read it.
        let mut peers: Vec<Peer> = Vec::new();
        let deadline = web_time::Instant::now() + wait;
        loop {
            let now = web_time::Instant::now();
            if now >= deadline { break; }
            let remaining = deadline - now;
            tokio::select! {
                _ = tokio::time::sleep(remaining) => break,
                ev = hive_in.next() => {
                    match ev {
                        Some((pid, mut stream)) if pid == peer_id => {
                            info!(target: "isheika::hive", "inbound hive stream opened");
                            match poll_until(&mut swarm, hive::read_peers(&mut stream)).await {
                                Ok(mut batch) => {
                                    info!(target: "isheika::hive", "read {} peers", batch.len());
                                    peers.append(&mut batch);
                                    break;
                                }
                                Err(e) => debug!(target: "isheika::hive", "read_peers err: {}", e),
                            }
                        }
                        Some(_) => {}
                        None => break,
                    }
                }
                ev = swarm.select_next_some() => {
                    if let SwarmEvent::ConnectionClosed { peer_id: pid, .. } = ev {
                        if pid == peer_id { break; }
                    }
                }
            }
        }
        Ok(peers)
    }
}

/// Poll the swarm in parallel with `fut` so that libp2p behaviours
/// (identify, libp2p-stream) can make progress while we wait on stream IO.
async fn poll_until<T, F: core::future::Future<Output = T>>(
    swarm: &mut Swarm<Behaviour>,
    fut: F,
) -> T {
    tokio::pin!(fut);
    loop {
        tokio::select! {
            r = &mut fut => return r,
            _ = swarm.select_next_some() => {}
        }
    }
}

async fn close_stream_polled<S: futures::AsyncWrite + Unpin>(swarm: &mut Swarm<Behaviour>, stream: &mut S) {
    use futures::AsyncWriteExt;
    let _ = poll_until(swarm, stream.close()).await;
}

fn ensure_ws(ma: &Multiaddr) -> Result<PeerId, TransportError> {
    if !is_ws_multiaddr(ma) {
        return Err(TransportError::NotWebSocket(ma.to_string()));
    }
    extract_peer_id(ma).ok_or(TransportError::MissingPeerId)
}

fn extract_peer_id(ma: &Multiaddr) -> Option<PeerId> {
    use libp2p::multiaddr::Protocol;
    for proto in ma.iter() {
        if let Protocol::P2p(pid) = proto {
            return Some(pid);
        }
    }
    None
}

fn dial(swarm: &mut Swarm<Behaviour>, peer_id: PeerId, peer_addr: &Multiaddr) -> Result<(), TransportError> {
    swarm
        .dial(
            DialOpts::peer_id(peer_id)
                .addresses(vec![peer_addr.clone()])
                .build(),
        )
        .map_err(|e| TransportError::DialFailed(e.to_string()))
}

fn accept(
    control: &mut libp2p_stream::Control,
    proto: StreamProtocol,
) -> Result<libp2p_stream::IncomingStreams, TransportError> {
    control
        .accept(proto)
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))
}

/// Wait for the connection to establish, the inbound identify exchange to land,
/// add the observed external address, push our updated identify info back, and
/// wait for the push to complete. This is the magic that makes bee proceed
/// immediately instead of waiting ~10s for our liveness signal. Returns the
/// remote underlay multiaddr observed at connection time.
async fn prep_connection(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    timeout: Duration,
) -> Result<Multiaddr, TransportError> {
    let deadline = web_time::Instant::now() + timeout;
    let mut peer_underlay: Option<Multiaddr> = None;
    let mut identify_received = false;
    let mut push_in_flight = false;
    loop {
        let now = web_time::Instant::now();
        if now >= deadline {
            return Err(TransportError::Timeout);
        }
        match tokio::time::timeout(deadline - now, swarm.select_next_some()).await {
            Err(_) => return Err(TransportError::Timeout),
            Ok(ev) => match ev {
                SwarmEvent::ConnectionEstablished { peer_id: pid, endpoint, .. } if pid == peer_id => {
                    info!(target: "isheika::transport", "connected to {}", pid);
                    peer_underlay = Some(endpoint.get_remote_address().clone());
                }
                SwarmEvent::OutgoingConnectionError { peer_id: Some(pid), error, .. } if pid == peer_id => {
                    return Err(TransportError::DialFailed(error.to_string()));
                }
                SwarmEvent::ConnectionClosed { peer_id: pid, .. } if pid == peer_id => {
                    return Err(TransportError::ConnectionClosed);
                }
                SwarmEvent::Behaviour(BehaviourEvent::Identify(idev)) => match idev {
                    libp2p::identify::Event::Received { peer_id: pid, info, .. } if pid == peer_id && !identify_received => {
                        identify_received = true;
                        info!(target: "isheika::transport", "identify received; observed_addr={}", info.observed_addr);
                        swarm.add_external_address(info.observed_addr.clone());
                        swarm.behaviour_mut().identify.push([peer_id]);
                        push_in_flight = true;
                    }
                    libp2p::identify::Event::Pushed { peer_id: pid, .. } if pid == peer_id && push_in_flight => {
                        info!(target: "isheika::transport", "identify push acknowledged");
                        return Ok(peer_underlay.unwrap_or_else(Multiaddr::empty));
                    }
                    _ => {}
                },
                _ => {}
            },
        }
    }
}

async fn do_handshake(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    control: &mut libp2p_stream::Control,
    hs_in: &mut libp2p_stream::IncomingStreams,
    underlay: &Multiaddr,
    signer: &SwarmSigner,
) -> Result<(), TransportError> {
    let local_peer_id = *swarm.local_peer_id();
    info!(target: "isheika::transport", "opening outbound handshake");
    let mut stream = poll_until(swarm, control.open_stream(peer_id, HANDSHAKE_PROTO))
        .await
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
    {
        let hs_run = handshake::run(&mut stream, signer, signer.network_id(), underlay, &local_peer_id, true);
        // Run handshake while still draining inbound handshake/swarm events.
        tokio::pin!(hs_run);
        loop {
            tokio::select! {
                r = &mut hs_run => { r?; break; }
                ev = hs_in.next() => {
                    if let Some((pid, mut s)) = ev {
                        if pid == peer_id {
                            let _ = poll_until(swarm,
                                respond_to_handshake(&mut s, signer, None, &local_peer_id)
                            ).await;
                        }
                    }
                }
                _ = swarm.select_next_some() => {}
            }
        }
    }
    close_stream_polled(swarm, &mut stream).await;
    info!(target: "isheika::transport", "outbound handshake complete");
    Ok(())
}

async fn do_pricing(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    control: &mut libp2p_stream::Control,
    pr_in: &mut libp2p_stream::IncomingStreams,
    timeout: Duration,
) -> Result<(), TransportError> {
    // Wait for inbound pricing first (peer announces threshold), then announce ours.
    let deadline = web_time::Instant::now() + timeout;
    let mut pr_in_done = false;
    while !pr_in_done {
        let now = web_time::Instant::now();
        if now >= deadline { return Err(TransportError::Timeout); }
        tokio::select! {
            _ = tokio::time::sleep(deadline - now) => return Err(TransportError::Timeout),
            ev = pr_in.next() => {
                if let Some((pid, mut stream)) = ev {
                    if pid == peer_id {
                        let _ = poll_until(swarm, read_then_write_empty_headers(&mut stream)).await;
                        let _ = poll_until(swarm, pricing::read_announcement(&mut stream)).await;
                        pr_in_done = true;
                    }
                }
            }
            _ = swarm.select_next_some() => {}
        }
    }
    info!(target: "isheika::transport", "opening outbound pricing");
    let mut stream = poll_until(swarm, control.open_stream(peer_id, PRICING_PROTO))
        .await
        .map_err(|e| TransportError::StreamControl(format!("{e:?}")))?;
    poll_until(swarm, write_then_read_empty_headers(&mut stream)).await?;
    poll_until(swarm, pricing::announce(&mut stream)).await?;
    close_stream_polled(swarm, &mut stream).await;
    info!(target: "isheika::transport", "outbound pricing complete");
    Ok(())
}

async fn respond_to_handshake<S>(
    stream: &mut S,
    signer: &SwarmSigner,
    observed_underlay: Option<&Multiaddr>,
    our_peer_id: &PeerId,
) -> Result<(), TransportError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use crate::proto::handshake as pb;
    use crate::protocols::framing::{read_message, write_message};

    let syn: pb::Syn = read_message(stream).await?;
    let _ = observed_underlay; // ignored — clients don't listen
    let our_underlay = {
        let s = format!("/ip4/127.0.0.1/tcp/1634/p2p/{our_peer_id}");
        s.parse::<Multiaddr>().unwrap().to_vec()
    };
    let signature = signer.sign_handshake(&our_underlay)?;
    let our_addr = pb::BzzAddress {
        underlay: our_underlay,
        signature: signature.to_vec(),
        overlay: signer.overlay().to_vec(),
    };
    let synack = pb::SynAck {
        syn: Some(pb::Syn { observed_underlay: syn.observed_underlay }),
        ack: Some(pb::Ack {
            address: Some(our_addr),
            network_id: signer.network_id(),
            full_node: true,
            nonce: signer.nonce().to_vec(),
            welcome_message: String::new(),
        }),
    };
    write_message(stream, &synack).await?;
    let _peer_ack: pb::Ack = read_message(stream).await?;
    Ok(())
}

async fn write_then_read_empty_headers<S>(stream: &mut S) -> Result<(), TransportError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use crate::proto::headers as hdr;
    use crate::protocols::framing::{read_message, write_message};
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    let _: hdr::Headers = read_message(stream).await?;
    Ok(())
}

async fn read_then_write_empty_headers<S>(stream: &mut S) -> Result<(), TransportError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use crate::proto::headers as hdr;
    use crate::protocols::framing::{read_message, write_message};
    let _: hdr::Headers = read_message(stream).await?;
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    Ok(())
}

async fn close_stream<S: futures::AsyncWrite + Unpin>(stream: &mut S) {
    use futures::AsyncWriteExt;
    let _ = stream.close().await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn build_swarm(t: &Transport) -> Result<Swarm<Behaviour>, TransportError> {
    use libp2p_core::{upgrade, Transport as _};

    let timeout = t.config.timeout;

    let swarm = SwarmBuilder::with_existing_identity(t.keypair.clone())
        .with_tokio()
        .with_other_transport(|key| {
            let tcp = libp2p_tcp::tokio::Transport::new(libp2p_tcp::Config::default());
            let ws = libp2p_websocket::Config::new(tcp);
            Ok(ws
                .upgrade(upgrade::Version::V1)
                .authenticate(noise::Config::new(key).map_err(|e| {
                    libp2p::TransportError::Other(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?)
                .multiplex(yamux::Config::default())
                .boxed())
        })
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_behaviour(|key| behaviour(key))
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(timeout))
        .build();
    Ok(swarm)
}

#[cfg(target_arch = "wasm32")]
async fn build_swarm(t: &Transport) -> Result<Swarm<Behaviour>, TransportError> {
    use libp2p_core::{upgrade, Transport as _};
    use libp2p::websocket_websys as ws_websys;

    let keypair = t.keypair.clone();
    let timeout = t.config.timeout;

    let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_wasm_bindgen()
        .with_other_transport(|key| {
            Ok(ws_websys::Transport::default()
                .upgrade(upgrade::Version::V1)
                .authenticate(noise::Config::new(key).map_err(|e| {
                    libp2p::TransportError::Other(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?)
                .multiplex(yamux::Config::default())
                .boxed())
        })
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_behaviour(|key| behaviour(key))
        .map_err(|e| TransportError::DialFailed(e.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(timeout))
        .build();
    Ok(swarm)
}
