//! Daemon inbound listener — bee-protocol responder.
//!
//! Owns a long-lived libp2p swarm bound to a user-configured listen
//! multiaddr (default `/ip4/0.0.0.0/tcp/1634/ws`). Accepts inbound
//! `libp2p_stream` substreams for the bee handshake, pricing, and
//! retrieval protocols and dispatches them to per-protocol responders.
//!
//! The retrieval responder reads from a shared [`ChunkCache`] so the
//! daemon can serve every chunk it has stamped (during its uploads)
//! and every chunk it has fetched on behalf of a caller. This is the
//! mechanism that lets a freshly-uploaded root hash resolve through
//! bzz.limo immediately: as long as one bee in the network routes
//! the retrieval lookup back to us, we hand the chunk over directly.
//!
//! Native-only (`#[cfg(not(target_arch = "wasm32"))]`). Wasm targets
//! have no inbound listener concept.

#![cfg(not(target_arch = "wasm32"))]

use core::time::Duration;
use std::sync::Arc;

use futures::StreamExt;
use libp2p::identity::Keypair;
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::cache::ChunkCache;
use crate::protocols::{hive, pricing, retrieval};
use crate::signer::SwarmSigner;
use crate::transport::{
    build_swarm_from, respond_to_handshake, BehaviourEvent, HANDSHAKE_PROTO, HIVE_PROTO,
    PRICING_PROTO, RETRIEVAL_PROTO,
};

#[derive(Debug, Error)]
pub enum InboundError {
    #[error("listen failed: {0}")]
    Listen(String),
    #[error("stream control: {0}")]
    StreamControl(String),
    #[error("build swarm: {0}")]
    Build(String),
}

/// Daemon-side inbound listener configuration.
pub struct InboundConfig {
    /// Multiaddr to bind. Typically `/ip4/0.0.0.0/tcp/1634/ws`.
    pub listen: Multiaddr,
    /// Publicly-routable multiaddr we advertise to peers in both the
    /// libp2p `identify` push and the bee handshake underlay. Must
    /// include the `/p2p/<our peer id>` tail. When `None`, the
    /// listener still works for local testing but bee peers won't add
    /// us to their kademlia tables (and therefore won't route
    /// retrieval requests back to us).
    pub advertise: Option<Multiaddr>,
    /// Daemon identity. Used for the bee handshake (overlay derived
    /// from the eth address + nonce) and as the libp2p keypair.
    pub signer: SwarmSigner,
    /// Per-substream timeout for protocol responders.
    pub op_timeout: Duration,
    /// Idle-connection timeout passed into the swarm config.
    pub idle_timeout: Duration,
    /// Shared chunk cache the retrieval responder reads from. Cloning
    /// is cheap (`Arc`).
    pub cache: ChunkCache,
}

/// Build the listener swarm, bind it, and run forever, dispatching
/// inbound protocol substreams to the appropriate responder. Returns
/// only on a fatal error (listen bind failure, behaviour failure).
pub async fn run(cfg: InboundConfig) -> Result<(), InboundError> {
    let keypair = derive_keypair(&cfg.signer);
    let our_peer_id = PeerId::from(keypair.public());
    let mut swarm = build_swarm_from(
        &keypair,
        cfg.idle_timeout,
        crate::protocols::stream_pool::DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES,
    )
        .await
        .map_err(|e| InboundError::Build(e.to_string()))?;
    swarm
        .listen_on(cfg.listen.clone())
        .map_err(|e| InboundError::Listen(format!("listen on {}: {e}", cfg.listen)))?;
    if let Some(addr) = cfg.advertise.as_ref() {
        // Without this, our libp2p identify message advertises the bind
        // address (typically 0.0.0.0), which peers can't dial back. With
        // it, identify pushes the publicly-routable address and bee
        // peers add us to their kademlia tables on the inbound
        // verification dial-back.
        swarm.add_external_address(addr.clone());
        info!(target: "isheika::inbound",
            "advertising external address {addr}");
    }

    // Accept inbound streams for the three protocols we serve.
    let mut control = swarm.behaviour().stream.new_control();
    let mut hs_in = control
        .accept(HANDSHAKE_PROTO)
        .map_err(|e| InboundError::StreamControl(format!("accept handshake: {e:?}")))?;
    let mut pr_in = control
        .accept(PRICING_PROTO)
        .map_err(|e| InboundError::StreamControl(format!("accept pricing: {e:?}")))?;
    let mut re_in = control
        .accept(RETRIEVAL_PROTO)
        .map_err(|e| InboundError::StreamControl(format!("accept retrieval: {e:?}")))?;
    let mut hive_in = control
        .accept(HIVE_PROTO)
        .map_err(|e| InboundError::StreamControl(format!("accept hive: {e:?}")))?;

    info!(
        target: "isheika::inbound",
        "inbound listener up on {} (peer_id {}, overlay {})",
        cfg.listen,
        our_peer_id,
        hex::encode(cfg.signer.overlay()),
    );

    let signer = Arc::new(cfg.signer);
    let cache = cfg.cache;
    let op_timeout = cfg.op_timeout;
    let advertised = Arc::new(cfg.advertise);

    loop {
        tokio::select! {
            // Drain swarm events so identify / connection lifecycle
            // make progress.
            ev = swarm.select_next_some() => {
                match ev {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!(target: "isheika::inbound", "listening on {address}");
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        debug!(target: "isheika::inbound",
                            "inbound connection from {peer_id} via {}", endpoint.get_remote_address());
                    }
                    SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                        debug!(target: "isheika::inbound",
                            "connection from {peer_id} closed: {cause:?}");
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Identify(ev)) => {
                        if let libp2p::identify::Event::Received { peer_id, info, .. } = ev {
                            debug!(target: "isheika::inbound",
                                "identify from {peer_id}: agent={} observed_addr={}",
                                info.agent_version, info.observed_addr);
                        }
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        debug!(target: "isheika::inbound", "incoming conn error: {error}");
                    }
                    _ => {}
                }
            }

            Some((peer_id, mut stream)) = hs_in.next() => {
                let signer = signer.clone();
                let advertised = advertised.clone();
                tokio::spawn(async move {
                    info!(target: "isheika::inbound",
                        "inbound handshake stream from {peer_id}");
                    let res = tokio::time::timeout(
                        op_timeout,
                        respond_to_handshake(&mut stream, &signer, None, advertised.as_ref().as_ref(), &our_peer_id, peer_id, true),
                    ).await;
                    match res {
                        Ok(Ok(())) => info!(target: "isheika::inbound",
                            "handshake responded to {peer_id}"),
                        Ok(Err(e)) => warn!(target: "isheika::inbound",
                            "handshake from {peer_id} failed: {e}"),
                        Err(_) => warn!(target: "isheika::inbound",
                            "handshake from {peer_id} timed out"),
                    }
                });
            }

            Some((peer_id, mut stream)) = pr_in.next() => {
                tokio::spawn(async move {
                    let res = tokio::time::timeout(
                        op_timeout,
                        pricing::respond_announcement(&mut stream),
                    ).await;
                    match res {
                        Ok(Ok(threshold)) => debug!(target: "isheika::inbound",
                            "pricing from {peer_id}: threshold {threshold}"),
                        Ok(Err(e)) => debug!(target: "isheika::inbound",
                            "pricing from {peer_id} failed: {e}"),
                        Err(_) => debug!(target: "isheika::inbound",
                            "pricing from {peer_id} timed out"),
                    }
                });
            }

            Some((peer_id, mut stream)) = re_in.next() => {
                let cache = cache.clone();
                tokio::spawn(async move {
                    let cache_len = cache.len();
                    info!(target: "isheika::inbound",
                        "inbound retrieval stream from {peer_id} (cache size {cache_len})");
                    let res = tokio::time::timeout(op_timeout, retrieval::respond(&mut stream, |addr| {
                        let hit = cache.get(addr).map(|c| (c.data.to_vec(), c.stamp.to_vec()));
                        if hit.is_some() {
                            info!(target: "isheika::inbound",
                                "retrieval HIT addr={} for {peer_id}",
                                hex::encode(addr));
                        } else {
                            debug!(target: "isheika::inbound",
                                "retrieval MISS addr={} for {peer_id}",
                                hex::encode(addr));
                        }
                        hit
                    })).await;
                    match res {
                        Ok(Ok(())) => debug!(target: "isheika::inbound",
                            "retrieval responded to {peer_id}"),
                        Ok(Err(e)) => debug!(target: "isheika::inbound",
                            "retrieval from {peer_id} failed: {e}"),
                        Err(_) => debug!(target: "isheika::inbound",
                            "retrieval from {peer_id} timed out"),
                    }
                });
            }

            Some((peer_id, mut stream)) = hive_in.next() => {
                tokio::spawn(async move {
                    let res = tokio::time::timeout(
                        op_timeout,
                        hive::respond_empty(&mut stream),
                    ).await;
                    match res {
                        Ok(Ok(())) => debug!(target: "isheika::inbound",
                            "hive responded (empty) to {peer_id}"),
                        Ok(Err(e)) => debug!(target: "isheika::inbound",
                            "hive from {peer_id} failed: {e}"),
                        Err(_) => debug!(target: "isheika::inbound",
                            "hive from {peer_id} timed out"),
                    }
                });
            }
        }
    }
}

/// Convenience: derive the libp2p peer-id from a `SwarmSigner` without
/// fully constructing a swarm. Used by the CLI to append `/p2p/<id>`
/// to a user-supplied `--advertise` multiaddr.
pub fn peer_id_from_identity(signer: &SwarmSigner) -> PeerId {
    PeerId::from(derive_keypair(signer).public())
}

/// Public version of [`derive_keypair`] used by the daemon to share
/// the same libp2p identity between the outbound `Transport` and the
/// inbound listener.
pub fn libp2p_keypair_from_identity(signer: &SwarmSigner) -> Keypair {
    derive_keypair(signer)
}

/// Derive a libp2p secp256k1 keypair from the daemon's `SwarmSigner`
/// private key. This keeps the libp2p peer-id stable for a given
/// `--identity` value across restarts — important so peers that
/// learn about us via hive can re-dial us at the same `peer_id`.
fn derive_keypair(signer: &SwarmSigner) -> Keypair {
    // alloy's signer exposes the raw key bytes via to_bytes(); libp2p
    // accepts the same 32-byte big-endian scalar.
    let bytes = signer.alloy_signer().to_bytes();
    let secret = libp2p::identity::secp256k1::SecretKey::try_from_bytes(bytes.to_vec())
        .expect("alloy signer produced an invalid secp256k1 scalar");
    let kp: libp2p::identity::secp256k1::Keypair = secret.into();
    Keypair::from(kp)
}
