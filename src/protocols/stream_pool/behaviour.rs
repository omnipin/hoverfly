use core::fmt;
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{StreamExt, channel::mpsc};
use libp2p::core::{Endpoint, Multiaddr, transport::PortUse};
use libp2p::identity::PeerId;
use libp2p::swarm::{
    self as swarm, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler,
    THandlerInEvent, THandlerOutEvent, ToSwarm, dial_opts::DialOpts,
};
use swarm::{
    ConnectionClosed, DialError, DialFailure, behaviour::ConnectionEstablished,
    dial_opts::PeerCondition,
};

use crate::protocols::stream_pool::{
    Control,
    handler::{DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES, Handler},
    shared::Shared,
};

/// A generic behaviour for stream-oriented protocols.
pub struct Behaviour {
    shared: Arc<Mutex<Shared>>,
    dial_receiver: mpsc::Receiver<PeerId>,
    /// Per-handler cap on concurrent outbound substream upgrades.
    /// Forwarded into each [`Handler`] this behaviour spawns.
    max_concurrent_upgrades: usize,
}

impl Default for Behaviour {
    fn default() -> Self {
        Self::new()
    }
}

impl Behaviour {
    pub fn new() -> Self {
        Self::with_max_concurrent_upgrades(DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES)
    }

    /// Like [`Self::new`] but with an explicit cap on how many outbound
    /// substream upgrades each [`Handler`] will surface to the swarm
    /// at once. Default is
    /// [`DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES`].
    pub fn with_max_concurrent_upgrades(max_concurrent_upgrades: usize) -> Self {
        let (dial_sender, dial_receiver) = mpsc::channel(0);

        Self {
            shared: Arc::new(Mutex::new(Shared::new(dial_sender))),
            dial_receiver,
            max_concurrent_upgrades: max_concurrent_upgrades.max(1),
        }
    }

    /// Obtain a new [`Control`].
    pub fn new_control(&self) -> Control {
        Control::new(self.shared.clone())
    }
}

/// The protocol is already registered.
#[derive(Debug)]
pub struct AlreadyRegistered;

impl fmt::Display for AlreadyRegistered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The protocol is already registered")
    }
}

impl std::error::Error for AlreadyRegistered {}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;
    type ToSwarm = ();

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new(
            peer,
            self.shared.clone(),
            Shared::lock(&self.shared).receiver(peer, connection_id),
            self.max_concurrent_upgrades,
        ))
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new(
            peer,
            self.shared.clone(),
            Shared::lock(&self.shared).receiver(peer, connection_id),
            self.max_concurrent_upgrades,
        ))
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                ..
            }) => Shared::lock(&self.shared).on_connection_established(connection_id, peer_id),
            FromSwarm::ConnectionClosed(ConnectionClosed { connection_id, .. }) => {
                Shared::lock(&self.shared).on_connection_closed(connection_id)
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id: Some(peer_id),
                error:
                    error @ (DialError::Transport(_)
                    | DialError::Denied { .. }
                    | DialError::NoAddresses
                    | DialError::WrongPeerId { .. }),
                ..
            }) => {
                let reason = error.to_string(); // We can only forward the string repr but it is better than nothing.

                Shared::lock(&self.shared).on_dial_failure(peer_id, reason)
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        libp2p::core::util::unreachable(event);
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Poll::Ready(Some(peer)) = self.dial_receiver.poll_next_unpin(cx) {
            return Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer)
                    .condition(PeerCondition::DisconnectedAndNotDialing)
                    .build(),
            });
        }

        Poll::Pending
    }
}
