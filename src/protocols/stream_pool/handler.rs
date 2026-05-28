use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{
    StreamExt as _,
    channel::{mpsc, oneshot},
};
use libp2p::identity::PeerId;
use libp2p::swarm::{
    self as swarm, ConnectionHandler, Stream, StreamProtocol,
    handler::{ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound},
};

use crate::protocols::stream_pool::{OpenStreamError, shared::Shared, upgrade::Upgrade};

/// Default cap on concurrent outbound substream upgrades per
/// connection when the caller (e.g. [`Behaviour::new`]) doesn't
/// override it. Upstream `libp2p_stream` hard-coded this at 1
/// (singular `pending_upgrade: Option<…>`) which serialised every
/// substream open behind the previous one. Our default is high
/// enough that the per-connection layer is rarely the bottleneck for
/// our pushsync workload (typically 8-32 concurrent pushes per
/// session at high `--concurrency`), but bounded so a runaway
/// producer doesn't queue thousands of in-flight protocol
/// negotiations through a single connection.
///
/// Tunable via CLI (`--substream-upgrade-cap` / `TransportConfig`).
/// Profiling under load (post-patch) showed per-push latency goes up
/// as this grows because yamux flow-control kicks in when many
/// concurrent substreams share one connection. The sweet spot is
/// likely workload-dependent — exposing it as a knob lets callers
/// A/B without recompiling.
pub const DEFAULT_MAX_CONCURRENT_OUTBOUND_UPGRADES: usize = 64;

pub struct Handler {
    remote: PeerId,
    shared: Arc<Mutex<Shared>>,
    /// Per-handler cap on concurrent outbound substream upgrades.
    max_concurrent_upgrades: usize,

    receiver: mpsc::Receiver<NewStream>,
    /// Outbound substream upgrades currently in flight, keyed by an
    /// id (`OutboundOpenInfo`) that comes back to us in the
    /// `FullyNegotiatedOutbound` / `DialUpgradeError` events so we
    /// can correlate the result with the original requester's
    /// oneshot sender.
    pending_upgrades: HashMap<
        u64,
        (
            StreamProtocol,
            oneshot::Sender<Result<Stream, OpenStreamError>>,
        ),
    >,
    /// Ready-to-emit substream requests we've pulled from the
    /// `NewStream` channel but haven't yet yielded to the swarm
    /// (we yield one per `poll()` call, the rest queue here).
    pending_emit: VecDeque<(u64, Upgrade)>,
    next_upgrade_id: u64,
}

impl Handler {
    pub(crate) fn new(
        remote: PeerId,
        shared: Arc<Mutex<Shared>>,
        receiver: mpsc::Receiver<NewStream>,
        max_concurrent_upgrades: usize,
    ) -> Self {
        Self {
            shared,
            receiver,
            max_concurrent_upgrades: max_concurrent_upgrades.max(1),
            pending_upgrades: HashMap::new(),
            pending_emit: VecDeque::new(),
            next_upgrade_id: 0,
            remote,
        }
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_upgrade_id;
        self.next_upgrade_id = self.next_upgrade_id.wrapping_add(1);
        id
    }
}

impl ConnectionHandler for Handler {
    type FromBehaviour = Infallible;
    type ToBehaviour = Infallible;
    type InboundProtocol = Upgrade;
    type OutboundProtocol = Upgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = u64;

    fn listen_protocol(&self) -> swarm::SubstreamProtocol<Self::InboundProtocol> {
        swarm::SubstreamProtocol::new(
            Upgrade {
                supported_protocols: Shared::lock(&self.shared).supported_inbound_protocols(),
            },
            (),
        )
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        swarm::ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::ToBehaviour,
        >,
    > {
        // First: emit any queued substream requests (we can only yield
        // one event per `poll` call, so we buffer extras here).
        if let Some((id, upgrade)) = self.pending_emit.pop_front() {
            return Poll::Ready(swarm::ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: swarm::SubstreamProtocol::new(upgrade, id),
            });
        }

        // Then: pull from `NewStream` channel until we either hit
        // `max_concurrent_upgrades` total in flight (pending +
        // emitted) or the channel returns Pending. Each pulled
        // request becomes either an immediate emit (this poll) or a
        // queued emit (next polls).
        while self.pending_upgrades.len() + self.pending_emit.len() < self.max_concurrent_upgrades {
            match self.receiver.poll_next_unpin(cx) {
                Poll::Ready(Some(new_stream)) => {
                    let id = self.alloc_id();
                    self.pending_upgrades
                        .insert(id, (new_stream.protocol.clone(), new_stream.sender));
                    let upgrade = Upgrade {
                        supported_protocols: vec![new_stream.protocol],
                    };
                    // First one fires this poll; rest queue.
                    if self.pending_emit.is_empty() {
                        return Poll::Ready(
                            swarm::ConnectionHandlerEvent::OutboundSubstreamRequest {
                                protocol: swarm::SubstreamProtocol::new(upgrade, id),
                            },
                        );
                    } else {
                        self.pending_emit.push_back((id, upgrade));
                    }
                }
                Poll::Ready(None) => break, // Sender is gone, no more work to do.
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        libp2p::core::util::unreachable(event)
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: (stream, protocol),
                info: (),
            }) => {
                Shared::lock(&self.shared).on_inbound_stream(self.remote, stream, protocol);
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: (stream, actual_protocol),
                info: id,
            }) => {
                let Some((expected_protocol, sender)) = self.pending_upgrades.remove(&id) else {
                    debug_assert!(
                        false,
                        "Negotiated an outbound stream with no matching pending upgrade id"
                    );
                    return;
                };
                debug_assert_eq!(expected_protocol, actual_protocol);

                let _ = sender.send(Ok(stream));
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, info: id }) => {
                let Some((p, sender)) = self.pending_upgrades.remove(&id) else {
                    debug_assert!(
                        false,
                        "Received a `DialUpgradeError` with no matching pending upgrade id"
                    );
                    return;
                };

                let error = match error {
                    swarm::StreamUpgradeError::Timeout => {
                        OpenStreamError::Io(io::Error::from(io::ErrorKind::TimedOut))
                    }
                    swarm::StreamUpgradeError::Apply(v) => libp2p::core::util::unreachable(v),
                    swarm::StreamUpgradeError::NegotiationFailed => {
                        OpenStreamError::UnsupportedProtocol(p)
                    }
                    swarm::StreamUpgradeError::Io(io) => OpenStreamError::Io(io),
                };

                let _ = sender.send(Err(error));
            }
            _ => {}
        }
    }
}

/// Message from a [`Control`](crate::protocols::stream_pool::Control) to
/// a [`ConnectionHandler`] to negotiate a new outbound stream.
#[derive(Debug)]
pub(crate) struct NewStream {
    pub(crate) protocol: StreamProtocol,
    pub(crate) sender: oneshot::Sender<Result<Stream, OpenStreamError>>,
}
