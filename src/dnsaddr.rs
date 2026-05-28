//! `/dnsaddr/...` resolution over DoH.
//!
//! Bee bootnodes are advertised as `/dnsaddr/<host>`. The actual TXT record
//! at `_dnsaddr.<host>` contains entries like `dnsaddr=/ip4/.../tcp/443/wss/p2p/...`.
//! Each TXT line may itself reference another `/dnsaddr/...` so we recurse.
//!
//! This module returns only multiaddrs that our transport stack can dial:
//! on WASM that means ws/wss only; on native we also accept plain tcp.

use libp2p::Multiaddr;
use std::collections::HashSet;
use thiserror::Error;

use crate::doh::{Doh, DohError};

#[derive(Debug, Error)]
pub enum DnsAddrError {
    #[error("not a /dnsaddr/ multiaddr")]
    NotDnsAddr,
    #[error("doh: {0}")]
    Doh(#[from] DohError),
    #[error("multiaddr parse: {0}")]
    Parse(String),
    #[error("recursion limit exceeded")]
    Recursion,
}

const MAX_DEPTH: usize = 5;
const TXT_PREFIX: &str = "dnsaddr=";

/// Resolve a `/dnsaddr/<host>` (or any multiaddr) into a flat set of dialable
/// multiaddrs. Returns only ws/wss results.
pub async fn resolve(ma: &Multiaddr, doh: &Doh) -> Result<Vec<Multiaddr>, DnsAddrError> {
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    resolve_recursive(ma, doh, 0, &mut out, &mut seen).await?;
    out.retain(is_dialable_multiaddr);
    Ok(out)
}

/// Resolve any number of input multiaddrs concurrently. Falls back to passing
/// through inputs that aren't `/dnsaddr/`.
pub async fn resolve_many(addrs: &[Multiaddr], doh: &Doh) -> Vec<Multiaddr> {
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for ma in addrs {
        let _ = resolve_recursive(ma, doh, 0, &mut out, &mut seen).await;
    }
    out.retain(is_dialable_multiaddr);
    out.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    out.dedup();
    out
}

async fn resolve_recursive(
    ma: &Multiaddr,
    doh: &Doh,
    depth: usize,
    out: &mut Vec<Multiaddr>,
    seen: &mut HashSet<String>,
) -> Result<(), DnsAddrError> {
    if depth > MAX_DEPTH {
        return Err(DnsAddrError::Recursion);
    }
    let key = ma.to_string();
    if !seen.insert(key.clone()) {
        return Ok(());
    }

    let host = match dnsaddr_host(ma) {
        Some(h) => h,
        None => {
            // Not a /dnsaddr/ — pass through.
            out.push(ma.clone());
            return Ok(());
        }
    };

    let qname = format!("_dnsaddr.{host}");
    let records = doh.txt(&qname).await?;
    for rec in records {
        if let Some(payload) = rec.strip_prefix(TXT_PREFIX) {
            let next: Multiaddr = match payload.parse() {
                Ok(m) => m,
                Err(e) => {
                    tracing::debug!(target: "isheika::dnsaddr", "skip {}: {}", payload, e);
                    continue;
                }
            };
            // Recurse via boxed future since this is async + recursive.
            Box::pin(resolve_recursive(&next, doh, depth + 1, out, seen)).await?;
        }
    }
    Ok(())
}

/// Extract the host from a `/dnsaddr/<host>` multiaddr.
fn dnsaddr_host(ma: &Multiaddr) -> Option<String> {
    use libp2p::multiaddr::Protocol;
    for proto in ma.iter() {
        if let Protocol::Dnsaddr(host) = proto {
            return Some(host.to_string());
        }
    }
    None
}

/// True if multiaddr contains a /ws or /wss component.
pub fn is_ws_multiaddr(ma: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    ma.iter()
        .any(|p| matches!(p, Protocol::Ws(_) | Protocol::Wss(_)))
}

/// True if multiaddr is a plain `/tcp/...` (not wrapped in `/ws`/`/wss`).
/// Used on native builds where we can dial bee's bare TCP endpoint
/// (e.g. mainnet bootnodes that don't expose WebSocket).
pub fn is_tcp_multiaddr(ma: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    let mut has_tcp = false;
    for proto in ma.iter() {
        match proto {
            Protocol::Ws(_) | Protocol::Wss(_) => return false,
            Protocol::Tcp(_) => has_tcp = true,
            _ => {}
        }
    }
    has_tcp
}

/// True if our transport stack can dial this multiaddr.
///
/// - Native CLI / `cfg(not(target_arch = "wasm32"))`: ws/wss **or** plain
///   tcp (libp2p-tcp + libp2p-websocket, combined via `or_transport`).
/// - WASM browser / `cfg(target_arch = "wasm32")`: ws/wss only — browsers
///   can't open raw TCP sockets.
///
/// Both targets also require an `/ip4/` host component: our transport
/// has no DNS resolver (`/dns4/`, `/dns6/`, `/dnsaddr/` are filtered)
/// and most consumer networks lack outbound IPv6 (`/ip6/` is filtered
/// too — peers reachable only over v6 just look offline). Filtering
/// here is much cheaper than burning a dial timeout per dead entry.
pub fn is_dialable_multiaddr(ma: &Multiaddr) -> bool {
    if !has_ip4_host(ma) {
        return false;
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        is_ws_multiaddr(ma) || is_tcp_multiaddr(ma)
    }
    #[cfg(target_arch = "wasm32")]
    {
        is_ws_multiaddr(ma)
    }
}

fn has_ip4_host(ma: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    ma.iter().any(|p| matches!(p, Protocol::Ip4(_)))
}
