//! Per-peer outbound dial rate limiter (GCRA) that **parks** instead of
//! refusing.
//!
//! ## Why this exists
//!
//! Bee's libp2p connection limiter
//! (`pkg/p2p/libp2p/libp2p.go::connLimiter`) admits roughly 10 dials/s
//! with a burst of 40 per `/32` source IP, *per bee node*. Exceed it and
//! bee silently drops the next connection — which surfaces to us as a
//! peer closing mid-push.
//!
//! The previous model enforced a flat 1 s minimum gap between dials to the
//! same peer and **refused** any earlier redial with
//! [`crate::transport::TransportError::DialTooSoon`]. The dispatcher turned
//! that refusal into "abandon this peer, try another". A wire-level trace of
//! a default-nonce upload found **3065 of 3163 errors were exactly that
//! refusal** (`PERFORMANCE.md`, "Vanity overlay" section) — peers bee would
//! have accepted milliseconds later, thrown away because our own cooldown
//! said "not yet".
//!
//! ## What changed
//!
//! This limiter mirrors vertex's `SelfRateLimiter`
//! (`nxm-rs/vertex:crates/net/ratelimiter`): a dial the per-peer bucket
//! cannot admit *yet* is **parked** on a computed delay rather than
//! issued-and-refused. The caller `await`s the returned delay and then
//! dials. Two wins over the flat cooldown:
//!
//! - **More permissive.** The old 1 s gap was ~1 dial/s — far under bee's
//!   ceiling. GCRA lets a just-retired good peer be redialed immediately
//!   (up to the burst) and only paces once the sustained rate is reached.
//! - **No wasted peers.** Parking absorbs the overage instead of erroring,
//!   so the dispatcher keeps the peer it wanted instead of rotating away.
//!
//! Parking is *bounded* (see [`DialRateLimiter::reserve_bounded`]): a wait
//! beyond the caller's budget still surfaces `DialTooSoon`, so a chunk
//! waiting on rotation can fall through to another peer rather than stall
//! behind a deeply-in-debt bucket.
//!
//! ## The algorithm (GCRA)
//!
//! The Generic Cell Rate Algorithm keeps one instant per peer — the
//! *theoretical arrival time* (`tat`) of the next conforming dial — and
//! nothing else. For emission interval `T = 1/rate` and burst tolerance
//! `τ = burst × T`:
//!
//! ```text
//! tat   = max(stored_tat, now)              // idle bucket snaps to now
//! wait  = (tat − now).saturating_sub(τ)     // 0 while within the burst
//! stored_tat = tat + T                       // reserve this slot
//! ```
//!
//! From an idle bucket the first `≈burst` dials return `wait = 0`
//! (immediate), after which `wait` grows in `T`-sized steps — exactly
//! bee's leaky-bucket shape, computed in O(1) with no background task.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Mutex;

use libp2p::PeerId;
use web_time::Instant;

/// GCRA emission interval `T = 1/rate`. Sized to **8 dials/s** per peer —
/// comfortably under bee's ~10 RPS ceiling (headroom for clock skew and
/// measurement error) while being 8× more responsive than the old flat
/// 1 s cooldown.
const EMISSION_INTERVAL: Duration = Duration::from_millis(125);

/// GCRA burst tolerance `τ = burst × T`. Sized to a **32-dial burst**
/// (under bee's burst of 40). Lets a freshly-retired peer be redialed
/// rapidly within a chunk's wall-clock window — the responsiveness the
/// old 1 s gap traded away — before pacing kicks in at [`EMISSION_INTERVAL`].
const BURST_TOLERANCE: Duration = Duration::from_millis(125 * 32);

/// Per-peer GCRA dial limiter. Clone-cheap via `Arc` at the call site;
/// the single `Mutex<HashMap>` is touched only at dial decision time
/// (never on the push/fetch hot path), so contention is negligible.
#[derive(Debug)]
pub struct DialRateLimiter {
    /// Theoretical-arrival-time per peer. Entry present == bucket has
    /// state; absent == fully idle (treated as `tat = now`). Never GC'd
    /// explicitly — a peer we stop dialing keeps one stale `Instant`
    /// until process exit, which is negligible next to the session pool
    /// itself. (If pools ever reach millions of distinct peers, prune on
    /// `tat < now` during `peek`.)
    tat: Mutex<HashMap<PeerId, Instant>>,
    emission_interval: Duration,
    burst_tolerance: Duration,
}

impl Default for DialRateLimiter {
    fn default() -> Self {
        Self::new(EMISSION_INTERVAL, BURST_TOLERANCE)
    }
}

impl DialRateLimiter {
    /// Build a limiter with explicit GCRA parameters. Prefer
    /// [`Default::default`] outside tests.
    pub fn new(emission_interval: Duration, burst_tolerance: Duration) -> Self {
        Self {
            tat: Mutex::new(HashMap::new()),
            emission_interval,
            burst_tolerance,
        }
    }

    /// Reserve a dial slot for `peer` **iff** the required park does not
    /// exceed `max_wait`, advancing the bucket only when the slot is
    /// taken.
    ///
    /// - `Ok(wait)` — slot reserved; the caller must `sleep(wait)` (which
    ///   is `Duration::ZERO` while inside the burst) and then dial.
    /// - `Err(wait)` — the bucket would make us wait longer than
    ///   `max_wait`; **no** slot is charged. The caller should surface
    ///   `DialTooSoon { wait }` so the dispatcher tries another peer. Not
    ///   charging keeps refused attempts from pushing the bucket further
    ///   into debt for a peer we never actually dial.
    pub fn reserve_bounded(&self, peer: PeerId, max_wait: Duration) -> Result<Duration, Duration> {
        self.reserve_at(peer, Instant::now(), max_wait)
    }

    /// The delay a dial to `peer` would incur **right now**, without
    /// reserving a slot. `Duration::ZERO` means "clear to dial". Used by
    /// the dispatcher's eligibility pre-filter so it prefers peers whose
    /// bucket is ready over ones that would park.
    pub fn peek(&self, peer: &PeerId) -> Duration {
        let now = Instant::now();
        let map = self.lock();
        Self::wait_for(&map, peer, now, self.burst_tolerance)
    }

    /// Core GCRA step against an injectable clock (testing seam).
    fn reserve_at(
        &self,
        peer: PeerId,
        now: Instant,
        max_wait: Duration,
    ) -> Result<Duration, Duration> {
        let mut map = self.lock();
        // Idle buckets have `tat` in the past; snap forward to `now` so
        // the burst allowance is measured from this dial, not from the
        // last one arbitrarily long ago.
        let tat = map.get(&peer).copied().unwrap_or(now).max(now);
        // Within the burst (`tat − now ≤ τ`) this saturates to zero.
        let wait = tat.duration_since(now).saturating_sub(self.burst_tolerance);
        if wait > max_wait {
            return Err(wait);
        }
        map.insert(peer, tat + self.emission_interval);
        Ok(wait)
    }

    /// Shared read helper for [`Self::peek`] and tests: the park a dial
    /// would incur given the current bucket state, no mutation.
    fn wait_for(
        map: &HashMap<PeerId, Instant>,
        peer: &PeerId,
        now: Instant,
        burst_tolerance: Duration,
    ) -> Duration {
        let tat = map.get(peer).copied().unwrap_or(now).max(now);
        tat.duration_since(now).saturating_sub(burst_tolerance)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PeerId, Instant>> {
        self.tat.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> PeerId {
        PeerId::random()
    }

    // Short, exact parameters for deterministic assertions: T = 100ms,
    // τ = 300ms (burst of 3 immediate dials from idle).
    fn limiter() -> DialRateLimiter {
        DialRateLimiter::new(Duration::from_millis(100), Duration::from_millis(300))
    }

    #[test]
    fn burst_is_admitted_immediately_then_paces() {
        let l = limiter();
        let p = peer();
        let t0 = Instant::now();
        let big = Duration::from_secs(60);

        // floor(τ/T)+1 = 4 dials from an idle bucket park for 0 (tat walks
        // t0+T, t0+2T, t0+3T, t0+4T; wait = (tat−now)−τ stays ≤ 0 until
        // tat exceeds t0+τ).
        for _ in 0..4 {
            assert_eq!(l.reserve_at(p, t0, big), Ok(Duration::ZERO));
        }
        // From here the bucket paces in T-sized steps: 5th waits ~T,
        // 6th ~2T, monotonically increasing.
        let w5 = l.reserve_at(p, t0, big).unwrap();
        let w6 = l.reserve_at(p, t0, big).unwrap();
        assert_eq!(w5, Duration::from_millis(100));
        assert_eq!(w6, Duration::from_millis(200));
        assert!(w6 > w5);
    }

    #[test]
    fn idle_bucket_snaps_forward_and_resets_burst() {
        let l = limiter();
        let p = peer();
        let t0 = Instant::now();
        let big = Duration::from_secs(60);

        // Exhaust the burst.
        for _ in 0..6 {
            let _ = l.reserve_at(p, t0, big);
        }
        // Long after the bucket has drained, a dial is immediate again.
        let much_later = t0 + Duration::from_secs(10);
        assert_eq!(l.reserve_at(p, much_later, big), Ok(Duration::ZERO));
    }

    #[test]
    fn over_budget_refuses_without_charging() {
        let l = limiter();
        let p = peer();
        let t0 = Instant::now();
        // Drive the bucket well past the burst so the next wait is large.
        for _ in 0..10 {
            let _ = l.reserve_at(p, t0, Duration::from_secs(60));
        }
        let peek_before = DialRateLimiter::wait_for(&l.lock(), &p, t0, l.burst_tolerance);
        // A tiny budget refuses...
        let refused = l.reserve_at(p, t0, Duration::from_millis(1));
        assert!(matches!(refused, Err(w) if w > Duration::from_millis(1)));
        // ...and the refusal did not advance the bucket.
        let peek_after = DialRateLimiter::wait_for(&l.lock(), &p, t0, l.burst_tolerance);
        assert_eq!(peek_before, peek_after);
    }

    #[test]
    fn peek_does_not_mutate() {
        let l = limiter();
        let p = peer();
        // Peeking an untouched bucket is zero and leaves it untouched.
        assert_eq!(l.peek(&p), Duration::ZERO);
        assert_eq!(l.reserve_bounded(p, Duration::from_secs(1)), Ok(Duration::ZERO));
    }

    #[test]
    fn distinct_peers_have_independent_buckets() {
        let l = limiter();
        let (a, b) = (peer(), peer());
        let t0 = Instant::now();
        let big = Duration::from_secs(60);
        // Saturate peer a.
        for _ in 0..10 {
            let _ = l.reserve_at(a, t0, big);
        }
        // Peer b is still fully idle.
        assert_eq!(l.reserve_at(b, t0, big), Ok(Duration::ZERO));
    }
}
