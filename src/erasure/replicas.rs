//! Dispersed replicas of the root chunk — a port of bee `pkg/replicas`.
//!
//! ## Why the root needs its own scheme
//!
//! Erasure coding protects a chunk by way of its *parent*: a node's data
//! children are recoverable from that node's parity siblings. The root has no
//! parent, so nothing in the tree protects it — and losing the root loses the
//! whole object, however much parity sits underneath. Bee's answer is to store
//! the root chunk again, several times, at addresses that are deliberately
//! spread across distinct neighbourhoods, so a single unlucky neighbourhood
//! can't take the object down with it.
//!
//! A replica is a **single-owner chunk** that wraps the root chunk verbatim:
//! - `id` is the root's address with byte 0 replaced by a "mined" byte,
//! - the owner is the fixed, well-known [`REPLICAS_OWNER`] (bee
//!   `swarm.ReplicasOwner`, the address of the private key `0x01` followed by
//!   31 zero bytes — deliberately public, since the point is that *anyone* can
//!   derive a replica's address from the root's),
//! - so the SOC address is `keccak256(id || owner)`, which a retriever can
//!   compute knowing only the root reference.
//!
//! Dispersal is the interesting part, and it is what this module ports. The
//! mined byte is *searched*, not counted: candidates `i = 0, 1, 2, …` are
//! tried, and one is kept only when its resulting SOC address falls into a
//! neighbourhood no earlier replica occupies. "Neighbourhood" is the top `d`
//! bits of the address, for `d` = 1..4, so level MEDIUM splits the space in 2,
//! STRONG in 4, INSANE in 8, PARANOID in 16. A candidate is placed at the
//! *coarsest* level whose bucket is still free, which is what keeps successive
//! replicas maximally far apart rather than merely distinct.
//!
//! Counts come from bee's `replicaCounts`: 0/2/4/8/16 for NONE..PARANOID. Note
//! that replicas are emitted for **any** non-NONE level, including objects too
//! small to carry parity at all — a one-chunk file at MEDIUM is one content
//! chunk plus two replicas.

use alloy_primitives::{hex, keccak256};
use nectar_primitives::Chunk as _;
use nectar_primitives::bmt::{DEFAULT_BODY_SIZE, HASH_SIZE};
use nectar_primitives::chunk::{ChunkAddress, ContentChunk, SingleOwnerChunk};

use super::Level;
use super::encoder::EncodeError;

/// bee `swarm.ReplicasOwner` — the eth address of the fixed replica signing
/// key. Hardcoded rather than derived so a mismatch shows up as a failing
/// address comparison instead of a silently different owner.
pub const REPLICAS_OWNER: [u8; 20] = hex!("dc5b20847f43d67928f49cd4f85d696b5a7617b5");

/// bee `redundancy.replicaCounts` — replicas stored per level, indexed by
/// level. The real counts needed to hold the error rate under 1e-6 are
/// 0/2/4/5/19; bee approximates with successive powers of two.
const REPLICA_COUNTS: [usize; 5] = [0, 2, 4, 8, 16];

/// bee `replicas.replicaIndexBases` — where each depth's slots start in the
/// 16-entry queue. Go's `[5]int{0, 2, 6, 14}` leaves the fifth element zero;
/// only indices 0..3 are ever read (`d = level - 1`).
const REPLICA_INDEX_BASES: [usize; 5] = [0, 2, 6, 14, 0];

/// A candidate replica: the mined byte that produced it and the SOC address it
/// lands on.
#[derive(Clone, Copy)]
struct Candidate {
    mined: u8,
    addr: [u8; HASH_SIZE],
}

/// bee `replicas.replicator` — searches mined bytes and keeps the ones that
/// land in unoccupied neighbourhoods.
struct Replicator {
    root: [u8; HASH_SIZE],
    /// Replicas in dispersal order. Slots are partitioned by depth via
    /// [`REPLICA_INDEX_BASES`]: 0..1 for depth 1, 2..3 for depth 2, 4..7 for
    /// depth 3, 8..15 for depth 4.
    queue: [Option<Candidate>; 16],
    /// Which neighbourhoods are taken, across all depths (2 + 4 + 8 + 16 = 30).
    exist: [bool; 30],
    /// Next free queue slot per depth. Seeded from `REPLICA_COUNTS` (bee seeds
    /// `sizes` with `GetReplicaCounts()`), which is what makes each depth write
    /// into its own slot range.
    sizes: [usize; 5],
    level: Level,
}

impl Replicator {
    fn new(root: [u8; HASH_SIZE], level: Level) -> Self {
        Replicator {
            root,
            queue: [None; 16],
            exist: [false; 30],
            sizes: REPLICA_COUNTS,
            level,
        }
    }

    /// bee `replicator.replicate`: the SOC address for mined byte `i`.
    fn replicate(&self, i: u8) -> Candidate {
        let mut id = self.root;
        id[0] = i;
        let mut buf = [0u8; HASH_SIZE + 20];
        buf[..HASH_SIZE].copy_from_slice(&id);
        buf[HASH_SIZE..].copy_from_slice(&REPLICAS_OWNER);
        Candidate {
            mined: i,
            addr: keccak256(buf).into(),
        }
    }

    /// bee `replicas.nh`: index into `exist` for `addr`'s neighbourhood at this
    /// level, i.e. the top `level` bits of the address offset by the level's
    /// base.
    fn nh(level: Level, addr: &[u8; HASH_SIZE]) -> usize {
        let d = level as usize;
        REPLICA_INDEX_BASES[d - 1] + (addr[0] >> (8 - d)) as usize
    }

    /// bee `replicator.add`. Returns `(placed, slot)`; `placed == 0` means the
    /// candidate's neighbourhood was already occupied at every depth, so it is
    /// discarded.
    ///
    /// Recursion runs from `level` down to 1 *before* placing, so a candidate
    /// is claimed by the coarsest depth with a free bucket — the finer depths
    /// only get what the coarse ones couldn't take.
    fn add(&mut self, c: Candidate, level: Level) -> (usize, usize) {
        if level == Level::None {
            return (0, 0);
        }
        let nh = Self::nh(level, &c.addr);
        if self.exist[nh] {
            return (0, 0);
        }
        self.exist[nh] = true;

        let weaker = Level::from_wire((level as u8) - 1).unwrap_or(Level::None);
        let (mut placed, mut slot) = self.add(c, weaker);
        if placed == 0 {
            let d = (level as usize) - 1;
            slot = self.sizes[d];
            self.sizes[d] += 1;
            if slot < self.queue.len() {
                self.queue[slot] = Some(c);
            }
            placed = REPLICA_COUNTS[level as usize];
        }
        (placed, slot)
    }

    /// bee `replicator.replicas`: try mined bytes in order, flushing the
    /// contiguous run of filled queue slots after each successful placement.
    fn run(&mut self) -> Vec<Candidate> {
        let want = REPLICA_COUNTS[self.level as usize];
        let mut out: Vec<Candidate> = Vec::with_capacity(want);
        let mut n = 0usize;
        let mut i: u16 = 0;
        while out.len() < want && i < 255 {
            let c = self.replicate(i as u8);
            i += 1;
            let (placed, _) = self.add(c, self.level);
            if placed == 0 {
                continue;
            }
            // Emit the filled prefix starting at `n`, stopping at the first
            // hole (a finer depth may have claimed a later slot first).
            let mut advanced = 0usize;
            for (idx, slot) in self.queue[n..].iter().enumerate() {
                advanced = idx;
                match slot {
                    Some(c) => out.push(*c),
                    None => break,
                }
            }
            n += advanced;
        }
        out.truncate(want);
        out
    }
}

/// Default level assumed when *reading*, mirroring bee's
/// `redundancy.DefaultDownloadLevel`.
///
/// A downloader cannot know the level an object was uploaded at until it holds
/// the root chunk — which is the very chunk it is trying to recover. So the
/// search assumes the widest level and walks its replicas in order. That is
/// safe because dispersal claims the coarsest free bucket first: the first two
/// addresses a PARANOID search yields are exactly the two a MEDIUM upload
/// stored, the first four are STRONG's, and so on. A narrower upload is found
/// in the early batches; the later addresses simply don't exist.
pub const DEFAULT_DOWNLOAD_LEVEL: Level = Level::Paranoid;

/// The dispersed replica addresses of `root`, in bee's dispersal order.
pub fn replica_addresses(root: ChunkAddress, level: Level) -> Vec<ChunkAddress> {
    if level == Level::None {
        return Vec::new();
    }
    Replicator::new(root.into(), level)
        .run()
        .into_iter()
        .map(|c| ChunkAddress::from(c.addr))
        .collect()
}

/// One replica fetch. A named `async fn` so the initial fill and the refill
/// produce the same future type for `FuturesUnordered`.
async fn fetch_one<G: super::WireGet>(store: &G, addr: ChunkAddress) -> Option<Vec<u8>> {
    store.get_wire(&addr).await.ok()
}

/// Recover a root chunk from its dispersed replicas after the direct fetch of
/// `root` has failed. Returns the root's own wire form (`span || payload`),
/// unwrapped from whichever replica answered first.
///
/// **Deviation from bee, deliberate.** bee's `replicas.Getter` races the direct
/// fetch against expanding batches of replicas on a 300 ms timer, so every root
/// retrieval issues two extra requests even when the direct one is about to
/// succeed. That is cheap for a full node with a warm kademlia; it is not cheap
/// here. Bounding in-flight chunk requests is the single biggest throughput
/// lever this client has — an unbounded erasure node fetch cost ~2x throughput
/// and ~5x the failures until it was capped — so replicas are a *fallback*,
/// tried only once the direct fetch has actually failed, and then with the same
/// kind of bounded window. The recovery path is slower than bee's; the common
/// path is untouched.
///
/// Replicas are verified by re-hashing: the fixed replica key is **public**, so
/// anyone can sign a well-formed SOC at a replica address wrapping arbitrary
/// content. The only thing that makes a replica trustworthy is that the chunk
/// it wraps BMT-hashes to the root we asked for.
pub async fn recover_root<G>(store: &G, root: ChunkAddress, level: Level) -> Option<Vec<u8>>
where
    G: super::WireGet,
{
    use futures::stream::{FuturesUnordered, StreamExt};

    let addrs = replica_addresses(root, level);
    if addrs.is_empty() {
        return None;
    }
    tracing::debug!(
        target: "hoverfly::erasure",
        "root {root} unretrievable; trying {} dispersed replica(s)", addrs.len()
    );

    // Ordered window: the early addresses are the ones a narrower upload would
    // have stored, so they are worth trying first.
    const WINDOW: usize = 4;
    let mut next = 0usize;
    let mut futs = FuturesUnordered::new();
    while next < WINDOW.min(addrs.len()) {
        futs.push(fetch_one(store, addrs[next]));
        next += 1;
    }
    while let Some(res) = futs.next().await {
        if next < addrs.len() {
            futs.push(fetch_one(store, addrs[next]));
            next += 1;
        }
        let Some(wire) = res else { continue };
        match unwrap_replica(&wire, root) {
            Some(inner) => {
                tracing::info!(
                    target: "hoverfly::erasure",
                    "recovered root {root} from a dispersed replica"
                );
                return Some(inner);
            }
            None => continue,
        }
    }
    None
}

/// Pull the wrapped chunk out of a replica's wire form, rejecting anything that
/// doesn't actually hash to `root`.
///
/// A SOC on the wire is `id(32) || signature(65) || span(8) || payload`.
fn unwrap_replica(wire: &[u8], root: ChunkAddress) -> Option<Vec<u8>> {
    const SOC_PREFIX: usize = HASH_SIZE + 65;
    if wire.len() <= SOC_PREFIX {
        return None;
    }
    let inner = &wire[SOC_PREFIX..];
    // The wrapped chunk must be the root itself. This is the whole security
    // check: the replica owner key is public, so the signature proves nothing.
    if super::wire_address(inner)? != root {
        return None;
    }
    Some(inner.to_vec())
}

/// Build the dispersed replicas of a root chunk.
///
/// `root_wire` is the root chunk in wire form (`span_LE_8 || payload`).
/// Returns `(address, wire)` pairs ready to stamp and push, empty for
/// [`Level::None`].
pub fn dispersed_replicas(
    root_wire: &[u8],
    level: Level,
) -> Result<Vec<(ChunkAddress, Vec<u8>)>, EncodeError> {
    if level == Level::None {
        return Ok(Vec::new());
    }
    let root = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(root_wire)
        .map_err(|e| EncodeError::Replica(e.to_string()))?;
    let root_addr: [u8; HASH_SIZE] = (*root.address()).into();

    let candidates = Replicator::new(root_addr, level).run();
    let mut out = Vec::with_capacity(candidates.len());
    for c in candidates {
        // nectar builds and signs the SOC (fixed replica key, `id[0] = mined`,
        // `id[1..] = root_addr[1..]`), matching bee's `soc.New(id, ch).Sign`.
        let soc = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new_dispersed_replica(
            c.mined,
            root.body().clone(),
        )
        .map_err(|e| EncodeError::Replica(e.to_string()))?;
        let addr = *soc.address();
        // The mined address drives the dispersal search, so if it disagreed
        // with the signed chunk's real address the whole selection would be
        // meaningless — and the replica unfindable from the root reference.
        if <[u8; HASH_SIZE]>::from(addr) != c.addr {
            return Err(EncodeError::Replica(
                "replica address does not match the mined address".into(),
            ));
        }
        out.push((addr, bytes::Bytes::from(soc).to_vec()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The owner constant must be the address of bee's fixed replica key. If
    /// these ever disagree, every replica we produce is unfindable.
    #[test]
    fn owner_matches_bee() {
        use alloy_signer_local::PrivateKeySigner;
        let mut pk = [0u8; 32];
        pk[0] = 1;
        let signer = PrivateKeySigner::from_slice(&pk).unwrap();
        assert_eq!(signer.address().as_slice(), &REPLICAS_OWNER);
    }

    /// Replica counts per level, straight from bee's table.
    #[test]
    fn counts_match_bee() {
        for (level, want) in [
            (Level::None, 0),
            (Level::Medium, 2),
            (Level::Strong, 4),
            (Level::Insane, 8),
            (Level::Paranoid, 16),
        ] {
            let root = [0x9eu8; 32];
            let got = Replicator::new(root, level).run().len();
            assert_eq!(got, want, "{level}");
        }
    }

    /// The replica addresses bee's own pipeline stores, captured from
    /// `cmd/ecref -replicas` (which drives `builder.NewPipelineBuilder` into an
    /// in-memory store and pulls the SOCs back out). Each case is
    /// `(root, level, [(mined_byte, soc_address)])` in bee's emission order.
    ///
    /// This is the test that matters. Matching *counts* is not enough: a
    /// retriever derives replica addresses from the root reference alone and
    /// will look nowhere else, so picking different mined bytes than bee would
    /// produce replicas nobody can find. Five distinct roots x four levels
    /// exercises the dispersal search over different address distributions.
    ///
    /// Compared as a *set*: bee stores replicas concurrently (a goroutine per
    /// replica) and they come back off an unordered store, so emission order is
    /// not a stable property of either implementation — and every replica is
    /// equally valid to a retriever, which tries them all.
    #[test]
    fn matches_bee_replica_addresses() {
        #[allow(clippy::type_complexity)]
        let cases: &[(&str, Level, &[(u8, &str)])] = &[
            (
                "b9a99970a4e649dc2db95432c1035b4d95a244eb796f2fa19700248a4a526b9b",
                Level::Medium,
                &[
                    (
                        0x00,
                        "f6884553092781c3703ff684106987a150f68bd15d88bfb4226c41540cde0716",
                    ),
                    (
                        0x01,
                        "075b13ad703111a1937177b7c3dc9fd1e542378bd06717fb62c77d6f9c9bed31",
                    ),
                ],
            ),
            (
                "b9a99970a4e649dc2db95432c1035b4d95a244eb796f2fa19700248a4a526b9b",
                Level::Strong,
                &[
                    (
                        0x00,
                        "f6884553092781c3703ff684106987a150f68bd15d88bfb4226c41540cde0716",
                    ),
                    (
                        0x01,
                        "075b13ad703111a1937177b7c3dc9fd1e542378bd06717fb62c77d6f9c9bed31",
                    ),
                    (
                        0x03,
                        "506b241b7e1d9cc1e2cf344bf6e6219223541d73f9b329ab2bf92c34b3853954",
                    ),
                    (
                        0x09,
                        "8efe23f3d5d58e025f6c359224e506b77fa68690cec9b630e780e7ae9062a1f5",
                    ),
                ],
            ),
            (
                "b9a99970a4e649dc2db95432c1035b4d95a244eb796f2fa19700248a4a526b9b",
                Level::Insane,
                &[
                    (
                        0x00,
                        "f6884553092781c3703ff684106987a150f68bd15d88bfb4226c41540cde0716",
                    ),
                    (
                        0x01,
                        "075b13ad703111a1937177b7c3dc9fd1e542378bd06717fb62c77d6f9c9bed31",
                    ),
                    (
                        0x03,
                        "506b241b7e1d9cc1e2cf344bf6e6219223541d73f9b329ab2bf92c34b3853954",
                    ),
                    (
                        0x07,
                        "c68d45ce62e49bd838d8b7a3ccd859f10509647255f2270bc682e8f49604f5c8",
                    ),
                    (
                        0x08,
                        "7a4ca1e1471737f58c6ed2f2446749c7b7a517c9861c4ba01d56fec302354c79",
                    ),
                    (
                        0x09,
                        "8efe23f3d5d58e025f6c359224e506b77fa68690cec9b630e780e7ae9062a1f5",
                    ),
                    (
                        0x0b,
                        "b721eab85761698001006e6afb72089dd91e9cb21ebdc832430b4d31d966d534",
                    ),
                    (
                        0x10,
                        "2ce66b746615b2937b20bc0cb24fe0a1fc8babc3ff3ed61aebf9585629ca3971",
                    ),
                ],
            ),
            (
                "b9a99970a4e649dc2db95432c1035b4d95a244eb796f2fa19700248a4a526b9b",
                Level::Paranoid,
                &[
                    (
                        0x00,
                        "f6884553092781c3703ff684106987a150f68bd15d88bfb4226c41540cde0716",
                    ),
                    (
                        0x01,
                        "075b13ad703111a1937177b7c3dc9fd1e542378bd06717fb62c77d6f9c9bed31",
                    ),
                    (
                        0x02,
                        "e2f7e16074e4d6b073d28788bf12c230ef17aede7db045fe86d94a7f46890ab2",
                    ),
                    (
                        0x03,
                        "506b241b7e1d9cc1e2cf344bf6e6219223541d73f9b329ab2bf92c34b3853954",
                    ),
                    (
                        0x04,
                        "1c443c3ff78750d5fe8fb2736f94a82380f500612459b33a52bcb7ab695c383c",
                    ),
                    (
                        0x07,
                        "c68d45ce62e49bd838d8b7a3ccd859f10509647255f2270bc682e8f49604f5c8",
                    ),
                    (
                        0x08,
                        "7a4ca1e1471737f58c6ed2f2446749c7b7a517c9861c4ba01d56fec302354c79",
                    ),
                    (
                        0x09,
                        "8efe23f3d5d58e025f6c359224e506b77fa68690cec9b630e780e7ae9062a1f5",
                    ),
                    (
                        0x0b,
                        "b721eab85761698001006e6afb72089dd91e9cb21ebdc832430b4d31d966d534",
                    ),
                    (
                        0x0f,
                        "951c8a16769e8e522f607058602533f3ef7e8bf4784fe4b5c3912732b29d0fda",
                    ),
                    (
                        0x10,
                        "2ce66b746615b2937b20bc0cb24fe0a1fc8babc3ff3ed61aebf9585629ca3971",
                    ),
                    (
                        0x12,
                        "6b36636c19677c41a681e2ab4b8288bdea6c580ed0dd811f56e997d150536270",
                    ),
                    (
                        0x17,
                        "a4349d927b10d187cc3ca8bb3b18f2108262b931192ed8fb293b29ccaf67fcda",
                    ),
                    (
                        0x19,
                        "460f05e4b9c2fafe117d0d7090cb59d6884522300032941d009d3b72df7fc165",
                    ),
                    (
                        0x2e,
                        "ded0058f8b58fb628e4bc5c9cb6dc1cab360a7f825dd1dcb8e786367a010b9fa",
                    ),
                    (
                        0x3c,
                        "372750025e855a7a06b96f56ff3452d5788e61ded5ffbf84bb1e2cb53f9f1e21",
                    ),
                ],
            ),
            (
                "ba83c6e69c4536d6f3ed8cee0211f312096787469f381ce6bdd011ed95d9ce48",
                Level::Medium,
                &[
                    (
                        0x00,
                        "656f456d4aa475afceb0e27af9dbc50777d4633e36f7fcfba79648a15eb77f5c",
                    ),
                    (
                        0x03,
                        "da97b514c0ac6091e5fa3ce4b53a8ff48083beaf6baecbf54b9842f94e5f419a",
                    ),
                ],
            ),
            (
                "ba83c6e69c4536d6f3ed8cee0211f312096787469f381ce6bdd011ed95d9ce48",
                Level::Strong,
                &[
                    (
                        0x00,
                        "656f456d4aa475afceb0e27af9dbc50777d4633e36f7fcfba79648a15eb77f5c",
                    ),
                    (
                        0x02,
                        "3859cc651682c144d1b7aec10e0f61f40a67d822f2916a7820bc8867f1e1efe4",
                    ),
                    (
                        0x03,
                        "da97b514c0ac6091e5fa3ce4b53a8ff48083beaf6baecbf54b9842f94e5f419a",
                    ),
                    (
                        0x05,
                        "8f3e4ba32f9bb909e7d35fe7f74fdb60c9bbdccbc5a62c2851fcfb4bc9eb012e",
                    ),
                ],
            ),
            (
                "ba83c6e69c4536d6f3ed8cee0211f312096787469f381ce6bdd011ed95d9ce48",
                Level::Insane,
                &[
                    (
                        0x00,
                        "656f456d4aa475afceb0e27af9dbc50777d4633e36f7fcfba79648a15eb77f5c",
                    ),
                    (
                        0x02,
                        "3859cc651682c144d1b7aec10e0f61f40a67d822f2916a7820bc8867f1e1efe4",
                    ),
                    (
                        0x03,
                        "da97b514c0ac6091e5fa3ce4b53a8ff48083beaf6baecbf54b9842f94e5f419a",
                    ),
                    (
                        0x05,
                        "8f3e4ba32f9bb909e7d35fe7f74fdb60c9bbdccbc5a62c2851fcfb4bc9eb012e",
                    ),
                    (
                        0x06,
                        "b0129766c3986603c112f955db5428321f452657a48c2a9c2eb4f14ea582d88b",
                    ),
                    (
                        0x0b,
                        "4ca5fa471ee5309020103a30ed251105f0f5d4f0ff46e70e6a72b43ab3828301",
                    ),
                    (
                        0x0c,
                        "f5864d2300fa3f8484bd1500955f646c9a1621d680bf84a8caa8a1f54f1f49e8",
                    ),
                    (
                        0x0d,
                        "02ce43f730b0f32e04216ab09538c49caeb1a8f203ed973250f0bd4c5988b565",
                    ),
                ],
            ),
            (
                "ba83c6e69c4536d6f3ed8cee0211f312096787469f381ce6bdd011ed95d9ce48",
                Level::Paranoid,
                &[
                    (
                        0x00,
                        "656f456d4aa475afceb0e27af9dbc50777d4633e36f7fcfba79648a15eb77f5c",
                    ),
                    (
                        0x02,
                        "3859cc651682c144d1b7aec10e0f61f40a67d822f2916a7820bc8867f1e1efe4",
                    ),
                    (
                        0x03,
                        "da97b514c0ac6091e5fa3ce4b53a8ff48083beaf6baecbf54b9842f94e5f419a",
                    ),
                    (
                        0x04,
                        "c182993daedb818d2dea3a1fd957fcddd200460ae8ec58464e6ecf90755b3994",
                    ),
                    (
                        0x05,
                        "8f3e4ba32f9bb909e7d35fe7f74fdb60c9bbdccbc5a62c2851fcfb4bc9eb012e",
                    ),
                    (
                        0x06,
                        "b0129766c3986603c112f955db5428321f452657a48c2a9c2eb4f14ea582d88b",
                    ),
                    (
                        0x07,
                        "2902322dac81565d36b7a87d09aafbd63e386691006b850632c53ea21ba2de4c",
                    ),
                    (
                        0x08,
                        "97d8840114ebc9a0bfe769218d8a59702e53a76afdaf96c27a3562875687f97f",
                    ),
                    (
                        0x0b,
                        "4ca5fa471ee5309020103a30ed251105f0f5d4f0ff46e70e6a72b43ab3828301",
                    ),
                    (
                        0x0c,
                        "f5864d2300fa3f8484bd1500955f646c9a1621d680bf84a8caa8a1f54f1f49e8",
                    ),
                    (
                        0x0d,
                        "02ce43f730b0f32e04216ab09538c49caeb1a8f203ed973250f0bd4c5988b565",
                    ),
                    (
                        0x0e,
                        "72088f02679e2fef9da8d5cc73553e99335dcea1bbf2d826bf00b4c5cf9415ee",
                    ),
                    (
                        0x12,
                        "1160bc93bbed84940159075a7322fc88bdea5ef46e1b4e7c9a2eb236884cbd0a",
                    ),
                    (
                        0x13,
                        "5d09b121a6b05abf14b8b104c97524d395666ac3106b4b026f8a3c1399d961e3",
                    ),
                    (
                        0x20,
                        "abfc115ce35426653b873e7af46c0e0f4f619340917f1bff1d5c061ad06a4fe3",
                    ),
                    (
                        0x22,
                        "e77210ff6e6194bb45bd1723c281289e64d34cb893de9bebc9923f73986fcf7f",
                    ),
                ],
            ),
            (
                "974f1096c572411cecfa174405f3f782abe436e42b0a2fab865aea175cd4fad9",
                Level::Medium,
                &[
                    (
                        0x00,
                        "cce77303b048293d6876c44b6a7de1edd5b2100bb361dae295246903e2a5d98f",
                    ),
                    (
                        0x01,
                        "4df264586363cc3e77ecd3ae9ce4dad10de776de16306d7304d7e22b3e757b6c",
                    ),
                ],
            ),
            (
                "4fffae8bfe050a55f0b1be2eddabacc2b9249f7763a65dd8cb840572b5599ab7",
                Level::Strong,
                &[
                    (
                        0x00,
                        "ba8afcb28acc5bb658ca7cdb30e7a0c578dbe0c51d75723a629b12679a1c3855",
                    ),
                    (
                        0x01,
                        "36299e5c617305d51c797de1502e0d5cfb45007b7c1f54186ce106ba6cef2651",
                    ),
                    (
                        0x03,
                        "c6a8834f301c426fff69f72af88cfdfe7cb910dc784213d1e6cb660df31cf130",
                    ),
                    (
                        0x06,
                        "51dd7b81d101e55f58c2fb5ecbd864902e059a3228dca88bda9e3b2afb7e6942",
                    ),
                ],
            ),
            (
                "71c060781ba98be591c2a2bd5864ad38dcb6d11166a3520a4d2529360915dc7b",
                Level::Insane,
                &[
                    (
                        0x00,
                        "92f21cfc5876e2b89e6d42ca3ddf78854425fc552ded600fe34b0ff1966e52eb",
                    ),
                    (
                        0x01,
                        "424856f2aeaa663f9bf15fe6f4ef11bbe99eb9ee4dab0ccedc08a435038e831d",
                    ),
                    (
                        0x02,
                        "ee70491e198bce66810ce2fb8976152f1e2a6ce296bb8a0b0893e537798103f6",
                    ),
                    (
                        0x03,
                        "2a2ea90e45b28c346dbf7fce0b7eab0605e43f6097a369e52e6ce18a9f47a675",
                    ),
                    (
                        0x07,
                        "b79bacd10cc142e84e39b2165f9fb5370ed5d339c43de3951220b4957f088f42",
                    ),
                    (
                        0x08,
                        "686024ae9e1897060f0eeacf920398e265bec1471530bc2116d47bc0b271b72e",
                    ),
                    (
                        0x09,
                        "1ece12c17859a4dcff0024c4a9e9223dbe3f465e0f2df5e3b812490c0537089b",
                    ),
                    (
                        0x0d,
                        "ce811de6ad92dd64c71d9546494e56f696ac03cd6b224024734e50f85a5a9fc6",
                    ),
                ],
            ),
            (
                "019ebbd10687c4887315b1224c925e08b0b946d50c769523acb3750685f47474",
                Level::Paranoid,
                &[
                    (
                        0x00,
                        "f85d76f42aed89436ae42b2fac321612f7ee1ecbb0182e64c08cb72da4b75ea3",
                    ),
                    (
                        0x01,
                        "746943c56c8a4122ea70b487c3876482e41e6981020962a39f3c875d0d89e777",
                    ),
                    (
                        0x02,
                        "4566598fd2d0242d4f2a0dc8d05acf794b3b40da6afb041c5ac9d7d9fc7b6ddd",
                    ),
                    (
                        0x03,
                        "8e6de01d9a167d4493eb0df8bead6067ea5a7c6f0ca4a5a0bd2c99a9669b9c44",
                    ),
                    (
                        0x04,
                        "a478b2943705f1bed0dab9f4bc8dd8a23cb0ef8fa144ee9bf6349ddb61ff589a",
                    ),
                    (
                        0x05,
                        "b072f16af196670d4a3b7e7743c2659cff926f741acfea30885adc6de60f9883",
                    ),
                    (
                        0x08,
                        "9316f1931a2de611616473aff0c95e4385ef7dbb537e25af5a5a76fccede20be",
                    ),
                    (
                        0x09,
                        "1713c750b7f7abe211715b28108d6155fc9718e67a23537e59014e900af8f9fb",
                    ),
                    (
                        0x0a,
                        "307f6317b048a67dc01776f153b6ebbab342449c96a32fa055da795aa77c239d",
                    ),
                    (
                        0x0b,
                        "53c9d4be216799b8b6d31d1e94bc1dd1b66455c0800901e188fb5cf72e6c3b81",
                    ),
                    (
                        0x0d,
                        "28b28bbac647f432d7e55d02818178ef3f2a4e30cf34dfbe0b604c30cd6f4ec0",
                    ),
                    (
                        0x0e,
                        "62e9bc9b81c4dc4db68fa82d3081508b232655030d523072a2a8b7f9f517dea5",
                    ),
                    (
                        0x10,
                        "0e9dc1417bbfb15f49294377b507c063a3d36758b51a70b2dca0a8d27b76794a",
                    ),
                    (
                        0x13,
                        "cd74b4bee3e311e030031db9af02412410202f4c89bc6d66dd1ac8f3bff594a2",
                    ),
                    (
                        0x18,
                        "ee9707f23ac00d958465d43d2e2150cd9621ea2aa5854f91aa97b9939204acfa",
                    ),
                    (
                        0x23,
                        "db4afd66ff58c48e08675a3533f9f2ac5da142a45917781c7ce27671d044962f",
                    ),
                ],
            ),
            (
                "aa5e1e394ffd97ca95420a1ac28ac75b806278f3c97ba4f9ab2cabb4e7dc2902",
                Level::Medium,
                &[
                    (
                        0x00,
                        "d13dbd5c5d8b36c7548e133db9715d5ea887a6d6e8022f8b41c6da46e221afda",
                    ),
                    (
                        0x01,
                        "1cdef276d53abf95a3531505cb40bfe845aafbcb9e71ad11f02435a7e0bb5c64",
                    ),
                ],
            ),
            (
                "738959fe2368bf84b5fe04c855765dca7a8fdf16d938db8bb300e39278b0b3c6",
                Level::Strong,
                &[
                    (
                        0x00,
                        "5ef2439b3d7ea233287eeb964043f951369d7e37ec30f6c7fbc406912203b7f4",
                    ),
                    (
                        0x01,
                        "b8493a0e40b9b0589507dfe3b38594a01e4982fad0a2e649a5d22595bade6755",
                    ),
                    (
                        0x07,
                        "dc9e165714aabac8dd2f0f00a0ade958417bceafe3561ad0a7c654d556157f3c",
                    ),
                    (
                        0x08,
                        "0b691637f12a2959c3654d098ce967ba8a0564d12fadb3ae5ab078644ffec9c8",
                    ),
                ],
            ),
            (
                "1c6a6a109af344d527f243dd26627a9925f4393be5e88033aff0d2208d5f6823",
                Level::Insane,
                &[
                    (
                        0x00,
                        "3f67290be342fdc3fb1ad14621cd31d347249cecbd6979b16aa73886bbeed1d2",
                    ),
                    (
                        0x03,
                        "8a023a9f3ef6d95a1516df4ad6a5945845bf120ba8b99a3eb5898ee98bdd0a12",
                    ),
                    (
                        0x04,
                        "7a84ff451600e669d25bd1df8659302e1c5ad8536c70a13efb6504eff5ddc931",
                    ),
                    (
                        0x06,
                        "b8b7bc47868bcb39cd156d45483f44e98174d78f084ca666605acc4f8e8fef42",
                    ),
                    (
                        0x07,
                        "df08c7f61fc67cace29f3746ef7bd419e09095c3830a60bc1aeb0963fcc7da87",
                    ),
                    (
                        0x10,
                        "085bf1e5e1b3eb95c32721744816123c2f4e25209ce134cf33351cbb09e32275",
                    ),
                    (
                        0x11,
                        "46ee2249051920d5491ec455666947ef4c84572792d390842c6d0fbf21bc49f4",
                    ),
                    (
                        0x18,
                        "e22cc98ac01fd005d2a58b542a386823ed03e83aad3e476a64262d89069e4af8",
                    ),
                ],
            ),
            (
                "bdab4a41851c97605883b632ae3dfe99d4bb8919482985065d1899f2ac2684cc",
                Level::Paranoid,
                &[
                    (
                        0x00,
                        "6bcdf00a2de4d542ba7ca65145a94db9ad46e31630a3b24c100957b4aa4aeae5",
                    ),
                    (
                        0x01,
                        "5cb9e0afb8b7423e7cb7d5cc4f234981245b2b11cf4f951fadbe5ae7e0d8a332",
                    ),
                    (
                        0x02,
                        "00c67e14c1026bfcfc18d16e5362c6e515db5c2916c14f61a74a02d0d7c3cd77",
                    ),
                    (
                        0x03,
                        "e876406f70965d7c568cd310ef6a1a7a87fd583acb1190ffb26c496494fcf8e6",
                    ),
                    (
                        0x05,
                        "aefa7de2573656c8e5b129d91be0f94bc993418219afce1cc6f96080a26dc96a",
                    ),
                    (
                        0x06,
                        "80c936c0d1f6d45ee014c999a837610fd38590150118888114a789d56622fa36",
                    ),
                    (
                        0x07,
                        "4099b37201d905b2db5eed0f17e9cd55a8f93a742a6fc3853a5e3e260b033983",
                    ),
                    (
                        0x0a,
                        "328b906a90263695baebac6977e7c187e0905e5b7394b6ca0ed295c66bd70825",
                    ),
                    (
                        0x0b,
                        "174c8ac50699f3f9eb87cc96f536fed190c654a5ea739968a324d054548e3137",
                    ),
                    (
                        0x0c,
                        "fb1fa0f1c4c01f08ac437d3c1d4ae8d410ef9095886fb5e29e9906b79c8ff6d3",
                    ),
                    (
                        0x0f,
                        "2e8999b83fdc3d2f7d78914a9f106c97a25584f8765a2ad0f45af8e65907c13e",
                    ),
                    (
                        0x12,
                        "b76584a59ccc976195a669a80865702f12d76f758282ea5b12f246485b41816b",
                    ),
                    (
                        0x16,
                        "98a5c32c2e4358c50e68ee1afb0f2c2dddcb19decc9ffe59696c9d63e9f8734a",
                    ),
                    (
                        0x21,
                        "7347e3831db0ed406ff809c77a46ffda372d22b45eb836a0356b80dfdb661fca",
                    ),
                    (
                        0x2b,
                        "d1df2ae71795e424f122074725b162557694c6ca9e9e3f79ae91af949d14a9d1",
                    ),
                    (
                        0x5a,
                        "c1a9132e90d55d3f0ad2fe5e71ffe404120ac02232ce4b26797d862cfa16fa41",
                    ),
                ],
            ),
            (
                "8c38a3e817cb12d1053c78521b56f7c7adde49d33970ceec104048f091ae1ea2",
                Level::Medium,
                &[
                    (
                        0x00,
                        "d65b58f286520c7ddb64c1396489e1fde4e2ad66441481eef85c12c516a2d111",
                    ),
                    (
                        0x01,
                        "0afee1d315f587a06baecf832c45beecc2b844d01cf7f6059c4b48921db3adba",
                    ),
                ],
            ),
            (
                "40ba4f406f59400daaad6d242b89f5375df663937277d07f94237582283cbd32",
                Level::Strong,
                &[
                    (
                        0x00,
                        "b9afa9f5b56feb9c7d64144c184625bd52d5ed691d3f39e5565fb9db41dd5218",
                    ),
                    (
                        0x01,
                        "7786e67ad94002a7709a49c679a593c842f78be539b00f647390fe22d35bd475",
                    ),
                    (
                        0x05,
                        "de11ec23a6f3f87a8ae828005d8fe4f77a833f7a0d5da53e06f9b87b4f7a38b2",
                    ),
                    (
                        0x09,
                        "03c4d16e143d5e1038c79e6e1b2bab8b9e82a8339b0983f590f0da889c7fb924",
                    ),
                ],
            ),
            (
                "92f826bab11448bc871eb3641b7abc69a0971502ecb66dc1c839e9821a37abd9",
                Level::Insane,
                &[
                    (
                        0x00,
                        "5b05a7484cd648c0f796e0334f90847c1134f855612592be2fe8f8a6b1667465",
                    ),
                    (
                        0x02,
                        "9461a4a44130f71f7ad5598f967b3e2045d40d7ba8ced140c3c4ab72f263ab0c",
                    ),
                    (
                        0x06,
                        "b2b713a081f378419e25d72a27109b89da2852ebe64a193fb756f23ee21fd1b0",
                    ),
                    (
                        0x07,
                        "cccfcbe436e364be527e080f048547aa9319ceaaab4b4e286d55a8b30eeb1785",
                    ),
                    (
                        0x0b,
                        "6837ccc7242c38421adeefd013736528c8f90e9fc65f88d9ac7f6cc1106ec1f8",
                    ),
                    (
                        0x14,
                        "3125529b32167dc9bc2212c1d255fbc97aedb661e0626d4699eb10876ba3566c",
                    ),
                    (
                        0x16,
                        "fba8fe0558cc288f8dcb463815792d909a5c3c136c8b32c98a65ec4a3bbf8801",
                    ),
                    (
                        0x29,
                        "12addba66f75830be9966c5c8033bad197843fe868af4bed0013808f6e2f125a",
                    ),
                ],
            ),
            (
                "4442fc7f99131af867a80a53a6bd76f2a43c15ff82f2b32a780963bd35dad190",
                Level::Paranoid,
                &[
                    (
                        0x00,
                        "077a17adc3b5aeb2943096b528026f21d56414fec86b7fe9e6c66ce5d08ca086",
                    ),
                    (
                        0x01,
                        "ae5a45c1c6b6b24c21f8392671f7d31c35ed305083836840b28ab50a3ebc30a1",
                    ),
                    (
                        0x03,
                        "f8fd3724ce4cddce0486aea7684251743da4ea081e0ce8e3d75fe493f8a5dce2",
                    ),
                    (
                        0x05,
                        "27510bac426c70ca7ab910a4e490be249aa945cc9566d190350a2957eea2adfc",
                    ),
                    (
                        0x06,
                        "55717f60f991b4a3babd8cd6e6294947ec1cb7efa06314d9f618d33132467c7c",
                    ),
                    (
                        0x09,
                        "8a497543afce3f90cfda535eb952165d7d855ce3d4758ae31ea5d829580f4ed6",
                    ),
                    (
                        0x0a,
                        "e327a25915c75fc2e8744a09ac5598ee9a72e88707616f171f9e4c1945d227b1",
                    ),
                    (
                        0x0c,
                        "b2ac0feda2ddd5d49f75c30abc0848a0cf2e4c707991ebd6d3c2d31d189bfd4f",
                    ),
                    (
                        0x0d,
                        "64f37ff3bc4df6f5db3d1462bfaf4bf0d75c79779da59b692714d06fd5fd3cdf",
                    ),
                    (
                        0x11,
                        "c00609b9482696a2fa640bb90be1bb09e75839b58929d5364e1f3e7d3c3615fe",
                    ),
                    (
                        0x14,
                        "470fd81b12649188d5f3ebb12125ff56da5a3b63ca5805d9a186188a2bb7835f",
                    ),
                    (
                        0x15,
                        "768e63a431da66f2c392a7160bba9410ba0b6b0680c53bb6d2f531384d278ccd",
                    ),
                    (
                        0x1c,
                        "da76f6130930805a28ec33ad03bacd4f225679d1f038ca6c47a0a526f485e0c4",
                    ),
                    (
                        0x26,
                        "13acf494623f96b0e7c8aafb97cac748e22c5ef7d3292fcc71ac0a7f15cb86bc",
                    ),
                    (
                        0x29,
                        "97fd51eb8408b0f90f253540c56fcdd6f549b97b2a27b44479e0a766f73ae2a6",
                    ),
                    (
                        0x2a,
                        "3d3153502c91f8e1104184e2c16d5c7e5418cc809e361dc2ac5789c7ef5b13d5",
                    ),
                ],
            ),
        ];
        for (root_hex, level, want) in cases {
            let mut root = [0u8; 32];
            root.copy_from_slice(&alloy_primitives::hex::decode(root_hex).unwrap());
            let mut got: Vec<(u8, String)> = Replicator::new(root, *level)
                .run()
                .iter()
                .map(|c| (c.mined, alloy_primitives::hex::encode(c.addr)))
                .collect();
            let mut want: Vec<(u8, String)> =
                want.iter().map(|(m, a)| (*m, (*a).to_string())).collect();
            got.sort();
            want.sort();
            assert_eq!(got, want, "root {root_hex} at {level}");
        }
    }

    #[test]
    fn narrower_levels_are_prefixes_of_wider_ones() {
        for seed in 0u8..64 {
            let root = [seed.wrapping_mul(37).wrapping_add(11); 32];
            let wide = Replicator::new(root, Level::Paranoid).run();
            for lvl in [Level::Medium, Level::Strong, Level::Insane] {
                let narrow = Replicator::new(root, lvl).run();
                let w: Vec<u8> = wide.iter().take(narrow.len()).map(|c| c.mined).collect();
                let n: Vec<u8> = narrow.iter().map(|c| c.mined).collect();
                assert_eq!(n, w, "seed={seed} {lvl}");
            }
        }
    }

    /// The read path derives replica addresses from the root and looks nowhere
    /// else, so this pins a pair confirmed to exist **on mainnet**: the root of
    /// a 33,545-byte object uploaded at MEDIUM, whose two replicas were then
    /// fetched back from a bee gateway's `/chunks` endpoint (both 200, both
    /// wrapping this root).
    #[test]
    fn matches_addresses_confirmed_on_mainnet() {
        let root = ChunkAddress::from(hex!(
            "1d31e218c1948758ae2be7fa0370db598d672cfcfaf1fe4c7ab413184a5d4303"
        ));
        let got: Vec<String> = replica_addresses(root, Level::Medium)
            .iter()
            .map(|a| alloy_primitives::hex::encode(a.as_bytes()))
            .collect();
        assert_eq!(
            got,
            vec![
                "77b1d0c6e74779acb4c91f829fd4aaafedec7b3c89862ab06e9ab5e9e68ed31e".to_string(),
                "89407a68eb715dad1cf05e379fb71d3701ac96b7bd3ddc953d70cde7206c1c57".to_string(),
            ]
        );
        // And the wider search a downloader actually runs must start with them.
        let wide = replica_addresses(root, DEFAULT_DOWNLOAD_LEVEL);
        assert_eq!(wide.len(), 16);
        assert_eq!(
            wide[..2]
                .iter()
                .map(|a| alloy_primitives::hex::encode(a.as_bytes()))
                .collect::<Vec<_>>(),
            got
        );
    }

    /// Dispersal is the whole point: replicas must land in *distinct*
    /// neighbourhoods at the level's granularity (top `level` bits).
    #[test]
    fn replicas_are_dispersed() {
        for level in [Level::Medium, Level::Strong, Level::Insane, Level::Paranoid] {
            let root = [0x9eu8; 32];
            let got = Replicator::new(root, level).run();
            let d = level as usize;
            let mut seen = std::collections::HashSet::new();
            for c in &got {
                // Buckets are only guaranteed distinct at the depth that
                // claimed each replica; at the full level granularity every
                // replica must still be in its own bucket.
                assert!(
                    seen.insert(c.addr[0] >> (8 - d)),
                    "{level}: duplicate neighbourhood"
                );
            }
        }
    }
}
