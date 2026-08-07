//! Client-side payment for metered lanes — `docs/pusher-incentives.md`
//! Stage 1, client half.
//!
//! Four jobs, in the order a client meets them:
//!
//! 1. **Pin the lane.** Parse and *verify* the signed quote from
//!    `/v1/status`, checking it against the identity in config rather than
//!    trusting what the lane says about itself (§7.3).
//! 2. **Get admitted.** Fetch a challenge, sign it, and carry the header on
//!    every `/v1/push` and `/v1/pay`.
//! 3. **Size the POST.** The challenge returns the credit line; a body that
//!    would exceed it is split rather than sent and refused (§7.2).
//! 4. **Settle.** Track what is owed by the same arithmetic the relay uses,
//!    and issue a cumulative cheque when it crosses `settle_every`.
//!
//! The client computes its bill from **bytes it sent**, not from anything
//! the relay reports. That is the property §8 is built on, and it is why
//! there is nothing here that verifies the relay's work: a disagreement is
//! arithmetic, visible immediately, and settled by not paying.

use crate::meter::Params;

/// A lane's signed `payment` block, after verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentQuote {
    pub beneficiary: [u8; 20],
    /// Recovered from `sig`. **This is what a client pins**, not the
    /// overlay — see [`PaymentQuote::verify`].
    pub node_eth_address: [u8; 20],
    pub overlay_nonce: [u8; 32],
    pub origin: String,
    pub chain_id: u64,
    pub params: Params,
    /// True when the relay enforces 402. Soft-mode lanes bill but serve.
    pub hard_enforcement: bool,
}

/// What a client pins in config for a lane it is willing to pay.
///
/// `PUSHER_URLS` is already a hardcoded list, so carrying two more fields
/// per entry costs nothing — and reading the beneficiary from `/v1/status`
/// at runtime instead would mean paying whoever answers the URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanePin {
    pub node_eth_address: [u8; 20],
    pub beneficiary: [u8; 20],
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QuoteError {
    #[error("quote field {0} missing or malformed")]
    Field(&'static str),
    #[error("quote signature does not verify: {0}")]
    Signature(String),
    #[error("quote signed by 0x{got} but this lane is pinned to 0x{want}")]
    WrongSigner { got: String, want: String },
    #[error("quote beneficiary 0x{got} is not the pinned 0x{want}")]
    WrongBeneficiary { got: String, want: String },
    #[error("advertised overlay does not derive from the signed identity")]
    OverlayMismatch,
    #[error("quote parameters are unusable: {0}")]
    BadParams(String),
    #[error("lane price {got} exceeds the client's ceiling {ceiling}")]
    TooExpensive { got: u128, ceiling: u128 },
}

impl PaymentQuote {
    /// Parse and verify a `/v1/status` `payment` block.
    ///
    /// `advertised_overlay` is the lane's own `overlay` field. Checking that
    /// it derives from the *signed* identity is what makes the overlay
    /// trustworthy at all: an overlay is
    /// `keccak(eth_addr ‖ network_id_LE8 ‖ nonce)`, so a signature alone
    /// yields the eth address while the nonce is neither transmitted nor
    /// derivable — which is why "pin `(url, overlay)`" was never
    /// implementable and the pin is on the address.
    pub fn verify(
        payment: &serde_json::Value,
        advertised_overlay: Option<&[u8; 32]>,
        network_id: u64,
        pin: Option<&LanePin>,
        price_ceiling_plur_per_kib: u128,
    ) -> Result<Self, QuoteError> {
        let sig_hex = payment
            .get("sig")
            .and_then(|s| s.as_str())
            .ok_or(QuoteError::Field("sig"))?;
        let sig = hex::decode(sig_hex.trim_start_matches("0x"))
            .map_err(|e| QuoteError::Signature(e.to_string()))?;

        // The relay signs the block *without* `sig`, and `serde_json`'s map
        // is a `BTreeMap`, so re-serializing after removing that one field
        // reproduces the signed bytes exactly.
        let mut unsigned = payment.clone();
        unsigned
            .as_object_mut()
            .ok_or(QuoteError::Field("payment"))?
            .remove("sig");
        let payload = unsigned.to_string();
        let node_eth_address =
            crate::signer::recover_eth_address_from_eip191(payload.as_bytes(), &sig)
                .map_err(|e| QuoteError::Signature(e.to_string()))?;

        let addr = |k: &'static str| -> Result<[u8; 20], QuoteError> {
            let s = payment.get(k).and_then(|x| x.as_str()).ok_or(QuoteError::Field(k))?;
            let raw = hex::decode(s.trim_start_matches("0x")).map_err(|_| QuoteError::Field(k))?;
            <[u8; 20]>::try_from(raw.as_slice()).map_err(|_| QuoteError::Field(k))
        };
        let plur = |k: &'static str| -> Result<u128, QuoteError> {
            payment
                .get(k)
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse().ok())
                .ok_or(QuoteError::Field(k))
        };

        let beneficiary = addr("beneficiary")?;
        let claimed_node = addr("node_eth_address")?;
        if claimed_node != node_eth_address {
            return Err(QuoteError::WrongSigner {
                got: hex::encode(node_eth_address),
                want: hex::encode(claimed_node),
            });
        }
        let overlay_nonce = {
            let s = payment
                .get("overlay_nonce")
                .and_then(|x| x.as_str())
                .ok_or(QuoteError::Field("overlay_nonce"))?;
            let raw =
                hex::decode(s.trim_start_matches("0x")).map_err(|_| QuoteError::Field("overlay_nonce"))?;
            <[u8; 32]>::try_from(raw.as_slice()).map_err(|_| QuoteError::Field("overlay_nonce"))?
        };

        // Pinning is the actual root of trust (§2): the lane URL over HTTPS
        // plus an identity the client already knew.
        if let Some(pin) = pin {
            if pin.node_eth_address != node_eth_address {
                return Err(QuoteError::WrongSigner {
                    got: hex::encode(node_eth_address),
                    want: hex::encode(pin.node_eth_address),
                });
            }
            if pin.beneficiary != beneficiary {
                return Err(QuoteError::WrongBeneficiary {
                    got: hex::encode(beneficiary),
                    want: hex::encode(pin.beneficiary),
                });
            }
        }

        if let Some(overlay) = advertised_overlay {
            let derived = crate::signer::derive_overlay(&node_eth_address, network_id, &overlay_nonce);
            if &derived != overlay {
                return Err(QuoteError::OverlayMismatch);
            }
        }

        let params = Params {
            price_plur_per_kib: plur("price_plur_per_kib")?,
            min_cheque_plur: plur("min_cheque_plur")?,
            settle_every_plur: plur("settle_every_plur")?,
            max_outstanding_plur: plur("max_outstanding_plur")?,
            credit_ratio: payment
                .get("credit_ratio")
                .and_then(|x| x.as_u64())
                .map(u128::from)
                .ok_or(QuoteError::Field("credit_ratio"))?,
        };
        // A lane whose parameters violate §10.1's invariant would brick this
        // client, so refuse it here rather than discovering it at the first
        // 402 with no cheque able to clear it.
        params.validate().map_err(QuoteError::BadParams)?;
        if params.price_plur_per_kib > price_ceiling_plur_per_kib {
            return Err(QuoteError::TooExpensive {
                got: params.price_plur_per_kib,
                ceiling: price_ceiling_plur_per_kib,
            });
        }

        Ok(Self {
            beneficiary,
            node_eth_address,
            overlay_nonce,
            origin: payment
                .get("origin")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            chain_id: payment
                .get("chain_id")
                .and_then(|x| x.as_u64())
                .ok_or(QuoteError::Field("chain_id"))?,
            params,
            hard_enforcement: payment.get("enforcement").and_then(|x| x.as_str()) == Some("hard"),
        })
    }
}

/// A challenge the relay issued, ready to sign.
#[derive(Debug, Clone)]
pub struct OfferedChallenge {
    pub nonce: [u8; 32],
    pub account: [u8; 20],
    pub batch: [u8; 32],
    pub origin: String,
    pub expiry_unix: u64,
    pub cap_plur: u128,
}

impl OfferedChallenge {
    pub fn parse(v: &serde_json::Value) -> Result<Self, String> {
        let fixed = |k: &str, n: usize| -> Result<Vec<u8>, String> {
            let s = v
                .get(k)
                .and_then(|x| x.as_str())
                .ok_or_else(|| format!("challenge: missing {k}"))?;
            let raw = hex::decode(s.trim_start_matches("0x"))
                .map_err(|e| format!("challenge {k}: {e}"))?;
            if raw.len() != n {
                return Err(format!("challenge {k}: want {n} bytes, got {}", raw.len()));
            }
            Ok(raw)
        };
        let mut nonce = [0u8; 32];
        nonce.copy_from_slice(&fixed("nonce", 32)?);
        let mut account = [0u8; 20];
        account.copy_from_slice(&fixed("account", 20)?);
        let mut batch = [0u8; 32];
        batch.copy_from_slice(&fixed("batch", 32)?);
        Ok(Self {
            nonce,
            account,
            batch,
            origin: v
                .get("origin")
                .and_then(|x| x.as_str())
                .ok_or("challenge: missing origin")?
                .to_string(),
            expiry_unix: v
                .get("expiry")
                .and_then(|x| x.as_u64())
                .ok_or("challenge: missing expiry")?,
            cap_plur: v
                .get("max_outstanding_plur")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse().ok())
                .ok_or("challenge: missing max_outstanding_plur")?,
        })
    }

    /// Sign it and produce the header value.
    ///
    /// Signing binds `origin`, which is what makes this header useless at
    /// any other relay even if it is observed in flight (§11.1).
    pub fn sign(
        &self,
        signer: &crate::signer::SwarmSigner,
        chain_id: u64,
    ) -> Result<String, String> {
        let sol = crate::signer::PushChallenge {
            nonce: alloy_primitives::B256::from(self.nonce),
            origin: self.origin.clone(),
            account: alloy_primitives::Address::from(self.account),
            batchId: alloy_primitives::B256::from(self.batch),
            expiry: alloy_primitives::U256::from(self.expiry_unix),
        };
        let sig = signer
            .sign_push_challenge(&sol, chain_id)
            .map_err(|e| e.to_string())?;
        let issued = crate::metered::IssuedChallenge {
            fields: crate::challenge::ChallengeFields {
                account: self.account,
                batch: self.batch,
                origin: self.origin.clone(),
                expiry_unix: self.expiry_unix,
                cap_plur: self.cap_plur,
            },
            nonce: self.nonce,
        };
        Ok(crate::metered::encode_challenge_header(&issued, &sig))
    }

    /// Re-fetch before this, rather than racing the expiry with a POST in
    /// flight. A challenge is cheap; a mid-upload 401 is not.
    pub fn stale_after(&self) -> u64 {
        self.expiry_unix.saturating_sub(30)
    }
}

/// Per-lane running total, tracked by the client from bytes it sent.
#[derive(Debug, Clone)]
pub struct LaneAccount {
    pub params: Params,
    pub beneficiary: [u8; 20],
    /// Billed and not yet covered by a cheque.
    owed_plur: u128,
    /// Dispatched but not yet answered. Mirrors the relay's `reserved`:
    /// bytes on the wire are not yet a debt, because the relay bills what
    /// it *admits*, and a POST it refuses (402) or drops costs nothing.
    /// Counting these as owed made the client sign cheques for several
    /// times what the relay had booked.
    pending_plur: u128,
    /// Total already promised to this beneficiary. Cheques are cumulative,
    /// so this only grows.
    cumulative_plur: u128,
}

impl LaneAccount {
    pub fn new(params: Params, beneficiary: [u8; 20]) -> Self {
        Self {
            params,
            beneficiary,
            owed_plur: 0,
            pending_plur: 0,
            cumulative_plur: 0,
        }
    }

    /// Restore the cumulative from the on-disk store, so a second CLI run
    /// does not issue a cheque the relay rejects as non-increasing.
    pub fn with_cumulative(mut self, cumulative_plur: u128) -> Self {
        self.cumulative_plur = cumulative_plur;
        self
    }

    pub fn owed(&self) -> u128 {
        self.owed_plur
    }

    /// Owed plus in-flight — the client's mirror of the relay's
    /// `owed + reserved`, and what the credit line actually binds on.
    pub fn outstanding(&self) -> u128 {
        self.owed_plur.saturating_add(self.pending_plur)
    }

    pub fn cumulative(&self) -> u128 {
        self.cumulative_plur
    }

    /// A POST is on the wire. Held as *pending*, not owed — see
    /// [`Self::pending_plur`].
    pub fn record_sent(&mut self, body_bytes: u64) {
        self.pending_plur = self
            .pending_plur
            .saturating_add(self.params.price_bytes(body_bytes));
    }

    /// A POST came back. `reached_relay` is false only when the relay
    /// refused it at admission (402) — before reading a byte, so neither
    /// side bills it. Any other outcome, *including a broken stream*, means
    /// the body arrived and the relay billed it, so we must too.
    ///
    /// This mirrors the relay's reserve→commit exactly, which is what keeps
    /// the two sides' arithmetic identical without them exchanging totals.
    pub fn record_answered(&mut self, body_bytes: u64, reached_relay: bool) {
        let price = self.params.price_bytes(body_bytes);
        self.pending_plur = self.pending_plur.saturating_sub(price);
        if reached_relay {
            self.owed_plur = self.owed_plur.saturating_add(price);
        }
    }

    /// A dedup hit costs nothing, so give it back when the ack says so
    /// (§8.2). The relay's claim only ever lowers the bill, so believing it
    /// is safe.
    pub fn refund_dedup(&mut self, body_bytes: u64) {
        self.owed_plur = self
            .owed_plur
            .saturating_sub(self.params.price_bytes(body_bytes));
    }

    pub fn should_settle(&self) -> bool {
        self.owed_plur >= self.params.settle_every_plur
    }

    /// The cumulative for the next cheque, or `None` when what is owed is
    /// still under the lane's dust floor and would be refused.
    pub fn next_cumulative(&self) -> Option<u128> {
        if self.owed_plur < self.params.min_cheque_plur {
            return None;
        }
        Some(self.cumulative_plur.saturating_add(self.owed_plur))
    }

    /// Drop debt the relay will not accept. See the caller in
    /// [`LanePayer::settle`] — this is a divergence artifact, not a
    /// discount, and it can only ever move in the client's favour by
    /// removing an obligation the counterparty has already disclaimed.
    pub fn forgive_phantom_debt(&mut self) {
        self.owed_plur = 0;
    }

    /// Call once a cheque for `cumulative` has been accepted.
    pub fn settled(&mut self, cumulative: u128) {
        let credited = cumulative.saturating_sub(self.cumulative_plur);
        self.cumulative_plur = cumulative;
        self.owed_plur = self.owed_plur.saturating_sub(credited);
    }

    /// Largest body this lane will admit right now, in bytes.
    ///
    /// The client sizes its POST to fit rather than discovering the ceiling
    /// as a 402 — which matters most for exactly the small batches §10.3
    /// exists to keep, whose whole credit line is under one full POST.
    pub fn max_body_bytes(&self, cap_plur: u128) -> u64 {
        let headroom = cap_plur.saturating_sub(self.owed_plur);
        let kib = headroom / self.params.price_plur_per_kib.max(1);
        (kib.saturating_mul(1024)).min(u64::MAX as u128) as u64
    }
}

/// Aggregate exposure across every beneficiary drawn on one chequebook.
///
/// Cumulative payouts are per `(chequebook, beneficiary)`, so N lanes are N
/// independent claims on **one** balance. Without this a cheque to the
/// second lane silently exceeds it and bounces — and §11.3's Sybil case is
/// exactly one operator presenting several beneficiaries.
#[derive(Debug, Default, Clone)]
pub struct TotalIssued {
    per_beneficiary: std::collections::BTreeMap<[u8; 20], u128>,
}

impl TotalIssued {
    pub fn total(&self) -> u128 {
        self.per_beneficiary.values().copied().sum()
    }

    pub fn issued_to(&self, beneficiary: &[u8; 20]) -> u128 {
        self.per_beneficiary.get(beneficiary).copied().unwrap_or(0)
    }

    /// Would raising this beneficiary's cumulative to `cumulative` push the
    /// total past the chequebook's balance?
    pub fn would_exceed(&self, beneficiary: &[u8; 20], cumulative: u128, balance: u128) -> bool {
        let others = self.total() - self.issued_to(beneficiary);
        others.saturating_add(cumulative) > balance
    }

    pub fn record(&mut self, beneficiary: [u8; 20], cumulative: u128) {
        let e = self.per_beneficiary.entry(beneficiary).or_insert(0);
        // Cumulatives only grow; a lower value is a stale report, not a
        // refund.
        if cumulative > *e {
            *e = cumulative;
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// The payment loop (docs/pusher-incentives.md §12)
// ──────────────────────────────────────────────────────────────────────

/// Everything the client needs to pay a metered lane.
///
/// The account key is the **batch owner's** signer — the same key that
/// stamps chunks (§6) — so a metered upload needs no extra credential and,
/// in a browser, no wallet prompt.
#[cfg(not(target_arch = "wasm32"))]
pub struct PaymentConfig {
    pub signer: crate::signer::SwarmSigner,
    pub batch: [u8; 32],
    pub chequebook: [u8; 20],
    pub chain_id: u64,
    /// Shared across lanes: N beneficiaries are N claims on **one** balance
    /// (§8.3), so the cumulative store has to be common.
    pub cheques: std::sync::Arc<std::sync::Mutex<crate::cheques::ChequeStore>>,
    /// On-chain liquid balance of the chequebook, read once at startup. A
    /// cheque that would push total issuance past this is not signed — it
    /// would be accepted and then fail at cashout, which looks like the
    /// relay's fault and costs the lane's trust rather than ours.
    pub balance_plur: u128,
}

/// Per-lane payment state: the verified quote, a cached capability, and the
/// running total.
#[cfg(not(target_arch = "wasm32"))]
pub struct LanePayer {
    pub base_url: String,
    pub quote: PaymentQuote,
    pub account: LaneAccount,
    header: Option<String>,
    header_stale_after: u64,
    cap_plur: u128,
}

#[cfg(not(target_arch = "wasm32"))]
impl LanePayer {
    pub fn new(base_url: String, quote: PaymentQuote, cumulative: u128) -> Self {
        let account = LaneAccount::new(quote.params, quote.beneficiary).with_cumulative(cumulative);
        Self {
            base_url,
            quote,
            account,
            header: None,
            header_stale_after: 0,
            cap_plur: 0,
        }
    }

    /// The credit line the relay last told us about, or 0 before the first
    /// challenge. Used to size POSTs (§7.2).
    pub fn cap_plur(&self) -> u128 {
        self.cap_plur
    }

    /// A valid challenge header, fetching and signing one if needed.
    ///
    /// Re-fetched 30 s before expiry rather than on failure: racing the
    /// expiry with a POST already in flight turns a cheap GET into a
    /// mid-upload 401.
    pub async fn header(
        &mut self,
        http: &reqwest::Client,
        cfg: &PaymentConfig,
    ) -> Result<&str, String> {
        let now = crate::challenge::now_unix();
        if self.header.is_none() || now >= self.header_stale_after {
            let account = *cfg.signer.eth_address();
            let url = format!(
                "{}/v1/challenge?account=0x{}&batch=0x{}",
                self.base_url.trim_end_matches('/'),
                hex::encode(account),
                hex::encode(cfg.batch),
            );
            let resp = http
                .get(&url)
                .timeout(std::time::Duration::from_secs(60))
                .send()
                .await
                .map_err(|e| format!("challenge fetch: {e}"))?;
            if !resp.status().is_success() {
                let code = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(format!("challenge {code}: {}", body.trim()));
            }
            let v: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("challenge json: {e}"))?;
            let offered = OfferedChallenge::parse(&v)?;
            self.cap_plur = offered.cap_plur;
            self.header_stale_after = offered.stale_after();
            self.header = Some(offered.sign(&cfg.signer, self.quote.chain_id)?);
        }
        Ok(self.header.as_deref().unwrap_or_default())
    }

    /// Largest POST body this lane will currently admit.
    pub fn max_body_bytes(&self) -> u64 {
        if self.cap_plur == 0 {
            return u64::MAX;
        }
        self.account.max_body_bytes(self.cap_plur)
    }

    /// Frames per POST this lane can ever afford, ignoring current debt.
    ///
    /// This is what the scheduler needs: a body larger than the *whole*
    /// credit line can never be admitted no matter how promptly we settle,
    /// so it must never be built. Computed against a full frame and then
    /// walked down until it genuinely fits, because the relay bills a
    /// KiB-rounded body and an off-by-one here is an unfixable 402 loop.
    pub fn max_frames(&self) -> usize {
        if self.cap_plur == 0 {
            return usize::MAX;
        }
        let frame = crate::pushframe::MAX_FRAME_LEN as u128;
        let mut n = (self.cap_plur / self.quote.params.price_plur_per_kib)
            .saturating_mul(1024)
            .checked_div(frame)
            .unwrap_or(0)
            .min(usize::MAX as u128) as usize;
        while n > 1 && self.quote.params.price_bytes(n as u64 * frame as u64) > self.cap_plur {
            n -= 1;
        }
        n.max(1)
    }

    /// Is there room for a POST of any useful size right now?
    ///
    /// Checked *before* asking the scheduler for work: taking an assignment
    /// and handing it back costs the chunks a retry attempt each time, so a
    /// tight credit line would exhaust their budget and fail the upload
    /// rather than merely pausing it.
    pub fn has_headroom(&self) -> bool {
        if self.cap_plur == 0 {
            return true;
        }
        // One frame is the smallest thing worth dispatching.
        let one_frame = self
            .quote
            .params
            .price_bytes(crate::pushframe::MAX_FRAME_LEN as u64);
        self.account.outstanding().saturating_add(one_frame) <= self.cap_plur
    }

    /// Would dispatching `body_bytes` right now exceed the credit line?
    /// The caller settles first if so, which is what keeps an upload from
    /// ever reaching its cap rather than recovering from it.
    pub fn would_exceed(&self, body_bytes: u64) -> bool {
        self.cap_plur > 0
            && self
                .account
                .outstanding()
                .saturating_add(self.quote.params.price_bytes(body_bytes))
                > self.cap_plur
    }

    /// Settle if there is enough owed to be worth a cheque.
    ///
    /// Returns the amount accepted, or `None` when nothing was owed above
    /// the lane's dust floor. Errors are the caller's cue to stop using the
    /// lane, not to retry blindly — a rejected cheque usually means the two
    /// sides disagree about the cumulative, which retrying cannot fix.
    pub async fn settle(
        &mut self,
        http: &reqwest::Client,
        cfg: &PaymentConfig,
    ) -> Result<Option<u128>, String> {
        let Some(cumulative) = self.account.next_cumulative() else {
            return Ok(None);
        };
        // Aggregate exposure across every beneficiary drawn on this one
        // chequebook (§8.3): the second lane's cheque is what silently
        // bounces without this.
        let key = crate::cheques::relay_key(&self.quote.beneficiary);
        {
            let store = cfg.cheques.lock().expect("cheque store poisoned");
            if store.would_exceed_balance(&key, cumulative, cfg.balance_plur) {
                return Err(format!(
                    "cheque for {cumulative} would push total issuance past the chequebook's \
                     {} balance across all lanes",
                    cfg.balance_plur
                ));
            }
        }
        let sig = cfg
            .signer
            .sign_cheque(
                &cfg.chequebook,
                &self.quote.beneficiary,
                alloy_primitives::U256::from(cumulative),
                self.quote.chain_id,
            )
            .map_err(|e| format!("sign cheque: {e}"))?;
        let body = crate::protocols::swap::encode_signed_cheque_json_pub(
            &cfg.chequebook,
            &self.quote.beneficiary,
            alloy_primitives::U256::from(cumulative),
            &sig,
        );
        let header = self.header(http, cfg).await?.to_string();
        let resp = http
            .post(format!("{}/v1/pay", self.base_url.trim_end_matches('/')))
            .header(crate::metered::CHALLENGE_HEADER, header)
            .header("content-type", "application/json")
            .body(body)
            .timeout(std::time::Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| format!("pay: {e}"))?;
        if !resp.status().is_success() {
            let code = resp.status();
            let text = resp.text().await.unwrap_or_default();
            // The relay's ledger is authoritative for what it will accept.
            // If it says nothing is owed, our extra is an artifact — bytes
            // we charged ourselves for a POST whose completion we never
            // saw, and which the relay therefore never billed. Carrying it
            // forever would only eat our own headroom, since a cheque for
            // it is refused every time.
            if text.contains("nothing owed") {
                self.account.forgive_phantom_debt();
                return Ok(None);
            }
            return Err(format!("pay {code}: {}", text.trim()));
        }
        // Record the cumulative *before* trusting the reply: we have
        // certainly issued it, and under-recording is what causes the next
        // cheque to be rejected as non-increasing.
        {
            let mut store = cfg.cheques.lock().expect("cheque store poisoned");
            store
                .set_cumulative(&key, cumulative)
                .map_err(|e| format!("cheque store: {e}"))?;
            let _ = store.save();
        }
        self.account.settled(cumulative);
        Ok(Some(cumulative))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signer::SwarmSigner;

    const KEY: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    const NONCE: [u8; 32] = [0u8; 32];

    fn node() -> SwarmSigner {
        SwarmSigner::from_hex_with_nonce(KEY, &format!("0x{}", hex::encode(NONCE)), 1).expect("key")
    }

    /// Build a quote exactly the way the relay does, so the test exercises
    /// the real signed bytes rather than a hand-rolled approximation.
    fn quote_json(beneficiary: [u8; 20]) -> serde_json::Value {
        let n = node();
        let p = Params::default();
        let mut body = serde_json::json!({
            "mode": "metered",
            "enforcement": "soft",
            "beneficiary": format!("0x{}", hex::encode(beneficiary)),
            "node_eth_address": format!("0x{}", hex::encode(n.eth_address())),
            "overlay_nonce": format!("0x{}", hex::encode(NONCE)),
            "origin": "relay-a.example",
            "chain_id": 100,
            "factory": format!("0x{}", hex::encode([0u8; 20])),
            "price_plur_per_kib": p.price_plur_per_kib.to_string(),
            "min_cheque_plur": p.min_cheque_plur.to_string(),
            "settle_every_plur": p.settle_every_plur.to_string(),
            "max_outstanding_plur": p.max_outstanding_plur.to_string(),
            "credit_ratio": p.credit_ratio as u64,
            "challenge_ttl_secs": 300,
        });
        let sig = n.sign_eip191(body.to_string().as_bytes()).expect("sign");
        body["sig"] = serde_json::Value::String(format!("0x{}", hex::encode(sig)));
        body
    }

    fn ceiling() -> u128 {
        Params::default().price_plur_per_kib * 4
    }

    #[test]
    fn a_signed_quote_verifies_and_yields_the_signing_identity() {
        let q = PaymentQuote::verify(&quote_json([3u8; 20]), None, 1, None, ceiling())
            .expect("must verify");
        assert_eq!(q.node_eth_address, *node().eth_address());
        assert_eq!(q.beneficiary, [3u8; 20]);
        assert_eq!(q.params, Params::default());
        assert!(!q.hard_enforcement);
    }

    /// The reason the pin is on the address and the nonce is published:
    /// the client can now check the lane's overlay claim instead of taking
    /// it on faith.
    #[test]
    fn the_advertised_overlay_must_derive_from_the_signed_identity() {
        let good = crate::signer::derive_overlay(node().eth_address(), 1, &NONCE);
        PaymentQuote::verify(&quote_json([3u8; 20]), Some(&good), 1, None, ceiling())
            .expect("a derivable overlay verifies");
        assert_eq!(
            PaymentQuote::verify(&quote_json([3u8; 20]), Some(&[9u8; 32]), 1, None, ceiling()),
            Err(QuoteError::OverlayMismatch)
        );
    }

    /// Tampering with any signed field must break the signature — otherwise
    /// a lane could serve one price and bill another.
    #[test]
    fn tampering_with_the_quote_breaks_it() {
        for field in ["price_plur_per_kib", "beneficiary", "origin", "chain_id"] {
            let mut q = quote_json([3u8; 20]);
            q[field] = match field {
                "price_plur_per_kib" => serde_json::json!("1"),
                "beneficiary" => serde_json::json!(format!("0x{}", hex::encode([0xAAu8; 20]))),
                "origin" => serde_json::json!("evil.example"),
                _ => serde_json::json!(1u64),
            };
            assert!(
                PaymentQuote::verify(&q, None, 1, None, ceiling()).is_err(),
                "tampering with {field} must be caught"
            );
        }
    }

    /// The root of trust: an identity the client already knew, not one the
    /// lane asserts about itself.
    #[test]
    fn a_quote_from_an_unpinned_identity_is_refused() {
        let pin = LanePin {
            node_eth_address: [0xEE; 20],
            beneficiary: [3u8; 20],
        };
        let e = PaymentQuote::verify(&quote_json([3u8; 20]), None, 1, Some(&pin), ceiling())
            .expect_err("must refuse");
        assert!(matches!(e, QuoteError::WrongSigner { .. }), "got {e:?}");
    }

    /// §11.3: a correctly-signed relay advertising someone else's
    /// beneficiary must not be paid.
    #[test]
    fn a_quote_with_an_unpinned_beneficiary_is_refused() {
        let pin = LanePin {
            node_eth_address: *node().eth_address(),
            beneficiary: [3u8; 20],
        };
        let e = PaymentQuote::verify(&quote_json([0xBB; 20]), None, 1, Some(&pin), ceiling())
            .expect_err("must refuse");
        assert!(matches!(e, QuoteError::WrongBeneficiary { .. }), "got {e:?}");
    }

    #[test]
    fn an_overpriced_or_bricking_lane_is_refused() {
        let e = PaymentQuote::verify(&quote_json([3u8; 20]), None, 1, None, 1)
            .expect_err("price ceiling");
        assert!(matches!(e, QuoteError::TooExpensive { .. }), "got {e:?}");

        // A lane whose dust floor exceeds its settlement window would brick
        // this client with no cheque able to clear the 402.
        let n = node();
        let mut body = quote_json([3u8; 20]);
        body["min_cheque_plur"] =
            serde_json::json!((Params::default().settle_every_plur * 87).to_string());
        let mut unsigned = body.clone();
        unsigned.as_object_mut().unwrap().remove("sig");
        let sig = n.sign_eip191(unsigned.to_string().as_bytes()).expect("sign");
        body["sig"] = serde_json::Value::String(format!("0x{}", hex::encode(sig)));
        let e = PaymentQuote::verify(&body, None, 1, None, ceiling()).expect_err("bricking lane");
        assert!(matches!(e, QuoteError::BadParams(_)), "got {e:?}");
    }

    #[test]
    fn a_challenge_round_trips_into_a_header_the_relay_accepts() {
        use crate::ledger::Ledger;
        use crate::metered::{MeterConfig, Metered};
        let acct_signer = node();
        let account = *acct_signer.eth_address();
        let m = Metered::new(
            MeterConfig {
                origins: vec!["relay-a.example".into()],
                beneficiary: [3u8; 20],
                chain_id: 100,
                factory: alloy_primitives::Address::ZERO,
                params: Params::default(),
                hard_mode: false,
            },
            Ledger::ephemeral(),
        );
        let issued = m
            .issue(account, [7u8; 32], 6_200_000_000_000_000_000, "relay-a.example", 1000)
            .expect("issue");
        // Straight through the wire form the relay actually serves.
        let offered = OfferedChallenge::parse(&issued.to_json()).expect("parse");
        let header = offered.sign(&acct_signer, 100).expect("sign");
        let v = m.verify_header(&header, 1000).expect("relay must accept");
        assert_eq!(v.account, account);
        assert_eq!(v.batch, [7u8; 32]);
    }

    #[test]
    fn owed_tracks_bytes_sent_and_a_cheque_clears_it() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        let body = 32 * 1024 * 1024;
        a.record_sent(body);
        a.record_answered(body, true);
        assert_eq!(a.owed(), p.price_bytes(body));
        assert!(a.should_settle(), "32 MiB crosses the settlement window");
        let c = a.next_cumulative().expect("above the dust floor");
        a.settled(c);
        assert_eq!(a.owed(), 0);
        assert_eq!(a.cumulative(), c);
    }

    /// Cheques are cumulative, so a second upload adds to the same running
    /// total rather than starting over — which is what a relay's
    /// monotonicity check requires.
    #[test]
    fn a_second_upload_grows_the_same_cumulative() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        a.record_sent(40 * 1024 * 1024);
        a.record_answered(40 * 1024 * 1024, true);
        let first = a.next_cumulative().expect("cheque");
        a.settled(first);
        a.record_sent(40 * 1024 * 1024);
        a.record_answered(40 * 1024 * 1024, true);
        let second = a.next_cumulative().expect("cheque");
        assert!(second > first, "cumulative must increase: {second} > {first}");
        assert_eq!(second - first, p.price_bytes(40 * 1024 * 1024));
    }

    #[test]
    fn a_cumulative_restored_from_disk_keeps_increasing() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]).with_cumulative(5_000_000_000_000_000);
        a.record_sent(40 * 1024 * 1024);
        a.record_answered(40 * 1024 * 1024, true);
        let c = a.next_cumulative().expect("cheque");
        assert!(
            c > 5_000_000_000_000_000,
            "a fresh run must not re-issue below what a previous run already sent"
        );
    }

    #[test]
    fn dust_owings_do_not_produce_a_cheque_the_relay_would_refuse() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        a.record_sent(1024);
        assert!(!a.should_settle());
        assert_eq!(a.next_cumulative(), None, "below the lane's dust floor");
    }

    /// The divergence a live run found: recording debt at dispatch made the
    /// client sign cheques for several times what the relay had booked,
    /// because a 402'd POST is never billed on the relay side.
    /// A POST whose response broke still cost the relay the bytes it read,
    /// so it must still be billed — otherwise the client silently
    /// under-pays for every interrupted stream (§7.3).
    #[test]
    fn an_interrupted_post_is_still_billed() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        let body = 64 * 1024;
        a.record_sent(body);
        a.record_answered(body, true); // stream broke, but the body arrived
        assert_eq!(a.owed(), p.price_bytes(body));
    }

    #[test]
    fn a_refused_post_never_becomes_debt() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        a.record_sent(100 * 4251);
        assert_eq!(a.owed(), 0, "in flight is not yet owed");
        assert!(a.outstanding() > 0, "but it does count against the cap");
        a.record_answered(100 * 4251, false); // 402
        assert_eq!(a.owed(), 0, "a refused POST is never billed");
        assert_eq!(a.outstanding(), 0, "and stops holding headroom");
    }

    #[test]
    fn an_accepted_post_becomes_debt_exactly_once() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        let body = 100 * 4251;
        a.record_sent(body);
        a.record_answered(body, true);
        assert_eq!(a.owed(), p.price_bytes(body));
        assert_eq!(a.outstanding(), a.owed(), "nothing left in flight");
    }

    /// Several POSTs dispatch before any completes; the cap must see their
    /// sum, or the client blows through it and 402s on the tail.
    #[test]
    fn concurrent_posts_all_count_against_the_cap() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        for _ in 0..8 {
            a.record_sent(64 * 1024);
        }
        assert_eq!(a.outstanding(), p.price_bytes(64 * 1024) * 8);
        assert_eq!(a.owed(), 0);
    }

    /// A client can charge itself for a POST whose completion it never
    /// saw; the relay never billed it, so it refuses the cheque. Carrying
    /// that debt forever would slowly eat the client's own credit line.
    #[test]
    fn phantom_debt_can_be_dropped() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        a.record_sent(4251);
        a.record_answered(4251, true);
        assert!(a.owed() > 0);
        a.forgive_phantom_debt();
        assert_eq!(a.owed(), 0);
        assert_eq!(a.outstanding(), 0, "and it stops holding headroom");
    }

    #[test]
    fn dedup_hits_are_refunded() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        a.record_sent(100 * 4251);
        a.record_answered(100 * 4251, true);
        let before = a.owed();
        a.refund_dedup(10 * 4251);
        assert!(a.owed() < before);
    }

    /// §7.2: size the POST to the credit line instead of discovering the
    /// ceiling as a 402. A dust batch gets a small but usable body.
    #[test]
    fn post_size_is_bounded_by_the_credit_line() {
        let p = Params::default();
        let a = LaneAccount::new(p, [3u8; 20]);
        let dust_cap = p.credit_line(100_000_000_000_000);
        let max = a.max_body_bytes(dust_cap);
        assert_eq!(max, 208 * 1024, "~208 KiB, matching the credit line");
        assert!(max >= 4251, "and still enough for at least one frame");
        // A rich batch is bounded by the global ceiling instead.
        let rich = a.max_body_bytes(p.max_outstanding_plur);
        assert!(rich > 512 * 4251, "a full POST fits comfortably");
    }

    #[test]
    fn headroom_shrinks_as_debt_accrues() {
        let p = Params::default();
        let mut a = LaneAccount::new(p, [3u8; 20]);
        let cap = p.max_outstanding_plur;
        let before = a.max_body_bytes(cap);
        a.record_sent(64 * 1024 * 1024);
        a.record_answered(64 * 1024 * 1024, true);
        assert!(a.max_body_bytes(cap) < before, "unpaid debt eats the line");
    }

    /// N lanes are N claims on one balance. The client must see the sum, or
    /// the second cheque bounces.
    #[test]
    fn total_issued_aggregates_across_beneficiaries() {
        let mut t = TotalIssued::default();
        t.record([1u8; 20], 600);
        t.record([2u8; 20], 300);
        assert_eq!(t.total(), 900);
        assert!(!t.would_exceed(&[1u8; 20], 700, 1000), "700 + 300 fits");
        assert!(t.would_exceed(&[1u8; 20], 800, 1000), "800 + 300 does not");
        assert!(
            !t.would_exceed(&[3u8; 20], 100, 1000),
            "a new beneficiary is checked against the others' total"
        );
    }

    #[test]
    fn a_stale_cumulative_report_never_lowers_the_total() {
        let mut t = TotalIssued::default();
        t.record([1u8; 20], 600);
        t.record([1u8; 20], 100);
        assert_eq!(t.total(), 600, "cumulatives only grow");
    }
}
