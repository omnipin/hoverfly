//! Cross-chain bridge to xDAI + BZZ on Gnosis via the Relay API.
//!
//! This is the *second* module in the crate that touches a network RPC
//! (the first is [`crate::batch`]). Like `batch`, it's fenced off behind
//! a dedicated CLI subcommand (`hoverfly bridge`) and a cargo feature
//! (`bridge`, on by default) so it never contaminates the RPC-free
//! upload / fetch / daemon paths. Unlike `batch`, it talks to two
//! services: the Relay REST API (quotes + status) and the **origin
//! chain's** JSON-RPC (to broadcast the deposit transaction Relay hands
//! back, and to read native-gas balances).
//!
//! ## Why this exists
//!
//! The README setup flow requires the signer's address to hold a little
//! xDAI (for gas) and some BZZ (for the postage batch) on Gnosis before
//! `hoverfly batch create` will work. Today the user leaves the CLI and
//! uses a web bridge UI to get there. `hoverfly bridge` keeps them in the
//! CLI: pay with a token on Ethereum / Base / Optimism / Arbitrum, receive
//! xDAI and/or BZZ on Gnosis.
//!
//! ## Relay model (permissionless)
//!
//! Relay is an intents/solver network. You `POST /quote/v2` describing the
//! intent (origin chain/token → destination chain/token), it returns a
//! `steps[]` recipe of transactions to sign on the **origin** chain. You
//! broadcast them, then poll `/intents/status/v3?requestId=...` until the
//! solver fills on the destination. No API key is required — one only
//! raises the rate limit. See <https://docs.relay.link>.
//!
//! ## "Both" target (xDAI + BZZ)
//!
//! A single Relay quote delivers *one* destination currency. To land both
//! xDAI and BZZ we use the same pattern as Ethersphere's Beeport app:
//! check the recipient's current xDAI balance on Gnosis, and only if it's
//! below a threshold do a small xDAI top-up swap first, then the BZZ swap.
//! In the common case (recipient already has gas) that's a single swap.
//!
//! ## Transaction shape
//!
//! Relay returns EIP-1559 fee fields (`maxFeePerGas` /
//! `maxPriorityFeePerGas`) for the L2 origins we support, so unlike
//! `batch.rs` (legacy type-0 on Gnosis) this module signs **type-2**
//! transactions. The signing helper lives here rather than in `batch.rs`
//! to keep the feature self-contained.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// Relay API base URL. Permissionless; an API key only raises rate limits.
pub const RELAY_API_BASE: &str = "https://api.relay.link";

/// Gnosis chain id — the Swarm mainnet settlement chain. Bridge
/// destination is always this.
pub const GNOSIS_CHAIN_ID: u64 = 100;

/// Native-token sentinel address used by Relay (and most aggregators) to
/// mean "the chain's native gas token" — ETH on L1/L2s, xDAI on Gnosis.
pub const NATIVE_TOKEN: &str = "0x0000000000000000000000000000000000000000";

/// BZZ ERC-20 on Gnosis (Swarm mainnet). Same address `batch.rs` uses;
/// re-declared here so the `bridge` feature stands alone if `batch` is
/// ever feature-gated separately. BZZ has 16 decimals (1 BZZ = 10^16).
pub const GNOSIS_BZZ_TOKEN: &str = "0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da";

/// Default xDAI balance (in wei, 18 decimals) below which `--to both`
/// performs a gas top-up before the BZZ swap. Mirrors Beeport's
/// `GAS_TOPUP_THRESHOLD_XDAI = 1.0`.
pub const DEFAULT_XDAI_TOPUP_THRESHOLD_WEI: u128 = 1_000_000_000_000_000_000; // 1.0 xDAI

/// Default xDAI amount (wei) to acquire when topping up. ~1 xDAI is
/// plenty for many postage-batch transactions on Gnosis.
pub const DEFAULT_XDAI_TOPUP_AMOUNT_WEI: u128 = 1_000_000_000_000_000_000; // 1.0 xDAI

/// Map a human chain name to its EVM chain id. Accepts the four origins we
/// support plus a bare numeric id (so users can pass any Relay-supported
/// chain by number). Returns `None` on unknown names.
pub fn chain_id_from_name(name: &str) -> Option<u64> {
    match name.trim().to_ascii_lowercase().as_str() {
        "ethereum" | "eth" | "mainnet" | "l1" => Some(1),
        "optimism" | "op" => Some(10),
        "base" => Some(8453),
        "arbitrum" | "arb" | "arbitrum-one" | "arbitrumone" => Some(42161),
        "gnosis" | "xdai" | "gnosischain" => Some(GNOSIS_CHAIN_ID),
        other => other.parse::<u64>().ok(),
    }
}

/// A resolved origin token: its contract address and decimal precision.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedToken {
    pub address: Address,
    pub decimals: u8,
}

/// Resolve `--from-token` to an address + decimals on `origin_chain_id`.
///
/// Three cases:
/// - `None` → the chain's **native** gas token (ETH on L1/L2s).
/// - `Some("0x…")` → a raw 20-byte address. Used verbatim; decimals fall
///   back to `raw_decimals` (the `--from-decimals` flag) since an address
///   alone carries no precision.
/// - `Some("USDC")` (a symbol) → looked up against Relay's `/chains`
///   token list for that chain, yielding both address **and** decimals so
///   the user never has to know either.
///
/// Symbol matching is case-insensitive and checks `featuredTokens` first
/// (curated, canonical), then `erc20Currencies`, then the native currency.
pub async fn resolve_token(
    relay: &RelayClient,
    origin_chain_id: u64,
    from_token: Option<&str>,
    raw_decimals: u8,
) -> Result<ResolvedToken, BridgeError> {
    let native: Address = NATIVE_TOKEN.parse().expect("native sentinel");
    match from_token {
        // Native gas token. Decimals are 18 on every EVM chain we support.
        None => Ok(ResolvedToken {
            address: native,
            decimals: 18,
        }),
        // Raw address: use as-is, decimals from the flag.
        Some(t) if is_hex_address(t) => {
            let bytes = parse_address_20(t)?;
            Ok(ResolvedToken {
                address: Address::from(bytes),
                decimals: raw_decimals,
            })
        }
        // Symbol: resolve against Relay's chain token list.
        Some(sym) => {
            let chain = relay.chain(origin_chain_id).await?;
            chain.find_token(sym).ok_or_else(|| {
                BridgeError::Relay(format!(
                    "token symbol '{sym}' not found on chain {origin_chain_id}; \
                     pass a raw 0x address with --from-token and --from-decimals instead"
                ))
            })
        }
    }
}

/// True when `s` looks like a 20-byte hex address: 40 hex chars, with or
/// without a `0x` prefix. A token symbol is never 40 hex chars, so this is
/// an unambiguous way to tell "raw address" from "symbol".
fn is_hex_address(s: &str) -> bool {
    let t = s.trim();
    let h = t
        .strip_prefix("0x")
        .or_else(|| t.strip_prefix("0X"))
        .unwrap_or(t);
    h.len() == 40 && h.bytes().all(|b| b.is_ascii_hexdigit())
}

fn parse_address_20(s: &str) -> Result<[u8; 20], BridgeError> {
    let trimmed = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    let bytes = hex::decode(trimmed)?;
    if bytes.len() != 20 {
        return Err(BridgeError::Rpc(format!(
            "address must be 20 bytes, got {}",
            bytes.len()
        )));
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// What to acquire on Gnosis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeTarget {
    /// BZZ only.
    Bzz,
    /// Native xDAI only.
    XDai,
    /// Both: conditional xDAI top-up (if recipient is below the
    /// threshold) followed by a BZZ swap.
    Both,
}

impl BridgeTarget {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "bzz" => Ok(Self::Bzz),
            "xdai" => Ok(Self::XDai),
            "both" => Ok(Self::Both),
            other => Err(format!("unknown --to '{other}' (use bzz | xdai | both)")),
        }
    }
}

/// Relay quote trade type.
#[derive(Debug, Clone, Copy)]
pub enum TradeType {
    /// Spend exactly `amount` of the origin currency, accept variable output.
    ExactInput,
    /// Receive exactly `amount` of the destination currency, spend variable input.
    ExactOutput,
}

impl TradeType {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExactInput => "EXACT_INPUT",
            Self::ExactOutput => "EXACT_OUTPUT",
        }
    }
}

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("relay api error: {0}")]
    Relay(String),
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("http transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error(
        "unsupported quote step kind '{0}' — only on-chain transaction steps are supported \
         (signature/permit-based gasless flows aren't). Use a native-gas origin."
    )]
    UnsupportedStep(String),
    #[error("origin transaction reverted (status=0)")]
    Reverted,
    #[error("receipt not found within timeout")]
    ReceiptTimeout,
    #[error("relay fill did not reach success within timeout (last status: {0})")]
    FillTimeout(String),
    #[error("relay fill failed/refunded (status: {0})")]
    FillFailed(String),
    #[error(
        "insufficient native gas on origin chain {chain_id}: balance is zero. \
         Relay covers destination gas but the origin deposit tx needs a little native token."
    )]
    NoOriginGas { chain_id: u64 },
}

/// Inputs for [`bridge`].
#[derive(Debug, Clone)]
pub struct BridgeParams {
    /// Origin EVM chain id (1 / 10 / 8453 / 42161, or any Relay chain).
    pub origin_chain_id: u64,
    /// Origin currency address (`NATIVE_TOKEN` for the native gas token).
    pub origin_currency: Address,
    /// Amount of the origin currency, in its smallest unit. Interpreted
    /// per `trade_type`.
    pub amount: U256,
    /// What to acquire on Gnosis.
    pub target: BridgeTarget,
    /// Recipient on Gnosis (defaults to the signer's address at the CLI).
    pub recipient: Address,
    /// Origin chain JSON-RPC, used to broadcast the deposit tx and read
    /// the origin native-gas balance.
    pub origin_rpc_url: String,
    /// Gnosis JSON-RPC, used for the `--to both` xDAI balance check.
    pub gnosis_rpc_url: String,
    /// Quote trade type.
    pub trade_type: TradeType,
    /// Optional Relay API key (raises rate limit only; not required).
    pub api_key: Option<String>,
    /// `--to both`: top up xDAI only if the recipient's balance is below
    /// this many wei.
    pub xdai_topup_threshold_wei: u128,
    /// `--to both`: how much xDAI (wei) to acquire when topping up.
    pub xdai_topup_amount_wei: u128,
    /// Receipt + fill polling timeout.
    pub timeout: Duration,
}

/// One leg of a completed bridge (a single Relay quote+execute).
#[derive(Debug, Clone)]
pub struct BridgeLeg {
    /// Human label: "xdai-topup" or "bzz" or "xdai".
    pub label: String,
    /// Relay requestId tying the origin deposit to the destination fill.
    pub request_id: String,
    /// Origin tx hashes submitted (approve + deposit, in order).
    pub origin_tx_hashes: Vec<B256>,
    /// Formatted destination output (e.g. "36.748" ) and symbol.
    pub output_formatted: String,
    pub output_symbol: String,
}

/// Result of a successful bridge.
#[derive(Debug, Clone)]
pub struct BridgeOutcome {
    pub recipient: Address,
    pub legs: Vec<BridgeLeg>,
}

/// Execute the bridge. For [`BridgeTarget::Both`] this may run two legs
/// (conditional xDAI top-up, then BZZ); otherwise one.
pub async fn bridge(
    signer: &PrivateKeySigner,
    params: BridgeParams,
) -> Result<BridgeOutcome, BridgeError> {
    let relay = RelayClient::new(params.api_key.clone());
    let origin_rpc = EvmRpc::new(params.origin_rpc_url.clone());

    // Pre-flight: Relay requires a little native gas on the origin to
    // broadcast the deposit tx. Fail early with a clear message rather
    // than deep inside tx submission.
    let origin_native = origin_rpc.native_balance(signer.address()).await?;
    if origin_native.is_zero() {
        return Err(BridgeError::NoOriginGas {
            chain_id: params.origin_chain_id,
        });
    }

    let bzz: Address = GNOSIS_BZZ_TOKEN.parse().expect("hardcoded valid");
    let native: Address = NATIVE_TOKEN.parse().expect("hardcoded valid");
    let mut legs = Vec::new();

    match params.target {
        BridgeTarget::XDai => {
            let leg = run_leg(
                signer,
                &relay,
                &origin_rpc,
                &params,
                "xdai",
                native,
                params.amount,
                params.trade_type,
            )
            .await?;
            legs.push(leg);
        }
        BridgeTarget::Bzz => {
            let leg = run_leg(
                signer,
                &relay,
                &origin_rpc,
                &params,
                "bzz",
                bzz,
                params.amount,
                params.trade_type,
            )
            .await?;
            legs.push(leg);
        }
        BridgeTarget::Both => {
            // 1. Check recipient xDAI on Gnosis; top up only if low.
            let gnosis_rpc = EvmRpc::new(params.gnosis_rpc_url.clone());
            let xdai_balance = gnosis_rpc.native_balance(params.recipient).await?;
            let threshold = U256::from(params.xdai_topup_threshold_wei);
            if xdai_balance < threshold {
                // EXACT_OUTPUT so we land a predictable xDAI amount.
                let leg = run_leg(
                    signer,
                    &relay,
                    &origin_rpc,
                    &params,
                    "xdai-topup",
                    native,
                    U256::from(params.xdai_topup_amount_wei),
                    TradeType::ExactOutput,
                )
                .await?;
                legs.push(leg);
            }
            // 2. BZZ swap with the (remaining) origin amount.
            let leg = run_leg(
                signer,
                &relay,
                &origin_rpc,
                &params,
                "bzz",
                bzz,
                params.amount,
                params.trade_type,
            )
            .await?;
            legs.push(leg);
        }
    }

    Ok(BridgeOutcome {
        recipient: params.recipient,
        legs,
    })
}

/// Quote → execute steps → poll fill, for a single destination currency.
async fn run_leg(
    signer: &PrivateKeySigner,
    relay: &RelayClient,
    origin_rpc: &EvmRpc,
    params: &BridgeParams,
    label: &str,
    destination_currency: Address,
    amount: U256,
    trade_type: TradeType,
) -> Result<BridgeLeg, BridgeError> {
    let quote = relay
        .quote(QuoteRequest {
            user: signer.address(),
            recipient: params.recipient,
            origin_chain_id: params.origin_chain_id,
            destination_chain_id: GNOSIS_CHAIN_ID,
            origin_currency: params.origin_currency,
            destination_currency,
            amount,
            trade_type,
        })
        .await?;

    let request_id = quote
        .steps
        .iter()
        .find_map(|s| s.request_id.clone())
        .ok_or_else(|| BridgeError::Relay("quote returned no requestId".into()))?;

    // Execute each transaction step on the origin chain, in order.
    let mut origin_tx_hashes = Vec::new();
    for step in &quote.steps {
        if step.kind != "transaction" {
            return Err(BridgeError::UnsupportedStep(step.kind.clone()));
        }
        for item in &step.items {
            let tx = &item.data;
            let hash = origin_rpc
                .send_eip1559(signer, params.origin_chain_id, tx)
                .await?;
            origin_rpc.wait_for_success(hash, params.timeout).await?;
            origin_tx_hashes.push(hash);
        }
    }

    // Poll Relay for the destination fill.
    relay.wait_for_fill(&request_id, params.timeout).await?;

    let (output_formatted, output_symbol) = quote
        .details
        .as_ref()
        .and_then(|d| d.currency_out.as_ref())
        .map(|c| {
            (
                c.amount_formatted.clone().unwrap_or_default(),
                c.currency.symbol.clone().unwrap_or_default(),
            )
        })
        .unwrap_or_default();

    Ok(BridgeLeg {
        label: label.to_string(),
        request_id,
        origin_tx_hashes,
        output_formatted,
        output_symbol,
    })
}

// ──────────────────────────────────────────────────────────────────────
// Relay REST client
// ──────────────────────────────────────────────────────────────────────

struct QuoteRequest {
    user: Address,
    recipient: Address,
    origin_chain_id: u64,
    destination_chain_id: u64,
    origin_currency: Address,
    destination_currency: Address,
    amount: U256,
    trade_type: TradeType,
}

/// Thin client over the Relay REST API. Public so [`resolve_token`] can
/// take it; the CLI builds one and reuses it for resolution + the bridge.
pub struct RelayClient {
    http: reqwest::Client,
    api_key: Option<String>,
}

impl RelayClient {
    pub fn new(api_key: Option<String>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            api_key,
        }
    }

    /// Fetch one chain's metadata (token lists) from `/chains`. Used by
    /// [`resolve_token`] to map a symbol → address + decimals.
    async fn chain(&self, chain_id: u64) -> Result<ChainMeta, BridgeError> {
        let resp = self
            .http
            .get(format!("{RELAY_API_BASE}/chains"))
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(BridgeError::Relay(format!("chains HTTP {status}: {text}")));
        }
        let chains: ChainsResponse = serde_json::from_str(&text)?;
        chains
            .chains
            .into_iter()
            .find(|c| c.id == chain_id)
            .ok_or_else(|| BridgeError::Relay(format!("chain {chain_id} not in Relay /chains")))
    }

    async fn quote(&self, req: QuoteRequest) -> Result<QuoteResponse, BridgeError> {
        let body = serde_json::json!({
            "user": fmt_addr(req.user),
            "recipient": fmt_addr(req.recipient),
            "originChainId": req.origin_chain_id,
            "destinationChainId": req.destination_chain_id,
            "originCurrency": fmt_addr(req.origin_currency),
            "destinationCurrency": fmt_addr(req.destination_currency),
            "amount": req.amount.to_string(),
            "tradeType": req.trade_type.as_str(),
        });
        let mut rb = self
            .http
            .post(format!("{RELAY_API_BASE}/quote/v2"))
            .json(&body);
        if let Some(key) = &self.api_key {
            rb = rb.header("Authorization", key);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(BridgeError::Relay(format!("quote HTTP {status}: {text}")));
        }
        serde_json::from_str(&text).map_err(BridgeError::Json)
    }

    /// Poll `/intents/status/v3` until the fill reaches a terminal state.
    async fn wait_for_fill(&self, request_id: &str, timeout: Duration) -> Result<(), BridgeError> {
        let start = std::time::Instant::now();
        let url = format!("{RELAY_API_BASE}/intents/status/v3?requestId={request_id}");
        let mut last = String::from("(none)");
        loop {
            let resp = self.http.get(&url).send().await?;
            if resp.status().is_success() {
                let st: StatusResponse = resp.json().await?;
                last = st.status.clone();
                match st.status.as_str() {
                    "success" => return Ok(()),
                    "failure" | "refund" => return Err(BridgeError::FillFailed(st.status)),
                    // "waiting" | "pending" | "depositing" → keep polling.
                    _ => {}
                }
            }
            if start.elapsed() > timeout {
                return Err(BridgeError::FillTimeout(last));
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

#[derive(Debug, Deserialize)]
struct QuoteResponse {
    steps: Vec<QuoteStep>,
    #[serde(default)]
    details: Option<QuoteDetails>,
}

#[derive(Debug, Deserialize)]
struct QuoteStep {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    items: Vec<QuoteStepItem>,
    #[serde(rename = "requestId", default)]
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QuoteStepItem {
    data: TxData,
}

/// The origin-chain transaction to broadcast. Relay returns EIP-1559 fee
/// fields for the chains we support.
#[derive(Debug, Deserialize)]
struct TxData {
    to: String,
    data: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    gas: Option<String>,
    #[serde(rename = "maxFeePerGas", default)]
    max_fee_per_gas: Option<String>,
    #[serde(rename = "maxPriorityFeePerGas", default)]
    max_priority_fee_per_gas: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QuoteDetails {
    #[serde(rename = "currencyOut", default)]
    currency_out: Option<CurrencyAmount>,
}

#[derive(Debug, Deserialize)]
struct CurrencyAmount {
    #[serde(rename = "amountFormatted", default)]
    amount_formatted: Option<String>,
    #[serde(default)]
    currency: CurrencyMeta,
}

#[derive(Debug, Default, Deserialize)]
struct CurrencyMeta {
    #[serde(default)]
    symbol: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusResponse {
    status: String,
}

// Relay `/chains` — only the bits we need to resolve a token symbol.

#[derive(Debug, Deserialize)]
struct ChainsResponse {
    chains: Vec<ChainMeta>,
}

#[derive(Debug, Deserialize)]
struct ChainMeta {
    id: u64,
    /// Native gas token (ETH / xDAI / …).
    #[serde(default)]
    currency: Option<TokenMeta>,
    /// Curated, canonical tokens — checked first so e.g. "USDC" resolves
    /// to the real USDC, not some lookalike in the long tail.
    #[serde(rename = "featuredTokens", default)]
    featured_tokens: Vec<TokenMeta>,
    /// Full bridgeable ERC-20 list.
    #[serde(rename = "erc20Currencies", default)]
    erc20_currencies: Vec<TokenMeta>,
}

#[derive(Debug, Deserialize)]
struct TokenMeta {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    address: Option<String>,
    #[serde(default)]
    decimals: Option<u8>,
}

impl ChainMeta {
    /// Find a token by case-insensitive symbol: featured tokens first,
    /// then the full ERC-20 list, then the native currency. Returns the
    /// resolved address + decimals.
    fn find_token(&self, symbol: &str) -> Option<ResolvedToken> {
        let want = symbol.trim().to_ascii_uppercase();
        let matches = |t: &TokenMeta| {
            t.symbol
                .as_deref()
                .map(|s| s.eq_ignore_ascii_case(&want))
                .unwrap_or(false)
        };
        self.featured_tokens
            .iter()
            .chain(self.erc20_currencies.iter())
            .chain(self.currency.iter())
            .find(|t| matches(t))
            .and_then(TokenMeta::resolve)
    }
}

impl TokenMeta {
    fn resolve(&self) -> Option<ResolvedToken> {
        let addr = self.address.as_deref()?;
        let bytes = parse_address_20(addr).ok()?;
        Some(ResolvedToken {
            address: Address::from(bytes),
            decimals: self.decimals.unwrap_or(18),
        })
    }
}

// ──────────────────────────────────────────────────────────────────────
// Minimal EVM JSON-RPC (origin-chain broadcast + native balance)
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct RpcReq<'a, P: Serialize> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: P,
}

#[derive(Debug, Deserialize)]
struct RpcResp<R> {
    result: Option<R>,
    error: Option<RpcErr>,
}

#[derive(Debug, Deserialize)]
struct RpcErr {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ReceiptResp {
    status: String,
}

struct EvmRpc {
    url: String,
    http: reqwest::Client,
}

impl EvmRpc {
    fn new(url: String) -> Self {
        Self {
            url,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        }
    }

    async fn raw<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
    ) -> Result<Option<R>, BridgeError> {
        let body = RpcReq {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let resp: RpcResp<R> = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(e) = resp.error {
            return Err(BridgeError::Rpc(format!(
                "{method}: {} (code {})",
                e.message, e.code
            )));
        }
        Ok(resp.result)
    }

    async fn native_balance(&self, addr: Address) -> Result<U256, BridgeError> {
        let hex_str: String = self
            .raw("eth_getBalance", (fmt_addr(addr), "latest"))
            .await?
            .ok_or_else(|| BridgeError::Rpc("eth_getBalance: empty".into()))?;
        let bytes = hex::decode(format!("{:0>64}", hex_str.trim_start_matches("0x")))?;
        Ok(U256::from_be_slice(&bytes))
    }

    async fn nonce(&self, addr: Address) -> Result<u64, BridgeError> {
        let hex_str: String = self
            .raw("eth_getTransactionCount", (fmt_addr(addr), "pending"))
            .await?
            .ok_or_else(|| BridgeError::Rpc("nonce: empty".into()))?;
        u64::from_str_radix(hex_str.trim_start_matches("0x"), 16)
            .map_err(|e| BridgeError::Rpc(format!("nonce parse: {e}")))
    }

    async fn gas_price(&self) -> Result<U256, BridgeError> {
        let hex_str: String = self
            .raw("eth_gasPrice", Vec::<()>::new())
            .await?
            .ok_or_else(|| BridgeError::Rpc("gasPrice: empty".into()))?;
        let bytes = hex::decode(format!("{:0>64}", hex_str.trim_start_matches("0x")))?;
        Ok(U256::from_be_slice(&bytes))
    }

    /// Broadcast a Relay step as a signed EIP-1559 (type-2) transaction.
    /// Falls back to `eth_gasPrice` for the fee fields when Relay omits
    /// them (it usually provides both).
    async fn send_eip1559(
        &self,
        signer: &PrivateKeySigner,
        chain_id: u64,
        tx: &TxData,
    ) -> Result<B256, BridgeError> {
        let from = signer.address();
        let nonce = self.nonce(from).await?;
        let to: Address = tx
            .to
            .parse()
            .map_err(|e| BridgeError::Rpc(format!("step `to` addr: {e}")))?;
        let value = parse_u256_opt(tx.value.as_deref())?;
        let data = hex::decode(tx.data.trim_start_matches("0x"))?;

        // Prefer Relay's fee fields; fall back to gasPrice for both caps.
        let (max_fee, max_prio) = match (
            parse_u256_opt(tx.max_fee_per_gas.as_deref())?,
            parse_u256_opt(tx.max_priority_fee_per_gas.as_deref())?,
        ) {
            (m, p) if !m.is_zero() => (m, p),
            _ => {
                let gp = self.gas_price().await?;
                (gp, gp)
            }
        };

        // Gas limit: trust Relay's estimate (+25% headroom like batch.rs),
        // else estimate.
        let gas = match parse_u256_opt(tx.gas.as_deref())? {
            g if !g.is_zero() => g.to::<u64>() * 125 / 100,
            _ => self.estimate_gas(from, to, value, &data).await?,
        };

        let raw = sign_eip1559_tx(
            signer, chain_id, nonce, max_prio, max_fee, gas, to, value, &data,
        )?;
        let hex_str: String = self
            .raw(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(&raw))],
            )
            .await?
            .ok_or_else(|| BridgeError::Rpc("sendRawTransaction: empty".into()))?;
        hex_str
            .parse()
            .map_err(|e| BridgeError::Rpc(format!("tx hash parse: {e}")))
    }

    async fn estimate_gas(
        &self,
        from: Address,
        to: Address,
        value: U256,
        data: &[u8],
    ) -> Result<u64, BridgeError> {
        let call = serde_json::json!({
            "from": fmt_addr(from),
            "to": fmt_addr(to),
            "value": format!("0x{:x}", value),
            "data": format!("0x{}", hex::encode(data)),
        });
        let hex_str: String = self
            .raw("eth_estimateGas", [call])
            .await?
            .ok_or_else(|| BridgeError::Rpc("estimateGas: empty".into()))?;
        let g = u64::from_str_radix(hex_str.trim_start_matches("0x"), 16)
            .map_err(|e| BridgeError::Rpc(format!("gas parse: {e}")))?;
        Ok(g * 125 / 100)
    }

    async fn wait_for_success(&self, tx: B256, timeout: Duration) -> Result<(), BridgeError> {
        let start = std::time::Instant::now();
        loop {
            let r: Option<ReceiptResp> = self
                .raw(
                    "eth_getTransactionReceipt",
                    [format!("0x{}", hex::encode(tx))],
                )
                .await?;
            if let Some(rcpt) = r {
                let ok =
                    u64::from_str_radix(rcpt.status.trim_start_matches("0x"), 16).unwrap_or(0) == 1;
                return if ok {
                    Ok(())
                } else {
                    Err(BridgeError::Reverted)
                };
            }
            if start.elapsed() > timeout {
                return Err(BridgeError::ReceiptTimeout);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// EIP-1559 (type-2) transaction signing
// ──────────────────────────────────────────────────────────────────────

/// Build a signed EIP-1559 (type-2) transaction. Returns the raw bytes
/// (with the `0x02` type prefix) for `eth_sendRawTransaction`.
///
/// Payload per EIP-1559:
///   0x02 ‖ rlp([chainId, nonce, maxPriorityFeePerGas, maxFeePerGas,
///               gasLimit, to, value, data, accessList])
/// Signing hash is keccak256 of that; the signed envelope appends
/// [yParity, r, s] to the same field list.
#[allow(clippy::too_many_arguments)]
fn sign_eip1559_tx(
    signer: &PrivateKeySigner,
    chain_id: u64,
    nonce: u64,
    max_priority_fee: U256,
    max_fee: U256,
    gas_limit: u64,
    to: Address,
    value: U256,
    data: &[u8],
) -> Result<Vec<u8>, BridgeError> {
    // Unsigned payload for the signing hash.
    let mut unsigned = vec![0x02u8];
    encode_1559_fields(
        &mut unsigned,
        chain_id,
        nonce,
        max_priority_fee,
        max_fee,
        gas_limit,
        to,
        value,
        data,
        None,
    );
    let sighash = keccak256(&unsigned);

    let sig = signer
        .sign_hash_sync(&sighash)
        .map_err(|e| BridgeError::Rpc(format!("sign: {e}")))?;

    // For type-2, the signature's `v` is the y-parity (0/1) directly — no
    // EIP-155 chain-id offset.
    let y_parity = sig.v() as u64;
    let mut signed = vec![0x02u8];
    encode_1559_fields(
        &mut signed,
        chain_id,
        nonce,
        max_priority_fee,
        max_fee,
        gas_limit,
        to,
        value,
        data,
        Some((y_parity, sig.r(), sig.s())),
    );
    Ok(signed)
}

/// Encode the EIP-1559 field list as an RLP list, appended to `out`
/// (which already holds the `0x02` type byte). When `sig` is `Some`,
/// the three signature fields are appended (signed envelope); when
/// `None`, the list ends after `accessList` (signing preimage).
#[allow(clippy::too_many_arguments)]
fn encode_1559_fields(
    out: &mut Vec<u8>,
    chain_id: u64,
    nonce: u64,
    max_priority_fee: U256,
    max_fee: U256,
    gas_limit: u64,
    to: Address,
    value: U256,
    data: &[u8],
    sig: Option<(u64, U256, U256)>,
) {
    let mut payload = Vec::new();
    chain_id.encode(&mut payload);
    nonce.encode(&mut payload);
    max_priority_fee.encode(&mut payload);
    max_fee.encode(&mut payload);
    gas_limit.encode(&mut payload);
    to.encode(&mut payload);
    value.encode(&mut payload);
    data.encode(&mut payload);
    // Empty access list: an RLP empty list (0xc0).
    let access_list_header = alloy_rlp::Header {
        list: true,
        payload_length: 0,
    };
    access_list_header.encode(&mut payload);
    if let Some((y_parity, r, s)) = sig {
        y_parity.encode(&mut payload);
        r.encode(&mut payload);
        s.encode(&mut payload);
    }
    let header = alloy_rlp::Header {
        list: true,
        payload_length: payload.len(),
    };
    header.encode(out);
    out.extend_from_slice(&payload);
}

// ──────────────────────────────────────────────────────────────────────
// helpers
// ──────────────────────────────────────────────────────────────────────

fn fmt_addr(a: Address) -> String {
    format!("0x{}", hex::encode(a))
}

/// Parse an optional decimal-or-hex string into U256. `None`/empty → zero.
fn parse_u256_opt(s: Option<&str>) -> Result<U256, BridgeError> {
    match s {
        None => Ok(U256::ZERO),
        Some(v) if v.is_empty() => Ok(U256::ZERO),
        Some(v) if v.starts_with("0x") || v.starts_with("0X") => U256::from_str_radix(&v[2..], 16)
            .map_err(|e| BridgeError::Rpc(format!("u256 hex: {e}"))),
        Some(v) => U256::from_str_radix(v, 10).map_err(|e| BridgeError::Rpc(format!("u256: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_names() {
        assert_eq!(chain_id_from_name("ethereum"), Some(1));
        assert_eq!(chain_id_from_name("Base"), Some(8453));
        assert_eq!(chain_id_from_name("OP"), Some(10));
        assert_eq!(chain_id_from_name("arbitrum"), Some(42161));
        assert_eq!(chain_id_from_name("gnosis"), Some(100));
        assert_eq!(chain_id_from_name("8453"), Some(8453));
        assert_eq!(chain_id_from_name("not-a-chain"), None);
    }

    #[test]
    fn target_parse() {
        assert_eq!(BridgeTarget::parse("bzz").unwrap(), BridgeTarget::Bzz);
        assert_eq!(BridgeTarget::parse("XDAI").unwrap(), BridgeTarget::XDai);
        assert_eq!(BridgeTarget::parse("Both").unwrap(), BridgeTarget::Both);
        assert!(BridgeTarget::parse("eth").is_err());
    }

    #[test]
    fn u256_opt_parsing() {
        assert_eq!(parse_u256_opt(None).unwrap(), U256::ZERO);
        assert_eq!(parse_u256_opt(Some("")).unwrap(), U256::ZERO);
        assert_eq!(
            parse_u256_opt(Some("1000000")).unwrap(),
            U256::from(1_000_000u64)
        );
        assert_eq!(
            parse_u256_opt(Some("0x0f4240")).unwrap(),
            U256::from(1_000_000u64)
        );
    }

    /// Deserialize a captured live quote response (2 USDC Base → BZZ
    /// Gnosis) to lock the wire shape we depend on.
    #[test]
    fn deserialize_quote_fixture() {
        let json = r#"{
            "steps":[
                {"id":"approve","kind":"transaction","items":[{"data":{
                    "to":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                    "data":"0x095ea7b3","value":"0","chainId":8453,
                    "gas":"73112","maxFeePerGas":"6667368","maxPriorityFeePerGas":"1167368"}}],
                 "requestId":"0x5d65a59b"},
                {"id":"deposit","kind":"transaction","items":[{"data":{
                    "to":"0x4cd00e387622c35bddb9b4c962c136462338bc31",
                    "data":"0xe8017952","value":"0","chainId":8453,
                    "gas":"76442","maxFeePerGas":"6667368","maxPriorityFeePerGas":"1167368"},
                    "check":{"endpoint":"/intents/status/v3?requestId=0x5d65a59b","method":"GET"}}],
                 "requestId":"0x5d65a59b"}
            ],
            "details":{"currencyOut":{"amountFormatted":"36.7487461924704631",
                "currency":{"symbol":"BZZ","decimals":16}}}
        }"#;
        let q: QuoteResponse = serde_json::from_str(json).unwrap();
        assert_eq!(q.steps.len(), 2);
        assert_eq!(q.steps[0].kind, "transaction");
        assert_eq!(
            q.steps[1].items[0].data.to,
            "0x4cd00e387622c35bddb9b4c962c136462338bc31"
        );
        assert_eq!(q.steps[0].request_id.as_deref(), Some("0x5d65a59b"));
        let d = q.details.unwrap();
        let out = d.currency_out.unwrap();
        assert_eq!(out.amount_formatted.as_deref(), Some("36.7487461924704631"));
        assert_eq!(out.currency.symbol.as_deref(), Some("BZZ"));
    }

    #[test]
    fn status_deserialize() {
        let s: StatusResponse =
            serde_json::from_str(r#"{"status":"waiting","quoteCreatedAt":1}"#).unwrap();
        assert_eq!(s.status, "waiting");
    }

    #[test]
    fn hex_address_detection() {
        assert!(is_hex_address("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"));
        assert!(is_hex_address("833589fcd6edb6e08f4c7c32d4f71b54bda02913")); // no 0x
        assert!(!is_hex_address("USDC"));
        assert!(!is_hex_address("0x1234")); // too short
        assert!(!is_hex_address(
            "0xZZ3589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        )); // non-hex
    }

    /// Symbol resolution against a captured `/chains` fixture for Base:
    /// case-insensitive, featured-first, returns address + decimals.
    #[test]
    fn find_token_by_symbol() {
        let json = r#"{
            "id":8453,"name":"base",
            "currency":{"symbol":"ETH","address":"0x0000000000000000000000000000000000000000","decimals":18},
            "featuredTokens":[
                {"symbol":"USDC","address":"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913","decimals":6}
            ],
            "erc20Currencies":[
                {"symbol":"WETH","address":"0x4200000000000000000000000000000000000006","decimals":18},
                {"symbol":"USDC","address":"0xdeadbeef00000000000000000000000000000000","decimals":6}
            ]
        }"#;
        let chain: ChainMeta = serde_json::from_str(json).unwrap();

        // Case-insensitive, and featured USDC wins over the erc20 lookalike.
        let usdc = chain.find_token("usdc").unwrap();
        assert_eq!(
            fmt_addr(usdc.address),
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        );
        assert_eq!(usdc.decimals, 6);

        // erc20-only token resolves too.
        let weth = chain.find_token("WETH").unwrap();
        assert_eq!(weth.decimals, 18);

        // Native currency is reachable by symbol.
        let eth = chain.find_token("eth").unwrap();
        assert_eq!(eth.decimals, 18);

        // Unknown symbol → None.
        assert!(chain.find_token("NOPE").is_none());
    }
}
