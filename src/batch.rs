//! Postage batch creation against the on-chain `PostageStamp` contract.
//!
//! Unlike every other module in this crate, `batch::create` makes
//! real on-chain RPC calls. The rest of isheika stays RPC-free —
//! this is the one exception, fenced off behind a dedicated CLI
//! subcommand (`isheika batch create`) so it doesn't contaminate
//! the upload / fetch / daemon paths.
//!
//! ## Flow
//!
//! Per bee's `postagecontract.CreateBatch`
//! (`~/Coding/forks/bee/pkg/postage/postagecontract/contract.go`):
//!
//! 1. Read `lastPrice` and `minimumValidityBlocks` from the
//!    `PostageStamp` contract → compute the minimum
//!    `initialBalancePerChunk` required for the 24h-validity rule.
//! 2. Read the signer's BZZ balance, verify it covers
//!    `initialBalancePerChunk * 2^depth`.
//! 3. `BZZ.approve(PostageStamp, initialBalancePerChunk * 2^depth)`.
//! 4. `PostageStamp.createBatch(owner, initialBalancePerChunk,
//!    depth, bucketDepth=16, nonce, immutable)` with a random nonce.
//! 5. Parse `BatchCreated(batchId, totalAmount, normalisedBalance,
//!    owner, depth, bucketDepth, immutableFlag)` event from the
//!    receipt logs → emit `batchId`.
//!
//! `batchId` could also be computed client-side as
//! `keccak256(abi.encode(signer, nonce))` (see `PostageStamp.sol`),
//! but parsing the event is the bee-canonical path and surfaces any
//! revert / reordering at the same time.
//!
//! ## Why hand-rolled, not alloy-provider
//!
//! `alloy-provider` would pull `alloy-consensus`, `alloy-trie`,
//! `c-kzg`, and the full transport stack — ~20 transitive crates
//! for one signed transaction. We already have `alloy-signer-local`
//! (for cheques) and `alloy-sol-types` (for ABI encoding); adding
//! `alloy-rlp` + reqwest JSON-RPC keeps the dep delta to one crate.
//!
//! ## Transaction shape
//!
//! Legacy EIP-155 transactions only (type-0). Gnosis chain (the
//! Swarm mainnet) supports EIP-1559 but legacy is universally
//! accepted, simpler to RLP-encode by hand, and tx fees are
//! negligible there regardless. The single-shot nature of batch
//! creation doesn't need fee optimization.

use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{sol, SolCall, SolEvent};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// Swarm-on-Gnosis mainnet `PostageStamp` contract address.
/// From bee's `go-storage-incentives-abi` v0.9.4.
pub const MAINNET_POSTAGE_STAMP: &str = "0x45a1502382541Cd610CC9068e88727426b696293";

/// Swarm-on-Gnosis mainnet BZZ ERC-20 token address.
pub const MAINNET_BZZ_TOKEN: &str = "0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da";

/// Gnosis chain ID.
pub const MAINNET_CHAIN_ID: u64 = 100;

/// Bucket depth `PostageStamp` requires. Hard-coded by bee as a
/// global constant (`postagecontract.BucketDepth = 16`) — the
/// contract rejects anything below `minimumBucketDepth` (also 16
/// on mainnet at deploy time) and anything `>= depth`.
pub const BUCKET_DEPTH: u8 = 16;

sol! {
    // PostageStamp.createBatch(address,uint256,uint8,uint8,bytes32,bool)
    function createBatch(
        address owner,
        uint256 initialBalancePerChunk,
        uint8 depth,
        uint8 bucketDepth,
        bytes32 nonce,
        bool immutable_
    ) external returns (bytes32);

    // PostageStamp.lastPrice() returns (uint64)
    function lastPrice() external view returns (uint64);

    // PostageStamp.minimumValidityBlocks() returns (uint64)
    function minimumValidityBlocks() external view returns (uint64);

    // ERC20.balanceOf(address)
    function balanceOf(address account) external view returns (uint256);

    // ERC20.allowance(address,address)
    function allowance(address owner, address spender) external view returns (uint256);

    // ERC20.approve(address,uint256)
    function approve(address spender, uint256 amount) external returns (bool);

    // event BatchCreated(
    //     bytes32 indexed batchId,
    //     uint256 totalAmount,
    //     uint256 normalisedBalance,
    //     address owner,
    //     uint8 depth,
    //     uint8 bucketDepth,
    //     bool immutableFlag
    // )
    event BatchCreated(
        bytes32 indexed batchId,
        uint256 totalAmount,
        uint256 normalisedBalance,
        address owner,
        uint8 depth,
        uint8 bucketDepth,
        bool immutableFlag
    );
}

#[derive(Debug, Error)]
pub enum BatchError {
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("rpc transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("transaction reverted (status=0) — check approve/balance/depth")]
    Reverted,
    #[error("insufficient BZZ balance: have {have} PLUR, need {need} PLUR")]
    InsufficientBalance { have: U256, need: U256 },
    #[error("initial balance per chunk too low for 24h validity: have {have}, need >= {need}")]
    InsufficientValidity { have: U256, need: U256 },
    #[error("invalid depth: must be > bucket depth ({BUCKET_DEPTH}), got {0}")]
    InvalidDepth(u8),
    #[error("BatchCreated event not found in receipt logs")]
    NoBatchEvent,
    #[error("receipt not found within timeout")]
    ReceiptTimeout,
    #[error("abi decode: {0}")]
    AbiDecode(String),
}

/// Inputs for `create_batch`.
#[derive(Debug, Clone)]
pub struct CreateBatchParams {
    /// JSON-RPC endpoint (e.g. `https://rpc.gnosischain.com`).
    pub rpc_url: String,
    /// Owner of the resulting batch. Defaults to the signer's
    /// address when invoked from the CLI.
    pub owner: Address,
    /// PostageStamp contract address (mainnet default elsewhere).
    pub postage_stamp: Address,
    /// BZZ ERC-20 token address (mainnet default elsewhere).
    pub bzz_token: Address,
    /// Per-chunk initial balance, in BZZ-PLUR (1 BZZ = 10^16 PLUR).
    /// Must be `>= lastPrice * minimumValidityBlocks` or the contract
    /// reverts with `InsufficientBalance`. The total BZZ pulled from
    /// the signer is `initial_balance_per_chunk * 2^depth`.
    pub initial_balance_per_chunk: U256,
    /// Batch depth. Stamp count = `2^depth`. Must be `> BUCKET_DEPTH` (16).
    pub depth: u8,
    /// Immutable batch flag. Mutable batches can be diluted/topped up;
    /// immutable batches cannot.
    pub immutable: bool,
    /// EIP-155 chain id (Gnosis mainnet = 100).
    pub chain_id: u64,
    /// Receipt polling timeout. Gnosis blocks are ~5s, so 120s covers
    /// >20 blocks of headroom.
    pub receipt_timeout: Duration,
}

impl CreateBatchParams {
    pub fn mainnet(rpc_url: String, signer_addr: Address) -> Self {
        Self {
            rpc_url,
            owner: signer_addr,
            postage_stamp: MAINNET_POSTAGE_STAMP.parse().expect("hardcoded valid"),
            bzz_token: MAINNET_BZZ_TOKEN.parse().expect("hardcoded valid"),
            initial_balance_per_chunk: U256::ZERO, // caller fills
            depth: 20,
            immutable: false,
            chain_id: MAINNET_CHAIN_ID,
            receipt_timeout: Duration::from_secs(120),
        }
    }
}

/// Result of a successful `create_batch` call.
#[derive(Debug, Clone)]
pub struct BatchCreatedInfo {
    pub batch_id: B256,
    pub total_amount: U256,
    pub normalised_balance: U256,
    pub owner: Address,
    pub depth: u8,
    pub bucket_depth: u8,
    pub immutable: bool,
    pub create_tx: B256,
    pub approve_tx: B256,
}

/// Run the full bee-style createBatch flow: read price/validity →
/// sanity-check balance → approve → createBatch → parse event.
pub async fn create_batch(
    signer: &PrivateKeySigner,
    params: CreateBatchParams,
) -> Result<BatchCreatedInfo, BatchError> {
    if params.depth <= BUCKET_DEPTH {
        return Err(BatchError::InvalidDepth(params.depth));
    }

    let rpc = EthRpc::new(params.rpc_url.clone());
    let from = signer.address();

    // Read on-chain state to compute / validate amounts.
    let last_price: u64 = rpc
        .call_view::<lastPriceCall, _>(params.postage_stamp, lastPriceCall {})
        .await?;
    let min_validity: u64 = rpc
        .call_view::<minimumValidityBlocksCall, _>(
            params.postage_stamp,
            minimumValidityBlocksCall {},
        )
        .await?;
    let min_initial =
        U256::from(last_price as u128).saturating_mul(U256::from(min_validity as u128));

    if params.initial_balance_per_chunk <= min_initial {
        return Err(BatchError::InsufficientValidity {
            have: params.initial_balance_per_chunk,
            need: min_initial,
        });
    }

    // total = initial * 2^depth (overflow check via U256 mul).
    let total = params
        .initial_balance_per_chunk
        .checked_mul(U256::from(1u128 << params.depth))
        .ok_or_else(|| BatchError::Rpc("total amount overflow".into()))?;

    let balance: U256 = rpc
        .call_view::<balanceOfCall, _>(
            params.bzz_token,
            balanceOfCall { account: from },
        )
        .await?;
    if balance < total {
        return Err(BatchError::InsufficientBalance {
            have: balance,
            need: total,
        });
    }

    // Approve. Skip if existing allowance already covers it.
    let current_allowance: U256 = rpc
        .call_view::<allowanceCall, _>(
            params.bzz_token,
            allowanceCall {
                owner: from,
                spender: params.postage_stamp,
            },
        )
        .await?;
    let approve_tx = if current_allowance >= total {
        // Already approved — return the zero hash to signal skipped.
        B256::ZERO
    } else {
        let approve_call = approveCall {
            spender: params.postage_stamp,
            amount: total,
        }
        .abi_encode();
        rpc.send_signed(signer, params.chain_id, params.bzz_token, &approve_call)
            .await?
    };
    if approve_tx != B256::ZERO {
        rpc.wait_for_success(approve_tx, params.receipt_timeout)
            .await?;
    }

    // createBatch with a random 32-byte nonce.
    let mut nonce_bytes = [0u8; 32];
    getrandom::fill(&mut nonce_bytes).map_err(|e| BatchError::Rpc(format!("getrandom: {e}")))?;
    let nonce = B256::from(nonce_bytes);

    let create_call = createBatchCall {
        owner: params.owner,
        initialBalancePerChunk: params.initial_balance_per_chunk,
        depth: params.depth,
        bucketDepth: BUCKET_DEPTH,
        nonce,
        immutable_: params.immutable,
    }
    .abi_encode();

    let create_tx = rpc
        .send_signed(signer, params.chain_id, params.postage_stamp, &create_call)
        .await?;
    let receipt = rpc
        .wait_for_success(create_tx, params.receipt_timeout)
        .await?;

    // Parse BatchCreated event from logs. The event signature topic
    // is keccak256("BatchCreated(bytes32,uint256,uint256,address,uint8,uint8,bool)").
    let topic = BatchCreated::SIGNATURE_HASH;
    for log in &receipt.logs {
        if log.address.parse::<Address>().ok() != Some(params.postage_stamp) {
            continue;
        }
        if log.topics.is_empty() || log.topics[0].parse::<B256>().ok() != Some(topic) {
            continue;
        }
        // Decode the event using alloy-sol-types
        let topics: Vec<B256> = log
            .topics
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        let data = hex::decode(log.data.trim_start_matches("0x"))
            .map_err(|e| BatchError::AbiDecode(format!("log data hex: {e}")))?;
        let decoded = BatchCreated::decode_raw_log(topics.iter().copied(), &data)
            .map_err(|e| BatchError::AbiDecode(format!("BatchCreated: {e}")))?;
        return Ok(BatchCreatedInfo {
            batch_id: decoded.batchId,
            total_amount: decoded.totalAmount,
            normalised_balance: decoded.normalisedBalance,
            owner: decoded.owner,
            depth: decoded.depth,
            bucket_depth: decoded.bucketDepth,
            immutable: decoded.immutableFlag,
            create_tx,
            approve_tx,
        });
    }
    Err(BatchError::NoBatchEvent)
}

// ──────────────────────────────────────────────────────────────────────
// Minimal JSON-RPC + tx signing
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
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    #[allow(dead_code)]
    id: Option<u64>,
    result: Option<R>,
    error: Option<RpcErr>,
}

#[derive(Debug, Deserialize)]
struct RpcErr {
    code: i64,
    message: String,
}

#[derive(Debug, Serialize)]
struct CallObj {
    from: String,
    to: String,
    data: String,
}

#[derive(Debug, Deserialize)]
struct ReceiptResp {
    status: String,
    #[serde(rename = "transactionHash")]
    #[allow(dead_code)]
    tx_hash: String,
    logs: Vec<RpcLog>,
}

#[derive(Debug, Deserialize)]
struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
}

struct EthRpc {
    url: String,
    http: reqwest::Client,
}

impl EthRpc {
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
    ) -> Result<R, BatchError> {
        let body = RpcReq {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let resp: RpcResp<R> = self.http.post(&self.url).json(&body).send().await?.json().await?;
        if let Some(e) = resp.error {
            return Err(BatchError::Rpc(format!("{}: {} (code {})", method, e.message, e.code)));
        }
        resp.result.ok_or_else(|| BatchError::Rpc(format!("{method}: empty result")))
    }

    /// `eth_call`-based view call. Returns the decoded sol return type.
    async fn call_view<C, T>(&self, to: Address, call: C) -> Result<C::Return, BatchError>
    where
        C: SolCall<Return = T>,
    {
        let data = format!("0x{}", hex::encode(call.abi_encode()));
        let result_hex: String = self
            .raw(
                "eth_call",
                (
                    CallObj {
                        from: format!("0x{}", hex::encode(Address::ZERO)),
                        to: format!("0x{}", hex::encode(to)),
                        data,
                    },
                    "latest",
                ),
            )
            .await?;
        let bytes = hex::decode(result_hex.trim_start_matches("0x"))?;
        C::abi_decode_returns(&bytes).map_err(|e| BatchError::AbiDecode(e.to_string()))
    }

    async fn nonce(&self, addr: Address) -> Result<u64, BatchError> {
        let hex_str: String = self
            .raw(
                "eth_getTransactionCount",
                (format!("0x{}", hex::encode(addr)), "pending"),
            )
            .await?;
        u64::from_str_radix(hex_str.trim_start_matches("0x"), 16)
            .map_err(|e| BatchError::Rpc(format!("nonce parse: {e}")))
    }

    async fn gas_price(&self) -> Result<U256, BatchError> {
        let hex_str: String = self.raw("eth_gasPrice", Vec::<()>::new()).await?;
        let bytes = hex::decode(format!("{:0>64}", hex_str.trim_start_matches("0x")))?;
        Ok(U256::from_be_slice(&bytes))
    }

    async fn estimate_gas(
        &self,
        from: Address,
        to: Address,
        data: &[u8],
    ) -> Result<u64, BatchError> {
        let hex_str: String = self
            .raw(
                "eth_estimateGas",
                [CallObj {
                    from: format!("0x{}", hex::encode(from)),
                    to: format!("0x{}", hex::encode(to)),
                    data: format!("0x{}", hex::encode(data)),
                }],
            )
            .await?;
        u64::from_str_radix(hex_str.trim_start_matches("0x"), 16)
            .map_err(|e| BatchError::Rpc(format!("gas parse: {e}")))
    }

    async fn send_signed(
        &self,
        signer: &PrivateKeySigner,
        chain_id: u64,
        to: Address,
        data: &[u8],
    ) -> Result<B256, BatchError> {
        let from = signer.address();
        let nonce = self.nonce(from).await?;
        let gas_price = self.gas_price().await?;
        // Bump gas estimate by 25% — `createBatch` calls into the
        // ordered-tree library which has variable cost depending on
        // tree depth.
        let gas = self.estimate_gas(from, to, data).await? * 125 / 100;
        let raw = sign_legacy_tx(signer, chain_id, nonce, gas_price, gas, to, data)?;
        let hex_str: String = self
            .raw(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(&raw))],
            )
            .await?;
        Ok(hex_str.parse().map_err(|e| BatchError::Rpc(format!("tx hash parse: {e}")))?)
    }

    async fn wait_for_success(
        &self,
        tx_hash: B256,
        timeout: Duration,
    ) -> Result<ReceiptResp, BatchError> {
        let start = std::time::Instant::now();
        loop {
            let r: Option<ReceiptResp> = self
                .raw(
                    "eth_getTransactionReceipt",
                    [format!("0x{}", hex::encode(tx_hash))],
                )
                .await?;
            if let Some(rcpt) = r {
                let ok = u64::from_str_radix(rcpt.status.trim_start_matches("0x"), 16)
                    .unwrap_or(0)
                    == 1;
                if !ok {
                    return Err(BatchError::Reverted);
                }
                return Ok(rcpt);
            }
            if start.elapsed() > timeout {
                return Err(BatchError::ReceiptTimeout);
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Legacy EIP-155 transaction signing
// ──────────────────────────────────────────────────────────────────────

/// Build a signed legacy (type-0) transaction per EIP-155.
/// Returns the RLP-encoded raw bytes ready for `eth_sendRawTransaction`.
fn sign_legacy_tx(
    signer: &PrivateKeySigner,
    chain_id: u64,
    nonce: u64,
    gas_price: U256,
    gas_limit: u64,
    to: Address,
    data: &[u8],
) -> Result<Vec<u8>, BatchError> {
    // EIP-155 signing hash: keccak256(rlp([nonce, gasPrice, gasLimit,
    //                                       to, value, data, chainId, 0, 0]))
    let mut sighash_payload = Vec::new();
    encode_legacy_fields(
        &mut sighash_payload,
        nonce,
        gas_price,
        gas_limit,
        to,
        U256::ZERO,
        data,
        Some(chain_id),
    );
    let sighash = keccak256(&sighash_payload);

    let sig = signer
        .sign_hash_sync(&sighash)
        .map_err(|e| BatchError::Rpc(format!("sign: {e}")))?;

    // EIP-155: v = chain_id * 2 + 35 + recovery_id
    let v: u64 = chain_id * 2 + 35 + (sig.v() as u64);
    let r = sig.r();
    let s = sig.s();

    let mut out = Vec::new();
    encode_legacy_signed(
        &mut out,
        nonce,
        gas_price,
        gas_limit,
        to,
        U256::ZERO,
        data,
        v,
        r,
        s,
    );
    Ok(out)
}

/// Encode the 9 RLP fields of a legacy tx envelope.
/// When `chain_id` is `Some`, this is the EIP-155 sighash preimage
/// (v=chain_id, r=0, s=0). When `None`, callers must use
/// `encode_legacy_signed` instead.
fn encode_legacy_fields(
    out: &mut Vec<u8>,
    nonce: u64,
    gas_price: U256,
    gas_limit: u64,
    to: Address,
    value: U256,
    data: &[u8],
    chain_id: Option<u64>,
) {
    let mut payload = Vec::new();
    nonce.encode(&mut payload);
    gas_price.encode(&mut payload);
    gas_limit.encode(&mut payload);
    to.encode(&mut payload);
    value.encode(&mut payload);
    data.encode(&mut payload);
    if let Some(cid) = chain_id {
        cid.encode(&mut payload);
        0u64.encode(&mut payload);
        0u64.encode(&mut payload);
    }
    let header = alloy_rlp::Header {
        list: true,
        payload_length: payload.len(),
    };
    header.encode(out);
    out.extend_from_slice(&payload);
}

fn encode_legacy_signed(
    out: &mut Vec<u8>,
    nonce: u64,
    gas_price: U256,
    gas_limit: u64,
    to: Address,
    value: U256,
    data: &[u8],
    v: u64,
    r: U256,
    s: U256,
) {
    let mut payload = Vec::new();
    nonce.encode(&mut payload);
    gas_price.encode(&mut payload);
    gas_limit.encode(&mut payload);
    to.encode(&mut payload);
    value.encode(&mut payload);
    data.encode(&mut payload);
    v.encode(&mut payload);
    r.encode(&mut payload);
    s.encode(&mut payload);
    let header = alloy_rlp::Header {
        list: true,
        payload_length: payload.len(),
    };
    header.encode(out);
    out.extend_from_slice(&payload);
}
