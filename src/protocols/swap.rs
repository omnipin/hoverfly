//! Bee SWAP protocol — `/swarm/swap/1.0.0/swap`.
//!
//! Wire-compatible with bee's `pkg/settlement/swap/swapprotocol`. Two
//! exchanges over libp2p-stream:
//!
//! 1. **Per-connection handshake** (bee triggers via `ConnectIn`/`ConnectOut`
//!    in `swapprotocol.go::Protocol()`). We open a `swap` substream
//!    and send `Handshake { Beneficiary: our_eth_address }`. Bee stores
//!    that as our beneficiary so it can later validate cheques drawn
//!    against our chequebook. We send empty headers first per the
//!    bee p2p protocol framework convention (`headers.go::sendHeaders`).
//!
//! 2. **Per-cheque emit** (we initiate via the `EmitCheque` stream
//!    every time accrued PLUR-debt warrants a monetary settlement).
//!    Sequence:
//!      a. Open new substream on the same connection.
//!      b. Write our empty `Headers`.
//!      c. Read bee's `Headers` — these carry `exchange` (PLUR-per-BZZ
//!         rate) and `deduction` keys, sourced from bee's on-chain
//!         priceoracle poll (see `swapprotocol.go::headler` at line 148).
//!      d. Write `EmitCheque { Cheque: json(SignedCheque) }`.
//!      e. No response. Bee closes the stream on success or resets it
//!         on validation failure.
//!
//! Bee's `ReceiveCheque` (`chequestore.go::ReceiveCheque`) performs an
//! on-chain `chequebook.issuer()` + `balance()` + `paidOut(beneficiary)`
//! triplet of RPC calls for every cheque, so the perceived latency of
//! a single cheque can be hundreds of ms. The integration in
//! `transport.rs` runs cheque emission off the dispatch path so it
//! doesn't block in-flight pushes.

use crate::proto::headers as hdr;
use crate::proto::swap as pb;
use crate::protocols::framing::{read_message, write_message, FrameError};
use alloy_primitives::U256;
use thiserror::Error;

pub const PROTOCOL: &str = "/swarm/swap/1.0.0/swap";

/// Per-stream header key names — must match bee verbatim
/// (`pkg/settlement/swap/headers/utilities.go`).
const HDR_EXCHANGE: &str = "exchange";
const HDR_DEDUCTION: &str = "deduction";

#[derive(Debug, Error)]
pub enum SwapError {
    #[error("frame: {0}")]
    Frame(#[from] FrameError),
    #[error("missing exchange header — bee priceoracle not yet warm or peer is non-paying")]
    NoExchangeRate,
    #[error("missing deduction header")]
    NoDeduction,
    #[error("json encode: {0}")]
    Json(String),
    #[error("amount overflows u256")]
    Overflow,
}

/// Decoded swap-stream headers.
#[derive(Debug, Clone)]
pub struct SettlementRates {
    /// PLUR-per-BZZ-wei exchange rate, set by bee's on-chain priceoracle.
    /// To convert a PLUR amount to BZZ-wei: `bzz = plur * exchange + deduction`.
    /// (Bee uses the same formula on the receive side in
    /// `chequestore.go:139` after subtracting deduction.)
    pub exchange_rate: U256,
    /// Per-cheque additive deduction (BZZ-wei). Usually 0 unless a peer
    /// is on the new-peer ramp.
    pub deduction: U256,
}

/// Hand-encoded JSON for `chequebook.SignedCheque` because Go's
/// `encoding/json` emits `*big.Int` as a **bare JSON number**, not a
/// string — and `CumulativePayout` is u256, which serde-json's number
/// type can't hold without losing precision. We bypass serde and
/// produce the exact bytes Go would emit, byte-for-byte, so bee's
/// `json.Unmarshal(req.Cheque, &signedCheque)` round-trips.
///
/// Field names are PascalCase to match the Go struct tag defaults
/// for `chequebook.SignedCheque` and its embedded `Cheque`. Go's
/// `Address.MarshalJSON` quotes the hex (with 0x prefix); `[]byte`
/// (the Signature field) is JSON-encoded as base64 standard encoding
/// by `encoding/json`.
fn encode_signed_cheque_json(
    chequebook: &[u8; 20],
    beneficiary: &[u8; 20],
    cumulative_payout: U256,
    signature: &[u8; 65],
) -> Vec<u8> {
    use base64::Engine;
    let cb_hex = hex::encode(chequebook);
    let bn_hex = hex::encode(beneficiary);
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(signature);
    let cumulative = cumulative_payout.to_string();
    // Go's `common.Address.MarshalJSON` emits the EIP-55 checksummed
    // form, but bee's decoder accepts any-case hex via `common.HexToAddress`
    // (and `UnmarshalJSON` on `common.Address` is case-insensitive).
    // We emit lowercase for simplicity; signature recovery is on the
    // typed-data hash, not on this JSON, so casing is irrelevant to
    // signature validity.
    format!(
        "{{\"Chequebook\":\"0x{cb_hex}\",\"Beneficiary\":\"0x{bn_hex}\",\
         \"CumulativePayout\":{cumulative},\"Signature\":\"{sig_b64}\"}}"
    )
    .into_bytes()
}

/// Outbound `Handshake { Beneficiary }`. Called once per session by
/// the connection-setup path. Caller exchanges empty headers first.
pub async fn send_handshake<S>(stream: &mut S, beneficiary: &[u8; 20]) -> Result<(), SwapError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    // Headers framework preamble (bee opens streams expecting this).
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    let _: hdr::Headers = read_message(stream).await?;

    let msg = pb::Handshake {
        beneficiary: beneficiary.to_vec(),
    };
    write_message(stream, &msg).await?;
    Ok(())
}

/// Open the `EmitCheque` exchange: write empty headers, read bee's
/// headers (with exchange rate + deduction), and return them so the
/// caller can compute the BZZ-wei amount for the cheque it's about
/// to sign and send.
pub async fn read_settlement_rates<S>(stream: &mut S) -> Result<SettlementRates, SwapError>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    write_message(stream, &hdr::Headers { headers: vec![] }).await?;
    let resp: hdr::Headers = read_message(stream).await?;

    let mut exchange: Option<U256> = None;
    let mut deduction: Option<U256> = None;
    for h in resp.headers {
        // Bee uses `big.Int.Bytes()` for serialization (big-endian,
        // minimum number of bytes, no leading zeros). U256::from_be_slice
        // accepts variable-length input as long as it's <= 32 bytes.
        if h.key == HDR_EXCHANGE {
            if h.value.len() > 32 {
                return Err(SwapError::Overflow);
            }
            exchange = Some(U256::from_be_slice(&h.value));
        } else if h.key == HDR_DEDUCTION {
            if h.value.len() > 32 {
                return Err(SwapError::Overflow);
            }
            deduction = Some(U256::from_be_slice(&h.value));
        }
    }
    let Some(exchange_rate) = exchange else {
        return Err(SwapError::NoExchangeRate);
    };
    let deduction = deduction.unwrap_or(U256::ZERO);
    Ok(SettlementRates {
        exchange_rate,
        deduction,
    })
}

/// Encode and send the cheque message. Stream is consumed by the
/// caller via drop after this returns (we don't read a response —
/// bee's handler closes on success, resets on failure).
pub async fn emit_cheque<S>(
    stream: &mut S,
    chequebook: &[u8; 20],
    beneficiary: &[u8; 20],
    cumulative_payout: U256,
    signature: &[u8; 65],
) -> Result<(), SwapError>
where
    S: futures::AsyncWrite + Unpin,
{
    let json = encode_signed_cheque_json(chequebook, beneficiary, cumulative_payout, signature);
    let msg = pb::EmitCheque { cheque: json };
    write_message(stream, &msg).await?;
    Ok(())
}
