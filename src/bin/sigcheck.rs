//! Compare isheika's signer output to k256's reference implementation.

use isheika::SwarmSigner;
use sha3::{Digest, Keccak256};

fn main() {
    let key = [0x42u8; 32];
    let network_id = 10u64;

    let signer = SwarmSigner::from_bytes(&key, network_id).unwrap();
    println!("eth_address: 0x{}", hex::encode(signer.eth_address()));
    println!("overlay:     {}", hex::encode(signer.overlay()));
    println!("nonce:       {}", hex::encode(signer.nonce()));
    println!();

    // Build sign payload exactly like bee handshake.
    let underlay_str = format!("/ip4/127.0.0.1/tcp/1634/p2p/12D3KooWDQzJEjMQrA9XJWeKjtuQk1FzfaZbHpQzCQ8gNCwGfH7m");
    let underlay: libp2p::Multiaddr = underlay_str.parse().unwrap();
    let underlay_bytes = underlay.to_vec();

    let sig = signer.sign_handshake(&underlay_bytes).unwrap();
    println!("signature (alloy): {}", hex::encode(sig));

    // Manually reproduce the digest (EIP-191 prefix + keccak).
    let mut payload = Vec::new();
    payload.extend_from_slice(b"bee-handshake-");
    payload.extend_from_slice(&underlay_bytes);
    payload.extend_from_slice(signer.overlay());
    payload.extend_from_slice(&network_id.to_be_bytes());

    let prefix = format!("\x19Ethereum Signed Message:\n{}", payload.len());
    let mut prefixed = prefix.into_bytes();
    prefixed.extend_from_slice(&payload);
    let digest: [u8; 32] = Keccak256::digest(&prefixed).into();
    println!("eip191 digest:     {}", hex::encode(digest));

    // Recover signer pubkey using k256.
    use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
    let mut rs = [0u8; 64];
    rs.copy_from_slice(&sig[..64]);
    let v = sig[64];
    let recovery = if v >= 27 { v - 27 } else { v };
    let recovery_id = RecoveryId::try_from(recovery).unwrap();
    let signature = Signature::from_slice(&rs).unwrap();

    let recovered = VerifyingKey::recover_from_prehash(&digest, &signature, recovery_id).unwrap();
    use k256::elliptic_curve::sec1::ToEncodedPoint;
    let point = recovered.to_encoded_point(false);
    let pubkey_bytes = &point.as_bytes()[1..];
    let eth_recovered = &Keccak256::digest(pubkey_bytes)[12..];
    println!("recovered eth:     0x{}", hex::encode(eth_recovered));
    println!("match: {}", eth_recovered == signer.eth_address());
}
