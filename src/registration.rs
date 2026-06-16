//! Pure storage-key helpers for the dotns reservation flow.
//!
//! Everything here is a pure function — Substrate hashers and the
//! `DotnsGateway::LiteLabelOwner` storage-key derivation used for the
//! idempotency check. Kept apart from the I/O modules (`attest`, `asset_hub`) so
//! it can be unit-tested without a chain connection.

use std::hash::Hasher;

use twox_hash::XxHash64;

/// Substrate `twox_128`: `concat(le(xxh64(data, seed=0)), le(xxh64(data, seed=1)))`.
pub fn twox_128(data: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];

    let mut h0 = XxHash64::with_seed(0);
    h0.write(data);
    out[0..8].copy_from_slice(&h0.finish().to_le_bytes());

    let mut h1 = XxHash64::with_seed(1);
    h1.write(data);
    out[8..16].copy_from_slice(&h1.finish().to_le_bytes());

    out
}

/// Substrate `blake2_128` — a 16-byte BLAKE2b digest.
fn blake2_128(data: &[u8]) -> [u8; 16] {
    use blake2::digest::consts::U16;
    use blake2::{Blake2b, Digest};
    let mut hasher: Blake2b<U16> = Blake2b::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Full storage key for `DotnsGateway::LiteLabelOwner(lite_label)` on Asset Hub:
/// `twox_128("DotnsGateway") ++ twox_128("LiteLabelOwner") ++ blake2_128(enc) ++ enc`
/// where `enc = SCALE(lite_label) = Compact(len) ++ lite_label` (the map uses the
/// `Blake2_128Concat` hasher over `BaseLabel`, which encodes identically to a
/// `Vec<u8>`). Unlike the fixed-size `AccountId` keys, `BaseLabel` is
/// variable-length, so the hashed material and concat tail include the
/// compact-length prefix. Used to skip names already reserved (idempotency).
pub fn lite_label_owner_key(lite_label: &[u8]) -> Vec<u8> {
    use codec::Encode;
    // SCALE encoding of BaseLabel(BoundedVec<u8,32>) == Vec<u8>: Compact(len) ++ bytes.
    let encoded = lite_label.to_vec().encode();
    let mut key = Vec::with_capacity(16 + 16 + 16 + encoded.len());
    key.extend_from_slice(&twox_128(b"DotnsGateway"));
    key.extend_from_slice(&twox_128(b"LiteLabelOwner"));
    key.extend_from_slice(&blake2_128(&encoded));
    key.extend_from_slice(&encoded);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lite_label_owner_key_layout_includes_compact_length_prefix() {
        let label = b"alice.11"; // 8 bytes → Compact(8) = 0x20 (8 << 2)
        let key = lite_label_owner_key(label);

        // prefix(16+16) ++ blake2_128(enc)(16) ++ enc(1 compact byte + 8 label bytes)
        assert_eq!(key.len(), 16 + 16 + 16 + 1 + label.len());
        assert_eq!(&key[0..16], &twox_128(b"DotnsGateway"));
        assert_eq!(&key[16..32], &twox_128(b"LiteLabelOwner"));

        let encoded = [&[0x20u8][..], &label[..]].concat();
        assert_eq!(&key[32..48], &blake2_128(&encoded));
        assert_eq!(&key[48..], &encoded[..]);
        // The compact-length prefix MUST be present, or the hashed key would be
        // wrong and the idempotency read would always miss.
        assert_eq!(key[48], 0x20);
    }

    #[test]
    fn lite_label_owner_key_varies_with_label() {
        assert_ne!(
            lite_label_owner_key(b"alice.11"),
            lite_label_owner_key(b"alice.12")
        );
    }
}
