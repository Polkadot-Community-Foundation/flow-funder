//! Pure helpers for the People-chain → Asset Hub funding flow.
//!
//! Everything here is a pure function — storage-key derivation, account
//! extraction from a storage-key tail, and the fund/skip decision. Kept apart
//! from the I/O modules (`people`, `asset_hub`) so it can be unit-tested over
//! all inputs without a chain connection.

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

/// Storage prefix for the `PeopleLite::LitePeople` map:
/// `twox_128("PeopleLite") ++ twox_128("LitePeople")` — 32 bytes.
pub fn lite_people_prefix() -> [u8; 32] {
    let mut out = [0u8; 32];
    out[0..16].copy_from_slice(&twox_128(b"PeopleLite"));
    out[16..32].copy_from_slice(&twox_128(b"LitePeople"));
    out
}

/// `0x`-prefixed lower-hex of the LitePeople prefix, for the
/// `archive_v1_storageDiff` `items[].key` field.
pub fn lite_people_prefix_hex() -> String {
    format!("0x{}", hex::encode(lite_people_prefix()))
}

/// Extract the 32-byte AccountId from the tail of a storage-key hex string.
///
/// `LitePeople` is a map; with the `Blake2_128Concat` / `Twox64Concat` hashers
/// the raw 32-byte key is appended verbatim after the hash, so the last 32 bytes
/// of the key ARE the account. Returns `None` for a key too short to contain one.
pub fn account_from_hex_tail(key_hex: &str) -> Option<[u8; 32]> {
    let body = key_hex.strip_prefix("0x").unwrap_or(key_hex);
    if body.len() < 64 {
        return None;
    }
    let tail = &body[body.len() - 64..];
    let bytes = hex::decode(tail).ok()?;
    bytes.try_into().ok()
}

/// Read `data.free` (the spendable balance) from a SCALE-encoded
/// `frame_system::AccountInfo`.
///
/// Layout: `nonce: u32, consumers: u32, providers: u32, sufficients: u32,
/// data: AccountData`, and `AccountData` begins with `free: u128`. We decode
/// only up to `free` and ignore the rest, so this stays correct even if
/// `AccountData`'s trailing fields differ across runtimes. Returns `None` if the
/// bytes are too short to contain `free`.
pub fn free_balance_from_account_info(bytes: &[u8]) -> Option<u128> {
    use codec::Decode;
    let mut cursor = bytes;
    u32::decode(&mut cursor).ok()?; // nonce
    u32::decode(&mut cursor).ok()?; // consumers
    u32::decode(&mut cursor).ok()?; // providers
    u32::decode(&mut cursor).ok()?; // sufficients
    u128::decode(&mut cursor).ok() // data.free
}

/// The fund/skip decision: fund only when the account holds strictly less than
/// the target amount.
///
/// This is what makes the bot idempotent across restarts and re-observations —
/// once an account has been funded to (or above) the target, `should_fund`
/// returns `false`, so re-processing the same account is a no-op.
pub fn should_fund(free_balance: u128, target_amount: u128) -> bool {
    free_balance < target_amount
}

/// Amount needed to bring `free_balance` up to `target_amount`.
/// Returns `None` when no funding is needed.
pub fn funding_shortfall(free_balance: u128, target_amount: u128) -> Option<u128> {
    target_amount
        .checked_sub(free_balance)
        .filter(|amount| *amount > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pinned against the value flow-people verified against the live People
    /// chain. If this breaks, either `twox_128` or the pallet/item names drifted.
    #[test]
    fn lite_people_prefix_matches_known_constant() {
        assert_eq!(
            lite_people_prefix_hex(),
            "0x276fc15f94f88f19ef554a8ff4374855e2491c72cc063f0098c08930123488ce"
        );
    }

    #[test]
    fn account_from_hex_tail_extracts_trailing_32_bytes() {
        let account = [0xabu8; 32];
        // prefix(32) ++ hasher(16) ++ account(32) — a realistic Blake2_128Concat key.
        let key = format!(
            "0x{}{}{}",
            "11".repeat(32),
            "22".repeat(16),
            hex::encode(account)
        );
        assert_eq!(account_from_hex_tail(&key), Some(account));
    }

    #[test]
    fn account_from_hex_tail_handles_missing_0x_prefix() {
        let account = [0x07u8; 32];
        let key = format!("{}{}", "33".repeat(16), hex::encode(account));
        assert_eq!(account_from_hex_tail(&key), Some(account));
    }

    #[test]
    fn account_from_hex_tail_rejects_short_key() {
        assert_eq!(account_from_hex_tail("0x1234"), None);
        assert_eq!(account_from_hex_tail(""), None);
    }

    #[test]
    fn account_from_hex_tail_rejects_non_hex_tail() {
        let key = format!("0x{}", "zz".repeat(32));
        assert_eq!(account_from_hex_tail(&key), None);
    }

    #[test]
    fn should_fund_only_below_target() {
        assert!(should_fund(0, 100));
        assert!(should_fund(99, 100));
        assert!(!should_fund(100, 100));
        assert!(!should_fund(101, 100));
    }

    #[test]
    fn funding_shortfall_only_returns_amount_needed_to_reach_target() {
        assert_eq!(funding_shortfall(0, 100), Some(100));
        assert_eq!(funding_shortfall(40, 100), Some(60));
        assert_eq!(funding_shortfall(99, 100), Some(1));
        assert_eq!(funding_shortfall(100, 100), None);
        assert_eq!(funding_shortfall(101, 100), None);
    }

    #[test]
    fn free_balance_reads_data_free_ignoring_trailing_fields() {
        use codec::Encode;
        let mut buf = Vec::new();
        7u32.encode_to(&mut buf); // nonce
        1u32.encode_to(&mut buf); // consumers
        2u32.encode_to(&mut buf); // providers
        0u32.encode_to(&mut buf); // sufficients
        1_234_567_890_u128.encode_to(&mut buf); // data.free
        9u128.encode_to(&mut buf); // data.reserved — ignored
        42u128.encode_to(&mut buf); // data.frozen — ignored
        assert_eq!(free_balance_from_account_info(&buf), Some(1_234_567_890));
    }

    #[test]
    fn free_balance_rejects_truncated_bytes() {
        assert_eq!(free_balance_from_account_info(&[]), None);
        assert_eq!(free_balance_from_account_info(&[0u8; 8]), None); // only 2 u32s
    }
}
