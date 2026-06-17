//! Reserving-account signer.
//!
//! Supports two on-disk key forms for the same sr25519 account:
//!   * a 32-byte sr25519 **mini-secret** (seed) or a BIP-39 mnemonic — handled by
//!     `subxt-signer` (`Subxt` variant), and
//!   * a 64-byte **Ed25519-expanded** secret key (the polkadot-js / PAPI sr25519
//!     private-key form, e.g. identity-backend's `ATTESTER_PROXY_PRIVATE_KEY`) —
//!     handled here (`Expanded` variant), because `subxt-signer` exposes no public
//!     constructor for it (`Keypair::from_ed25519_bytes` is `pub(crate)` + feature
//!     gated).
//!
//! The signing operation is byte-identical to `subxt-signer`'s: sign over a
//! `schnorrkel::signing_context(b"substrate")` transcript and wrap the 64-byte
//! signature as `MultiSignature::Sr25519`. subxt applies the >256-byte payload
//! hashing before calling `sign`, so this layer signs the bytes verbatim.

use schnorrkel::signing_context;
use subxt::tx::Signer;
use subxt::utils::{AccountId32, MultiSignature};

use crate::asset_hub::AssetHubConfig;

/// Substrate sr25519 signing context — same constant `subxt-signer` uses.
const SIGNING_CTX: &[u8] = b"substrate";

/// A signer for the reserving account.
pub enum ReserverSigner {
    /// 32-byte mini-secret / mnemonic, via `subxt-signer`.
    Subxt(Box<subxt_signer::sr25519::Keypair>),
    /// 64-byte Ed25519-expanded secret, via `schnorrkel` directly.
    Expanded {
        keypair: Box<schnorrkel::Keypair>,
        account: AccountId32,
    },
}

// Account-only Debug — never expose secret key material (and satisfies the Debug
// bound that `Result::unwrap_err` requires on the Ok type in tests).
impl std::fmt::Debug for ReserverSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReserverSigner({})", self.account())
    }
}

impl ReserverSigner {
    /// Build from a `subxt-signer` keypair (32-byte seed / mnemonic path).
    pub fn from_subxt(keypair: subxt_signer::sr25519::Keypair) -> Self {
        ReserverSigner::Subxt(Box::new(keypair))
    }

    /// Build from a 64-byte Ed25519-expanded sr25519 secret key (key ‖ nonce), i.e.
    /// the polkadot-js / PAPI private-key form. Same expansion `schnorrkel` uses for
    /// polkadot-js compatibility, so this derives the identical account.
    pub fn from_ed25519_expanded(bytes: &[u8]) -> Result<Self, String> {
        let secret = schnorrkel::SecretKey::from_ed25519_bytes(bytes)
            .map_err(|e| format!("invalid 64-byte expanded sr25519 secret: {e}"))?;
        let public = secret.to_public();
        let account = AccountId32(public.to_bytes());
        Ok(ReserverSigner::Expanded {
            keypair: Box::new(schnorrkel::Keypair { public, secret }),
            account,
        })
    }

    /// The SS58/AccountId of this signer.
    pub fn account(&self) -> AccountId32 {
        match self {
            ReserverSigner::Subxt(kp) => AccountId32(kp.public_key().0),
            ReserverSigner::Expanded { account, .. } => *account,
        }
    }
}

impl Signer<AssetHubConfig> for ReserverSigner {
    fn account_id(&self) -> AccountId32 {
        self.account()
    }

    fn sign(&self, signer_payload: &[u8]) -> MultiSignature {
        match self {
            // subxt-signer already signs over the b"substrate" context + wraps Sr25519.
            ReserverSigner::Subxt(kp) => MultiSignature::Sr25519(kp.sign(signer_payload).0),
            ReserverSigner::Expanded { keypair, .. } => {
                let sig = keypair.sign(signing_context(SIGNING_CTX).bytes(signer_payload));
                MultiSignature::Sr25519(sig.to_bytes())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schnorrkel::{ExpansionMode, MiniSecretKey};

    /// The 64-byte path must derive the SAME account and produce a VALID signature
    /// for the same underlying key as the 32-byte mini-secret path (which matches
    /// subxt-signer). This is the proof that feeding identity-backend's 64-byte
    /// `ATTESTER_PROXY_PRIVATE_KEY` yields its account (5GjU), not a different one.
    #[test]
    fn expanded_64_matches_mini_secret_32() {
        let seed = [7u8; 32];

        // Reference: subxt-signer from the 32-byte mini-secret.
        let reference = subxt_signer::sr25519::Keypair::from_secret_key(seed).unwrap();
        let ref_pub = reference.public_key().0;

        // The 64-byte Ed25519-expanded form of the same mini-secret.
        let expanded = MiniSecretKey::from_bytes(&seed)
            .unwrap()
            .expand_to_keypair(ExpansionMode::Ed25519)
            .secret
            .to_ed25519_bytes();

        let signer = ReserverSigner::from_ed25519_expanded(&expanded).unwrap();

        // Same account.
        assert_eq!(signer.account().0, ref_pub, "expanded key derived a different account");

        // Same-key signature verifies against the shared public key.
        let msg = b"flow-funder reserve_name payload";
        let MultiSignature::Sr25519(sig_bytes) = <ReserverSigner as Signer<AssetHubConfig>>::sign(&signer, msg)
        else {
            panic!("expected Sr25519 signature");
        };
        let pk = schnorrkel::PublicKey::from_bytes(&ref_pub).unwrap();
        let sig = schnorrkel::Signature::from_bytes(&sig_bytes).unwrap();
        assert!(
            pk.verify_simple(SIGNING_CTX, msg, &sig).is_ok(),
            "signature from the expanded key did not verify"
        );
    }
}
