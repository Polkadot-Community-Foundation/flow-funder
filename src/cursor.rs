//! Persisted resume cursor — the hash of the last People-chain block whose
//! registrations were ALL fully handled (funded, skipped, or dry-run).
//!
//! On reconnect/restart the watcher resumes its `archive_v1_storageDiff` from
//! this hash instead of the current finalized head, so registrations that landed
//! while the bot was down are not missed. The cursor advances only after a block
//! is fully handled, so a fund failure leaves it pinned and the block is
//! re-processed later (the balance check dedups the accounts already funded).

use std::path::PathBuf;

use subxt::utils::H256;
use tokio::fs;

/// A file-backed `H256` cursor with atomic writes (write-temp-then-rename).
pub struct CursorStore {
    path: PathBuf,
}

impl CursorStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Load the saved cursor, or `None` if the file is absent or empty
    /// (first boot). A malformed file is an error — we must not silently treat a
    /// corrupt cursor as "first boot" and resync from the tip.
    pub async fn load(&self) -> Result<Option<H256>, Box<dyn std::error::Error + Send + Sync>> {
        match fs::read_to_string(&self.path).await {
            Ok(contents) => {
                let trimmed = contents.trim();
                if trimmed.is_empty() {
                    return Ok(None);
                }
                Ok(Some(parse_h256(trimmed)?))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Atomically persist `hash`. Writes a sibling temp file and renames it over
    /// the target, so a crash mid-write can never leave a half-written cursor.
    pub async fn save(&self, hash: H256) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, format!("{hash:?}")).await?;
        fs::rename(&tmp, &self.path).await?;
        Ok(())
    }
}

/// Parse a `0x`-prefixed (or bare) 32-byte hex hash into an `H256`.
pub fn parse_h256(s: &str) -> Result<H256, Box<dyn std::error::Error + Send + Sync>> {
    let body = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(body)?;
    let arr: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| format!("expected 32 bytes, got {}", bytes.len()))?;
    Ok(H256::from(arr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_h256_roundtrips_debug_format() {
        let original = H256::from([0x3au8; 32]);
        // The watcher writes the cursor via `format!("{hash:?}")`, so the parser
        // must accept exactly that representation.
        let rendered = format!("{original:?}");
        assert_eq!(parse_h256(&rendered).unwrap(), original);
    }

    #[test]
    fn parse_h256_accepts_bare_hex() {
        let bytes = [0x07u8; 32];
        let bare = hex::encode(bytes);
        assert_eq!(parse_h256(&bare).unwrap(), H256::from(bytes));
    }

    #[test]
    fn parse_h256_rejects_wrong_length() {
        assert!(parse_h256("0x1234").is_err());
    }

    #[tokio::test]
    async fn save_then_load_roundtrips() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "flow-funder-cursor-test-{}.txt",
            std::process::id()
        ));
        let store = CursorStore::new(&path);

        assert!(store.load().await.unwrap().is_none(), "absent file → None");

        let hash = H256::from([0xc5u8; 32]);
        store.save(hash).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(hash));

        // Overwrite is atomic and observable.
        let hash2 = H256::from([0xd6u8; 32]);
        store.save(hash2).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(hash2));

        let _ = fs::remove_file(&path).await;
    }
}
