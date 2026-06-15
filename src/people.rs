//! People-chain watcher helpers.
//!
//! `diff_added_accounts` runs one `archive_v1_storageDiff` over the
//! `PeopleLite::LitePeople` prefix and returns the accounts whose entry was
//! *added* between two blocks. The block loop and cursor handling live in `main`
//! so funding stays synchronous per block: a block's cursor only advances once
//! all of its registrations have been handled.

use std::time::Duration;

use subxt::utils::H256;
use subxt_rpcs::{rpc_params, RpcClient};
use tracing::warn;

use crate::registration::{account_from_hex_tail, lite_people_prefix_hex};

/// Max time to wait for the next `storageDiff` message before treating the
/// subscription as dead (a WS that stays open but stops sending).
const DIFF_MSG_TIMEOUT: Duration = Duration::from_secs(120);

/// Hard cap on `storageDiff` messages per diff, to bound memory under a
/// misbehaving node. A genuine catch-up over a long gap can be large, but not
/// unbounded.
const MAX_DIFF_MSGS: usize = 1_000_000;

/// Classification of one `archive_v1_storageDiff` event — pure, so the routing
/// can be unit-tested without a chain.
#[derive(Debug, PartialEq)]
pub(crate) enum DiffEvent {
    /// An `added` entry whose key yielded an AccountId.
    Added([u8; 32]),
    /// An `added` entry whose key had no 32-byte AccountId tail.
    AddedNoAccount,
    /// `modified` / `deleted` / unknown event, or an `added` with no key — not a
    /// new registration.
    Ignored,
    /// The terminator: the diff is complete.
    Done,
    /// A `storageError` / `operationError` event.
    Error(String),
}

/// Route one diff message. `type ∈ {"added","modified","deleted"}`; only `added`
/// is a brand-new account. `modified` is an existing account whose info changed
/// (already funded, or funding is a no-op via the balance check); `deleted` is
/// never a registration.
pub(crate) fn classify_diff_event(msg: &serde_json::Value) -> DiffEvent {
    match msg.get("event").and_then(|v| v.as_str()).unwrap_or("") {
        "storageDiff" => {
            if msg.get("type").and_then(|v| v.as_str()) != Some("added") {
                return DiffEvent::Ignored;
            }
            match msg.get("key").and_then(|v| v.as_str()) {
                Some(key) => match account_from_hex_tail(key) {
                    Some(account) => DiffEvent::Added(account),
                    None => DiffEvent::AddedNoAccount,
                },
                None => DiffEvent::Ignored,
            }
        }
        "storageDiffDone" => DiffEvent::Done,
        "storageError" | "operationError" => DiffEvent::Error(format!("storageDiff error: {msg}")),
        _ => DiffEvent::Ignored,
    }
}

/// Run one `archive_v1_storageDiff` over the LitePeople prefix and return the
/// accounts whose entry was *added* between `prev_hash` and `block_hash`.
///
/// Returns `Err` if the subscription ends before the `storageDiffDone`
/// terminator (e.g. a mid-diff WS drop): a truncated list must NOT be mistaken
/// for a complete one, or the caller would advance the cursor past registrations
/// it never saw. The caller's error path keeps the cursor pinned.
pub async fn diff_added_accounts(
    rpc: &RpcClient,
    block_hash: H256,
    prev_hash: H256,
) -> Result<Vec<[u8; 32]>, Box<dyn std::error::Error + Send + Sync>> {
    let items = serde_json::json!([
        { "key": lite_people_prefix_hex(), "returnType": "value" },
    ]);
    let block_hex = format!("{block_hash:?}");
    let prev_hex = format!("{prev_hash:?}");

    let mut sub = rpc
        .subscribe::<serde_json::Value>(
            "archive_v1_storageDiff",
            rpc_params![block_hex, items, prev_hex],
            "archive_v1_storageDiff_stopStorageDiff",
        )
        .await?;

    let mut added = Vec::new();
    let mut done = false;
    let mut count = 0usize;

    loop {
        let next = tokio::time::timeout(DIFF_MSG_TIMEOUT, sub.next())
            .await
            .map_err(|_| "storageDiff timed out waiting for the next message")?;
        let Some(msg_result) = next else {
            // Stream ended without a terminator → abnormal, see fn doc.
            break;
        };
        let msg = msg_result?;

        count += 1;
        if count > MAX_DIFF_MSGS {
            return Err(format!("storageDiff exceeded {MAX_DIFF_MSGS} messages — aborting").into());
        }

        match classify_diff_event(&msg) {
            DiffEvent::Added(account) => added.push(account),
            DiffEvent::AddedNoAccount => warn!("added LitePeople key has no AccountId tail"),
            DiffEvent::Ignored => {}
            DiffEvent::Done => {
                done = true;
                break;
            }
            DiffEvent::Error(e) => return Err(e.into()),
        }
    }

    if done {
        Ok(added)
    } else {
        Err("storageDiff subscription ended without storageDiffDone".into())
    }
}

/// Fetch the current finalized head — the watcher's starting `prev_block_hash`
/// on first boot (when no cursor has been persisted yet).
pub async fn finalized_head(
    rpc: &RpcClient,
) -> Result<H256, Box<dyn std::error::Error + Send + Sync>> {
    let head: H256 = rpc.request("chain_getFinalizedHead", rpc_params![]).await?;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account_key_hex(account: [u8; 32]) -> String {
        // prefix(32) ++ Blake2_128Concat hasher(16) ++ account(32)
        format!("0x{}{}{}", "11".repeat(32), "22".repeat(16), hex::encode(account))
    }

    #[test]
    fn classify_added_with_account() {
        let account = [0x9au8; 32];
        let msg = serde_json::json!({
            "event": "storageDiff",
            "type": "added",
            "key": account_key_hex(account),
            "value": "0xdead",
        });
        assert_eq!(classify_diff_event(&msg), DiffEvent::Added(account));
    }

    #[test]
    fn classify_added_without_account_tail() {
        let msg = serde_json::json!({ "event": "storageDiff", "type": "added", "key": "0x1234" });
        assert_eq!(classify_diff_event(&msg), DiffEvent::AddedNoAccount);
    }

    #[test]
    fn classify_modified_and_deleted_are_ignored() {
        for change in ["modified", "deleted"] {
            let msg = serde_json::json!({
                "event": "storageDiff",
                "type": change,
                "key": account_key_hex([0x01u8; 32]),
            });
            assert_eq!(classify_diff_event(&msg), DiffEvent::Ignored);
        }
    }

    #[test]
    fn classify_added_missing_key_is_ignored() {
        let msg = serde_json::json!({ "event": "storageDiff", "type": "added" });
        assert_eq!(classify_diff_event(&msg), DiffEvent::Ignored);
    }

    #[test]
    fn classify_done() {
        let msg = serde_json::json!({ "event": "storageDiffDone" });
        assert_eq!(classify_diff_event(&msg), DiffEvent::Done);
    }

    #[test]
    fn classify_errors() {
        for event in ["storageError", "operationError"] {
            let msg = serde_json::json!({ "event": event });
            assert!(matches!(classify_diff_event(&msg), DiffEvent::Error(_)));
        }
    }

    #[test]
    fn classify_unknown_event_is_ignored() {
        let msg = serde_json::json!({ "event": "somethingElse" });
        assert_eq!(classify_diff_event(&msg), DiffEvent::Ignored);
    }
}
