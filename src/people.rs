//! People-chain watcher helpers.
//!
//! `diff_added_accounts` runs one `archive_v1_storageDiff` over the
//! `PeopleLite::LitePeople` prefix and returns the accounts whose entry was
//! *added* between two blocks. The block loop and cursor handling live in `main`
//! so funding stays synchronous per block: a block's cursor only advances once
//! all of its registrations have been handled.

use subxt::utils::{AccountId32, H256};
use subxt_rpcs::{rpc_params, RpcClient};
use tracing::warn;

use crate::registration::{account_from_hex_tail, lite_people_prefix_hex};

/// A newly-observed `LitePeople` account and the block it first appeared in.
#[derive(Debug, Clone)]
pub struct NewRegistration {
    pub account: AccountId32,
    pub block_number: u64,
}

/// Run one `archive_v1_storageDiff` over the LitePeople prefix and return the
/// accounts whose entry was *added* between `prev_hash` and `block_hash`.
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

    while let Some(msg_result) = sub.next().await {
        let msg = msg_result?;
        match msg.get("event").and_then(|v| v.as_str()).unwrap_or("") {
            "storageDiff" => {
                // type ∈ {"added","modified","deleted"}. Only "added" is a brand
                // new account; "modified" is an existing account whose info
                // changed (already funded, or funding is a no-op via the balance
                // check), and "deleted" is never a registration.
                let change_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if change_type != "added" {
                    continue;
                }
                let Some(key) = msg.get("key").and_then(|v| v.as_str()) else {
                    continue;
                };
                match account_from_hex_tail(key) {
                    Some(account) => added.push(account),
                    None => warn!(%key, "added LitePeople key has no AccountId tail"),
                }
            }
            "storageDiffDone" => break,
            "storageError" | "operationError" => {
                return Err(format!("storageDiff error: {msg}").into());
            }
            _ => continue,
        }
    }

    Ok(added)
}

/// Fetch the current finalized head — the watcher's starting `prev_block_hash`
/// on first boot (when no cursor has been persisted yet).
pub async fn finalized_head(
    rpc: &RpcClient,
) -> Result<H256, Box<dyn std::error::Error + Send + Sync>> {
    let head: H256 = rpc.request("chain_getFinalizedHead", rpc_params![]).await?;
    Ok(head)
}
