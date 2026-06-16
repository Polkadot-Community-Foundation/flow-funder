//! Small People-chain RPC helpers shared by the watcher loop.

use subxt::utils::H256;
use subxt_rpcs::{rpc_params, RpcClient};

/// Fetch the current finalized head — the watcher's starting point on first boot
/// (when no cursor has been persisted yet).
pub async fn finalized_head(
    rpc: &RpcClient,
) -> Result<H256, Box<dyn std::error::Error + Send + Sync>> {
    let head: H256 = rpc.request("chain_getFinalizedHead", rpc_params![]).await?;
    Ok(head)
}
