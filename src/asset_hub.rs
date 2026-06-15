//! Asset Hub funding side.
//!
//! Holds a custom subxt `Config` pinned to Paseo Asset Hub Next's live
//! transaction-extension set (verified with the `dump-extensions` bin — see the
//! `AssetHubConfig` comment), a native-token balance query, and the
//! `balances.transfer_keep_alive` submission with nonce caching + retry.

use std::collections::HashMap;
use std::time::Duration;

use scale_info::PortableRegistry;
use subxt::client::OnlineClientAtBlockT;
use subxt::config::transaction_extensions::{
    ChargeAssetTxPayment, CheckGenesis, CheckMetadataHash, CheckMortality, CheckNonce,
    CheckSpecVersion, CheckTxVersion,
};
use subxt::config::{
    ClientState, Config, DefaultExtrinsicParamsBuilder, SubstrateConfig, TransactionExtension,
    TransactionExtensions,
};
use subxt::dynamic::Value;
use subxt::tx::{TransactionInBlock, TransactionProgress, TransactionStatus};
use subxt::utils::AccountId32;
use subxt::OnlineClient;
use subxt_rpcs::{rpc_params, RpcClient};
use subxt_signer::sr25519::Keypair;
use tracing::{info, warn};

use crate::registration::{free_balance_from_account_info, funding_shortfall, system_account_key};

/// Max accounts per `state_queryStorageAt` balance read. Keeps each request a
/// reasonable size while still collapsing a big catch-up into a handful of RPCs.
const READ_CHUNK: usize = 512;

/// Max time to wait for a submitted tx to finalize before treating the attempt
/// as failed (retriable). ~5× a generous block time — a tx stuck in the pool
/// must not hang the per-block loop forever.
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(120);

const BACKOFF_BASE_MS: u64 = 500;
const BACKOFF_SHIFT_CAP: u32 = 4;
const BACKOFF_CEIL_MS: u64 = 5_000;

// ---------------------------------------------------------------------------
// Extension macros (mirrors mission-control's flow-attester/chain.rs)
// ---------------------------------------------------------------------------

/// An extension whose `extra` is a single fixed byte and which has no implicit
/// data — e.g. `struct{ Option<…> }` encoded as its `None` variant (0x00), or
/// `struct{ bool }` encoded as `false` (0x00).
macro_rules! define_simple_extension {
    ($name:ident, $identifier:expr, $value:expr) => {
        pub struct $name;

        impl frame_decode::extrinsics::TransactionExtension<PortableRegistry> for $name {
            const NAME: &str = $identifier;

            fn encode_value_to(
                &self,
                _type_id: u32,
                _type_resolver: &PortableRegistry,
                v: &mut Vec<u8>,
            ) -> Result<(), frame_decode::extrinsics::TransactionExtensionError> {
                v.push($value);
                Ok(())
            }

            fn encode_implicit_to(
                &self,
                _type_id: u32,
                _type_resolver: &PortableRegistry,
                _v: &mut Vec<u8>,
            ) -> Result<(), frame_decode::extrinsics::TransactionExtensionError> {
                Ok(())
            }
        }

        impl<T: Config> TransactionExtension<T> for $name {
            type Decoded = u8;
            type Params = ();

            fn new(
                _client: &ClientState<T>,
                _params: Self::Params,
            ) -> Result<Self, subxt::error::TransactionExtensionError> {
                Ok($name)
            }
        }
    };
}

/// An extension that contributes NO `extra` bytes and no implicit data — e.g.
/// an empty struct (`AuthorizeCall`, `CheckNonZeroSender`, `CheckWeight`,
/// `EthSetOrigin`) or `()` (`StorageWeightReclaim`).
macro_rules! define_empty_extension {
    ($name:ident, $identifier:expr) => {
        pub struct $name;

        impl frame_decode::extrinsics::TransactionExtension<PortableRegistry> for $name {
            const NAME: &str = $identifier;

            fn encode_value_to(
                &self,
                _type_id: u32,
                _type_resolver: &PortableRegistry,
                _v: &mut Vec<u8>,
            ) -> Result<(), frame_decode::extrinsics::TransactionExtensionError> {
                Ok(())
            }

            fn encode_implicit_to(
                &self,
                _type_id: u32,
                _type_resolver: &PortableRegistry,
                _v: &mut Vec<u8>,
            ) -> Result<(), frame_decode::extrinsics::TransactionExtensionError> {
                Ok(())
            }
        }

        impl<T: Config> TransactionExtension<T> for $name {
            type Decoded = ();
            type Params = ();

            fn new(
                _client: &ClientState<T>,
                _params: Self::Params,
            ) -> Result<Self, subxt::error::TransactionExtensionError> {
                Ok($name)
            }
        }
    };
}

// Paseo Asset Hub Next custom extensions. VERIFIED against live metadata
// (`dump-extensions wss://paseo-asset-hub-next-rpc.polkadot.io`): each `As*` /
// `AuthorizeValueTransfer` is `struct{ Option<…> }` → None = 0x00; RestrictOrigins
// is `struct{ bool }` → false = 0x00.
define_simple_extension!(AuthorizeValueTransfer, "AuthorizeValueTransfer", 0x00);
define_simple_extension!(AsPgas, "AsPgas", 0x00);
define_simple_extension!(AsRingAlias, "AsRingAlias", 0x00);
define_simple_extension!(AsDotnsGateway, "AsDotnsGateway", 0x00);
define_simple_extension!(RestrictOrigins, "RestrictOrigins", 0x00);

// Empty extensions — no extra bytes, no implicit data.
define_empty_extension!(AuthorizeCall, "AuthorizeCall");
define_empty_extension!(CheckNonZeroSender, "CheckNonZeroSender");
define_empty_extension!(CheckWeight, "CheckWeight");
define_empty_extension!(EthSetOrigin, "EthSetOrigin");
define_empty_extension!(StorageWeightReclaim, "StorageWeightReclaim");

// ---------------------------------------------------------------------------
// Asset Hub config
// ---------------------------------------------------------------------------

/// Config for Paseo Asset Hub Next.
#[derive(Debug, Clone)]
pub struct AssetHubConfig(SubstrateConfig);

impl Default for AssetHubConfig {
    fn default() -> Self {
        AssetHubConfig(SubstrateConfig::new())
    }
}

impl Config for AssetHubConfig {
    type AccountId = <SubstrateConfig as Config>::AccountId;
    type Address = subxt::utils::MultiAddress<Self::AccountId, ()>;
    type Signature = <SubstrateConfig as Config>::Signature;
    type Hasher = <SubstrateConfig as Config>::Hasher;
    type Header = <SubstrateConfig as Config>::Header;
    type AssetId = <SubstrateConfig as Config>::AssetId;
    // VERIFIED against live paseo-asset-hub-next metadata: the runtime's 17
    // transaction extensions, in on-wire order. subxt matches each by NAME, so
    // the set must be complete and ordered.
    type TransactionExtensions = (
        AuthorizeValueTransfer,     // 0
        AuthorizeCall,              // 1
        AsPgas,                     // 2
        AsRingAlias,                // 3
        AsDotnsGateway,             // 4
        RestrictOrigins,            // 5
        CheckNonZeroSender,         // 6
        CheckSpecVersion,           // 7
        CheckTxVersion,             // 8
        CheckGenesis<Self>,         // 9
        CheckMortality<Self>,       // 10
        CheckNonce,                 // 11
        CheckWeight,                // 12
        ChargeAssetTxPayment<Self>, // 13
        CheckMetadataHash,          // 14
        EthSetOrigin,               // 15
        StorageWeightReclaim,       // 16
    );

    fn genesis_hash(&self) -> Option<subxt::config::HashFor<Self>> {
        self.0.genesis_hash()
    }

    fn spec_and_transaction_version_for_block_number(
        &self,
        block_number: u64,
    ) -> Option<(u32, u32)> {
        self.0
            .spec_and_transaction_version_for_block_number(block_number)
    }

    fn metadata_for_spec_version(&self, spec_version: u32) -> Option<subxt::metadata::ArcMetadata> {
        self.0.metadata_for_spec_version(spec_version)
    }

    fn set_metadata_for_spec_version(
        &self,
        spec_version: u32,
        metadata: subxt::metadata::ArcMetadata,
    ) {
        self.0.set_metadata_for_spec_version(spec_version, metadata)
    }
}

/// Build extrinsic params for `AssetHubConfig`'s extensions with `account_nonce`
/// pinned. Every custom/empty extension takes `()`; only nonce, mortality, and
/// the asset-tx charge carry meaningful values, sourced from subxt's default
/// builder.
fn build_params(
    account_nonce: u64,
) -> <<AssetHubConfig as Config>::TransactionExtensions as TransactionExtensions<AssetHubConfig>>::Params
{
    let (_verify, _spec, _tx, nonce, _genesis, mortality, asset, _charge, meta) =
        DefaultExtrinsicParamsBuilder::<AssetHubConfig>::new()
            .nonce(account_nonce)
            .build();
    (
        (),        // 0  AuthorizeValueTransfer
        (),        // 1  AuthorizeCall
        (),        // 2  AsPgas
        (),        // 3  AsRingAlias
        (),        // 4  AsDotnsGateway
        (),        // 5  RestrictOrigins
        (),        // 6  CheckNonZeroSender
        (),        // 7  CheckSpecVersion
        (),        // 8  CheckTxVersion
        (),        // 9  CheckGenesis
        mortality, // 10 CheckMortality
        nonce,     // 11 CheckNonce
        (),        // 12 CheckWeight
        asset,     // 13 ChargeAssetTxPayment
        meta,      // 14 CheckMetadataHash
        (),        // 15 EthSetOrigin
        (),        // 16 StorageWeightReclaim
    )
}

// ---------------------------------------------------------------------------
// Funder
// ---------------------------------------------------------------------------

/// Outcome of funding all the registrations observed in one block.
#[derive(Debug)]
pub struct BatchOutcome {
    /// Accounts included in a submitted + finalized batch.
    pub funded: usize,
    /// Accounts already at/above target — nothing submitted for them.
    pub skipped: usize,
    /// One extrinsic hash per `batch_all` chunk submitted. Empty if nothing
    /// needed funding.
    pub tx_hashes: Vec<String>,
}

/// What a dry run would do for one block's registrations.
#[derive(Debug)]
pub struct DryRunReport {
    pub would_fund: usize,
    pub skipped: usize,
    /// Number of `batch_all` chunks the funding would be split into.
    pub batches: usize,
    /// Total SCALE-encoded call-data bytes across all chunks. Producing these
    /// encodes each `batch_all` against live metadata, so it doubles as proof
    /// that `Utility.batch_all` and the inner calls exist and encode — without
    /// submitting anything.
    pub encoded_bytes: usize,
}

/// Owns the Asset Hub connection, the signer, and the funding policy.
pub struct Funder {
    api: OnlineClient<AssetHubConfig>,
    /// Raw RPC over the same connection as `api`, for batched `state_queryStorageAt`.
    rpc: RpcClient,
    signer: Keypair,
    signer_account: AccountId32,
    target_amount: u128,
    max_retries: u32,
    /// Max `transfer_keep_alive` calls per `batch_all` extrinsic.
    batch_size: usize,
    /// Locally-advanced nonce; see `submit_batch`.
    next_nonce: Option<u64>,
}

impl Funder {
    pub async fn connect(
        url: &str,
        signer: Keypair,
        target_amount: u128,
        max_retries: u32,
        batch_size: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let rpc = RpcClient::from_url(url).await?;
        let api = OnlineClient::<AssetHubConfig>::from_rpc_client(rpc.clone()).await?;
        let signer_account = AccountId32(signer.public_key().0);
        Ok(Self {
            api,
            rpc,
            signer,
            signer_account,
            target_amount,
            max_retries,
            batch_size: batch_size.max(1),
            next_nonce: None,
        })
    }

    pub fn signer_account(&self) -> &AccountId32 {
        &self.signer_account
    }

    /// Rebuild the Asset Hub client after a connection error and drop the cached
    /// nonce so the next submission re-reads it from chain.
    pub async fn reconnect(
        &mut self,
        url: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Reset the cached nonce up front: even if the rebuild below fails, the
        // old nonce must not survive into a later attempt.
        self.next_nonce = None;
        let rpc = RpcClient::from_url(url).await?;
        self.api = OnlineClient::<AssetHubConfig>::from_rpc_client(rpc.clone()).await?;
        self.rpc = rpc;
        Ok(())
    }

    /// Read many accounts' free balances over this funder's connection.
    pub async fn query_free_balances(
        &self,
        accounts: &[AccountId32],
    ) -> Result<Vec<u128>, Box<dyn std::error::Error + Send + Sync>> {
        fetch_free_balances(&self.rpc, accounts).await
    }

    /// Query balances (batched) and split `accounts` into those that still need
    /// funding — paired with their shortfall to target — and a count of those
    /// already at or above target.
    async fn plan_funding(
        &self,
        accounts: &[AccountId32],
    ) -> Result<(Vec<(AccountId32, u128)>, usize), Box<dyn std::error::Error + Send + Sync>> {
        let balances = self.query_free_balances(accounts).await?;
        let mut to_fund = Vec::new();
        let mut skipped = 0usize;
        for (account, free) in accounts.iter().zip(balances) {
            match funding_shortfall(free, self.target_amount) {
                Some(shortfall) => to_fund.push((*account, shortfall)),
                None => skipped += 1,
            }
        }
        Ok((to_fund, skipped))
    }

    /// Fund every account in a block in as few transactions as possible: the
    /// accounts needing funding are chunked into `batch_size`-sized
    /// `Utility.batch_all` extrinsics, each submitted once. Already-funded
    /// accounts are skipped (idempotent). A chunk failure propagates as `Err`,
    /// so the caller pins the cursor and the block is retried — the balance
    /// check then skips the chunks that already landed.
    pub async fn fund_batch(
        &mut self,
        accounts: &[AccountId32],
    ) -> Result<BatchOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let (to_fund, skipped) = self.plan_funding(accounts).await?;
        if to_fund.is_empty() {
            return Ok(BatchOutcome { funded: 0, skipped, tx_hashes: Vec::new() });
        }

        let mut funded = 0usize;
        let mut tx_hashes = Vec::new();
        for chunk in to_fund.chunks(self.batch_size) {
            let tx_hash = self.submit_batch(chunk).await?;
            funded += chunk.len();
            tx_hashes.push(tx_hash);
        }
        Ok(BatchOutcome { funded, skipped, tx_hashes })
    }

    /// Dry-run planning: compute who would be funded and encode each `batch_all`
    /// chunk against live metadata (via `call_data`) to prove it's valid —
    /// without submitting anything.
    pub async fn dry_run_batch(
        &self,
        accounts: &[AccountId32],
    ) -> Result<DryRunReport, Box<dyn std::error::Error + Send + Sync>> {
        let (to_fund, skipped) = self.plan_funding(accounts).await?;
        let mut encoded_bytes = 0usize;
        let mut batches = 0usize;
        if !to_fund.is_empty() {
            let tx_client = self.api.tx().await?;
            for chunk in to_fund.chunks(self.batch_size) {
                let payload = build_batch_payload(chunk);
                encoded_bytes += tx_client.call_data(&payload)?.len();
                batches += 1;
            }
        }
        Ok(DryRunReport {
            would_fund: to_fund.len(),
            skipped,
            batches,
            encoded_bytes,
        })
    }

    /// Submit one `Utility.batch_all` of `transfer_keep_alive` calls, waiting for
    /// finalized inclusion, with nonce caching + bounded retry on transient pool
    /// rejections. `batch_all` is atomic: either every transfer in the chunk
    /// lands or none do, so a failed chunk leaves no partial state — the caller
    /// pins the cursor and the chunk is retried cleanly.
    async fn submit_batch(
        &mut self,
        chunk: &[(AccountId32, u128)],
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let payload = build_batch_payload(chunk);
        let batch_len = chunk.len();
        let mut last_err: Option<String> = None;

        for attempt in 0..=self.max_retries {
            match self.try_submit_once(&payload, batch_len, attempt).await {
                AttemptOutcome::Done(tx_hash) => return Ok(tx_hash),
                AttemptOutcome::Retry(err) if attempt < self.max_retries => {
                    // Any failed attempt invalidates the cached nonce.
                    self.next_nonce = None;
                    warn!(batch_len, attempt, error = %err, "batch attempt failed — refreshing nonce and retrying");
                    last_err = Some(err);
                    backoff(attempt).await;
                }
                // Out of retries, or non-retriable: give up. Reset the nonce so a
                // later attempt (after the block is reprocessed) re-reads it.
                AttemptOutcome::Retry(err) | AttemptOutcome::Fatal(err) => {
                    self.next_nonce = None;
                    return Err(err.into());
                }
            }
        }

        self.next_nonce = None;
        Err(last_err
            .unwrap_or_else(|| "exhausted submission retries".to_string())
            .into())
    }

    /// One submission attempt: acquire/use the cached nonce, submit, and wait
    /// (bounded by `FINALIZE_TIMEOUT`) for finalized success. Returns a single
    /// outcome the retry loop interprets uniformly, so retry/classification logic
    /// lives in exactly one place (no drifting branches — finding #8).
    async fn try_submit_once(
        &mut self,
        payload: &subxt::transactions::DynamicPayload<Vec<Value>>,
        batch_len: usize,
        attempt: u32,
    ) -> AttemptOutcome {
        let mut tx_client = match self.api.tx().await {
            Ok(c) => c,
            Err(e) => return AttemptOutcome::Retry(format!("tx client: {e}")),
        };

        let nonce = match self.next_nonce {
            Some(n) => n,
            None => match tx_client.account_nonce(&self.signer_account).await {
                Ok(n) => {
                    self.next_nonce = Some(n);
                    n
                }
                Err(e) => return AttemptOutcome::Retry(format!("nonce fetch: {e}")),
            },
        };

        info!(batch_len, attempt, nonce, "submitting Utility.batch_all of transfer_keep_alive");

        let progress = match tx_client
            .sign_and_submit_then_watch(payload, &self.signer, build_params(nonce))
            .await
        {
            Ok(p) => p,
            Err(e) => return classify_submit_err(format!("submission: {e}")),
        };

        let tx_hash = format!("{:?}", progress.extrinsic_hash());

        let outcome = tokio::time::timeout(FINALIZE_TIMEOUT, async {
            let in_block = wait_for_finalized(progress).await?;
            in_block
                .wait_for_success()
                .await
                .map_err(|e| format!("on-chain: {e}"))?;
            Ok::<(), String>(())
        })
        .await;

        match outcome {
            Err(_elapsed) => AttemptOutcome::Retry("finalization timed out".to_string()),
            Ok(Err(e)) => classify_submit_err(e),
            Ok(Ok(())) => {
                self.next_nonce = Some(nonce + 1);
                info!(batch_len, tx_hash = %tx_hash, "batch finalized");
                AttemptOutcome::Done(tx_hash)
            }
        }
    }
}

/// The result of one submission attempt, interpreted uniformly by the retry
/// loop so classification lives in one place.
enum AttemptOutcome {
    Done(String),
    Retry(String),
    Fatal(String),
}

/// Classify a submission error string as retriable or fatal.
fn classify_submit_err(message: String) -> AttemptOutcome {
    if is_retriable(&message) {
        AttemptOutcome::Retry(message)
    } else {
        AttemptOutcome::Fatal(message)
    }
}

/// Read many accounts' free balances in one `state_queryStorageAt` per
/// `READ_CHUNK` accounts (instead of a fetch per account), returning a free
/// balance for each input account in order. Absent account → 0. A free function
/// so a read-only consumer (e.g. the health balance gauge) can use it with its
/// own RPC connection, no signer required.
pub async fn fetch_free_balances(
    rpc: &RpcClient,
    accounts: &[AccountId32],
) -> Result<Vec<u128>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::with_capacity(accounts.len());
    for chunk in accounts.chunks(READ_CHUNK) {
        let keys: Vec<String> = chunk
            .iter()
            .map(|a| format!("0x{}", hex::encode(system_account_key(&a.0))))
            .collect();

        // [{ "block": "0x..", "changes": [["0xkey", "0xvalue" | null], …] }]
        let result: Vec<serde_json::Value> = rpc
            .request("state_queryStorageAt", rpc_params![keys.clone()])
            .await?;

        let mut values: HashMap<String, String> = HashMap::new();
        for set in &result {
            let Some(changes) = set.get("changes").and_then(|c| c.as_array()) else {
                continue;
            };
            for pair in changes {
                let Some(arr) = pair.as_array() else { continue };
                if let (Some(key), Some(value)) =
                    (arr.first().and_then(|k| k.as_str()), arr.get(1).and_then(|v| v.as_str()))
                {
                    values.insert(key.to_lowercase(), value.to_string());
                }
                // A null value means the account does not exist yet → free 0.
            }
        }

        for key in &keys {
            match values.get(&key.to_lowercase()) {
                Some(value_hex) => {
                    let bytes = hex::decode(value_hex.strip_prefix("0x").unwrap_or(value_hex))?;
                    out.push(
                        free_balance_from_account_info(&bytes)
                            .ok_or("failed to decode System.Account AccountInfo")?,
                    );
                }
                None => out.push(0),
            }
        }
    }
    Ok(out)
}

/// Build a `Utility.batch_all([Balances.transfer_keep_alive(dest, shortfall), …])`
/// dynamic payload for one chunk of (account, shortfall) pairs.
fn build_batch_payload(
    chunk: &[(AccountId32, u128)],
) -> subxt::transactions::DynamicPayload<Vec<Value>> {
    let calls: Vec<Value> = chunk
        .iter()
        .map(|(account, shortfall)| {
            Value::unnamed_variant(
                "Balances",
                [Value::unnamed_variant(
                    "transfer_keep_alive",
                    [
                        // dest: MultiAddress::Id(account)
                        Value::unnamed_variant("Id", [Value::from_bytes(account.0)]),
                        // value: Balance
                        Value::u128(*shortfall),
                    ],
                )],
            )
        })
        .collect();

    subxt::dynamic::tx("Utility", "batch_all", vec![Value::unnamed_composite(calls)])
}

/// Drain the status stream and return the finalized status. A best-block
/// inclusion is not durable enough to advance the People cursor.
async fn wait_for_finalized<C>(
    mut progress: TransactionProgress<AssetHubConfig, C>,
) -> Result<TransactionInBlock<AssetHubConfig, C>, String>
where
    C: OnlineClientAtBlockT<AssetHubConfig> + Clone,
{
    while let Some(status) = progress.next().await {
        let status = status.map_err(|e: subxt::error::TransactionProgressError| e.to_string())?;
        match status {
            TransactionStatus::InFinalizedBlock(in_block) => return Ok(in_block),
            TransactionStatus::NoLongerInBestBlock => continue,
            TransactionStatus::Error { message } => return Err(format!("stream error: {message}")),
            TransactionStatus::Invalid { message } => return Err(format!("invalid: {message}")),
            TransactionStatus::Dropped { message } => return Err(format!("dropped: {message}")),
            _ => continue,
        }
    }
    Err("tx progress stream ended before finalization".into())
}

/// Substrate/subxt error fragments that indicate a transient, retriable
/// submission failure (nonce drift, mempool churn). Matched as lowercased
/// substrings — a pragmatic classification over subxt's stringly-typed errors.
const RETRIABLE_SUBSTRINGS: &[&str] = &[
    "outdated",
    "stale",
    "future",
    "priority is too low",
    "dropped",
    "usurped",
    "invalid transaction",
    "transaction pool",
];

fn is_retriable(err: &str) -> bool {
    let lower = err.to_lowercase();
    RETRIABLE_SUBSTRINGS.iter().any(|needle| lower.contains(needle))
}

/// Exponential backoff with a cap: `base × 2^min(attempt, shift_cap)`, ceilinged.
fn backoff_delay(attempt: u32) -> Duration {
    let millis = BACKOFF_BASE_MS.saturating_mul(1u64 << attempt.min(BACKOFF_SHIFT_CAP));
    Duration::from_millis(millis.min(BACKOFF_CEIL_MS))
}

async fn backoff(attempt: u32) {
    tokio::time::sleep(backoff_delay(attempt)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_retriable_matches_transient_fragments() {
        for s in [
            "Transaction is Outdated",
            "priority is too low",
            "tx was dropped",
            "Usurped by another tx",
            "Invalid Transaction",
            "the transaction pool is full",
            "stale",
            "future",
        ] {
            assert!(is_retriable(s), "{s:?} should be retriable");
        }
    }

    #[test]
    fn is_retriable_rejects_fatal_errors() {
        for s in ["on-chain: FundsUnavailable", "BadOrigin", "metadata mismatch", ""] {
            assert!(!is_retriable(s), "{s:?} should be fatal");
        }
    }

    #[test]
    fn backoff_delay_grows_then_caps() {
        assert_eq!(backoff_delay(0), Duration::from_millis(500));
        assert_eq!(backoff_delay(1), Duration::from_millis(1000));
        assert_eq!(backoff_delay(2), Duration::from_millis(2000));
        assert_eq!(backoff_delay(3), Duration::from_millis(4000));
        // 2^4 × 500 = 8000, ceilinged to 5000.
        assert_eq!(backoff_delay(4), Duration::from_millis(5000));
        assert_eq!(backoff_delay(100), Duration::from_millis(5000));
        assert_eq!(backoff_delay(u32::MAX), Duration::from_millis(5000));
    }
}
