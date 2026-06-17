//! Asset Hub dotns-reservation side.
//!
//! Holds a custom subxt `Config` pinned to Summit Asset Hub's live
//! transaction-extension set (verified with the `dump-extensions` bin — see the
//! `AssetHubConfig` comment), the `DotnsGateway::LiteLabelOwner` idempotency read,
//! and the `DotnsGateway::reserve_name` submission with nonce caching + retry. The
//! signing key must hold `AttestationAllowance` on the gateway pallet.

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

use crate::attest::ReservationInputs;
use crate::registration::lite_label_owner_key;

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

// Summit Asset Hub custom extensions. VERIFIED against live metadata
// (`dump-extensions wss://summit-asset-hub-rpc.polkadot.io`): each `As*` /
// `AuthorizeValueTransfer` is `struct{ Option<…> }` → None = 0x00; RestrictOrigins
// is `struct{ bool }` → false = 0x00. (Summit's 17-extension set is byte-identical
// in name+order to paseo-asset-hub-next's, so this pin carried over unchanged.)
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

/// Config for Summit Asset Hub.
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
    // VERIFIED against live summit-asset-hub-rpc metadata: the runtime's 17
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

/// Outcome of reserving every name observed in one block.
#[derive(Debug)]
pub struct BlockReserveOutcome {
    /// Names submitted in a finalized `reserve_name` extrinsic.
    pub reserved: usize,
    /// Names already reserved on-chain — nothing submitted for them.
    pub skipped: usize,
    /// One extrinsic hash per `reserve_name` submitted. Empty if nothing was new.
    pub tx_hashes: Vec<String>,
}

/// What a dry run would do for one block's names.
#[derive(Debug)]
pub struct DryRunReport {
    pub would_reserve: usize,
    pub skipped: usize,
    /// Total SCALE-encoded call-data bytes across the names that would be
    /// reserved. Producing these encodes each `reserve_name` against live
    /// metadata, so it doubles as proof the call and its harvested args encode —
    /// without submitting anything.
    pub encoded_bytes: usize,
}

/// Owns the Asset Hub connection and the allowance-holding signer.
pub struct Reserver {
    api: OnlineClient<AssetHubConfig>,
    /// Raw RPC over the same connection as `api`, for `state_queryStorageAt`.
    rpc: RpcClient,
    signer: Keypair,
    signer_account: AccountId32,
    /// When set, submit reservations wrapped in `Proxy.proxy(real = this, …)` —
    /// for when the `AttestationAllowance` is held by this real account and the
    /// signer is its proxy delegate, rather than the signer holding it directly.
    proxy_for: Option<AccountId32>,
    /// Max `reserve_name` calls per `Utility.force_batch` extrinsic.
    batch_size: usize,
    max_retries: u32,
    /// Locally-advanced nonce; see `submit`.
    next_nonce: Option<u64>,
}

impl Reserver {
    pub async fn connect(
        url: &str,
        signer: Keypair,
        proxy_for: Option<AccountId32>,
        batch_size: usize,
        max_retries: u32,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let rpc = RpcClient::from_url(url).await?;
        let api = OnlineClient::<AssetHubConfig>::from_rpc_client(rpc.clone()).await?;
        let signer_account = AccountId32(signer.public_key().0);
        Ok(Self {
            api,
            rpc,
            signer,
            signer_account,
            proxy_for,
            batch_size: batch_size.max(1),
            max_retries,
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

    /// Whether a name is already reserved — i.e. `DotnsGateway::LiteLabelOwner`
    /// has an entry for it. Makes reservation idempotent: re-processing a block
    /// re-reads this and skips names that already landed.
    pub async fn is_name_reserved(
        &self,
        lite_label: &[u8],
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let key = format!("0x{}", hex::encode(lite_label_owner_key(lite_label)));
        // [{ "block": "0x..", "changes": [["0xkey", "0xvalue" | null], …] }]
        let result: Vec<serde_json::Value> = self
            .rpc
            .request("state_queryStorageAt", rpc_params![[key.clone()]])
            .await?;
        for set in &result {
            let Some(changes) = set.get("changes").and_then(|c| c.as_array()) else {
                continue;
            };
            for pair in changes {
                let Some(arr) = pair.as_array() else { continue };
                let key_matches = arr
                    .first()
                    .and_then(|k| k.as_str())
                    .is_some_and(|k| k.eq_ignore_ascii_case(&key));
                // A present, non-null value means the name is owned/reserved.
                let has_value = arr.get(1).is_some_and(|v| !v.is_null());
                if key_matches && has_value {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Reserve every name harvested from one block. Names not already reserved
    /// are chunked into `batch_size`-sized `Utility.force_batch` extrinsics (one
    /// submission each, optionally proxy-wrapped — mirroring identity-backend),
    /// each awaited to finalization. A submission failure propagates as `Err`, so
    /// the caller pins the cursor and the block is retried — the `LiteLabelOwner`
    /// check then skips names that already landed.
    ///
    /// `force_batch` is non-atomic: an individual `reserve_name` that fails on
    /// chain does not revert the others, so one bad name can't block the rest of
    /// the batch (it also won't be retried once the extrinsic finalizes).
    pub async fn reserve_block(
        &mut self,
        inputs: &[ReservationInputs],
    ) -> Result<BlockReserveOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let (to_reserve, skipped) = self.filter_unreserved(inputs).await?;

        let mut reserved = 0usize;
        let mut tx_hashes = Vec::new();
        for chunk in to_reserve.chunks(self.batch_size) {
            let payload = build_batch_payload(chunk, self.proxy_for.as_ref());
            let tx_hash = self.submit(&payload, &batch_label(chunk)).await?;
            reserved += chunk.len();
            tx_hashes.push(tx_hash);
        }
        Ok(BlockReserveOutcome {
            reserved,
            skipped,
            tx_hashes,
        })
    }

    /// Dry-run: count names that would be reserved (skipping already-reserved
    /// ones) and encode each `force_batch` against live metadata to prove it's
    /// valid — without submitting anything.
    pub async fn dry_run_block(
        &self,
        inputs: &[ReservationInputs],
    ) -> Result<DryRunReport, Box<dyn std::error::Error + Send + Sync>> {
        let (to_reserve, skipped) = self.filter_unreserved(inputs).await?;
        let tx_client = self.api.tx().await?;
        let mut encoded_bytes = 0usize;
        for chunk in to_reserve.chunks(self.batch_size) {
            let payload = build_batch_payload(chunk, self.proxy_for.as_ref());
            encoded_bytes += tx_client.call_data(&payload)?.len();
        }
        Ok(DryRunReport {
            would_reserve: to_reserve.len(),
            skipped,
            encoded_bytes,
        })
    }

    /// Split inputs into those not yet reserved (to submit) and a count of those
    /// already reserved on chain (skipped), via the `LiteLabelOwner` check.
    async fn filter_unreserved<'a>(
        &self,
        inputs: &'a [ReservationInputs],
    ) -> Result<(Vec<&'a ReservationInputs>, usize), Box<dyn std::error::Error + Send + Sync>> {
        let mut to_reserve = Vec::new();
        let mut skipped = 0usize;
        for input in inputs {
            if self.is_name_reserved(&input.lite_label).await? {
                skipped += 1;
            } else {
                to_reserve.push(input);
            }
        }
        Ok((to_reserve, skipped))
    }

    /// Submit one `DotnsGateway.reserve_name`, waiting for finalized inclusion,
    /// with nonce caching + bounded retry on transient pool rejections.
    async fn submit(
        &mut self,
        payload: &subxt::transactions::DynamicPayload<Vec<Value>>,
        label: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut last_err: Option<String> = None;

        for attempt in 0..=self.max_retries {
            match self.try_submit_once(payload, label, attempt).await {
                AttemptOutcome::Done(tx_hash) => return Ok(tx_hash),
                AttemptOutcome::Retry(err) if attempt < self.max_retries => {
                    // Any failed attempt invalidates the cached nonce.
                    self.next_nonce = None;
                    warn!(label, attempt, error = %err, "reserve_name attempt failed — refreshing nonce and retrying");
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
        label: &str,
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

        info!(
            label,
            attempt, nonce, "submitting DotnsGateway.reserve_name"
        );

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
                info!(label, tx_hash = %tx_hash, "reserve_name finalized");
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

/// Build one `DotnsGateway::reserve_name(...)` as a `RuntimeCall` value, for
/// nesting inside `Utility.force_batch`, from the inputs harvested out of a
/// `PeopleLite::attest` call.
///
/// NOTE: this 7-arg shape matches the individuality `w3s-dotnsgateway-workaround`
/// branch (commit "[W3S only] … same signature as in people-lite pallet"), which
/// makes `reserve_name` verify the same message as `attest` so the attest
/// signatures are reusable — the ONLY configuration in which a People-chain watcher
/// can reserve without re-collecting a fresh per-name signature.
///
/// ⚠️ Asset Hub runtime prerequisite (see README): Summit Asset Hub must run this
/// workaround pallet. The STANDARD pallet — currently live on summit-asset-hub-rpc
/// (verified) — has a different 6-arg shape (`…, lite_label, chat_key,
/// reserved_base_label, signed_at`; no ring_vrf_key/proof) and verifies the
/// candidate's signature over a SCALE tuple binding the attester + `signed_at`. That
/// signature is NOT present in the People `attest` extrinsic, so the bot cannot
/// produce it; encoding (and, were it to encode, on-chain verification) fails against
/// the standard pallet. The bot is intentionally built for the workaround pallet.
///
/// The first four args are passed through verbatim as the decoded `Value`s — they
/// re-encode structurally against Asset Hub's metadata (same runtime types). The
/// last three are rebuilt from harvested bytes so they match the target
/// `BaseLabel`/`ChatKey` newtypes rather than the bare `BoundedVec`/`[u8;65]` the
/// People chain encoded them as.
fn build_reserve_name_call(input: &ReservationInputs) -> Value {
    let reserved_base_label = match &input.reserved_base_label {
        Some(label) => Value::unnamed_variant("Some", [Value::from_bytes(label)]),
        None => Value::unnamed_variant("None", []),
    };

    let args = vec![
        input.candidate.clone(),
        input.candidate_signature.clone(),
        input.ring_vrf_key.clone(),
        input.proof_of_ownership.clone(),
        Value::from_bytes(&input.lite_label), // lite_label: BaseLabel
        Value::from_bytes(input.chat_key),    // chat_key: ChatKey([u8; 65])
        reserved_base_label,                  // reserved_base_label: Option<BaseLabel>
    ];

    Value::unnamed_variant(
        "DotnsGateway",
        [Value::unnamed_variant("reserve_name", args)],
    )
}

/// Build a `Utility.force_batch([reserve_name, …])` payload for one chunk of
/// names, optionally wrapped in `Proxy.proxy(real, Some(Any), call)` when the
/// signer is a proxy delegate of the allowance-holding account. Mirrors how
/// identity-backend submits (force_batch under a proxy, `force_proxy_type = Any`).
/// `force_batch` is non-atomic, so a single failing `reserve_name` doesn't revert
/// the batch.
fn build_batch_payload(
    chunk: &[&ReservationInputs],
    proxy_for: Option<&AccountId32>,
) -> subxt::transactions::DynamicPayload<Vec<Value>> {
    let calls: Vec<Value> = chunk.iter().map(|i| build_reserve_name_call(i)).collect();

    match proxy_for {
        // Direct: the signer holds the gateway AttestationAllowance.
        None => subxt::dynamic::tx(
            "Utility",
            "force_batch",
            vec![Value::unnamed_composite(calls)],
        ),
        // Proxied: Proxy.proxy(real, Some(ProxyType::Any), Utility.force_batch([…])).
        Some(real) => {
            let batch_call = Value::unnamed_variant(
                "Utility",
                [Value::unnamed_variant(
                    "force_batch",
                    [Value::unnamed_composite(calls)],
                )],
            );
            let proxy_args = vec![
                // real: MultiAddress::Id(account)
                Value::unnamed_variant("Id", [Value::from_bytes(real.0)]),
                // force_proxy_type: Option<ProxyType> = Some(Any)
                Value::unnamed_variant("Some", [Value::unnamed_variant("Any", [])]),
                // call: RuntimeCall
                batch_call,
            ];
            subxt::dynamic::tx("Proxy", "proxy", proxy_args)
        }
    }
}

/// A short label for one batch's log line: count plus the first username.
fn batch_label(chunk: &[&ReservationInputs]) -> String {
    match chunk.first() {
        Some(first) => format!(
            "{} name(s) ({}…)",
            chunk.len(),
            String::from_utf8_lossy(&first.lite_label)
        ),
        None => "0 names".to_string(),
    }
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
    RETRIABLE_SUBSTRINGS
        .iter()
        .any(|needle| lower.contains(needle))
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
        for s in [
            "on-chain: FundsUnavailable",
            "BadOrigin",
            "metadata mismatch",
            "",
        ] {
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
