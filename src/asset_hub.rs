//! Asset Hub funding side.
//!
//! Holds a custom subxt `Config` pinned to Paseo Asset Hub Next's live
//! transaction-extension set (verified with the `dump-extensions` bin — see the
//! `AssetHubConfig` comment), a native-token balance query, and the
//! `balances.transfer_keep_alive` submission with nonce caching + retry.

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
use subxt_signer::sr25519::Keypair;
use tracing::{error, info, warn};

use crate::people::NewRegistration;
use crate::registration::{free_balance_from_account_info, funding_shortfall, should_fund};

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

/// What happened when we processed one registration.
#[derive(Debug)]
pub enum FundOutcome {
    /// A transfer was submitted and included in a best block.
    Funded { tx_hash: String, free_before: u128 },
    /// The account already holds at least the target — nothing to do.
    Skipped { free: u128 },
}

/// Owns the Asset Hub connection, the signer, and the funding policy.
pub struct Funder {
    api: OnlineClient<AssetHubConfig>,
    signer: Keypair,
    signer_account: AccountId32,
    target_amount: u128,
    max_retries: u32,
    /// Locally-advanced nonce; see `submit_transfer`.
    next_nonce: Option<u64>,
}

impl Funder {
    pub async fn connect(
        url: &str,
        signer: Keypair,
        target_amount: u128,
        max_retries: u32,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let api = OnlineClient::<AssetHubConfig>::from_url(url).await?;
        let signer_account = AccountId32(signer.public_key().0);
        Ok(Self {
            api,
            signer,
            signer_account,
            target_amount,
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
        self.api = OnlineClient::<AssetHubConfig>::from_url(url).await?;
        self.next_nonce = None;
        Ok(())
    }

    /// Query an account's free balance via `System.Account`. Absent account → 0.
    pub async fn query_free_balance(
        &self,
        account: &AccountId32,
    ) -> Result<u128, Box<dyn std::error::Error + Send + Sync>> {
        let at = self.api.at_current_block().await?;
        let addr: subxt::storage::DynamicAddress = subxt::dynamic::storage("System", "Account");
        let value = at
            .storage()
            .try_fetch(addr, vec![Value::from_bytes(account.0)])
            .await?;
        match value {
            Some(stored) => free_balance_from_account_info(stored.bytes())
                .ok_or_else(|| "failed to decode System.Account AccountInfo".into()),
            None => Ok(0),
        }
    }

    /// Fund one registration: skip if already at/above target, else submit a
    /// `transfer_keep_alive` for the shortfall to the target.
    pub async fn fund(
        &mut self,
        registration: &NewRegistration,
    ) -> Result<FundOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let account = &registration.account;
        let free = self.query_free_balance(account).await?;

        if !should_fund(free, self.target_amount) {
            return Ok(FundOutcome::Skipped { free });
        }

        let amount = funding_shortfall(free, self.target_amount)
            .ok_or("account unexpectedly has no funding shortfall")?;
        let tx_hash = self.submit_transfer(account, amount).await?;
        Ok(FundOutcome::Funded {
            tx_hash,
            free_before: free,
        })
    }

    /// Submit `Balances.transfer_keep_alive(dest, amount)`, waiting for
    /// finalized inclusion, with nonce caching + bounded retry on the pool's
    /// transient rejections (mirrors flow-attester's submission loop).
    async fn submit_transfer(
        &mut self,
        account: &AccountId32,
        transfer_amount: u128,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let dest = Value::unnamed_variant("Id", [Value::from_bytes(account.0)]);
        let amount = Value::u128(transfer_amount);
        let payload = subxt::dynamic::tx("Balances", "transfer_keep_alive", vec![dest, amount]);

        let mut last_err: Option<String> = None;

        for attempt in 0..=self.max_retries {
            let mut tx_client = self.api.tx().await?;

            let nonce = match self.next_nonce {
                Some(n) => n,
                None => {
                    let n = tx_client.account_nonce(&self.signer_account).await?;
                    self.next_nonce = Some(n);
                    n
                }
            };

            info!(account = %account, attempt, nonce, transfer_amount, "submitting transfer_keep_alive");

            let progress = match tx_client
                .sign_and_submit_then_watch(&payload, &self.signer, build_params(nonce))
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    let err = e.to_string();
                    if is_retriable(&err) && attempt < self.max_retries {
                        self.next_nonce = None; // force a chain refetch
                        warn!(account = %account, attempt, error = %err, "pool rejected tx — refreshing nonce and retrying");
                        last_err = Some(err);
                        backoff(attempt).await;
                        continue;
                    }
                    return Err(format!("submission failed: {err}").into());
                }
            };

            let tx_hash = format!("{:?}", progress.extrinsic_hash());

            match wait_for_finalized(progress).await {
                Ok(in_block) => match in_block.wait_for_success().await {
                    Ok(_events) => {
                        self.next_nonce = Some(nonce + 1);
                        info!(account = %account, tx_hash = %tx_hash, "transfer finalized");
                        return Ok(tx_hash);
                    }
                    Err(e) => {
                        let err = e.to_string();
                        if is_retriable(&err) && attempt < self.max_retries {
                            self.next_nonce = None;
                            warn!(account = %account, attempt, error = %err, "tx failed at inclusion — retrying");
                            last_err = Some(err);
                            backoff(attempt).await;
                            continue;
                        }
                        error!(account = %account, error = %e, "transfer failed on-chain");
                        return Err(format!("on-chain error: {e}").into());
                    }
                },
                Err(e) => {
                    if is_retriable(&e) && attempt < self.max_retries {
                        self.next_nonce = None;
                        warn!(account = %account, attempt, error = %e, "tx dropped before finalization — retrying");
                        last_err = Some(e);
                        backoff(attempt).await;
                        continue;
                    }
                    return Err(format!("finalization wait failed: {e}").into());
                }
            }
        }

        Err(last_err
            .unwrap_or_else(|| "exhausted submission retries".to_string())
            .into())
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

fn is_retriable(err: &str) -> bool {
    let lower = err.to_lowercase();
    lower.contains("outdated")
        || lower.contains("stale")
        || lower.contains("future")
        || lower.contains("priority is too low")
        || lower.contains("dropped")
        || lower.contains("usurped")
        || lower.contains("invalid transaction")
        || lower.contains("transaction pool")
}

async fn backoff(attempt: u32) {
    let millis = 500u64.saturating_mul(1u64 << attempt.min(4));
    tokio::time::sleep(Duration::from_millis(millis.min(5_000))).await;
}
