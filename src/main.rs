//! flow-funder — watch the People chain for newly-registered lite identities and
//! fund each one with native tokens on Asset Hub.
//!
//! Pattern (after mission-control's flow-people + flow-attester):
//!   * a People-chain watcher runs `archive_v1_storageDiff` over the
//!     `PeopleLite::LitePeople` prefix per finalized block and collects every
//!     newly *added* account (`people` module);
//!   * for each account, the funder checks its Asset Hub balance and submits
//!     `Balances.transfer_keep_alive` if it's below target (`asset_hub` module).
//!     The balance check makes the bot idempotent — an account already at/above
//!     target is skipped, so retries and restarts never double-fund.
//!
//! Funding is synchronous per block and gated by a persisted cursor
//! (`cursor` module): a block's cursor advances only once ALL of its accounts
//! have been handled. So
//!   * a connection loss resumes from the cursor (not the finalized head) — no
//!     registration in the downtime window is missed; and
//!   * a fund failure leaves the cursor pinned, so the block is re-diffed and the
//!     failed account retried, while already-funded accounts are skipped.
//!
//! On first boot (no cursor) the watcher starts from the current finalized head,
//! so existing accounts are never retroactively funded.

mod asset_hub;
mod cursor;
mod people;
mod registration;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, Json, Router};
use clap::Parser;
use serde::Serialize;
use subxt::config::SubstrateConfig;
use subxt::utils::{AccountId32, H256};
use subxt::OnlineClient;
use subxt_rpcs::RpcClient;
use subxt_signer::sr25519::Keypair;
use subxt_signer::SecretUri;
use tokio::sync::{Notify, RwLock};
use tracing::{error, info, warn};

use crate::asset_hub::Funder;
use crate::cursor::CursorStore;
use crate::people::{diff_added_accounts, finalized_head};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Clone)]
#[command(name = "flow-funder")]
struct Cli {
    /// WebSocket URL of the People chain node (must expose archive RPC methods).
    #[arg(
        long,
        env = "PEOPLE_NODE_URL",
        default_value = "wss://paseo-people-next-system-rpc.polkadot.io"
    )]
    people_url: String,

    /// WebSocket URL of the Asset Hub node to fund accounts on.
    #[arg(
        long,
        env = "ASSET_HUB_NODE_URL",
        default_value = "wss://paseo-asset-hub-next-rpc.polkadot.io"
    )]
    asset_hub_url: String,

    /// Sr25519 seed phrase for the funding account. Required unless --dry-run is set.
    #[arg(long, env = "FUNDER_SEED_PHRASE")]
    seed_phrase: Option<String>,

    /// Derivation path appended to the seed (e.g. "//Alice"). Defaults to //Alice.
    #[arg(long, env = "FUNDER_DERIVATION_PATH")]
    derivation_path: Option<String>,

    /// Target free balance for each newly-registered account, in plancks.
    /// Paseo's native token (PAS) has 10 decimals, so 10_000_000_000 = 1 PAS.
    #[arg(long, env = "FUNDER_AMOUNT_PLANCK", default_value = "10000000000")]
    amount: u128,

    /// Fat-finger guard: reject `--amount` above this many plancks at startup
    /// (default 100_000 PAS). Raise it deliberately for a high-value funder.
    #[arg(long, env = "FUNDER_MAX_AMOUNT_PLANCK", default_value = "1000000000000000")]
    max_amount: u128,

    /// Maximum submission retries per transfer on transient pool rejections.
    #[arg(long, env = "FUNDER_MAX_SUBMIT_RETRIES", default_value = "3")]
    max_submit_retries: u32,

    /// Max accounts per `Utility.batch_all` extrinsic. A block's
    /// needing-funding accounts are funded in chunks of this size (one tx each),
    /// so catch-up after downtime collapses many transfers into a few txs.
    #[arg(long, env = "FUNDER_BATCH_SIZE", default_value = "100")]
    batch_size: usize,

    /// File holding the resume cursor (last fully-handled People block hash).
    #[arg(
        long,
        env = "FUNDER_CURSOR_FILE",
        default_value = "flow-funder-cursor.txt"
    )]
    cursor_file: String,

    /// Detect and log registrations but never submit a transfer (or persist a
    /// cursor — a dry run must not advance a real cursor).
    #[arg(long, env = "FUNDER_DRY_RUN", default_value = "false")]
    dry_run: bool,

    /// Port for the health check HTTP server.
    #[arg(long, env = "FUNDER_HEALTH_PORT", default_value = "3033")]
    health_port: u16,

    /// Address to bind the health server to. Defaults to localhost; set to
    /// `0.0.0.0` only if `/health` must be reachable off-host (it exposes
    /// operational counters and has no auth).
    #[arg(long, env = "FUNDER_HEALTH_BIND", default_value = "127.0.0.1")]
    health_bind: String,
}

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const RECONNECT_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Max wait for the next finalized block before treating the People-chain
/// subscription as dead (a WS that stays open but goes silent).
const BLOCK_RECV_TIMEOUT: Duration = Duration::from_secs(120);
/// How often the health gauge refreshes the funding key's balance.
const BALANCE_GAUGE_INTERVAL: Duration = Duration::from_secs(60);
/// `fund_failures` at/above this flips `/health` status to `unhealthy`.
const UNHEALTHY_FAILURE_THRESHOLD: u64 = 10;

// ---------------------------------------------------------------------------
// Health endpoint
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    people_connected: Arc<RwLock<bool>>,
    asset_hub_connected: Arc<RwLock<bool>>,
    last_block_at: Arc<RwLock<Option<u64>>>,
    cursor_block_hash: Arc<RwLock<Option<String>>>,
    funding_key_balance: Arc<RwLock<Option<u128>>>,
    registrations_seen: Arc<AtomicU64>,
    accounts_funded: Arc<AtomicU64>,
    accounts_skipped: Arc<AtomicU64>,
    fund_failures: Arc<AtomicU64>,
    started_at: std::time::Instant,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    people_connected: bool,
    asset_hub_connected: bool,
    last_block_at: Option<u64>,
    cursor_block_hash: Option<String>,
    funding_key_balance: Option<u128>,
    registrations_seen: u64,
    accounts_funded: u64,
    accounts_skipped: u64,
    fund_failures: u64,
    uptime_seconds: u64,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let people_connected = *state.people_connected.read().await;
    let asset_hub_connected = *state.asset_hub_connected.read().await;
    let fund_failures = state.fund_failures.load(Ordering::Relaxed);

    // Dynamic status: a hardcoded "ok" while disconnected or failing is useless
    // to an operator/agent watching the endpoint.
    let status = if fund_failures >= UNHEALTHY_FAILURE_THRESHOLD {
        "unhealthy"
    } else if !people_connected || !asset_hub_connected || fund_failures > 0 {
        "degraded"
    } else {
        "ok"
    };

    Json(HealthResponse {
        status,
        people_connected,
        asset_hub_connected,
        last_block_at: *state.last_block_at.read().await,
        cursor_block_hash: state.cursor_block_hash.read().await.clone(),
        funding_key_balance: *state.funding_key_balance.read().await,
        registrations_seen: state.registrations_seen.load(Ordering::Relaxed),
        accounts_funded: state.accounts_funded.load(Ordering::Relaxed),
        accounts_skipped: state.accounts_skipped.load(Ordering::Relaxed),
        fund_failures,
        uptime_seconds: state.started_at.elapsed().as_secs(),
    })
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "flow_funder=info".into()),
        )
        .init();

    let cli = Cli::parse();

    // Fat-finger guard on the funding amount (#17).
    if cli.amount == 0 {
        return Err("FUNDER_AMOUNT_PLANCK must be greater than 0".into());
    }
    if cli.amount > cli.max_amount {
        return Err(format!(
            "FUNDER_AMOUNT_PLANCK ({}) exceeds --max-amount-planck ({}); raise the ceiling deliberately if intended",
            cli.amount, cli.max_amount
        )
        .into());
    }

    let state = AppState {
        people_connected: Arc::new(RwLock::new(false)),
        asset_hub_connected: Arc::new(RwLock::new(false)),
        last_block_at: Arc::new(RwLock::new(None)),
        cursor_block_hash: Arc::new(RwLock::new(None)),
        funding_key_balance: Arc::new(RwLock::new(None)),
        registrations_seen: Arc::new(AtomicU64::new(0)),
        accounts_funded: Arc::new(AtomicU64::new(0)),
        accounts_skipped: Arc::new(AtomicU64::new(0)),
        fund_failures: Arc::new(AtomicU64::new(0)),
        started_at: std::time::Instant::now(),
    };

    // Bind the health endpoint in `main` so a bind failure fails startup loudly,
    // instead of a `.expect` panic silently killing a spawned task (#6).
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", cli.health_bind, cli.health_port))
        .await
        .map_err(|e| {
            format!("failed to bind health endpoint on {}:{}: {e}", cli.health_bind, cli.health_port)
        })?;
    info!(bind = %cli.health_bind, port = cli.health_port, "health endpoint listening");
    {
        let health_state = state.clone();
        tokio::spawn(async move {
            let app = Router::new()
                .route("/health", axum::routing::get(health))
                .with_state(health_state);
            if let Err(e) = axum::serve(listener, app).await {
                error!(error = %e, "health server stopped");
            }
        });
    }

    let signer = build_signer(&cli)?;
    let mut funder = Funder::connect(
        &cli.asset_hub_url,
        signer,
        cli.amount,
        cli.max_submit_retries,
        cli.batch_size,
    )
    .await
    .map_err(|e| format!("failed to connect to Asset Hub: {e}"))?;
    *state.asset_hub_connected.write().await = true;
    let signer_account = *funder.signer_account();

    let cursor_store = CursorStore::new(cli.cursor_file.clone());

    info!(
        people_url = %cli.people_url,
        asset_hub_url = %cli.asset_hub_url,
        funder = %signer_account,
        amount_planck = cli.amount,
        batch_size = cli.batch_size,
        cursor_file = %cli.cursor_file,
        dry_run = cli.dry_run,
        "starting flow-funder (LitePeople storageDiff → Asset Hub batch_all transfer_keep_alive)"
    );

    // Periodically refresh the funding key's balance on a dedicated read-only
    // connection so /health surfaces funding exhaustion proactively (#10).
    {
        let state = state.clone();
        let url = cli.asset_hub_url.clone();
        let account = signer_account;
        tokio::spawn(async move {
            loop {
                match RpcClient::from_url(&url).await {
                    Ok(rpc) => {
                        match asset_hub::fetch_free_balances(&rpc, std::slice::from_ref(&account)).await {
                            Ok(balances) => {
                                *state.funding_key_balance.write().await = balances.first().copied();
                            }
                            Err(e) => warn!(error = %e, "balance gauge query failed"),
                        }
                    }
                    Err(e) => warn!(error = %e, "balance gauge connect failed"),
                }
                tokio::time::sleep(BALANCE_GAUGE_INTERVAL).await;
            }
        });
    }

    // Graceful shutdown (#15): first Ctrl-C sets the flag so the current cycle
    // finishes its in-flight work and stops before the next block; a second
    // Ctrl-C force-quits.
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_notify = Arc::new(Notify::new());
    {
        let shutdown = shutdown.clone();
        let shutdown_notify = shutdown_notify.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("shutdown signal received — finishing in-flight work; press Ctrl-C again to force-quit");
                shutdown.store(true, Ordering::SeqCst);
                shutdown_notify.notify_waiters();
            }
            let _ = tokio::signal::ctrl_c().await;
            warn!("second shutdown signal — force-quitting");
            std::process::exit(130);
        });
    }

    let mut backoff = RECONNECT_BACKOFF_INITIAL;

    while !shutdown.load(Ordering::SeqCst) {
        match run_people_cycle(&cli, &mut funder, &cursor_store, &state, &shutdown).await {
            Ok(()) => {
                // Clean stream end (or graceful stop): reset backoff.
                backoff = RECONNECT_BACKOFF_INITIAL;
            }
            Err(e) => {
                *state.people_connected.write().await = false;
                error!(error = %e, backoff_secs = backoff.as_secs(), "People connection cycle errored");
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = shutdown_notify.notified() => {}
                }
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }

    info!(
        registrations_seen = state.registrations_seen.load(Ordering::Relaxed),
        accounts_funded = state.accounts_funded.load(Ordering::Relaxed),
        accounts_skipped = state.accounts_skipped.load(Ordering::Relaxed),
        fund_failures = state.fund_failures.load(Ordering::Relaxed),
        uptime_seconds = state.started_at.elapsed().as_secs(),
        "shutting down"
    );
    Ok(())
}

/// Whether a processed block's cursor may advance, or must stay pinned for retry.
enum ProcessOutcome {
    Advance,
    Pin,
}

/// One People-chain connection cycle: connect, resolve the starting block from
/// the persisted cursor (or the finalized head on first boot), then process
/// finalized blocks until the subscription ends, the stream goes silent, or
/// shutdown is requested.
async fn run_people_cycle(
    cli: &Cli,
    funder: &mut Funder,
    cursor_store: &CursorStore,
    state: &AppState,
    shutdown: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(url = %cli.people_url, "connecting to People chain");
    let rpc = RpcClient::from_url(&cli.people_url).await?;
    let api = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc.clone()).await?;
    *state.people_connected.write().await = true;

    // Resume from the persisted cursor; only first boot starts at the tip.
    let mut prev_block_hash = match cursor_store.load().await? {
        Some(hash) => {
            info!(cursor = %format_args!("{hash:?}"), "resuming from persisted cursor");
            *state.cursor_block_hash.write().await = Some(format!("{hash:?}"));
            hash
        }
        None => {
            let head = finalized_head(&rpc).await?;
            info!(head = %format_args!("{head:?}"), "first boot — starting from finalized head");
            head
        }
    };

    let mut sub = api.stream_blocks().await?;
    info!("subscribed to finalized blocks");

    loop {
        // Stop between blocks (never mid-submit) so in-flight funding completes.
        if shutdown.load(Ordering::SeqCst) {
            info!("stopping People cycle for shutdown");
            return Ok(());
        }

        // Bound the wait: a WS that stays open but stops sending must trigger a
        // reconnect, not an indefinite hang (#4).
        let block = match tokio::time::timeout(BLOCK_RECV_TIMEOUT, sub.next()).await {
            Ok(Some(block_result)) => block_result?,
            Ok(None) => return Ok(()), // stream closed cleanly (#16)
            Err(_) => return Err("People block stream timed out — reconnecting".into()),
        };

        let block_number = block.number();
        let block_hash = block.hash();
        *state.last_block_at.write().await = Some(unix_timestamp_now());

        match process_block(cli, funder, state, &rpc, block_number, block_hash, prev_block_hash).await
        {
            ProcessOutcome::Advance => {
                // Advance the live cursor. Persist it too, unless this is a dry
                // run (a dry run must not advance a real cursor past unfunded
                // accounts).
                prev_block_hash = block_hash;
                if !cli.dry_run {
                    cursor_store.save(block_hash).await?;
                    *state.cursor_block_hash.write().await = Some(format!("{block_hash:?}"));
                }
            }
            ProcessOutcome::Pin => {
                // Leave prev_block_hash unchanged so the next diff re-covers this
                // block; the balance check skips accounts already funded.
            }
        }
    }
}

/// Diff one block, log its registrations, and fund them. Returns whether the
/// cursor may advance. Pure plumbing extracted from the cycle loop (#19).
async fn process_block(
    cli: &Cli,
    funder: &mut Funder,
    state: &AppState,
    rpc: &RpcClient,
    block_number: u64,
    block_hash: H256,
    prev_block_hash: H256,
) -> ProcessOutcome {
    let added = match diff_added_accounts(rpc, block_hash, prev_block_hash).await {
        Ok(accounts) => accounts,
        Err(e) => {
            warn!(block = block_number, error = %e, "storageDiff failed — leaving cursor pinned");
            return ProcessOutcome::Pin;
        }
    };

    let accounts: Vec<AccountId32> = added.into_iter().map(AccountId32).collect();
    for account in &accounts {
        state.registrations_seen.fetch_add(1, Ordering::Relaxed);
        info!(
            account = %account,
            block = block_number,
            block_hash = %format_args!("{block_hash:?}"),
            "new lite identity registered on People chain"
        );
    }

    if accounts.is_empty() {
        return ProcessOutcome::Advance;
    }

    if handle_block(funder, &accounts, block_number, cli, state).await {
        ProcessOutcome::Advance
    } else {
        warn!(
            block = block_number,
            "block has unhandled accounts — cursor pinned; block will be retried (funded accounts are skipped)"
        );
        ProcessOutcome::Pin
    }
}

/// Handle all of a block's registrations in one batched pass. Returns `true` if
/// the block was fully handled (funded/skipped, or — in dry-run — planned),
/// `false` if funding failed (so the caller pins the cursor and retries later).
async fn handle_block(
    funder: &mut Funder,
    accounts: &[AccountId32],
    block_number: u64,
    cli: &Cli,
    state: &AppState,
) -> bool {
    if cli.dry_run {
        match funder.dry_run_batch(accounts).await {
            Ok(report) => info!(
                block = block_number,
                would_fund = report.would_fund,
                skipped = report.skipped,
                batches = report.batches,
                encoded_bytes = report.encoded_bytes,
                "dry-run — batch_all encodes against live metadata; not submitting"
            ),
            Err(e) => warn!(block = block_number, error = %e, "dry-run batch planning failed"),
        }
        return true;
    }

    match funder.fund_batch(accounts).await {
        Ok(outcome) => {
            *state.asset_hub_connected.write().await = true;
            state
                .accounts_funded
                .fetch_add(outcome.funded as u64, Ordering::Relaxed);
            state
                .accounts_skipped
                .fetch_add(outcome.skipped as u64, Ordering::Relaxed);
            info!(
                block = block_number,
                funded = outcome.funded,
                skipped = outcome.skipped,
                batches = outcome.tx_hashes.len(),
                tx_hashes = ?outcome.tx_hashes,
                "block funded"
            );
            true
        }
        Err(e) => {
            state.fund_failures.fetch_add(1, Ordering::Relaxed);
            error!(block = block_number, error = %e, "batch funding failed — attempting Asset Hub reconnect");
            match funder.reconnect(&cli.asset_hub_url).await {
                Ok(()) => *state.asset_hub_connected.write().await = true,
                Err(re) => {
                    *state.asset_hub_connected.write().await = false;
                    error!(error = %re, "Asset Hub reconnect failed; will retry when the block is reprocessed");
                }
            }
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_signer(cli: &Cli) -> Result<Keypair, Box<dyn std::error::Error>> {
    let seed_phrase = match cli.seed_phrase.as_deref() {
        Some(seed_phrase) => seed_phrase,
        None if cli.dry_run => {
            "bottom drive obey lake curtain smoke basket hold race lonely fit walk"
        }
        None => {
            return Err("FUNDER_SEED_PHRASE or --seed-phrase is required for live funding".into());
        }
    };
    let derivation_path = cli
        .derivation_path
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("//Alice");

    let suri = format!("{seed_phrase}{derivation_path}");
    let uri: SecretUri = suri
        .parse()
        .map_err(|e| format!("invalid secret URI: {e}"))?;
    let keypair = Keypair::from_uri(&uri).map_err(|e| format!("invalid seed phrase: {e}"))?;
    Ok(keypair)
}

fn unix_timestamp_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli_for_signer(dry_run: bool, seed_phrase: Option<&str>) -> Cli {
        Cli {
            people_url: "ws://people".to_string(),
            asset_hub_url: "ws://asset-hub".to_string(),
            seed_phrase: seed_phrase.map(str::to_string),
            derivation_path: None,
            amount: 100,
            max_amount: 1_000_000_000_000_000,
            max_submit_retries: 3,
            batch_size: 100,
            cursor_file: "cursor.txt".to_string(),
            dry_run,
            health_port: 3033,
            health_bind: "127.0.0.1".to_string(),
        }
    }

    #[test]
    fn build_signer_requires_explicit_seed_for_live_funding() {
        let err = build_signer(&cli_for_signer(false, None)).unwrap_err();
        assert!(err.to_string().contains("required for live funding"));
    }

    #[test]
    fn build_signer_allows_dev_seed_for_dry_run() {
        assert!(build_signer(&cli_for_signer(true, None)).is_ok());
    }
}
