//! flow-funder — watch the People chain for newly-registered lite identities and
//! fund each one with native tokens on Asset Hub.
//!
//! Pattern (after mission-control's flow-people + flow-attester):
//!   * a People-chain watcher runs `archive_v1_storageDiff` over the
//!     `PeopleLite::LitePeople` prefix per finalized block and emits every newly
//!     *added* account on an mpsc channel (`people` module);
//!   * a funder consumes the channel, checks the account's Asset Hub balance,
//!     and submits `Balances.transfer_keep_alive` if it's below target
//!     (`asset_hub` module). The balance check makes the bot idempotent — an
//!     account already at/above target is skipped, so restarts never double-fund.
//!
//! Unlike flow-people, there is NO genesis bootstrap: the watcher starts from
//! the current finalized head, so only registrations that happen while the bot
//! runs are funded — existing accounts are never retroactively funded.

mod asset_hub;
mod people;
mod registration;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, Json, Router};
use clap::Parser;
use serde::Serialize;
use subxt::config::SubstrateConfig;
use subxt::OnlineClient;
use subxt_rpcs::RpcClient;
use subxt_signer::sr25519::Keypair;
use subxt_signer::SecretUri;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use crate::asset_hub::{FundOutcome, Funder};
use crate::people::{finalized_head, watch_and_emit, NewRegistration};

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

    /// Sr25519 seed phrase for the funding account. Defaults to the dev mnemonic.
    #[arg(long, env = "FUNDER_SEED_PHRASE")]
    seed_phrase: Option<String>,

    /// Derivation path appended to the seed (e.g. "//Alice"). Defaults to //Alice.
    #[arg(long, env = "FUNDER_DERIVATION_PATH")]
    derivation_path: Option<String>,

    /// Amount to transfer per newly-registered account, in plancks.
    /// Paseo's native token (PAS) has 10 decimals, so 10_000_000_000 = 1 PAS.
    #[arg(long, env = "FUNDER_AMOUNT_PLANCK", default_value = "10000000000")]
    amount: u128,

    /// Maximum submission retries per transfer on transient pool rejections.
    #[arg(long, env = "FUNDER_MAX_SUBMIT_RETRIES", default_value = "3")]
    max_submit_retries: u32,

    /// Detect and log registrations but never submit a transfer.
    #[arg(long, env = "FUNDER_DRY_RUN", default_value = "false")]
    dry_run: bool,

    /// Port for the health check HTTP server.
    #[arg(long, env = "FUNDER_HEALTH_PORT", default_value = "3033")]
    health_port: u16,
}

// ---------------------------------------------------------------------------
// Health endpoint
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    people_connected: Arc<RwLock<bool>>,
    last_block_at: Arc<RwLock<Option<String>>>,
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
    last_block_at: Option<String>,
    registrations_seen: u64,
    accounts_funded: u64,
    accounts_skipped: u64,
    fund_failures: u64,
    uptime_seconds: u64,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        people_connected: *state.people_connected.read().await,
        last_block_at: state.last_block_at.read().await.clone(),
        registrations_seen: state.registrations_seen.load(Ordering::Relaxed),
        accounts_funded: state.accounts_funded.load(Ordering::Relaxed),
        accounts_skipped: state.accounts_skipped.load(Ordering::Relaxed),
        fund_failures: state.fund_failures.load(Ordering::Relaxed),
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

    let state = AppState {
        people_connected: Arc::new(RwLock::new(false)),
        last_block_at: Arc::new(RwLock::new(None)),
        registrations_seen: Arc::new(AtomicU64::new(0)),
        accounts_funded: Arc::new(AtomicU64::new(0)),
        accounts_skipped: Arc::new(AtomicU64::new(0)),
        fund_failures: Arc::new(AtomicU64::new(0)),
        started_at: std::time::Instant::now(),
    };

    // Health endpoint.
    let health_state = state.clone();
    let health_port = cli.health_port;
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", axum::routing::get(health))
            .with_state(health_state);
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{health_port}"))
            .await
            .expect("failed to bind health port");
        info!(port = health_port, "health endpoint listening");
        axum::serve(listener, app).await.expect("health server failed");
    });

    let signer = build_signer(&cli)?;
    let funder = Funder::connect(
        &cli.asset_hub_url,
        signer,
        cli.amount,
        cli.max_submit_retries,
    )
    .await
    .map_err(|e| format!("failed to connect to Asset Hub: {e}"))?;

    info!(
        people_url = %cli.people_url,
        asset_hub_url = %cli.asset_hub_url,
        funder = %funder.signer_account(),
        amount_planck = cli.amount,
        dry_run = cli.dry_run,
        "starting flow-funder (LitePeople storageDiff → Asset Hub transfer_keep_alive)"
    );

    // The funder consumes registrations from the watcher over this channel; a
    // bounded buffer applies backpressure if funding lags block production.
    let (tx, rx) = mpsc::channel::<NewRegistration>(1024);

    let consumer = tokio::spawn(run_funder(
        funder,
        rx,
        cli.clone(),
        state.clone(),
    ));

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received");
                break;
            }
            result = run_people_cycle(&cli, &tx, &state) => {
                match result {
                    Ok(()) => {
                        info!("People connection cycle ended cleanly — reconnecting");
                        backoff = Duration::from_secs(1);
                    }
                    Err(e) => {
                        *state.people_connected.write().await = false;
                        error!(error = %e, backoff_secs = backoff.as_secs(), "People connection cycle errored");
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = &mut shutdown => { info!("shutdown during backoff"); break; }
                        }
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
        }
    }

    drop(tx);
    let _ = consumer.await;

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

/// One People-chain connection cycle: connect, resolve the finalized head as the
/// starting cursor, then watch finalized blocks until the subscription ends.
async fn run_people_cycle(
    cli: &Cli,
    tx: &mpsc::Sender<NewRegistration>,
    state: &AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(url = %cli.people_url, "connecting to People chain");
    let rpc = RpcClient::from_url(&cli.people_url).await?;
    let api = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc.clone()).await?;
    *state.people_connected.write().await = true;

    // Start from the current finalized head — only react to registrations that
    // happen from now on; never retroactively fund existing accounts.
    let head = finalized_head(&rpc).await?;
    info!(head = %format_args!("{head:?}"), "starting from finalized head");

    watch_and_emit(&api, &rpc, head, tx).await
}

/// Consume registrations and fund each. Reconnects the Asset Hub client on a
/// funding error (most often a dropped WS) so the bot self-heals.
async fn run_funder(
    mut funder: Funder,
    mut rx: mpsc::Receiver<NewRegistration>,
    cli: Cli,
    state: AppState,
) {
    while let Some(registration) = rx.recv().await {
        state.registrations_seen.fetch_add(1, Ordering::Relaxed);
        *state.last_block_at.write().await = Some(unix_timestamp_now());

        if cli.dry_run {
            match funder.query_free_balance(&registration.account).await {
                Ok(free) => info!(
                    account = %registration.account,
                    block = registration.block_number,
                    free,
                    target = cli.amount,
                    would_fund = free < cli.amount,
                    "dry-run — not submitting"
                ),
                Err(e) => warn!(account = %registration.account, error = %e, "dry-run balance query failed"),
            }
            continue;
        }

        match funder.fund(&registration).await {
            Ok(FundOutcome::Funded { tx_hash, free_before }) => {
                state.accounts_funded.fetch_add(1, Ordering::Relaxed);
                info!(
                    account = %registration.account,
                    block = registration.block_number,
                    free_before,
                    tx_hash = %tx_hash,
                    "funded newly-registered account"
                );
            }
            Ok(FundOutcome::Skipped { free }) => {
                state.accounts_skipped.fetch_add(1, Ordering::Relaxed);
                info!(
                    account = %registration.account,
                    free,
                    target = cli.amount,
                    "skipped — already at/above target"
                );
            }
            Err(e) => {
                state.fund_failures.fetch_add(1, Ordering::Relaxed);
                error!(account = %registration.account, error = %e, "funding failed — attempting Asset Hub reconnect");
                if let Err(re) = funder.reconnect(&cli.asset_hub_url).await {
                    error!(error = %re, "Asset Hub reconnect failed; will retry on next registration");
                }
            }
        }
    }
    info!("funder channel closed — consumer exiting");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_signer(cli: &Cli) -> Result<Keypair, Box<dyn std::error::Error>> {
    let seed_phrase = cli
        .seed_phrase
        .as_deref()
        .unwrap_or("bottom drive obey lake curtain smoke basket hold race lonely fit walk");
    let derivation_path = cli
        .derivation_path
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("//Alice");

    let suri = format!("{seed_phrase}{derivation_path}");
    let uri: SecretUri = suri.parse().map_err(|e| format!("invalid secret URI: {e}"))?;
    let keypair = Keypair::from_uri(&uri).map_err(|e| format!("invalid seed phrase: {e}"))?;
    Ok(keypair)
}

fn unix_timestamp_now() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", d.as_secs())
}
