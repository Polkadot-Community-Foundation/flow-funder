//! flow-funder — watch the People chain for newly-registered lite identities and
//! reserve each one's username as a dotns name on Asset Hub.
//!
//! Pattern:
//!   * a People-chain watcher streams finalized blocks and decodes every
//!     `PeopleLite::attest` call (including those nested in `Utility` batches),
//!     harvesting the username plus the crypto inputs the identity-backend already
//!     signed (`attest` module);
//!   * for each username it submits `DotnsGateway::reserve_name` on Asset Hub,
//!     signed with the allowance-holding key (`asset_hub` module). This works
//!     because `reserve_name` and `attest` verify the byte-identical message, so
//!     the `candidate_signature`/`proof_of_ownership` from `attest` also validate
//!     `reserve_name` — every input comes straight off-chain.
//!     A `DotnsGateway::LiteLabelOwner` check makes it idempotent — a name already
//!     reserved is skipped, so retries and restarts never double-submit.
//!
//! Reservation is synchronous per block and gated by a persisted cursor
//! (`cursor` module): a block's cursor advances only once ALL of its names have
//! been handled. So
//!   * a connection loss resumes from the cursor (not the finalized head) — no
//!     registration in the downtime window is missed; the watcher walks every
//!     block from the cursor up to the streamed head; and
//!   * a reservation failure leaves the cursor pinned, so the block is re-decoded
//!     and the failed name retried, while already-reserved names are skipped.
//!
//! On first boot (no cursor) the watcher starts from the current finalized head
//! unless `--start-from-genesis` is set, in which case it walks from block 0 up
//! to the streamed finalized head.

mod asset_hub;
mod attest;
mod chain;
mod cursor;
mod registration;
mod signer;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, Json, Router};
use clap::Parser;
use serde::Serialize;
use subxt::config::{Header, SubstrateConfig};
use subxt::utils::{AccountId32, H256};
use subxt::OnlineClient;
use subxt_rpcs::RpcClient;
use subxt_signer::sr25519::Keypair;
use subxt_signer::SecretUri;
use tokio::sync::{Notify, RwLock};
use tracing::{error, info, warn};

use crate::asset_hub::Reserver;
use crate::attest::reservations_in_block;
use crate::chain::finalized_head;
use crate::cursor::CursorStore;

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
        default_value = "wss://summit-people-rpc.polkadot.io"
    )]
    people_url: String,

    /// WebSocket URL of the Asset Hub node hosting the dotns gateway.
    #[arg(
        long,
        env = "ASSET_HUB_NODE_URL",
        default_value = "wss://summit-asset-hub-rpc.polkadot.io"
    )]
    asset_hub_url: String,

    /// Sr25519 seed phrase for the reserving (signing) account. With proxy
    /// delegation this is the delegate key. One of --seed-phrase or --secret-key
    /// is required unless --dry-run is set.
    #[arg(long, env = "RESERVER_SEED_PHRASE")]
    seed_phrase: Option<String>,

    /// Raw 32-byte sr25519 secret key (hex, with or without 0x) for the signing
    /// account — the format identity-backend uses (`ATTESTER_PROXY_PRIVATE_KEY`).
    /// Takes precedence over --seed-phrase. Ignores --derivation-path.
    #[arg(long, env = "RESERVER_SECRET_KEY")]
    secret_key: Option<String>,

    /// Derivation path appended to the seed phrase (e.g. "//Alice"). Defaults to
    /// //Alice. Only applies to --seed-phrase, not --secret-key.
    #[arg(long, env = "RESERVER_DERIVATION_PATH")]
    derivation_path: Option<String>,

    /// SS58 address of the account that holds `AttestationAllowance` on the dotns
    /// gateway, when the signer is its proxy delegate rather than the holder
    /// itself. If set, `reserve_name` is submitted wrapped in `Proxy.proxy(real =
    /// this account, …)`. Omit when the signer holds the allowance directly.
    #[arg(long, env = "RESERVER_PROXY_FOR")]
    proxy_for: Option<String>,

    /// Max `reserve_name` calls per `Utility.force_batch` extrinsic (matches
    /// identity-backend's dotns batch size).
    #[arg(long, env = "RESERVER_BATCH_SIZE", default_value = "50")]
    batch_size: usize,

    /// Maximum submission retries per batch on transient pool rejections.
    #[arg(long, env = "RESERVER_MAX_SUBMIT_RETRIES", default_value = "3")]
    max_submit_retries: u32,

    /// Max People-chain blocks to decode ahead before submitting reservations.
    /// During backfill this lets sparse registrations from multiple blocks share
    /// one Asset Hub batch, while the persisted cursor still advances in order.
    #[arg(long, env = "RESERVER_SCAN_WINDOW_BLOCKS", default_value = "500")]
    scan_window_blocks: u64,

    /// File holding the resume cursor (last fully-handled People block hash).
    #[arg(
        long,
        env = "RESERVER_CURSOR_FILE",
        default_value = "flow-reserver-cursor.txt"
    )]
    cursor_file: String,

    /// On first boot (no cursor file), process from People-chain block 0 instead
    /// of starting at the current finalized head. Existing cursors still win; use
    /// a fresh cursor file to intentionally backfill from genesis.
    #[arg(long, env = "RESERVER_START_FROM_GENESIS", default_value = "false")]
    start_from_genesis: bool,

    /// Detect and log registrations but never submit a `reserve_name` (or persist
    /// a cursor — a dry run must not advance a real cursor).
    #[arg(long, env = "RESERVER_DRY_RUN", default_value = "false")]
    dry_run: bool,

    /// Port for the health check HTTP server.
    #[arg(long, env = "RESERVER_HEALTH_PORT", default_value = "3033")]
    health_port: u16,

    /// Address to bind the health server to. Defaults to localhost; set to
    /// `0.0.0.0` only if `/health` must be reachable off-host (it exposes
    /// operational counters and has no auth).
    #[arg(long, env = "RESERVER_HEALTH_BIND", default_value = "127.0.0.1")]
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
/// `reserve_failures` at/above this flips `/health` status to `unhealthy`.
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
    registrations_seen: Arc<AtomicU64>,
    names_reserved: Arc<AtomicU64>,
    names_skipped: Arc<AtomicU64>,
    reserve_failures: Arc<AtomicU64>,
    started_at: std::time::Instant,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    people_connected: bool,
    asset_hub_connected: bool,
    last_block_at: Option<u64>,
    cursor_block_hash: Option<String>,
    registrations_seen: u64,
    names_reserved: u64,
    names_skipped: u64,
    reserve_failures: u64,
    uptime_seconds: u64,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let people_connected = *state.people_connected.read().await;
    let asset_hub_connected = *state.asset_hub_connected.read().await;
    let reserve_failures = state.reserve_failures.load(Ordering::Relaxed);

    // Dynamic status: a hardcoded "ok" while disconnected or repeatedly failing is
    // useless to an operator/agent watching the endpoint.
    let status = if reserve_failures >= UNHEALTHY_FAILURE_THRESHOLD {
        "unhealthy"
    } else if !people_connected || !asset_hub_connected {
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
        registrations_seen: state.registrations_seen.load(Ordering::Relaxed),
        names_reserved: state.names_reserved.load(Ordering::Relaxed),
        names_skipped: state.names_skipped.load(Ordering::Relaxed),
        reserve_failures,
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
        asset_hub_connected: Arc::new(RwLock::new(false)),
        last_block_at: Arc::new(RwLock::new(None)),
        cursor_block_hash: Arc::new(RwLock::new(None)),
        registrations_seen: Arc::new(AtomicU64::new(0)),
        names_reserved: Arc::new(AtomicU64::new(0)),
        names_skipped: Arc::new(AtomicU64::new(0)),
        reserve_failures: Arc::new(AtomicU64::new(0)),
        started_at: std::time::Instant::now(),
    };

    // Bind the health endpoint in `main` so a bind failure fails startup loudly,
    // instead of a `.expect` panic silently killing a spawned task.
    let listener =
        tokio::net::TcpListener::bind(format!("{}:{}", cli.health_bind, cli.health_port))
            .await
            .map_err(|e| {
                format!(
                    "failed to bind health endpoint on {}:{}: {e}",
                    cli.health_bind, cli.health_port
                )
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
    let proxy_for = match cli.proxy_for.as_deref() {
        Some(s) => Some(
            s.parse::<AccountId32>()
                .map_err(|e| format!("invalid --proxy-for SS58 address {s:?}: {e}"))?,
        ),
        None => None,
    };
    let mut reserver = Reserver::connect(
        &cli.asset_hub_url,
        signer,
        proxy_for,
        cli.batch_size,
        cli.max_submit_retries,
    )
    .await
    .map_err(|e| format!("failed to connect to Asset Hub: {e}"))?;
    *state.asset_hub_connected.write().await = true;
    let signer_account = *reserver.signer_account();

    let cursor_store = CursorStore::new(cli.cursor_file.clone());

    info!(
        people_url = %cli.people_url,
        asset_hub_url = %cli.asset_hub_url,
        reserver = %signer_account,
        proxy_for = ?proxy_for.as_ref().map(|a| a.to_string()),
        batch_size = cli.batch_size,
        cursor_file = %cli.cursor_file,
        dry_run = cli.dry_run,
        "starting flow-funder (PeopleLite::attest → DotnsGateway::reserve_name)"
    );

    // Graceful shutdown: first Ctrl-C sets the flag so the current cycle finishes
    // its in-flight work and stops before the next block; a second Ctrl-C
    // force-quits.
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
        match run_people_cycle(&cli, &mut reserver, &cursor_store, &state, &shutdown).await {
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
        names_reserved = state.names_reserved.load(Ordering::Relaxed),
        names_skipped = state.names_skipped.load(Ordering::Relaxed),
        reserve_failures = state.reserve_failures.load(Ordering::Relaxed),
        uptime_seconds = state.started_at.elapsed().as_secs(),
        "shutting down"
    );
    Ok(())
}

/// One People-chain connection cycle: connect, resolve the starting block from
/// the persisted cursor (or the finalized head on first boot), then process every
/// finalized block up to the streamed head until the subscription ends, the
/// stream goes silent, or shutdown is requested.
async fn run_people_cycle(
    cli: &Cli,
    reserver: &mut Reserver,
    cursor_store: &CursorStore,
    state: &AppState,
    shutdown: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(url = %cli.people_url, "connecting to People chain");
    let rpc = RpcClient::from_url(&cli.people_url).await?;
    let api = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc.clone()).await?;
    *state.people_connected.write().await = true;

    // Resolve the last fully-handled block. By default first boot starts at the
    // tip; explicit backfill mode starts from genesis when no cursor exists.
    let mut last_number = match cursor_store.load().await? {
        Some(hash) => {
            let number = api.at_block(hash).await?.block_header().await?.number();
            info!(cursor = %format_args!("{hash:?}"), number, "resuming from persisted cursor");
            *state.cursor_block_hash.write().await = Some(format!("{hash:?}"));
            number
        }
        None if cli.start_from_genesis => {
            let genesis = api.at_block(0u64).await?;
            let hash = genesis.block_hash();
            info!(genesis = %format_args!("{hash:?}"), "first boot with --start-from-genesis — starting from block 0");
            0
        }
        None => {
            let head = finalized_head(&rpc).await?;
            let number = api.at_block(head).await?.block_header().await?.number();
            info!(head = %format_args!("{head:?}"), number, "first boot — starting from finalized head");
            number
        }
    };

    let mut sub = api.stream_blocks().await?;
    info!("subscribed to finalized blocks");

    loop {
        // Stop between blocks (never mid-submit) so in-flight work completes.
        if shutdown.load(Ordering::SeqCst) {
            info!("stopping People cycle for shutdown");
            return Ok(());
        }

        // Bound the wait: a WS that stays open but stops sending must trigger a
        // reconnect, not an indefinite hang.
        let block = match tokio::time::timeout(BLOCK_RECV_TIMEOUT, sub.next()).await {
            Ok(Some(block_result)) => block_result?,
            Ok(None) => return Ok(()), // stream closed cleanly
            Err(_) => return Err("People block stream timed out — reconnecting".into()),
        };
        let target = block.number();
        *state.last_block_at.write().await = Some(unix_timestamp_now());

        // Decode a bounded window of finalized People blocks ahead of the cursor,
        // then reserve all names from that window together. This preserves ordered
        // cursor advancement while allowing sparse backfill registrations from
        // multiple People blocks to share Asset Hub transactions.
        while last_number < target {
            if shutdown.load(Ordering::SeqCst) {
                info!("stopping People cycle for shutdown");
                return Ok(());
            }

            let queued = match collect_block_window(&api, state, last_number, target, cli).await {
                Ok(queued) => queued,
                Err(e) => {
                    warn!(
                        next_block = last_number + 1,
                        error = %e,
                        "decoding attests failed — leaving cursor pinned"
                    );
                    break;
                }
            };

            if queued.is_empty() {
                break;
            }

            if shutdown.load(Ordering::SeqCst) {
                info!("stopping People cycle for shutdown");
                return Ok(());
            }

            if handle_window(reserver, &queued, cli, state).await {
                for block in queued {
                    last_number = block.number;
                    // Persist the cursor, unless this is a dry run (a dry run must
                    // not advance a real cursor past unreserved names).
                    if !cli.dry_run {
                        cursor_store.save(block.hash).await?;
                        *state.cursor_block_hash.write().await = Some(format!("{:?}", block.hash));
                    }
                }
            } else {
                warn!(
                    first_block = queued.first().map(|b| b.number),
                    last_block = queued.last().map(|b| b.number),
                    "window has unreserved names — cursor pinned; window will be retried (reserved names are skipped)"
                );
                break;
            }
        }
    }
}

struct QueuedBlock {
    number: u64,
    hash: H256,
    inputs: Vec<attest::ReservationInputs>,
}

/// Decode a bounded range of People blocks ahead of the cursor. The window stops
/// once it has enough names to fill at least one Asset Hub batch, or once the
/// configured scan window / streamed target is reached. Empty blocks are queued
/// too, so their cursors can advance after the combined reservation succeeds.
async fn collect_block_window(
    api: &OnlineClient<SubstrateConfig>,
    state: &AppState,
    last_number: u64,
    target: u64,
    cli: &Cli,
) -> Result<Vec<QueuedBlock>, Box<dyn std::error::Error + Send + Sync>> {
    let scan_limit = cli.scan_window_blocks.max(1);
    let name_limit = cli.batch_size.max(1);
    let mut queued = Vec::new();
    let mut queued_names = 0usize;

    while last_number + (queued.len() as u64) < target
        && (queued.len() as u64) < scan_limit
        && queued_names < name_limit
    {
        let number = last_number + queued.len() as u64 + 1;
        let at = api.at_block(number).await?;
        let hash = at.block_hash();
        let inputs = reservations_in_block(&at).await?;

        for input in &inputs {
            state.registrations_seen.fetch_add(1, Ordering::Relaxed);
            info!(
                username = %String::from_utf8_lossy(&input.lite_label),
                block = number,
                block_hash = %format_args!("{hash:?}"),
                "username registered on People chain"
            );
        }

        queued_names += inputs.len();
        queued.push(QueuedBlock {
            number,
            hash,
            inputs,
        });
    }

    Ok(queued)
}

/// Reserve all names in a decoded window. Returns `true` if the whole window was
/// handled and its cursors may advance, or `false` if the first cursor must stay
/// pinned for retry.
async fn handle_window(
    reserver: &mut Reserver,
    queued: &[QueuedBlock],
    cli: &Cli,
    state: &AppState,
) -> bool {
    let first_block = queued.first().map(|b| b.number).unwrap_or_default();
    let last_block = queued.last().map(|b| b.number).unwrap_or_default();
    let blocks = queued.len();
    let inputs: Vec<_> = queued
        .iter()
        .flat_map(|block| block.inputs.iter().cloned())
        .collect();

    if inputs.is_empty() {
        return true;
    }

    if cli.dry_run {
        match reserver.dry_run_block(&inputs).await {
            Ok(report) => info!(
                first_block,
                last_block,
                blocks,
                registrations = inputs.len(),
                would_reserve = report.would_reserve,
                skipped = report.skipped,
                encoded_bytes = report.encoded_bytes,
                "dry-run — reserve_name encodes against live metadata; not submitting"
            ),
            Err(e) => warn!(
                first_block,
                last_block,
                blocks,
                error = %e,
                "dry-run reservation planning failed"
            ),
        }
        return true;
    }

    match reserver.reserve_block(&inputs).await {
        Ok(outcome) => {
            *state.asset_hub_connected.write().await = true;
            state
                .names_reserved
                .fetch_add(outcome.reserved as u64, Ordering::Relaxed);
            state
                .names_skipped
                .fetch_add(outcome.skipped as u64, Ordering::Relaxed);
            info!(
                first_block,
                last_block,
                blocks,
                registrations = inputs.len(),
                reserved = outcome.reserved,
                skipped = outcome.skipped,
                tx_hashes = ?outcome.tx_hashes,
                "window reserved"
            );
            true
        }
        Err(e) => {
            state.reserve_failures.fetch_add(1, Ordering::Relaxed);
            error!(
                first_block,
                last_block,
                blocks,
                registrations = inputs.len(),
                error = %e,
                "reservation failed — attempting Asset Hub reconnect"
            );
            match reserver.reconnect(&cli.asset_hub_url).await {
                Ok(()) => *state.asset_hub_connected.write().await = true,
                Err(re) => {
                    *state.asset_hub_connected.write().await = false;
                    error!(error = %re, "Asset Hub reconnect failed; will retry when the window is reprocessed");
                }
            }
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_signer(cli: &Cli) -> Result<signer::ReserverSigner, Box<dyn std::error::Error>> {
    // A raw secret key takes precedence. Two accepted forms for the same account:
    //   * 32-byte sr25519 mini-secret (seed), or
    //   * 64-byte Ed25519-expanded secret key — identity-backend's
    //     ATTESTER_PROXY_PRIVATE_KEY (polkadot-js / PAPI sr25519 private key).
    if let Some(hex_key) = cli.secret_key.as_deref() {
        let bytes = hex::decode(hex_key.trim().strip_prefix("0x").unwrap_or(hex_key.trim()))
            .map_err(|e| format!("invalid --secret-key hex: {e}"))?;
        return match bytes.len() {
            32 => {
                let secret: [u8; 32] = bytes.try_into().expect("checked len == 32");
                Ok(signer::ReserverSigner::from_subxt(
                    Keypair::from_secret_key(secret)
                        .map_err(|e| format!("invalid 32-byte --secret-key: {e}"))?,
                ))
            }
            64 => signer::ReserverSigner::from_ed25519_expanded(&bytes).map_err(Into::into),
            n => Err(format!(
                "--secret-key must be a 32-byte sr25519 mini-secret or a 64-byte \
                 Ed25519-expanded secret key (hex); got {n} bytes"
            )
            .into()),
        };
    }

    let seed_phrase = match cli.seed_phrase.as_deref() {
        Some(seed_phrase) => seed_phrase,
        None if cli.dry_run => {
            "bottom drive obey lake curtain smoke basket hold race lonely fit walk"
        }
        None => {
            return Err(
                "one of --secret-key or --seed-phrase is required (the signing key — \
                 with proxy delegation, identity-backend's ATTESTER_PROXY_PRIVATE_KEY) \
                 for live reservation"
                    .into(),
            );
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
    Ok(signer::ReserverSigner::from_subxt(keypair))
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
            secret_key: None,
            derivation_path: None,
            proxy_for: None,
            batch_size: 50,
            max_submit_retries: 3,
            scan_window_blocks: 500,
            cursor_file: "cursor.txt".to_string(),
            start_from_genesis: false,
            dry_run,
            health_port: 3033,
            health_bind: "127.0.0.1".to_string(),
        }
    }

    #[test]
    fn build_signer_requires_explicit_seed_for_live_reservation() {
        let err = build_signer(&cli_for_signer(false, None)).unwrap_err();
        assert!(err.to_string().contains("required"));
    }

    #[test]
    fn build_signer_allows_dev_seed_for_dry_run() {
        assert!(build_signer(&cli_for_signer(true, None)).is_ok());
    }

    #[test]
    fn build_signer_accepts_raw_secret_key_hex() {
        let mut cli = cli_for_signer(false, None);
        cli.secret_key = Some(format!("0x{}", "11".repeat(32))); // 32-byte mini-secret
        assert!(build_signer(&cli).is_ok());
    }

    #[test]
    fn build_signer_rejects_wrong_length_secret_key() {
        let mut cli = cli_for_signer(false, None);
        cli.secret_key = Some("0x1234".to_string());
        let err = build_signer(&cli).unwrap_err().to_string();
        assert!(err.contains("got 2 bytes"), "unexpected error: {err}");
    }

    #[test]
    fn build_signer_accepts_64_byte_expanded_secret_key() {
        // 64-byte Ed25519-expanded secret of the all-7s mini-secret.
        use schnorrkel::{ExpansionMode, MiniSecretKey};
        let expanded = MiniSecretKey::from_bytes(&[7u8; 32])
            .unwrap()
            .expand_to_keypair(ExpansionMode::Ed25519)
            .secret
            .to_ed25519_bytes();
        let mut cli = cli_for_signer(false, None);
        cli.secret_key = Some(format!("0x{}", hex::encode(expanded)));
        assert!(build_signer(&cli).is_ok());
    }
}
