# flow-funder

`flow-funder` watches finalized People-chain blocks for `PeopleLite::attest` calls and reserves each attested username as a dotns name on Asset Hub.

The bot is designed to run continuously. It persists a cursor after fully handling a People-chain block, so it can resume after restarts without missing registrations.

## What It Does

```text
People chain                                      Asset Hub
------------                                      ---------
finalized block N
      |
      | decode PeopleLite::attest calls
      | including calls nested in Utility batches
      v
username + signed ownership proof
      |
      | check DotnsGateway::LiteLabelOwner
      v
already reserved? ---- yes ---> skip
      |
      no
      v
DotnsGateway::reserve_name
batched with Utility.force_batch
```

The bot uses the candidate signature and proof of ownership already present in the People-chain attest call. Asset Hub transactions are signed by the configured reserving key, optionally wrapped through `Proxy.proxy` when `RESERVER_PROXY_FOR` is set (this must match the identity backend configuration).

## Reliability

The cursor file stores the hash of the last fully handled People-chain block. The cursor only advances after every reservation in the scanned window has been reserved or skipped.

| Failure | Behaviour |
| --- | --- |
| People RPC disconnects | Reconnects with exponential backoff and resumes from the persisted cursor. |
| Asset Hub RPC disconnects | Reconnects and retries the unhandled window. Already-reserved names are skipped. |
| Reservation tx fails | Cursor stays pinned; the same window is retried. |
| Process crashes | Restart resumes from the persisted cursor. |
| Dry run | Detects and logs registrations, but does not submit transactions or persist cursor progress. |
| No cursor on first boot | Starts at finalized head by default, or at block 0 with `--start-from-genesis`. |

During backfill, `RESERVER_SCAN_WINDOW_BLOCKS` controls how many People blocks are decoded before submitting reservations. Sparse registrations across many blocks can share one Asset Hub batch, while the cursor still advances in order.

## Configuration

All options are available as CLI flags and environment variables.

| Env var | Flag | Default | Meaning |
| --- | --- | --- | --- |
| `PEOPLE_NODE_URL` | `--people-url` | `wss://paseo-people-next-system-rpc.polkadot.io` | People-chain websocket endpoint. Archive mode/backfill requires archive RPC support. |
| `ASSET_HUB_NODE_URL` | `--asset-hub-url` | `wss://paseo-asset-hub-next-rpc.polkadot.io` | Asset Hub websocket endpoint. |
| `RESERVER_SEED_PHRASE` | `--seed-phrase` | required live | Sr25519 mnemonic for the reserving signer (MUST have allowance). |
| `RESERVER_SECRET_KEY` | `--secret-key` | unset | Raw 32-byte sr25519 secret key hex. Takes precedence over seed phrase. |
| `RESERVER_DERIVATION_PATH` | `--derivation-path` | unset | Derivation path appended to `RESERVER_SEED_PHRASE`. |
| `RESERVER_PROXY_FOR` | `--proxy-for` | unset | SS58 account that holds dotns allowance when the signer is a proxy delegate. |
| `RESERVER_BATCH_SIZE` | `--batch-size` | `3` | Max `reserve_name` calls per Asset Hub batch. Batches larger than this are prone to fail. |
| `RESERVER_MAX_SUBMIT_RETRIES` | `--max-submit-retries` | `3` | Retries for transient submit/finalization failures. |
| `RESERVER_SCAN_WINDOW_BLOCKS` | `--scan-window-blocks` | `500` | Max People blocks to scan before submitting a batch. |
| `RESERVER_CURSOR_FILE` | `--cursor-file` | `flow-reserver-cursor.txt` | Cursor file path. Persist this in production. |
| `RESERVER_START_FROM_GENESIS` | `--start-from-genesis` | `false` | On first boot with no cursor, backfill from block 0. Existing cursor wins. |
| `RESERVER_DRY_RUN` | `--dry-run` | `false` | Log registrations without submitting or advancing cursor. |
| `RESERVER_HEALTH_PORT` | `--health-port` | `3033` | Health HTTP port. |
| `RESERVER_HEALTH_BIND` | `--health-bind` | `127.0.0.1` | Health bind address. Use `0.0.0.0` inside containers. |
| `RUST_LOG` | n/a | `flow_funder=info` | Tracing filter. |

Use `.env.example` as the template for local env files. Do not commit real mnemonics, secret keys, or proxy addresses.

## Local Run

Build and test:

```bash
cargo build --release
cargo test
```

Run a dry run from the current finalized head:

```bash
set -a
source .env.next
set +a

RESERVER_DRY_RUN=true cargo run --release --bin flow-funder
```

Run live in archive mode from a fresh cursor:

```bash
mkdir -p logs

set -a
source .env.next
set +a

timeout 5h ./target/release/flow-funder \
  --start-from-genesis \
  --cursor-file logs/next-genesis-backfill-cursor.txt \
  --health-port 3033 \
  >> logs/next-run.log 2>&1 &

echo $! > logs/next-run.pid
```

Resume using an existing cursor:

```bash
timeout 5h ./target/release/flow-funder \
  --start-from-genesis \
  --cursor-file logs/next-genesis-backfill-cursor-20260616-195714.txt \
  --health-port 3033 \
  >> logs/next-run.log 2>&1 &

echo $! > logs/next-run.pid
```

`--start-from-genesis` is safe with an existing cursor: the cursor takes precedence. It only matters on first boot when the cursor file does not exist.

Stop the local run:

```bash
kill "$(cat logs/next-run.pid)"
```

## Health And Monitoring

The bot serves:

```bash
curl -fsS http://127.0.0.1:3033/health
```

Example response:

```json
{
  "status": "ok",
  "people_connected": true,
  "asset_hub_connected": true,
  "last_block_at": 1781688789,
  "cursor_block_hash": "0x754bdcbf562491ae3400a95b8e07076f1c3a35c89bd7a49435385370904b72a5",
  "registrations_seen": 0,
  "names_reserved": 0,
  "names_skipped": 0,
  "reserve_failures": 0,
  "uptime_seconds": 13
}
```

Status meanings:

| Status | Meaning |
| --- | --- |
| `ok` | Both RPC connections are up and repeated reservation failures are below threshold. |
| `degraded` | People RPC or Asset Hub RPC is disconnected. |
| `unhealthy` | Reservation failures reached the unhealthy threshold. |
| curl fails | Process crashed, health server is unavailable, or the port is wrong. |

Useful checks:

```bash
tail -f logs/next-run.log
curl -fsS http://127.0.0.1:3033/health
ps -ef | rg 'flow-funder|target/release/flow-funder|timeout 5h'
```

## Docker

Build the local image:

```bash
docker build -t flow-funder:test .
```

Run the bot in Docker with a persisted cursor:

```bash
docker rm -f flow-funder-next 2>/dev/null || true

set -a
source .env.next
set +a

docker run -d \
  --name flow-funder-next \
  --restart unless-stopped \
  --env-file .env.next \
  -e RESERVER_HEALTH_BIND=0.0.0.0 \
  -v "$PWD/logs:/data" \
  -p 3033:3033 \
  flow-funder:test \
  --start-from-genesis \
  --cursor-file /data/next-genesis-backfill-cursor-20260616-195714.txt
```

Monitor Docker:

```bash
docker ps --filter name=flow-funder-next
docker logs -f flow-funder-next
curl -fsS http://127.0.0.1:3033/health
docker inspect flow-funder-next --format 'status={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}} restarts={{.RestartCount}}'
```

The Dockerfile includes a healthcheck that calls `/health` and requires `"status":"ok"`. `docker ps` will show `(healthy)` or `(unhealthy)`.

Note: Docker `--env-file` keeps quote characters literally. Prefer unquoted values in Docker env files, especially for `RESERVER_PROXY_FOR`. If a local env file must keep quotes, source it in the shell and override the quoted variables with `-e NAME="$NAME"`.

## Repo Layout

| File | Responsibility |
| --- | --- |
| `src/attest.rs` | Decodes `PeopleLite::attest` calls and extracts reservation data. |
| `src/asset_hub.rs` | Builds and submits dotns reservation batches on Asset Hub. |
| `src/chain.rs` | Chain helpers, including finalized-head lookup. |
| `src/cursor.rs` | Atomic file-backed cursor store. |
| `src/main.rs` | CLI, health endpoint, reconnect loop, block windowing, and cursor advancement. |
| `src/registration.rs` | dotns storage-key helpers and idempotency checks. |
| `src/bin/dump_extensions.rs` | Developer helper for inspecting runtime transaction extensions. |

## Re-Pinning Asset Hub Transaction Extensions

`AssetHubConfig` in `src/asset_hub.rs` is pinned to the chain transaction extensions. If a runtime upgrade changes extension metadata, re-check it:

```bash
cargo run --bin dump-extensions -- wss://paseo-asset-hub-next-rpc.polkadot.io
```

Then update the extension tuple and encoding in `src/asset_hub.rs`.
