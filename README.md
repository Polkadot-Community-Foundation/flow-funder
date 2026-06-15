# flow-funder

A standalone bot that watches the **People chain** for newly-registered lite
identities and **funds each one** with native tokens on **Asset Hub**.

It reuses the storage-diff technique from `mission-control`'s `flow-people`
worker (subscribe to blocks → `archive_v1_storageDiff` over a storage prefix →
decode the changes) and the chain-submission technique from its `flow-attester`
worker (a custom subxt `Config` pinned to the chain's transaction extensions,
with nonce caching + retry).

## What it does

```
People chain                                   Asset Hub
────────────                                   ─────────
finalized block N ──┐
                    │ archive_v1_storageDiff over PeopleLite::LitePeople
                    │ (block N vs block N-1)
                    ▼
        new "added" entries ──► AccountIds ──► state_queryStorageAt (batched balances)
                                                 │
                                    keep those with free < target
                                       │
                                       ▼
                 Utility.batch_all([ transfer_keep_alive(acct, target - free), … ])
                              (chunked by --batch-size, one tx per chunk)
```

1. **Detect.** Subscribe to finalized People-chain blocks. For each block, run
   `archive_v1_storageDiff` over the `PeopleLite::LitePeople` map prefix against
   the previous block. Every key that is **added** is a newly-registered lite
   identity; the AccountId is the last 32 bytes of the storage key.
2. **Check (idempotent, batched).** Read all the block's accounts' free balances
   on Asset Hub in one `state_queryStorageAt` per `READ_CHUNK` accounts. The
   People-chain and Asset Hub share the same `AccountId32`, so no address
   translation is needed.
3. **Fund (batched).** Take the accounts still below target and submit them as
   `Utility.batch_all` extrinsics of `transfer_keep_alive(dest, target - free)`,
   chunked into `--batch-size` calls per tx, each waiting for finalized inclusion
   with bounded retry on transient pool rejections. So a block with hundreds of
   registrations (e.g. a catch-up after downtime) becomes a handful of txs, not
   one per account.

The balance check is what makes the bot **idempotent**: an account already at or
above the target is skipped, so re-observing it or restarting the bot never
double-funds. There is **no genesis bootstrap** — on first boot the watcher
starts from the current finalized head, so it funds only registrations that
happen while it runs, never retroactively funding every account already on chain.

## Reliability — what happens when a connection is lost

Funding is **synchronous per block** and gated by a **persisted cursor** (the
hash of the last block whose registrations were all handled, written atomically
to `--cursor-file`). A block's cursor advances only once every account in it has
been funded, skipped, or — in dry-run — observed. From that one rule:

| Failure | Behaviour |
| --- | --- |
| **People chain WS drops** | The cycle errors; the outer loop reconnects with exponential backoff (1s→60s) and **resumes the storageDiff from the persisted cursor**, so every registration in the downtime window is still diffed and funded. No gap. |
| **Asset Hub WS drops mid-fund** | The `batch_all` fails → the block is **not** marked handled → the **cursor stays pinned**. `batch_all` is atomic (no partial chunk), so the funder reconnects and the block is re-diffed on the next tick (and on any restart); the batched balance read skips accounts already funded by earlier chunks and re-batches the rest. No account is silently dropped. |
| **Transient `storageDiff` failure** | The block is skipped without advancing the cursor, so the next diff re-covers the range. A mid-diff drop (stream ends before the `storageDiffDone` terminator) is treated as an error — a truncated account list is never mistaken for a complete one. |
| **Silent WS (open but no messages)** | Both the block subscription and each `storageDiff` message are bounded by a 120s timeout; a stall surfaces as an error and triggers reconnect, rather than hanging the loop. |
| **Stuck / never-included tx** | The finalization wait is bounded (120s); a timeout is retriable, so the loop re-submits instead of hanging forever with `/health` still reporting ok. |
| **Transient tx-pool rejection** (stale nonce, dropped/usurped) | Retried in-place up to `--max-submit-retries`, refetching the nonce each attempt. |
| **Process crash / restart** | Resumes from the persisted cursor. The cursor only ever points at a fully-handled block, so nothing between it and the tip is lost. |
| **Ctrl-C** | Graceful: the current block's in-flight funding finishes, then the loop stops before the next block. A second Ctrl-C force-quits. |

Trade-off: a **permanently** unfundable account (e.g. the funding key is out of
balance) pins the cursor and keeps retrying every block — by design, it fails
loudly (`fund_failures` climbs, the cursor stops advancing) rather than silently
skipping. Top the key up and it drains on the next tick. A dry run loads the
cursor but never writes one, so it can't advance a real cursor past accounts it
didn't fund.

## Run

```bash
# Build
cargo build --release

# Dry run — detect + log, never submit (recommended first):
cargo run --release --bin flow-funder -- --dry-run

# Live, with an explicit funding key and target balance:
FUNDER_SEED_PHRASE="…twelve words…" \
FUNDER_DERIVATION_PATH=//funder \
FUNDER_AMOUNT_PLANCK=10000000000 \
cargo run --release --bin flow-funder
```

All flags have `--long` and env-var forms — see `.env.example` or `--help`.
Defaults target **Paseo people-next** and **Paseo Asset Hub next**. Live
runs require `FUNDER_SEED_PHRASE`; dry runs may use the dev `//Alice` key.

A health endpoint is served at `GET http://127.0.0.1:3033/health`:

```json
{
  "status": "ok",            // "degraded" if disconnected or any fund_failures;
                             // "unhealthy" past the failure threshold
  "people_connected": true,
  "asset_hub_connected": true,
  "last_block_at": 1781530676,
  "cursor_block_hash": null,
  "funding_key_balance": 63443372669010,   // refreshed periodically
  "registrations_seen": 0,
  "accounts_funded": 0,
  "accounts_skipped": 0,
  "fund_failures": 0,
  "uptime_seconds": 20
}
```

`status` is computed, not hardcoded, so an operator/agent can detect a
disconnect or funding-key exhaustion proactively. `fund_failures` counts fund
*attempts* that failed (including transient ones the reconnect recovers from),
not unique accounts.

## Configuration

| Env var | Flag | Default | Meaning |
| --- | --- | --- | --- |
| `PEOPLE_NODE_URL` | `--people-url` | `wss://paseo-people-next-system-rpc.polkadot.io` | People chain WS (needs archive RPC) |
| `ASSET_HUB_NODE_URL` | `--asset-hub-url` | `wss://paseo-asset-hub-next-rpc.polkadot.io` | Asset Hub WS |
| `FUNDER_SEED_PHRASE` | `--seed-phrase` | required live | Funding account mnemonic |
| `FUNDER_DERIVATION_PATH` | `--derivation-path` | `//Alice` | Path appended to the seed |
| `FUNDER_AMOUNT_PLANCK` | `--amount` | `10000000000` (1 PAS) | Target free balance per account, in plancks |
| `FUNDER_MAX_AMOUNT_PLANCK` | `--max-amount` | `1000000000000000` (100k PAS) | Startup fat-finger ceiling for `--amount` |
| `FUNDER_MAX_SUBMIT_RETRIES` | `--max-submit-retries` | `3` | Retries on transient pool errors |
| `FUNDER_BATCH_SIZE` | `--batch-size` | `100` | Max `transfer_keep_alive` calls per `batch_all` tx |
| `FUNDER_CURSOR_FILE` | `--cursor-file` | `flow-funder-cursor.txt` | Resume cursor (last fully-handled block) |
| `FUNDER_DRY_RUN` | `--dry-run` | `false` | Detect/log only, never submit or persist |
| `FUNDER_HEALTH_PORT` | `--health-port` | `3033` | Health server port |
| `FUNDER_HEALTH_BIND` | `--health-bind` | `127.0.0.1` | Health bind address (`0.0.0.0` to expose off-host) |

## Layout

| File | Responsibility |
| --- | --- |
| `src/registration.rs` | Pure core — storage-key derivation (`twox_128`, `System.Account` key via `Blake2_128Concat`), account extraction from the key tail, `AccountInfo` free-balance decode, and the shortfall-to-target decision. Fully unit-tested. |
| `src/people.rs` | People-chain helpers — `archive_v1_storageDiff` over the LitePeople prefix (returns the added accounts) and the finalized-head lookup. |
| `src/asset_hub.rs` | Asset Hub side — custom `AssetHubConfig` (transaction extensions pinned to live metadata), batched balance reads (`state_queryStorageAt`), and chunked `Utility.batch_all` submission. |
| `src/cursor.rs` | Persisted resume cursor — atomic file-backed `H256` store + hash parser. Unit-tested. |
| `src/main.rs` | CLI, health endpoint, and the per-block watch→fund→advance-cursor loop with reconnect/backoff. |
| `src/bin/dump_extensions.rs` | Dev helper. Prints a chain's transaction-extension order + each extension's `extra` shape, so `AssetHubConfig` is pinned to verified metadata rather than guessed. |

### Re-pinning the Asset Hub config

`AssetHubConfig` in `src/asset_hub.rs` declares the chain's transaction
extensions **by name, in on-wire order** — subxt matches them against metadata,
so the set must be exact. If the runtime upgrades and the extension set changes,
re-derive it:

```bash
cargo run --bin dump-extensions -- wss://paseo-asset-hub-next-rpc.polkadot.io
```

and update the `TransactionExtensions` tuple, the `build_params` tuple, and the
per-extension encoding (`define_simple_extension!` = single `0x00` byte for
`Option`-`None` / `bool`-`false`; `define_empty_extension!` = no bytes).

## Notes

- The People chain and Asset Hub use the same `AccountId32`; the bot funds the
  exact account observed on the People chain.
- Funding uses `transfer_keep_alive` so a transfer can never reap (delete) the
  destination by leaving it below the existential deposit.
- Funding is batched: a block's needing-funding accounts go out as chunked
  `Utility.batch_all` extrinsics (`--batch-size` calls each), and their balances
  are read in batched `state_queryStorageAt` calls — so a large catch-up costs a
  few txs and a few reads, not one of each per account.
- Tested end-to-end against live Paseo in dry-run: detection of real
  registrations, the batched balance reads, and `batch_all` call encoding against
  live metadata are verified (a 585-account catch-up planned into 6 `batch_all`
  txs); the actual submission path is intentionally exercised only with a funded
  key.
