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
        new "added" entry?  ──► AccountId ──► query System.Account balance
                                                 │
                                    free < target?
                                       │ yes
                                       ▼
                              Balances.transfer_keep_alive(account, amount)
```

1. **Detect.** Subscribe to finalized People-chain blocks. For each block, run
   `archive_v1_storageDiff` over the `PeopleLite::LitePeople` map prefix against
   the previous block. Every key that is **added** is a newly-registered lite
   identity; the AccountId is the last 32 bytes of the storage key.
2. **Check (idempotent).** Query the account's free balance on Asset Hub via
   `System.Account`. The People-chain and Asset Hub share the same `AccountId32`,
   so no address translation is needed.
3. **Fund.** If the balance is below the target, submit
   `Balances.transfer_keep_alive(dest, amount)` from the funding account, waiting
   for best-block inclusion with bounded retry on transient pool rejections.

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
| **Asset Hub WS drops mid-fund** | The fund fails → the account's block is **not** marked handled → the **cursor stays pinned**. The funder reconnects, and the block is re-diffed on the next tick (and on any restart); the balance check skips accounts already funded and retries the one that failed. The account is never silently dropped. |
| **Transient `storageDiff` failure** | The block is skipped without advancing the cursor (and retried once against the real parent hash for reorgs), so the next diff re-covers the range. |
| **Transient tx-pool rejection** (stale nonce, dropped/usurped) | Retried in-place up to `--max-submit-retries`, refetching the nonce each attempt. |
| **Process crash / restart** | Resumes from the persisted cursor. The cursor only ever points at a fully-handled block, so nothing between it and the tip is lost. |

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

# Live, with an explicit funding key and amount:
FUNDER_SEED_PHRASE="…twelve words…" \
FUNDER_DERIVATION_PATH=//funder \
FUNDER_AMOUNT_PLANCK=10000000000 \
cargo run --release --bin flow-funder
```

All flags have `--long` and env-var forms — see `.env.example` or `--help`.
Defaults target **Paseo people-next** and **Paseo Asset Hub next**, signing with
the dev `//Alice` key (override `FUNDER_SEED_PHRASE` for anything real).

A health endpoint is served at `GET http://localhost:3033/health` reporting
connection state, registrations seen, accounts funded/skipped, and failures.

## Configuration

| Env var | Flag | Default | Meaning |
| --- | --- | --- | --- |
| `PEOPLE_NODE_URL` | `--people-url` | `wss://paseo-people-next-system-rpc.polkadot.io` | People chain WS (needs archive RPC) |
| `ASSET_HUB_NODE_URL` | `--asset-hub-url` | `wss://paseo-asset-hub-next-rpc.polkadot.io` | Asset Hub WS |
| `FUNDER_SEED_PHRASE` | `--seed-phrase` | dev mnemonic | Funding account mnemonic |
| `FUNDER_DERIVATION_PATH` | `--derivation-path` | `//Alice` | Path appended to the seed |
| `FUNDER_AMOUNT_PLANCK` | `--amount` | `10000000000` (1 PAS) | Transfer per account, in plancks |
| `FUNDER_MAX_SUBMIT_RETRIES` | `--max-submit-retries` | `3` | Retries on transient pool errors |
| `FUNDER_CURSOR_FILE` | `--cursor-file` | `flow-funder-cursor.txt` | Resume cursor (last fully-handled block) |
| `FUNDER_DRY_RUN` | `--dry-run` | `false` | Detect/log only, never submit or persist |
| `FUNDER_HEALTH_PORT` | `--health-port` | `3033` | Health server port |

## Layout

| File | Responsibility |
| --- | --- |
| `src/registration.rs` | Pure core — storage-key prefix (`twox_128`), account extraction from the key tail, `AccountInfo` free-balance decode, and the fund/skip decision. Fully unit-tested. |
| `src/people.rs` | People-chain helpers — `archive_v1_storageDiff` over the LitePeople prefix (returns the added accounts) and the finalized-head lookup. |
| `src/asset_hub.rs` | Asset Hub side — custom `AssetHubConfig` (transaction extensions pinned to live metadata), balance query, and `transfer_keep_alive` submission. |
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
- Tested end-to-end against live Paseo in dry-run: detection of real
  registrations and the Asset Hub balance query are verified; the transfer
  submission path is intentionally exercised only with a funded key.
