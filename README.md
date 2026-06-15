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
double-funds. There is **no genesis bootstrap** — the watcher starts from the
current finalized head, so it funds only registrations that happen while it runs,
never retroactively funding every account already on chain.

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
| `FUNDER_DRY_RUN` | `--dry-run` | `false` | Detect/log only, never submit |
| `FUNDER_HEALTH_PORT` | `--health-port` | `3033` | Health server port |

## Layout

| File | Responsibility |
| --- | --- |
| `src/registration.rs` | Pure core — storage-key prefix (`twox_128`), account extraction from the key tail, `AccountInfo` free-balance decode, and the fund/skip decision. Fully unit-tested. |
| `src/people.rs` | People-chain watcher — finalized-block subscription + `archive_v1_storageDiff`, emits new accounts on a channel. |
| `src/asset_hub.rs` | Asset Hub side — custom `AssetHubConfig` (transaction extensions pinned to live metadata), balance query, and `transfer_keep_alive` submission. |
| `src/main.rs` | CLI, health endpoint, and the watcher↔funder orchestration with reconnect/backoff. |
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
