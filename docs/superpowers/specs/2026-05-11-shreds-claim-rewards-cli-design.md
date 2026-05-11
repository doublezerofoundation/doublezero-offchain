# Shreds Claim Rewards CLI — Design Spec

**Date:** 2026-05-11
**Issue:** [malbeclabs/infra#1201](https://github.com/malbeclabs/infra/issues/1201) — *solana-cli: allow validator client teams to claim rewards*
**Smart contract:** `doublezero-shred-subscription` v0.6.2 (`dzshrr3yL57SB13sJPYHYo3TV8Bo1i1FxkyrZr3bKNE`)

## Goal

Let validator-client teams (the entities behind `ValidatorClientRewards` PDAs) drain accumulated reward holdings into a destination token account via `doublezero-solana shreds`. Also expose the prerequisite permissionless `init-holding` and a read-only `show` for diagnostics.

## Background

On-chain (`~/src/malbec/doublezero-shred-subscription`), reward distribution into per-client holdings works like this:

1. **Setup.** Admin runs `InitializeValidatorClientRewards` to create a `ValidatorClientRewards` PDA keyed by `client_id: u16` with a `manager_key`.
2. **Per-epoch init.** Anyone runs `InitializeClaimHoldingAccount(epoch)` to create a non-ATA SPL token account at `[b"claim", vcr_pda, epoch.to_le_bytes(), mint]` owned by the VCR PDA. This increments `vcr.claim_holding_count` via `saturating_add(1)`.
3. **Funding.** The off-chain pipeline (sweep, journal swap, etc.) deposits reward tokens into the holding accounts. Not part of this CLI.
4. **Claim.** The `vcr.manager_key` runs `ClaimValidatorClientRewards(Vec<ClaimHoldingId>)` to drain N holdings into a destination, close each, and recover rent to `program_config.shred_oracle_key`. `claim_holding_count` decrements by N via `saturating_sub(1)` per holding.

The off-chain SDK already speaks to the VCR PDA via `find_validator_client_rewards_address` and exposes `SetValidatorClientRewardsProportion`. Nothing for `InitializeClaimHoldingAccount` or `ClaimValidatorClientRewards` exists today.

## Non-goals

- **No `InitializeValidatorClientRewards` support.** Admin-signed; out of scope. Teams already have their VCR set up by ops.
- **No auto-discovery of which epochs have funded holdings.** Users supply an explicit list via repeated `--subscription-epoch`. A future `list-holdings` subcommand could add `getProgramAccounts` discovery.
- **No offchain-auth (ed25519) path for `claim`.** The on-chain instruction doesn't support one. `set-proportion` likewise stays single-signer.
- **No auto-create of destination ATA.** Print a copy-paste `spl-token create-account` suggestion and bail. Predictable rent spend, no surprises.
- **No auto-batching across multiple txs.** If the requested batch overflows tx size, bail with the explicit split count.

## CLI surface

`crates/solana-cli/src/command/shreds/validator_client_rewards.rs` becomes a directory. The `ValidatorClientRewards(...)` variant in `ShredsSubcommand` stays; the inner subcommand enum gains three variants.

```
doublezero-solana shreds validator-client-rewards <SUBCMD>
  set-proportion        (existing, still hidden via #[command(hide = true)])
  init-holding          (NEW, permissionless)
  claim                 (NEW, VCR.manager-signed)
  show                  (NEW, read-only)
```

The parent command (`ValidatorClientRewards`) becomes visible in `--help`. `set-proportion` stays hidden on the variant.

### `init-holding`

Permissionless. `-k` is the rent payer.

```
doublezero-solana shreds validator-client-rewards init-holding \
    --client-id <u16> \
    --rewards-token-mint <PUBKEY> \
    --subscription-epoch <u64> [--subscription-epoch <u64> ...]
```

Flow:
1. Derive the VCR PDA from `client_id` and verify it exists (parse + discriminator check). Bail if not.
2. For each `subscription_epoch`, derive the holding PDA. Pre-flight via `getMultipleAccounts`: collect epochs whose holding account doesn't exist yet. Skip already-existing with a warning.
3. Build one `InitializeClaimHoldingAccount(epoch)` ix per missing epoch. Prepend `CheckCliVersion`. Single tx.
4. Pre-flight tx size estimation (trial-tx via `try_batch_instructions_with_common_signers`). If it overflows, bail with computed `--subscription-epoch` batch size.
5. Send. Print each newly-created holding PDA address.

### `claim`

VCR.manager-signed. `-k` IS the manager and pays fees.

```
doublezero-solana shreds validator-client-rewards claim \
    --client-id <u16> \
    --rewards-token-mint <PUBKEY> \
    --subscription-epoch <u64> [--subscription-epoch <u64> ...] \
    [--destination-token-account <PUBKEY>]
```

Defaults: `--destination-token-account` defaults to ATA(manager, mint).

Flow:
1. Derive VCR + every holding PDA + ProgramConfig address.
2. One `getMultipleAccounts` call for: VCR, ProgramConfig, every holding PDA, destination ATA. Plus a separate `get_account` for the mint (to read decimals for display).
3. Validate:
   - VCR exists, discriminator matches.
   - `vcr.manager_key == wallet.pubkey()`. Bail with both addresses printed otherwise.
   - Every holding exists, `spl_token_interface::state::Account::unpack(&data)` succeeds, owner is `spl_token_interface::ID`, and the unpacked `mint` matches `--rewards-token-mint`. Bail listing offenders otherwise.
   - ProgramConfig parses; extract `shred_oracle_key` to use as `rent_beneficiary`. (Read from chain rather than env table — handles fork/localnet correctly.)
   - Destination ATA exists, is an SPL token account, mint matches. If missing, bail with `spl-token create-account --owner <manager> <mint> --fee-payer <payer>` suggestion. Wrong-mint → bail listing expected vs actual.
   - Warn (don't fail) per holding with balance 0.
4. Build `Vec<ClaimHoldingId>` by re-deriving each holding's bump (`find_program_address` after the fact, since we already validated the address). Order matches the user's `--subscription-epoch` order.
5. Build one `ClaimValidatorClientRewards(Vec<ClaimHoldingId>)` ix with the meta order in §SDK below. Prepend `CheckCliVersion`. Single tx.
6. Pre-flight tx size; bail with split count on overflow.
7. Send. Print:
   - Transaction signature.
   - Per-holding balance drained.
   - Total amount claimed.
   - Final `claim_holding_count` post-tx (re-fetch VCR).

### `show`

Read-only. No keypair required beyond connection options.

```
doublezero-solana shreds validator-client-rewards show \
    --client-id <u16> \
    [--rewards-token-mint <PUBKEY>] \
    [--subscription-epoch <u64> ...]
```

- `--client-id` alone: VCR address, `manager_key`, `short_description`, `claim_holding_count`. If VCR is uninitialized: print `"Validator client rewards not initialized for client-id <N>"` and exit 0.
- `+ --rewards-token-mint`: same plus the ATA(manager, mint) address and balance (if it exists).
- `+ --subscription-epoch`: also list each requested holding's address, existence, and SPL balance. Missing holding → `(not initialized)`, no error.

### `set-proportion`

Unchanged. Body moved verbatim from `validator_client_rewards.rs` to `validator_client_rewards/set_proportion.rs`. Hidden via `#[command(hide = true)]` on the variant.

## SDK additions

### `crates/solana-sdk/src/shred_subscription/state.rs`

Add seed prefix + finder + parser + offset/discriminator constants:

```rust
pub const CLAIM_HOLDING_SEED_PREFIX: &[u8] = b"claim";

pub fn find_claim_holding_address(
    parent_pda_key: &Pubkey,
    subscription_epoch: u64,
    mint_key: &Pubkey,
) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            CLAIM_HOLDING_SEED_PREFIX,
            parent_pda_key.as_ref(),
            &subscription_epoch.to_le_bytes(),
            mint_key.as_ref(),
        ],
        &crate::shred_subscription::ID,
    )
}

// ProgramConfig: add an offset constant + parser for shred_oracle_key.
// Existing PROGRAM_CONFIG_DISCRIMINATOR and PROGRAM_CONFIG_FLAGS_OFFSET stay.
//
// Layout (Pod with 8-byte discriminator prefix):
//   [0..8)     discriminator
//   [8..16)    flags: Flags (u64)
//   [16..48)   admin_key: Pubkey
//   [48..52)   closed_for_requests_grace_period_slots: u32
//   [52..56)   _padding
//   [56..88)   shred_oracle_key: Pubkey   ← we need this
//   ...
pub const PROGRAM_CONFIG_SHRED_ORACLE_KEY_OFFSET: usize = DISCRIMINATOR_LEN + 48;

pub fn parse_program_config_shred_oracle_key(data: &[u8]) -> Option<Pubkey> { /* ... */ }

pub const VALIDATOR_CLIENT_REWARDS_DISCRIMINATOR: Discriminator<DISCRIMINATOR_LEN> =
    Discriminator::new_sha2(b"dz::account::validator_client_rewards");

// Layout (Pod with 8-byte discriminator prefix):
//   [0..8)     discriminator
//   [8..10)    client_id: u16
//   [10..11)   bump_seed: u8
//   [11..16)   _padding_0: [u8; 5]
//   [16..48)   manager_key: Pubkey
//   [48..112)  short_description_bytes: [u8; 64]
//   [112..116) claim_holding_count: u32
//   ...        (remaining fields irrelevant for the CLI today)
pub const VCR_CLIENT_ID_OFFSET: usize = DISCRIMINATOR_LEN;
pub const VCR_MANAGER_KEY_OFFSET: usize = DISCRIMINATOR_LEN + 8;
pub const VCR_SHORT_DESCRIPTION_OFFSET: usize = DISCRIMINATOR_LEN + 40;
pub const VCR_CLAIM_HOLDING_COUNT_OFFSET: usize = DISCRIMINATOR_LEN + 104;

pub struct ValidatorClientRewardsInfo {
    pub client_id: u16,
    pub manager_key: Pubkey,
    pub short_description: Option<String>,
    pub claim_holding_count: u32,
}

pub fn parse_validator_client_rewards(data: &[u8]) -> Option<ValidatorClientRewardsInfo> { /* ... */ }
```

The `short_description` parse follows the on-chain pattern: trim trailing zero bytes, validate UTF-8, return `Some(...)` if non-empty else `None`.

### `crates/solana-sdk/src/shred_subscription/instruction/mod.rs`

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ClaimHoldingId {
    pub subscription_epoch: u64,
    pub bump_seed: u8,
}
```

Add two variants to `ShredSubscriptionInstructionData`:

```rust
InitializeClaimHoldingAccount(u64),
ClaimValidatorClientRewards(Vec<ClaimHoldingId>),
```

Discriminator constants (sha2):
- `INITIALIZE_CLAIM_HOLDING_ACCOUNT = Discriminator::new_sha2(b"dz::ix::initialize_claim_holding_account")`
- `CLAIM_VALIDATOR_CLIENT_REWARDS = Discriminator::new_sha2(b"dz::ix::claim_validator_client_rewards")`

Wire through `Self::serialize` and `Self::try_from_slice` (or equivalent) to round-trip both. Borsh handles `Vec<T>` and `u64` natively; no manual encoding needed.

### `crates/solana-sdk/src/shred_subscription/instruction/account.rs`

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeClaimHoldingAccountAccounts {
    pub parent_pda_key: Pubkey,
    pub payer_key: Pubkey,
    pub new_claim_holding_account_key: Pubkey,
    pub mint_key: Pubkey,
}

impl InitializeClaimHoldingAccountAccounts {
    pub fn new(
        client_id: u16,
        subscription_epoch: u64,
        mint_key: &Pubkey,
        payer_key: &Pubkey,
    ) -> Self {
        let parent_pda_key = state::find_validator_client_rewards_address(client_id).0;
        let new_claim_holding_account_key =
            state::find_claim_holding_address(&parent_pda_key, subscription_epoch, mint_key).0;
        Self { parent_pda_key, payer_key: *payer_key, new_claim_holding_account_key, mint_key: *mint_key }
    }
}

impl From<InitializeClaimHoldingAccountAccounts> for Vec<AccountMeta> {
    fn from(accounts: InitializeClaimHoldingAccountAccounts) -> Self {
        vec![
            AccountMeta::new(accounts.parent_pda_key, false),
            AccountMeta::new(accounts.payer_key, true),
            AccountMeta::new(accounts.new_claim_holding_account_key, false),
            AccountMeta::new_readonly(accounts.mint_key, false),
            AccountMeta::new_readonly(spl_token_interface::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ]
    }
}
```

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimValidatorClientRewardsAccounts {
    pub program_config_key: Pubkey,
    pub validator_client_rewards_key: Pubkey,
    pub manager_key: Pubkey,
    pub destination_token_account_key: Pubkey,
    pub rent_beneficiary_key: Pubkey,
    pub claim_holding_account_keys: Vec<Pubkey>,
}

impl ClaimValidatorClientRewardsAccounts {
    pub fn new(
        client_id: u16,
        manager_key: &Pubkey,
        destination_token_account_key: &Pubkey,
        rent_beneficiary_key: &Pubkey,
        mint_key: &Pubkey,
        subscription_epochs: &[u64],
    ) -> Self {
        let validator_client_rewards_key = state::find_validator_client_rewards_address(client_id).0;
        let claim_holding_account_keys = subscription_epochs
            .iter()
            .map(|epoch| state::find_claim_holding_address(&validator_client_rewards_key, *epoch, mint_key).0)
            .collect();
        Self {
            program_config_key: state::find_program_config_address().0,
            validator_client_rewards_key,
            manager_key: *manager_key,
            destination_token_account_key: *destination_token_account_key,
            rent_beneficiary_key: *rent_beneficiary_key,
            claim_holding_account_keys,
        }
    }
}

impl From<ClaimValidatorClientRewardsAccounts> for Vec<AccountMeta> {
    fn from(accounts: ClaimValidatorClientRewardsAccounts) -> Self {
        let mut metas = vec![
            AccountMeta::new_readonly(accounts.program_config_key, false),
            AccountMeta::new(accounts.validator_client_rewards_key, false),
            AccountMeta::new_readonly(accounts.manager_key, true),
            AccountMeta::new(accounts.destination_token_account_key, false),
            AccountMeta::new(accounts.rent_beneficiary_key, false),
            AccountMeta::new_readonly(spl_token_interface::ID, false),
        ];
        metas.extend(accounts.claim_holding_account_keys.into_iter().map(|k| AccountMeta::new(k, false)));
        metas
    }
}
```

Both meta orders must match on-chain byte-for-byte. Tests assert this.

### `payments.rs`

Extend the non-exhaustive `ShredSubscriptionInstructionData` match arm to no-op-handle the two new variants (same as publisher-rewards did).

## Error handling

| Where | Failure | Behavior |
| --- | --- | --- |
| `init-holding` | VCR not initialized | bail with `client_id` printed |
| `init-holding` | Holding already exists | skip with warning; continue with remaining |
| `init-holding` | All requested already exist | print `"All requested claim holdings already initialized"`, exit 0 |
| `init-holding` | Tx size overflow | bail with computed split-count suggestion |
| `claim` | VCR not initialized | bail |
| `claim` | Manager mismatch | bail with both addresses printed |
| `claim` | Holding missing / wrong mint / wrong owner | bail listing offenders |
| `claim` | Destination ATA missing | bail with `spl-token create-account` suggestion |
| `claim` | Destination wrong mint | bail listing expected vs actual |
| `claim` | Holding balance 0 | warn, continue (rent still recovered) |
| `claim` | Tx size overflow | bail with computed split-count suggestion |
| `claim` | On-chain rent_beneficiary mismatch | propagate program error; we read it from chain so this shouldn't happen unless `ProgramConfig` was reconfigured mid-flight |
| `show` | VCR missing | print "not initialized" line, exit 0 |
| `show` | Holding missing | print `(not initialized)` for that row, exit 0 |
| any | RPC transport error | propagate via `?` (don't conflate with "not initialized") |

RPC pattern: `get_account_with_commitment(..., CommitmentConfig::confirmed()).await.context(...)?.value` — only `Ok(None)` triggers "not initialized" branching.

## Testing

### SDK unit tests (`crates/solana-sdk/src/shred_subscription/`)

- `state::tests::find_claim_holding_address_known_seed` — frozen `(parent, epoch, mint)` triple → assert PDA bytes match a precomputed vector.
- `state::tests::parse_program_config_shred_oracle_key_*` — happy path + short buffer + wrong discriminator.
- `state::tests::parse_validator_client_rewards_happy_path` — synthetic bytes round-trip all fields.
- `state::tests::parse_validator_client_rewards_short_buffer_returns_none`.
- `state::tests::parse_validator_client_rewards_wrong_discriminator_returns_none`.
- `instruction::tests::round_trip_initialize_claim_holding_account`.
- `instruction::tests::round_trip_claim_validator_client_rewards_*` — empty, 1-entry, 8-entry Vec.
- `instruction::tests::frozen_bytes_initialize_claim_holding_account` — assert exact serialized bytes against a precomputed vector (cross-crate canary against discriminator drift or variant-index reordering).
- `instruction::tests::frozen_bytes_claim_validator_client_rewards`.
- `instruction::account::tests::initialize_claim_holding_account_metas_order` — exact `Vec<AccountMeta>` order + writable/signer flags.
- `instruction::account::tests::claim_validator_client_rewards_metas_order` — same for 6 fixed + N holdings.

### CLI unit tests (`crates/solana-cli/src/command/shreds/validator_client_rewards/`)

- `init_holding::tests` — clap parse: required args enforced, repeated `--subscription-epoch` accumulates.
- `claim::tests::resolve_destination_*` — pure helper. With override → returns override. Without → returns `ATA(manager, mint)`.
- `claim::tests::validate_manager_*` — pure helper that takes `(parsed_vcr_info, wallet_pubkey)` and returns `Result<()>`.
- `claim::tests::validate_holdings_*` — pure helper taking `(holding_addr, account_data, expected_mint)` returning `Result<u64>` (balance) or descriptive error.
- `show::tests::render_*` — formatting helpers take parsed inputs and return rendered strings.
- `mod::tests` — clap dispatch resolves to the right subcommand variant.

### Fork test (`sh/test_doublezero_solana_fork.sh`)

Add a `### Validator-client claim commands.` block at the end. The flow:

1. **Bake a synthetic VCR PDA into genesis.** Extend the `crates/solana-fork/src/main.rs` loader: write a `validator-client-rewards.json` account file with:
   - Address = `find_validator_client_rewards_address(65535).0` (chosen to avoid collision with real client_ids).
   - Owner = shred-subscription program ID.
   - Data = correct `ValidatorClientRewards` byte layout: discriminator + `client_id=65535` + bump + `manager_key` = pubkey of `test-ledger/validator-keypair.json` (deterministic from `solana-test-validator`'s fork setup; can be computed once and hard-coded into the loader, or computed at fork-startup from the actual keypair file).
   - Lamports = rent-exempt for the data size.
   - Pass via `--account <PDA_ADDRESS> <PATH>` to `solana-test-validator`.

2. **Fork test script steps:**
   - `solana-keygen new` a fresh `claim_payer.json`, airdrop.
   - Use `spl-token create-token` to create a test mint with `claim_payer.json` as authority. Capture `MINT`.
   - Use `spl-token create-account` to create destination ATA owned by the test wallet (validator-keypair) for `MINT`.
   - Run `doublezero-solana shreds validator-client-rewards show --client-id 65535 -ul` to verify the baked VCR. Expect `claim_holding_count=0`.
   - Run `init-holding --client-id 65535 --rewards-token-mint $MINT --subscription-epoch 100 -ul`. Pay rent from `claim_payer.json`.
   - `spl-token mint-to $MINT 1000000 <holding_pda> --mint-authority claim_payer.json` to fund the holding (1.0 token at 6 decimals).
   - Run `claim --client-id 65535 --rewards-token-mint $MINT --subscription-epoch 100 -ul -k test-ledger/validator-keypair.json --destination-token-account $DEST`. Verify success.
   - Re-run `show ... --rewards-token-mint $MINT --subscription-epoch 100` to verify `claim_holding_count=0` and holding shows `(not initialized)`.
   - Clean up: `rm claim_payer.json`.

3. **No god-mode required.** Synthetic VCR baking happens at `--account` flag time before validator startup.

## Out-of-scope future work

- `list-holdings` subcommand (memcmp scan).
- Auto-batching `claim` into multiple txs.
- Offchain-auth path (would require an on-chain instruction extension).
- Auto-create destination ATA.
- Per-mint preferred destination defaulting (would require on-chain VCR field).
