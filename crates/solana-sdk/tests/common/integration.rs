//! Integration test helpers for running the `doublezero-solana` CLI against a
//! real `solana-test-validator` instance.
//!
//! Pattern: start validator → admin setup → run CLI → verify state.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use solana_client::rpc_client::RpcClient;
use solana_program_pack::Pack;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    message::{VersionedMessage, v0::Message},
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};
use spl_token_interface::{instruction as token_instruction, state::Mint};

use super::*;

// ---------------------------------------------------------------------------
// Test validator
// ---------------------------------------------------------------------------

// TODO: Replace `solana-test-validator` with `doublezero-solana-fork` crate.

/// A running `solana-test-validator` process with its RPC URL.
pub struct TestValidator {
    child: Child,
    pub rpc_url: String,
    _ledger_dir: tempfile::TempDir,
}

impl TestValidator {
    /// Start a `solana-test-validator` with the mock reservation program
    /// deployed as an upgradeable program (required for `SetAdmin` which
    /// checks the program data account).
    pub fn start(program_so_path: &Path, upgrade_authority: &Pubkey) -> Self {
        // Use a temp directory for the ledger to avoid conflicts.
        let ledger_dir = tempfile::tempdir().expect("failed to create temp ledger dir");

        // Use a random port to avoid conflicts with other tests.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to random port");
        let rpc_port = listener.local_addr().unwrap().port();
        drop(listener);

        // Also pick a random faucet port.
        let faucet_listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind faucet to random port");
        let faucet_port = faucet_listener.local_addr().unwrap().port();
        drop(faucet_listener);

        let child = Command::new("solana-test-validator")
            .arg("--ledger")
            .arg(ledger_dir.path())
            .arg("--rpc-port")
            .arg(rpc_port.to_string())
            .arg("--faucet-port")
            .arg(faucet_port.to_string())
            .arg("--upgradeable-program")
            .arg(program_id().to_string())
            .arg(program_so_path)
            .arg(upgrade_authority.to_string())
            .arg("--reset")
            .arg("--quiet")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start solana-test-validator");

        let rpc_url = format!("http://127.0.0.1:{rpc_port}");

        let validator = Self {
            child,
            rpc_url: rpc_url.clone(),
            _ledger_dir: ledger_dir,
        };

        // Poll until the validator is ready.
        let rpc = RpcClient::new_with_commitment(&rpc_url, CommitmentConfig::confirmed());
        let start = Instant::now();
        let timeout = Duration::from_secs(30);

        loop {
            if start.elapsed() > timeout {
                panic!("solana-test-validator did not become healthy within {timeout:?}");
            }
            if rpc.get_health().is_ok() {
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }

        validator
    }

    pub fn rpc_client(&self) -> RpcClient {
        RpcClient::new_with_commitment(&self.rpc_url, CommitmentConfig::confirmed())
    }
}

impl Drop for TestValidator {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Program state setup
// ---------------------------------------------------------------------------

pub struct SetupResult {
    pub admin_signer: Keypair,
    pub oracle_signer: Keypair,
    pub device_key: Pubkey,
    pub exchange_key: Pubkey,
    pub usdc_mint_key: Pubkey,
    pub usdc_mint_authority: Keypair,
}

/// Send all admin instructions to bootstrap program state via RPC.
///
/// This mirrors `SeatTestSetup::new()` in `common/mod.rs` but uses `RpcClient`
/// instead of `BanksClient`, and sends a `TestSetup` instruction instead of
/// patching raw account bytes.
pub fn setup_program_state(rpc: &RpcClient, payer: &Keypair) -> SetupResult {
    let admin_signer = Keypair::new();
    let oracle_signer = Keypair::new();
    let device_key = Pubkey::new_unique();
    let exchange_key = Pubkey::new_unique();

    // 1. InitializeProgram
    send_admin_ix(
        rpc,
        payer,
        &[payer],
        InitializeProgramAccounts::new(&payer.pubkey()),
        &AdminInstructionData::InitializeProgram,
    );

    // 2. SetAdmin
    let owner_signer = payer; // payer is the upgrade authority
    send_admin_ix(
        rpc,
        payer,
        &[owner_signer],
        SetAdminAccounts::new(&program_id(), &owner_signer.pubkey()),
        &AdminInstructionData::SetAdmin(admin_signer.pubkey()),
    );

    // 3. ConfigureProgram — create USDC mint first so we can set it.
    let usdc_mint_authority = Keypair::new();
    let usdc_mint_key = create_usdc_mint(rpc, payer, &usdc_mint_authority);

    for setting in [
        ProgramConfiguration::Flag(ProgramFlagConfiguration::IsPaused(false)),
        ProgramConfiguration::Oracle(oracle_signer.pubkey()),
        ProgramConfiguration::UsdcMint(usdc_mint_key),
    ] {
        send_admin_ix(
            rpc,
            payer,
            &[&admin_signer],
            ConfigureProgramAccounts::new(&admin_signer.pubkey()),
            &AdminInstructionData::ConfigureProgram(setting),
        );
    }

    // 4. InitializeMetroHistory
    send_admin_ix(
        rpc,
        payer,
        &[&oracle_signer],
        InitializeMetroHistoryAccounts::new(
            &oracle_signer.pubkey(),
            &payer.pubkey(),
            &exchange_key,
        ),
        &AdminInstructionData::InitializeMetroHistory(exchange_key),
    );

    // 5. InitializeDeviceHistory
    send_admin_ix(
        rpc,
        payer,
        &[&oracle_signer],
        InitializeDeviceHistoryAccounts::new(
            &oracle_signer.pubkey(),
            &payer.pubkey(),
            &exchange_key,
            &device_key,
        ),
        &AdminInstructionData::InitializeDeviceHistory(device_key),
    );

    // 6. TestSetup — force EC to OpenForRequests + write price entries.
    // (SetDeviceEnabled is not needed: InitializeDeviceHistory already
    //  enables the device and increments total_enabled_devices.)
    send_admin_ix(
        rpc,
        payer,
        &[&admin_signer],
        TestSetupAccounts::new(&admin_signer.pubkey(), &exchange_key, &device_key),
        &AdminInstructionData::TestSetup {
            subscription_epoch: FIRST_EPOCH,
            metro_usdc_price: 100,
            device_premium: 0,
        },
    );

    SetupResult {
        admin_signer,
        oracle_signer,
        device_key,
        exchange_key,
        usdc_mint_key,
        usdc_mint_authority,
    }
}

// ---------------------------------------------------------------------------
// User funding
// ---------------------------------------------------------------------------

/// Fund a user with SOL (via airdrop) and USDC (via mint).
pub fn fund_user(
    rpc: &RpcClient,
    payer: &Keypair,
    user: &Keypair,
    usdc_mint_key: &Pubkey,
    usdc_mint_authority: &Keypair,
    usdc_amount: u64,
) -> Pubkey {
    // Airdrop SOL.
    let sig = rpc
        .request_airdrop(&user.pubkey(), 2 * LAMPORTS_PER_SOL)
        .expect("airdrop failed");
    confirm_transaction(rpc, &sig);

    // Create user USDC ATA.
    let user_ata = create_ata(rpc, payer, &user.pubkey(), usdc_mint_key);

    // Mint USDC to user.
    let mint_ix = token_instruction::mint_to(
        &spl_token_interface::ID,
        usdc_mint_key,
        &user_ata,
        &usdc_mint_authority.pubkey(),
        &[],
        usdc_amount,
    )
    .expect("mint_to instruction");

    send_and_confirm(rpc, payer, &[mint_ix], &[usdc_mint_authority]);

    user_ata
}

// ---------------------------------------------------------------------------
// CLI runner
// ---------------------------------------------------------------------------

/// Run the `doublezero-solana` CLI binary and capture output.
pub fn run_cli(args: &[&str]) -> (ExitStatus, String, String) {
    // Find the workspace root (two levels up from crates/solana-sdk).
    let workspace_root = workspace_root();
    let binary = workspace_root.join("target/debug/doublezero-solana");

    let output = Command::new(&binary)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("Failed to run CLI binary at {}: {e}", binary.display()));

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    (output.status, stdout, stderr)
}

/// Write a keypair to a fixed path (Solana CLI JSON format).
///
/// Uses a deterministic path so we don't leak temp directories across runs.
pub fn write_keypair_to_tempfile(keypair: &Keypair) -> PathBuf {
    let dir = PathBuf::from("/tmp/doublezero-e2e-cli");
    fs::create_dir_all(&dir).expect("create keypair dir");
    let path = dir.join("keypair.json");
    let bytes: Vec<u8> = keypair.to_bytes().to_vec();
    let json = serde_json::to_string(&bytes).expect("serialize keypair");
    fs::write(&path, json).expect("write keypair file");
    path
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    // Walk up from CARGO_MANIFEST_DIR to find the workspace root.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|p| p.join("Cargo.lock").exists())
        .unwrap_or(&manifest_dir)
        .to_path_buf()
}

fn send_admin_ix<A: Into<Vec<solana_sdk::instruction::AccountMeta>>>(
    rpc: &RpcClient,
    payer: &Keypair,
    extra_signers: &[&Keypair],
    accounts: A,
    data: &AdminInstructionData,
) {
    let ix = build_instruction(accounts, data);
    send_and_confirm(rpc, payer, &[ix], extra_signers);
}

fn send_and_confirm(
    rpc: &RpcClient,
    payer: &Keypair,
    instructions: &[Instruction],
    extra_signers: &[&Keypair],
) {
    let recent_blockhash = rpc.get_latest_blockhash().expect("get blockhash");

    // Deduplicate signers by pubkey (payer may also appear in extra_signers).
    let mut seen = std::collections::HashSet::new();
    let mut all_signers: Vec<&Keypair> = Vec::with_capacity(1 + extra_signers.len());
    for signer in std::iter::once(&payer).chain(extra_signers.iter()) {
        if seen.insert(signer.pubkey()) {
            all_signers.push(signer);
        }
    }

    let message = Message::try_compile(&payer.pubkey(), instructions, &[], recent_blockhash)
        .expect("compile message");

    let transaction = VersionedTransaction::try_new(VersionedMessage::V0(message), &all_signers)
        .expect("sign transaction");

    rpc.send_and_confirm_transaction(&transaction)
        .expect("send_and_confirm failed");
}

fn confirm_transaction(rpc: &RpcClient, sig: &solana_sdk::signature::Signature) {
    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(15) {
            panic!("transaction {sig} not confirmed within 15s");
        }
        if rpc.confirm_transaction(sig).unwrap_or(false) {
            return;
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn create_usdc_mint(rpc: &RpcClient, payer: &Keypair, mint_authority: &Keypair) -> Pubkey {
    let mint_keypair = Keypair::new();
    let mint_rent = rpc
        .get_minimum_balance_for_rent_exemption(Mint::LEN)
        .expect("get rent");

    let create_account_ix = solana_sdk::system_instruction::create_account(
        &payer.pubkey(),
        &mint_keypair.pubkey(),
        mint_rent,
        Mint::LEN as u64,
        &spl_token_interface::ID,
    );

    let init_mint_ix = token_instruction::initialize_mint(
        &spl_token_interface::ID,
        &mint_keypair.pubkey(),
        &mint_authority.pubkey(),
        Some(&mint_authority.pubkey()),
        6, // USDC decimals
    )
    .expect("init_mint instruction");

    send_and_confirm(
        rpc,
        payer,
        &[create_account_ix, init_mint_ix],
        &[&mint_keypair],
    );

    mint_keypair.pubkey()
}

fn create_ata(rpc: &RpcClient, payer: &Keypair, owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    let ata =
        spl_associated_token_account_interface::address::get_associated_token_address(owner, mint);

    let ix = spl_associated_token_account_interface::instruction::create_associated_token_account_idempotent(
        &payer.pubkey(),
        owner,
        mint,
        &spl_token_interface::ID,
    );

    send_and_confirm(rpc, payer, &[ix], &[]);

    ata
}
