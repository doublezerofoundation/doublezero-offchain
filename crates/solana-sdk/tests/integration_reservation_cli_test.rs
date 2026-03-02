//! Integration tests for the `doublezero-solana reservation` CLI commands.
//!
//! These tests run the actual `doublezero-solana` binary against a real
//! `solana-test-validator` instance with a mock reservation program.
//!
//! Prerequisites:
//!   cargo build-sbf --manifest-path programs/mock-reservation-program/Cargo.toml
//!   cargo build -p doublezero-solana-cli --features experimental
//!
//! Run:
//!   SBF_OUT_DIR=programs/mock-reservation-program/target/deploy \
//!     cargo test -p doublezero-solana-sdk integration -- --nocapture

mod common;

use std::net::Ipv4Addr;

use common::integration::{self, TestValidator};
use doublezero_solana_sdk::reservation::state;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::{Keypair, Signer};

const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);

#[test]
fn integration_reservation_cli_workflow() {
    let program_so = find_program_so();

    // The payer is also the upgrade authority (needed by SetAdmin).
    let payer = Keypair::new();

    // 1. Start validator.
    let validator = TestValidator::start(&program_so, &payer.pubkey());
    let rpc = validator.rpc_client();

    // Fund the payer (it's a new keypair, not the validator's built-in payer).
    airdrop(&rpc, &payer.pubkey(), 10);

    // 2. Setup program state.
    let setup = integration::setup_program_state(&rpc, &payer);

    // 3. Fund user.
    let user = Keypair::new();
    let user_usdc_amount = 500_000_000u64; // 500 USDC
    let _user_ata = integration::fund_user(
        &rpc,
        &payer,
        &user,
        &setup.usdc_mint_key,
        &setup.usdc_mint_authority,
        user_usdc_amount,
    );

    // Write user keypair to temp file for CLI.
    let user_keypair_path = integration::write_keypair_to_tempfile(&user);
    let keypair_path_str = user_keypair_path.to_str().unwrap();
    let device_str = setup.device_key.to_string();
    let usdc_mint_str = setup.usdc_mint_key.to_string();

    // 4. CLI: initialize-seat (no --amount in per-seat PDA model)
    let (status, stdout, stderr) = integration::run_cli(&[
        "reservation",
        "initialize-seat",
        "--url",
        &validator.rpc_url,
        "--keypair",
        keypair_path_str,
        "--device",
        &device_str,
        "--client-ip",
        "10.0.0.1",
        "--usdc-mint",
        &usdc_mint_str,
    ]);
    assert!(
        status.success(),
        "initialize-seat failed:\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Verify seat created via RPC.
    let client_ip_bits = u32::from(CLIENT_IP);
    let (seat_key, _) = state::find_client_seat_address(&setup.device_key, client_ip_bits);
    let seat_account = rpc.get_account(&seat_key).expect("fetch seat account");
    let (device_key, ip, _tenure, _epoch, funding_account_key) =
        state::parse_client_seat(&seat_account.data).expect("parse seat");
    assert_eq!(device_key, setup.device_key);
    assert_eq!(ip, CLIENT_IP);
    assert_eq!(funding_account_key, user.pubkey());

    // Verify token PDA was created with 0 balance.
    let (token_pda_key, _) = state::find_token_pda_address(&seat_key, &setup.usdc_mint_key);
    let token_pda_account = rpc.get_account(&token_pda_key).expect("fetch token PDA");
    let token_state: spl_token_interface::state::Account =
        solana_program_pack::Pack::unpack(&token_pda_account.data).expect("parse token PDA");
    assert_eq!(
        token_state.amount, 0,
        "token PDA should start with 0 balance"
    );
    assert_eq!(token_state.mint, setup.usdc_mint_key);

    // Verify PaymentEscrow also created by initialize-seat.
    let (escrow_key, _) = state::find_payment_escrow_address(&seat_key, &user.pubkey());
    let escrow_account = rpc.get_account(&escrow_key).expect("fetch escrow account");
    let (escrow_seat_key, escrow_authority, escrow_balance) =
        state::parse_payment_escrow(&escrow_account.data).expect("parse escrow");
    assert_eq!(escrow_seat_key, seat_key);
    assert_eq!(escrow_authority, user.pubkey());
    assert_eq!(escrow_balance, 0);

    // 5b. CLI: price (filtered by device)
    let (status, stdout, stderr) = integration::run_cli(&[
        "reservation",
        "price",
        "--url",
        &validator.rpc_url,
        "--device",
        &device_str,
    ]);
    assert!(
        status.success(),
        "price failed:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("100"),
        "price output should contain base price 100, got:\n{stdout}"
    );
    assert!(
        stdout.contains(&device_str),
        "price output should contain device key, got:\n{stdout}"
    );

    // 6. CLI: list
    let (status, stdout, stderr) =
        integration::run_cli(&["reservation", "list", "--url", &validator.rpc_url]);
    assert!(
        status.success(),
        "list failed:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("10.0.0.1"),
        "list output should contain client IP, got:\n{stdout}"
    );

    // 7. CLI: withdraw (backed by ClosePaymentEscrow — escrow has 0 balance)
    let (status, stdout, stderr) = integration::run_cli(&[
        "reservation",
        "withdraw",
        "--url",
        &validator.rpc_url,
        "--keypair",
        keypair_path_str,
        "--device",
        &device_str,
        "--client-ip",
        "10.0.0.1",
        "--usdc-mint",
        &usdc_mint_str,
    ]);
    assert!(
        status.success(),
        "withdraw failed:\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Verify escrow closed.
    let escrow_result = rpc.get_account(&escrow_key);
    assert!(
        escrow_result.is_err() || escrow_result.unwrap().data.is_empty(),
        "escrow account should be closed after withdraw"
    );

    // Validator is killed on drop.
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn find_program_so() -> std::path::PathBuf {
    // SBF_OUT_DIR is set by the test runner.
    if let Ok(dir) = std::env::var("SBF_OUT_DIR") {
        let path = std::path::PathBuf::from(dir).join("mock_reservation_program.so");
        if path.exists() {
            return path;
        }
    }

    // Fallback: check relative to workspace root.
    let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|p| p.join("Cargo.lock").exists())
        .unwrap()
        .to_path_buf();
    let path = workspace_root
        .join("programs/mock-reservation-program/target/deploy/mock_reservation_program.so");
    assert!(
        path.exists(),
        "mock_reservation_program.so not found at {}. Run: cargo build-sbf --manifest-path programs/mock-reservation-program/Cargo.toml",
        path.display()
    );
    path
}

fn airdrop(rpc: &RpcClient, pubkey: &solana_sdk::pubkey::Pubkey, sol: u64) {
    let sig = rpc
        .request_airdrop(pubkey, sol * solana_sdk::native_token::LAMPORTS_PER_SOL)
        .expect("airdrop");
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(15) {
            panic!("airdrop not confirmed within 15s");
        }
        if rpc.confirm_transaction(&sig).unwrap_or(false) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}
