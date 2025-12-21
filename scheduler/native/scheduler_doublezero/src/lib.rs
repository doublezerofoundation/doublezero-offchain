use std::sync::Arc;

use anyhow::Result;
use doublezero_solana_client_tools::{
    payer::{Wallet, try_load_keypair},
    rpc::{DoubleZeroLedgerConnection, SolanaConnection},
};
use doublezero_solana_sdk::revenue_distribution::fetch::try_fetch_config;
use doublezero_solana_validator_debt::{
    rpc::SolanaValidatorDebtConnectionOptions,
    solana_debt_calculator::SolanaDebtCalculator,
    transaction::{DebtCollectionResults, Transaction},
    worker,
};
use rustler::{Error as NifError, NifStruct};
use tokio::runtime::Runtime;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(NifStruct)]
#[module = "Scheduler.ValidatorDebt.DebtCollections"]
pub struct DebtCollections {
    pub debt_collections: Vec<DebtCollection>,
}

#[derive(NifStruct)]
#[module = "Scheduler.ValidatorDebt.DebtCollection"]
pub struct DebtCollection {
    pub dz_epoch: String,
    pub total_paid: String,
    pub total_debt: String,
    pub already_paid: String,
    pub outstanding_debt: String,
    pub total_validators: String,
    pub insufficient_funds_count: String,
}

#[derive(NifStruct)]
#[module = "Scheduler.ValidatorDebt.Debt"]
pub struct Debt {
    pub validator_id: String,
    pub amount: u64,
    pub result: Option<String>,
    pub success: bool,
}

#[rustler::nif]
pub fn initialize_tracing_subscriber() -> Result<(), NifError> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false),
        )
        .init();

    Ok(())
}

#[rustler::nif]
pub fn post_debt_summary(
    insufficient_funds_count: usize,
    total_debt: u64,
    total_paid: u64,
) -> Result<(), NifError> {
    Runtime::new()
        .map_err(display_to_nif_error)?
        .block_on(async {
            async_post_debt_summary(insufficient_funds_count, total_debt, total_paid).await
        })
        .map_err(display_to_nif_error)?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn pay_debt(
    dz_epoch: u64,
    ledger_rpc: String,
    solana_rpc: String,
) -> Result<DebtCollection, NifError> {
    // Block the current thread and wait for the async operation to complete.
    let tx_results = Runtime::new()
        .map_err(display_to_nif_error)?
        .block_on(async { async_pay_debt(dz_epoch, ledger_rpc, solana_rpc).await })
        .map_err(display_to_nif_error)?;
    let debt_collection = DebtCollection {
        dz_epoch: tx_results.dz_epoch.to_string(),
        already_paid: tx_results.already_paid.to_string(),
        total_debt: tx_results.total_debt.to_string(),
        total_paid: tx_results.total_paid.to_string(),
        outstanding_debt: (tx_results.total_debt - tx_results.total_paid).to_string(),
        total_validators: tx_results.total_validators.to_string(),
        insufficient_funds_count: tx_results.insufficient_funds_count.to_string(),
    };

    Ok(debt_collection)
}

#[rustler::nif]
pub fn initialize_distribution(solana_rpc: String) -> Result<(), NifError> {
    Runtime::new()
        .map_err(display_to_nif_error)?
        .block_on(async {
            let wallet = Wallet {
                connection: SolanaConnection::new(solana_rpc),
                signer: try_load_keypair(None)?,
                compute_unit_price_ix: None,
                verbose: false,
                fee_payer: None,
                dry_run: false,
            };

            worker::try_initialize_distribution(
                wallet, //
                None,   // dz_env
                false,  // bypass_dz_epoch_check
                None,   // record_accountant_key
                false,  // enable_debt_write_off
            )
            .await
        })
        .map_err(display_to_nif_error)?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn pay_debt_for_all_epochs(
    ledger_rpc: String,
    solana_rpc: String,
) -> Result<DebtCollections, NifError> {
    let ledger_rpc_client = DoubleZeroLedgerConnection::new(ledger_rpc);

    // TODO: collect debt collection results for all epochs and put into a single slack message
    let debt_collection_results = Runtime::new()
        .map_err(display_to_nif_error)?
        .block_on(async {
            let wallet = Wallet {
                connection: SolanaConnection::new(solana_rpc),
                signer: try_load_keypair(None)?,
                compute_unit_price_ix: None,
                verbose: false,
                fee_payer: None,
                dry_run: false,
            };

            worker::pay_all_solana_validator_debt(wallet, ledger_rpc_client).await
        })
        .map_err(display_to_nif_error)?;

    let debt_collections: Vec<DebtCollection> = debt_collection_results
        .iter()
        .map(|dc| DebtCollection {
            dz_epoch: dc.dz_epoch.to_string(),
            already_paid: dc.already_paid.to_string(),
            total_paid: dc.total_paid.to_string(),
            total_debt: dc.total_debt.to_string(),
            outstanding_debt: (dc.total_debt - dc.total_paid).to_string(),
            total_validators: dc.total_validators.to_string(),
            insufficient_funds_count: dc.insufficient_funds_count.to_string(),
        })
        .collect();

    Ok(DebtCollections { debt_collections })
}

#[rustler::nif]
pub fn current_dz_epoch(ledger_rpc: String) -> Result<u64, NifError> {
    let dz_epoch = Runtime::new()
        .map_err(display_to_nif_error)?
        .block_on(async { async_current_dz_epoch(ledger_rpc).await })
        .map_err(display_to_nif_error)?;

    Ok(dz_epoch)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn calculate_distribution(
    dz_epoch: u64,
    ledger_rpc: String,
    solana_rpc: String,
    post_to_slack: bool,
) -> Result<(), NifError> {
    let rt = tokio::runtime::Runtime::new().map_err(display_to_nif_error)?;

    // Block the current thread and wait for the async operation to complete.
    rt.block_on(async {
        async_calculate_distribution(dz_epoch, ledger_rpc, solana_rpc, post_to_slack).await
    })
    .map_err(display_to_nif_error)?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn finalize_distribution(
    dz_epoch: u64,
    ledger_rpc: String,
    solana_rpc: String,
) -> Result<(), NifError> {
    let rt = tokio::runtime::Runtime::new().map_err(display_to_nif_error)?;

    // Block the current thread and wait for the async operation to complete.
    rt.block_on(async { async_finalize_distribution(dz_epoch, ledger_rpc, solana_rpc).await })
        .map_err(display_to_nif_error)?;

    Ok(())
}
async fn async_post_debt_summary(
    insufficient_funds_count: usize,
    total_debt: u64,
    total_paid: u64,
) -> Result<()> {
    let client = reqwest::Client::new();

    let header = "Total Debt Collection";
    let table_header = vec![
        "Total Paid".to_string(),
        "Total Debt".to_string(),
        "Total Outstanding".to_string(),
        "Total Insufficient Funds Count".to_string(),
    ];
    let table_values = vec![
        format!("{:.9} SOL", total_paid as f64 * 1e-9),
        format!("{:.9} SOL", total_debt as f64 * 1e-9),
        format!("{:.9} SOL", (total_debt - total_paid) as f64 * 1e-9),
        insufficient_funds_count.to_string(),
    ];
    slack_notifier::validator_debt::post_to_slack(
        None,
        &client,
        header,
        table_header,
        table_values,
    )
    .await?;
    Ok(())
}

async fn async_pay_debt(
    dz_epoch: u64,
    ledger_rpc: String,
    solana_rpc: String,
) -> Result<DebtCollectionResults> {
    let sc = SolanaConnection::new(solana_rpc);

    let ledger_rpc_client = DoubleZeroLedgerConnection::new(ledger_rpc);

    let wallet = Wallet {
        connection: sc,
        signer: try_load_keypair(None)?,
        compute_unit_price_ix: None,
        verbose: false,
        fee_payer: None,
        dry_run: false,
    };

    let (_, config) = try_fetch_config(&wallet.connection).await?;

    let tx_results =
        worker::pay_solana_validator_debt(&wallet, &ledger_rpc_client, dz_epoch, &config).await?;

    worker::post_debt_collection_to_slack(tx_results.clone(), false, None).await?;

    Ok(tx_results)
}

async fn async_calculate_distribution(
    dz_epoch: u64,
    ledger_rpc: String,
    solana_rpc: String,
    post_to_slack: bool,
) -> Result<()> {
    let connection_options = SolanaValidatorDebtConnectionOptions {
        solana_url_or_moniker: Some(solana_rpc),
        dz_ledger_url: ledger_rpc,
    };
    let solana_debt_calculator: SolanaDebtCalculator =
        SolanaDebtCalculator::try_from(connection_options)?;
    let keypair = try_load_keypair(None)?;
    let arc_keypair = Arc::new(keypair);
    let transaction = Transaction::new(arc_keypair, false, false);

    let write_summary =
        worker::calculate_distribution(&solana_debt_calculator, transaction, dz_epoch, false)
            .await?;

    if post_to_slack {
        slack_notifier::validator_debt::post_distribution_to_slack(
            None,
            write_summary.solana_epoch,
            write_summary.dz_epoch,
            false,
            write_summary.total_debt,
            write_summary.total_validators,
            write_summary.transaction_id,
        )
        .await?;
    }

    Ok(())
}

async fn async_finalize_distribution(
    dz_epoch: u64,
    ledger_rpc: String,
    solana_rpc: String,
) -> Result<()> {
    let connection_options = SolanaValidatorDebtConnectionOptions {
        solana_url_or_moniker: Some(solana_rpc),
        dz_ledger_url: ledger_rpc,
    };
    let solana_debt_calculator: SolanaDebtCalculator =
        SolanaDebtCalculator::try_from(connection_options)?;
    let keypair = try_load_keypair(None)?;
    let arc_keypair = Arc::new(keypair);
    let transaction = Transaction::new(arc_keypair, false, false);

    worker::finalize_distribution(&solana_debt_calculator, transaction, dz_epoch).await?;
    Ok(())
}

async fn async_current_dz_epoch(ledger_rpc: String) -> Result<u64> {
    let ledger_rpc_client = DoubleZeroLedgerConnection::new(ledger_rpc);
    let dz_epoch_info = ledger_rpc_client.get_epoch_info().await?;
    Ok(dz_epoch_info.epoch)
}

fn display_to_nif_error(e: impl std::fmt::Display) -> NifError {
    NifError::Term(Box::new(e.to_string()))
}

rustler::init!("Elixir.Scheduler.DoubleZero");
