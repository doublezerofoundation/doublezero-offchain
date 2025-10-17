use anyhow::{Result, bail};
use clap::Args;
use doublezero_program_tools::instruction::try_build_instruction;
use doublezero_sol_conversion_interface::{
    ID,
    instruction::{SolConversionInstructionData, account::BuySolAccounts},
};
use doublezero_solana_client_tools::payer::{SolanaPayerOptions, Wallet};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, pubkey::Pubkey};

use crate::command::revenue_distribution::{
    SolConversionState, try_request_oracle_conversion_price,
};

#[derive(Debug, Args, Clone)]
pub struct ConvertSolCommand {
    /// Limit price defaults to the current SOL/2Z oracle price.
    #[arg(long, value_name = "DECIMAL")]
    limit_price: Option<String>,

    /// Token account must be owned by the signer.
    #[arg(long, value_name = "PUBKEY")]
    source_token_account: Option<Pubkey>,

    /// For testing purposes if RPC is a mainnet-beta fork.
    #[arg(long, short = 'm')]
    mainnet_fork: bool,

    #[command(flatten)]
    solana_payer_options: SolanaPayerOptions,
}

impl ConvertSolCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let Self {
            limit_price: limit_price_str,
            source_token_account: source_token_account_key,
            mainnet_fork: is_mainnet_fork,
            solana_payer_options,
        } = self;

        let mut wallet = Wallet::try_from(solana_payer_options)?;
        let wallet_key = wallet.pubkey();

        wallet.connection.cache_if_mainnet().await?;

        let dz_mint_key = if is_mainnet_fork || wallet.connection.is_mainnet {
            doublezero_revenue_distribution::env::mainnet::DOUBLEZERO_MINT_KEY
        } else {
            doublezero_revenue_distribution::env::development::DOUBLEZERO_MINT_KEY
        };

        let oracle_price_data = try_request_oracle_conversion_price().await?;

        let limit_price = match limit_price_str {
            Some(limit_price_str) => parse_bid_price_to_u64(limit_price_str)?,
            None => oracle_price_data.swap_rate,
        };

        let user_token_account_key = source_token_account_key.unwrap_or(
            spl_associated_token_account_interface::address::get_associated_token_address(
                &wallet_key,
                &dz_mint_key,
            ),
        );

        let SolConversionState {
            program_state: (_, sol_conversion_program_state),
            ..
        } = SolConversionState::try_fetch(&wallet.connection).await?;

        let mut instructions = Vec::new();
        let compute_unit_limit = 80_000;

        let buy_sol_ix = try_build_instruction(
            &ID,
            BuySolAccounts::new(
                &sol_conversion_program_state.fills_registry_key,
                &user_token_account_key,
                &dz_mint_key,
                &wallet_key,
            ),
            &SolConversionInstructionData::BuySol {
                limit_price,
                oracle_price_data,
            },
        )?;
        instructions.push(buy_sol_ix);

        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            compute_unit_limit,
        ));

        if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
            instructions.push(compute_unit_price_ix.clone());
        }

        let transaction = wallet.new_transaction(&instructions).await?;
        let tx_sig = wallet.send_or_simulate_transaction(&transaction).await?;

        if let Some(tx_sig) = tx_sig {
            println!("Buy SOL: {tx_sig}");
            wallet.print_verbose_output(&[tx_sig]).await?;
        }

        Ok(())
    }
}

//

fn parse_bid_price_to_u64(bid_price_str: String) -> Result<u64> {
    const SCALE_FACTOR: f64 = 1e8;

    let bid_price_str = bid_price_str.trim();

    if bid_price_str.is_empty() {
        bail!("Bid price cannot be empty");
    }

    let bid_price = bid_price_str
        .parse::<f64>()
        .map_err(|_| anyhow::anyhow!("Invalid bid price: '{bid_price_str}'"))?;

    if bid_price <= 0.0 {
        bail!("Bid price must be a positive value");
    }

    if bid_price > (u64::MAX as f64 / SCALE_FACTOR) {
        bail!("Bid price too large");
    }

    // Check that value is at most 8 decimal places.
    if let Some(decimal_index) = bid_price_str.find('.') {
        let decimal_places = bid_price_str.len() - decimal_index - 1;
        if decimal_places > 8 {
            bail!("Bid price cannot have more than 8 decimal places");
        }
    }

    Ok((bid_price * SCALE_FACTOR).round() as u64)
}
