use anyhow::{Context, Result, bail};
use clap::Args;
use doublezero_solana_client_tools::payer::{SolanaPayerOptions, Wallet};
use doublezero_solana_sdk::shred_subscription::state::{
    ValidatorClientRewardsInfo, find_claim_holding_address, find_validator_client_rewards_address,
    parse_validator_client_rewards,
};
use solana_sdk::{commitment_config::CommitmentConfig, program_pack::Pack, pubkey::Pubkey};
use spl_associated_token_account_interface::address::get_associated_token_address;

/*
   doublezero-solana shreds validator-client-rewards show \
       --client-id <ID> [--rewards-token-mint <PUBKEY>] \
       [--subscription-epoch <EPOCH> ...]
*/

#[derive(Debug, Args)]
pub struct ShowCommand {
    /// Validator client ID.
    #[arg(long)]
    pub client_id: u16,
    /// Filter to a specific token mint when listing holdings.
    #[arg(long)]
    pub rewards_token_mint: Option<Pubkey>,
    /// One or more subscription epochs to inspect. Requires --rewards-token-mint.
    #[arg(long = "subscription-epoch", num_args = 0..)]
    pub subscription_epochs: Vec<u64>,
    #[command(flatten)]
    pub solana_payer_options: SolanaPayerOptions,
}

pub(crate) fn render_vcr_summary(
    client_id: u16,
    vcr_key: &Pubkey,
    info: &ValidatorClientRewardsInfo,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Validator client rewards (client_id={client_id})\n"
    ));
    out.push_str(&format!("  PDA                 : {vcr_key}\n"));
    out.push_str(&format!("  manager             : {}\n", info.manager_key));
    out.push_str(&format!(
        "  description         : {}\n",
        info.short_description.as_deref().unwrap_or("(none)")
    ));
    out.push_str(&format!(
        "  claim holding count : {}\n",
        info.claim_holding_count
    ));
    out
}

// Format is grep'd by sh/test_doublezero_solana_fork.sh — keep
// "  epoch <num>  <pda>  balance=<n>" stable or update the grep.
pub(crate) fn render_holding_row(epoch: u64, holding_key: &Pubkey, amount: Option<u64>) -> String {
    match amount {
        Some(amt) => format!("  epoch {epoch:>5}  {holding_key}  balance={amt}"),
        None => format!("  epoch {epoch:>5}  {holding_key}  (not initialized)"),
    }
}

pub(crate) fn render_manager_ata_row(ata: &Pubkey, amount: Option<u64>) -> String {
    match amount {
        Some(amt) => format!("  manager ATA  {ata}  balance={amt}"),
        None => format!("  manager ATA  {ata}  (not initialized)"),
    }
}

impl ShowCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        if !self.subscription_epochs.is_empty() && self.rewards_token_mint.is_none() {
            bail!("--subscription-epoch requires --rewards-token-mint");
        }

        let dz_connection = self
            .solana_payer_options
            .connection_options
            .clone()
            .into_shred_subscription_connection();
        let mut wallet = Wallet::try_from(self.solana_payer_options)?;
        wallet.connection = dz_connection;

        let vcr_key = find_validator_client_rewards_address(self.client_id).0;
        let vcr_account = wallet
            .connection
            .get_account_with_commitment(&vcr_key, CommitmentConfig::confirmed())
            .await
            .with_context(|| format!("fetching VCR PDA {vcr_key}"))?
            .value;
        let vcr_data = match vcr_account {
            Some(acct) => acct.data,
            None => {
                println!(
                    "Validator client rewards not initialized for client-id {} (PDA {vcr_key})",
                    self.client_id
                );
                return Ok(());
            }
        };
        let info = parse_validator_client_rewards(&vcr_data)
            .with_context(|| format!("failed to parse ValidatorClientRewards at {vcr_key}"))?;
        print!("{}", render_vcr_summary(self.client_id, &vcr_key, &info));

        // When a mint is supplied, always print the manager's ATA address and
        // balance. Per-epoch holding rows are only listed when the user also
        // supplies one or more `--subscription-epoch` values.
        if let Some(mint) = self.rewards_token_mint {
            println!("Claim holdings for mint {mint}:");
            let manager_ata = get_associated_token_address(&info.manager_key, &mint);
            let ata_account = wallet
                .connection
                .get_account_with_commitment(&manager_ata, CommitmentConfig::confirmed())
                .await
                .with_context(|| format!("fetching manager ATA {manager_ata}"))?
                .value;
            let ata_amount = match ata_account {
                Some(acct) if acct.owner == spl_token_interface::ID => {
                    spl_token_interface::state::Account::unpack(&acct.data)
                        .ok()
                        .map(|t| t.amount)
                }
                _ => None,
            };
            println!("{}", render_manager_ata_row(&manager_ata, ata_amount));

            if !self.subscription_epochs.is_empty() {
                let holding_keys: Vec<Pubkey> = self
                    .subscription_epochs
                    .iter()
                    .map(|e| find_claim_holding_address(&vcr_key, *e, &mint).0)
                    .collect();
                let holding_accounts = wallet
                    .connection
                    .get_multiple_accounts(&holding_keys)
                    .await
                    .with_context(|| "fetching claim holdings")?;
                for ((epoch, key), maybe_acct) in self
                    .subscription_epochs
                    .iter()
                    .zip(holding_keys.iter())
                    .zip(holding_accounts.into_iter())
                {
                    let amount = match maybe_acct {
                        Some(acct) if acct.owner == spl_token_interface::ID => {
                            spl_token_interface::state::Account::unpack(&acct.data)
                                .ok()
                                .map(|t| t.amount)
                        }
                        _ => None,
                    };
                    println!("{}", render_holding_row(*epoch, key, amount));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        cmd: ShowCommand,
    }

    #[test]
    fn parses_minimum_args() {
        let cli = Cli::try_parse_from(["test", "--client-id", "7"]).unwrap();
        assert_eq!(cli.cmd.client_id, 7);
        assert!(cli.cmd.rewards_token_mint.is_none());
        assert!(cli.cmd.subscription_epochs.is_empty());
    }

    #[test]
    fn parses_full_inspection_args() {
        let mint = Pubkey::new_unique();
        let cli = Cli::try_parse_from([
            "test",
            "--client-id",
            "7",
            "--rewards-token-mint",
            &mint.to_string(),
            "--subscription-epoch",
            "100",
            "--subscription-epoch",
            "101",
        ])
        .unwrap();
        assert_eq!(cli.cmd.rewards_token_mint, Some(mint));
        assert_eq!(cli.cmd.subscription_epochs, vec![100u64, 101]);
    }

    #[test]
    fn render_vcr_summary_uses_none_when_description_empty() {
        let info = ValidatorClientRewardsInfo {
            client_id: 7,
            manager_key: Pubkey::new_from_array([1u8; 32]),
            short_description: None,
            claim_holding_count: 0,
        };
        let key = Pubkey::new_from_array([2u8; 32]);
        let out = render_vcr_summary(7, &key, &info);
        assert!(out.contains("description         : (none)"));
        assert!(out.contains("claim holding count : 0"));
        assert!(out.contains(&info.manager_key.to_string()));
        assert!(out.contains(&key.to_string()));
    }

    #[test]
    fn render_vcr_summary_renders_description() {
        let info = ValidatorClientRewardsInfo {
            client_id: 7,
            manager_key: Pubkey::new_from_array([1u8; 32]),
            short_description: Some("acme".to_string()),
            claim_holding_count: 4,
        };
        let key = Pubkey::new_from_array([2u8; 32]);
        let out = render_vcr_summary(7, &key, &info);
        assert!(out.contains("description         : acme"));
        assert!(out.contains("claim holding count : 4"));
    }

    #[test]
    fn render_holding_row_present_and_absent() {
        let key = Pubkey::new_from_array([3u8; 32]);
        let present = render_holding_row(100, &key, Some(1_234_567));
        let absent = render_holding_row(101, &key, None);
        assert!(present.contains("epoch   100"));
        assert!(present.contains("balance=1234567"));
        assert!(absent.contains("(not initialized)"));
    }

    #[test]
    fn render_manager_ata_row_present_and_absent() {
        let ata = Pubkey::new_from_array([4u8; 32]);
        let present = render_manager_ata_row(&ata, Some(9_876_543));
        let absent = render_manager_ata_row(&ata, None);
        assert!(present.contains("manager ATA"));
        assert!(present.contains(&ata.to_string()));
        assert!(present.contains("balance=9876543"));
        assert!(absent.contains("manager ATA"));
        assert!(absent.contains(&ata.to_string()));
        assert!(absent.contains("(not initialized)"));
    }
}
