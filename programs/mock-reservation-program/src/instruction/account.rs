use doublezero_program_tools::get_program_data_address;
use solana_instruction::AccountMeta;
use solana_pubkey::Pubkey;
use solana_system_interface::program as system_program;

use crate::state::{DeviceHistory, ExecutionController, MetroHistory, ProgramConfig, find_token_pda_address};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeProgramAccounts {
    pub payer_key: Pubkey,
    pub new_program_config_key: Pubkey,
    pub new_execution_controller_key: Pubkey,
}

impl InitializeProgramAccounts {
    pub fn new(payer_key: &Pubkey) -> Self {
        let new_program_config_key = ProgramConfig::find_address().0;
        let new_execution_controller_key = ExecutionController::find_address().0;

        Self {
            payer_key: *payer_key,
            new_program_config_key,
            new_execution_controller_key,
        }
    }
}

impl From<InitializeProgramAccounts> for Vec<AccountMeta> {
    fn from(accounts: InitializeProgramAccounts) -> Self {
        let InitializeProgramAccounts {
            payer_key,
            new_program_config_key,
            new_execution_controller_key,
        } = accounts;

        vec![
            AccountMeta::new(payer_key, true),
            AccountMeta::new(new_program_config_key, false),
            AccountMeta::new(new_execution_controller_key, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAdminAccounts {
    pub program_data_key: Pubkey,
    pub upgrade_authority_key: Pubkey,
    pub program_config_key: Pubkey,
}

impl SetAdminAccounts {
    pub fn new(program_id: &Pubkey, upgrade_authority_key: &Pubkey) -> Self {
        Self {
            program_data_key: get_program_data_address(program_id).0,
            upgrade_authority_key: *upgrade_authority_key,
            program_config_key: ProgramConfig::find_address().0,
        }
    }
}

impl From<SetAdminAccounts> for Vec<AccountMeta> {
    fn from(accounts: SetAdminAccounts) -> Self {
        let SetAdminAccounts {
            program_data_key,
            upgrade_authority_key,
            program_config_key,
        } = accounts;

        vec![
            AccountMeta::new_readonly(program_data_key, false),
            AccountMeta::new_readonly(upgrade_authority_key, true),
            AccountMeta::new(program_config_key, false),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigureProgramAccounts {
    pub program_config_key: Pubkey,
    pub admin_key: Pubkey,
}

impl ConfigureProgramAccounts {
    pub fn new(admin_key: &Pubkey) -> Self {
        Self {
            program_config_key: ProgramConfig::find_address().0,
            admin_key: *admin_key,
        }
    }
}

impl From<ConfigureProgramAccounts> for Vec<AccountMeta> {
    fn from(accounts: ConfigureProgramAccounts) -> Self {
        let ConfigureProgramAccounts {
            program_config_key,
            admin_key,
        } = accounts;

        vec![
            AccountMeta::new(program_config_key, false),
            AccountMeta::new_readonly(admin_key, true),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeMetroHistoryAccounts {
    pub program_config_key: Pubkey,
    pub oracle_key: Pubkey,
    pub execution_controller_key: Pubkey,
    pub payer_key: Pubkey,
    pub new_metro_history_key: Pubkey,
}

impl InitializeMetroHistoryAccounts {
    pub fn new(oracle_key: &Pubkey, payer_key: &Pubkey, exchange_key: &Pubkey) -> Self {
        let program_config_key = ProgramConfig::find_address().0;
        let execution_controller_key = ExecutionController::find_address().0;
        let new_metro_history_key = MetroHistory::find_address(exchange_key).0;

        Self {
            program_config_key,
            oracle_key: *oracle_key,
            execution_controller_key,
            payer_key: *payer_key,
            new_metro_history_key,
        }
    }
}

impl From<InitializeMetroHistoryAccounts> for Vec<AccountMeta> {
    fn from(accounts: InitializeMetroHistoryAccounts) -> Self {
        let InitializeMetroHistoryAccounts {
            program_config_key,
            oracle_key,
            execution_controller_key,
            payer_key,
            new_metro_history_key,
        } = accounts;

        vec![
            AccountMeta::new_readonly(program_config_key, false),
            AccountMeta::new_readonly(oracle_key, true),
            AccountMeta::new(execution_controller_key, false),
            AccountMeta::new(payer_key, true),
            AccountMeta::new(new_metro_history_key, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeDeviceHistoryAccounts {
    pub program_config_key: Pubkey,
    pub oracle_key: Pubkey,
    pub execution_controller_key: Pubkey,
    pub metro_history_key: Pubkey,
    pub payer_key: Pubkey,
    pub new_device_history_key: Pubkey,
    pub new_device_history_usdc_token_pda_key: Pubkey,
    pub usdc_mint_key: Pubkey,
}

impl InitializeDeviceHistoryAccounts {
    pub fn new(
        oracle_key: &Pubkey,
        payer_key: &Pubkey,
        exchange_key: &Pubkey,
        device_key: &Pubkey,
        usdc_mint_key: &Pubkey,
    ) -> Self {
        let program_config_key = ProgramConfig::find_address().0;
        let execution_controller_key = ExecutionController::find_address().0;
        let metro_history_key = MetroHistory::find_address(exchange_key).0;
        let new_device_history_key = DeviceHistory::find_address(device_key).0;
        let new_device_history_usdc_token_pda_key =
            find_token_pda_address(&new_device_history_key, usdc_mint_key).0;

        Self {
            program_config_key,
            oracle_key: *oracle_key,
            execution_controller_key,
            metro_history_key,
            payer_key: *payer_key,
            new_device_history_key,
            new_device_history_usdc_token_pda_key,
            usdc_mint_key: *usdc_mint_key,
        }
    }
}

impl From<InitializeDeviceHistoryAccounts> for Vec<AccountMeta> {
    fn from(accounts: InitializeDeviceHistoryAccounts) -> Self {
        let InitializeDeviceHistoryAccounts {
            program_config_key,
            oracle_key,
            execution_controller_key,
            metro_history_key,
            payer_key,
            new_device_history_key,
            new_device_history_usdc_token_pda_key,
            usdc_mint_key,
        } = accounts;

        vec![
            AccountMeta::new_readonly(program_config_key, false),
            AccountMeta::new_readonly(oracle_key, true),
            AccountMeta::new(execution_controller_key, false),
            AccountMeta::new(metro_history_key, false),
            AccountMeta::new(payer_key, true),
            AccountMeta::new(new_device_history_key, false),
            AccountMeta::new(new_device_history_usdc_token_pda_key, false),
            AccountMeta::new_readonly(usdc_mint_key, false),
            AccountMeta::new_readonly(spl_token_interface::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetDeviceEnabledAccounts {
    pub program_config_key: Pubkey,
    pub admin_key: Pubkey,
    pub device_history_key: Pubkey,
    pub execution_controller_key: Pubkey,
}

impl SetDeviceEnabledAccounts {
    pub fn new(admin_key: &Pubkey, device_key: &Pubkey) -> Self {
        let program_config_key = ProgramConfig::find_address().0;
        let device_history_key = DeviceHistory::find_address(device_key).0;
        let execution_controller_key = ExecutionController::find_address().0;

        Self {
            program_config_key,
            admin_key: *admin_key,
            device_history_key,
            execution_controller_key,
        }
    }
}

impl From<SetDeviceEnabledAccounts> for Vec<AccountMeta> {
    fn from(accounts: SetDeviceEnabledAccounts) -> Self {
        let SetDeviceEnabledAccounts {
            program_config_key,
            admin_key,
            device_history_key,
            execution_controller_key,
        } = accounts;

        vec![
            AccountMeta::new_readonly(program_config_key, false),
            AccountMeta::new_readonly(admin_key, true),
            AccountMeta::new(device_history_key, false),
            AccountMeta::new(execution_controller_key, false),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvanceExecutionPhaseAccounts {
    pub program_config_key: Pubkey,
    pub oracle_key: Pubkey,
    pub execution_controller_key: Pubkey,
}

impl AdvanceExecutionPhaseAccounts {
    pub fn new(oracle_key: &Pubkey) -> Self {
        let program_config_key = ProgramConfig::find_address().0;
        let execution_controller_key = ExecutionController::find_address().0;

        Self {
            program_config_key,
            oracle_key: *oracle_key,
            execution_controller_key,
        }
    }
}

impl From<AdvanceExecutionPhaseAccounts> for Vec<AccountMeta> {
    fn from(accounts: AdvanceExecutionPhaseAccounts) -> Self {
        let AdvanceExecutionPhaseAccounts {
            program_config_key,
            oracle_key,
            execution_controller_key,
        } = accounts;

        vec![
            AccountMeta::new_readonly(program_config_key, false),
            AccountMeta::new_readonly(oracle_key, true),
            AccountMeta::new(execution_controller_key, false),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateMetroUsdcPriceAccounts {
    pub program_config_key: Pubkey,
    pub oracle_key: Pubkey,
    pub execution_controller_key: Pubkey,
    pub metro_history_key: Pubkey,
}

impl UpdateMetroUsdcPriceAccounts {
    pub fn new(oracle_key: &Pubkey, exchange_key: &Pubkey) -> Self {
        let program_config_key = ProgramConfig::find_address().0;
        let execution_controller_key = ExecutionController::find_address().0;
        let metro_history_key = MetroHistory::find_address(exchange_key).0;

        Self {
            program_config_key,
            oracle_key: *oracle_key,
            execution_controller_key,
            metro_history_key,
        }
    }
}

impl From<UpdateMetroUsdcPriceAccounts> for Vec<AccountMeta> {
    fn from(accounts: UpdateMetroUsdcPriceAccounts) -> Self {
        let UpdateMetroUsdcPriceAccounts {
            program_config_key,
            oracle_key,
            execution_controller_key,
            metro_history_key,
        } = accounts;

        vec![
            AccountMeta::new_readonly(program_config_key, false),
            AccountMeta::new_readonly(oracle_key, true),
            AccountMeta::new_readonly(execution_controller_key, false),
            AccountMeta::new(metro_history_key, false),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestSetupAccounts {
    pub program_config_key: Pubkey,
    pub admin_key: Pubkey,
    pub execution_controller_key: Pubkey,
    pub metro_history_key: Pubkey,
    pub device_history_key: Pubkey,
}

impl TestSetupAccounts {
    pub fn new(
        admin_key: &Pubkey,
        exchange_key: &Pubkey,
        device_key: &Pubkey,
    ) -> Self {
        Self {
            program_config_key: ProgramConfig::find_address().0,
            admin_key: *admin_key,
            execution_controller_key: ExecutionController::find_address().0,
            metro_history_key: MetroHistory::find_address(exchange_key).0,
            device_history_key: DeviceHistory::find_address(device_key).0,
        }
    }
}

impl From<TestSetupAccounts> for Vec<AccountMeta> {
    fn from(accounts: TestSetupAccounts) -> Self {
        let TestSetupAccounts {
            program_config_key,
            admin_key,
            execution_controller_key,
            metro_history_key,
            device_history_key,
        } = accounts;

        vec![
            AccountMeta::new_readonly(program_config_key, false),
            AccountMeta::new_readonly(admin_key, true),
            AccountMeta::new(execution_controller_key, false),
            AccountMeta::new(metro_history_key, false),
            AccountMeta::new(device_history_key, false),
        ]
    }
}
