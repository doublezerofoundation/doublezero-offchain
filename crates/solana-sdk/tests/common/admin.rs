//! Local replicas of on-chain admin instruction types.
//!
//! These types mirror the admin-only instruction builders and data enums from
//! the on-chain program. They are used exclusively for integration test setup
//! (bootstrapping program state before testing the user-facing SDK
//! instructions).

use std::io;

use borsh::BorshSerialize;
use doublezero_solana_sdk::{
    DISCRIMINATOR_LEN, Discriminator,
    reservation::state::{
        find_device_history_address, find_execution_controller_address, find_metro_history_address,
        find_program_config_address, find_token_pda_address,
    },
};
use solana_loader_v3_interface::get_program_data_address;
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `ExecutionPhase::OpenForRequests` as a raw byte (matches the on-chain
/// `#[repr(u8)]` enum).
pub const EXECUTION_PHASE_OPEN_FOR_REQUESTS: u8 = 2;

// ---------------------------------------------------------------------------
// Admin instruction data
// ---------------------------------------------------------------------------

/// Admin instruction discriminators (SHA2-8, matching the on-chain program).
const INITIALIZE_PROGRAM: Discriminator<DISCRIMINATOR_LEN> =
    Discriminator::new_sha2(b"dz::ix::initialize_program");
const SET_ADMIN: Discriminator<DISCRIMINATOR_LEN> = Discriminator::new_sha2(b"dz::ix::set_admin");
const CONFIGURE_PROGRAM: Discriminator<DISCRIMINATOR_LEN> =
    Discriminator::new_sha2(b"dz::ix::configure_program");
const INITIALIZE_METRO_HISTORY: Discriminator<DISCRIMINATOR_LEN> =
    Discriminator::new_sha2(b"dz::ix::initialize_metro_history");
const INITIALIZE_DEVICE_HISTORY: Discriminator<DISCRIMINATOR_LEN> =
    Discriminator::new_sha2(b"dz::ix::initialize_device_history");
const SET_DEVICE_ENABLED: Discriminator<DISCRIMINATOR_LEN> =
    Discriminator::new_sha2(b"dz::ix::set_device_enabled");
const TEST_SETUP: Discriminator<DISCRIMINATOR_LEN> = Discriminator::new_sha2(b"dz::ix::test_setup");

/// Instruction data for admin-only on-chain instructions.
pub enum AdminInstructionData {
    InitializeProgram,
    SetAdmin(Pubkey),
    ConfigureProgram(ProgramConfiguration),
    InitializeMetroHistory(Pubkey),
    InitializeDeviceHistory(Pubkey),
    SetDeviceEnabled(bool),
    TestSetup {
        subscription_epoch: u64,
        metro_usdc_price: u16,
        device_premium: i16,
    },
}

impl BorshSerialize for AdminInstructionData {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Self::InitializeProgram => INITIALIZE_PROGRAM.serialize(writer),
            Self::SetAdmin(key) => {
                SET_ADMIN.serialize(writer)?;
                key.serialize(writer)
            }
            Self::ConfigureProgram(config) => {
                CONFIGURE_PROGRAM.serialize(writer)?;
                config.serialize(writer)
            }
            Self::InitializeMetroHistory(key) => {
                INITIALIZE_METRO_HISTORY.serialize(writer)?;
                key.serialize(writer)
            }
            Self::InitializeDeviceHistory(key) => {
                INITIALIZE_DEVICE_HISTORY.serialize(writer)?;
                key.serialize(writer)
            }
            Self::SetDeviceEnabled(enabled) => {
                SET_DEVICE_ENABLED.serialize(writer)?;
                enabled.serialize(writer)
            }
            Self::TestSetup {
                subscription_epoch,
                metro_usdc_price,
                device_premium,
            } => {
                TEST_SETUP.serialize(writer)?;
                subscription_epoch.serialize(writer)?;
                metro_usdc_price.serialize(writer)?;
                device_premium.serialize(writer)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Program configuration enums (standard Borsh derive — variant index matters)
// ---------------------------------------------------------------------------

#[derive(BorshSerialize)]
pub enum ProgramConfiguration {
    Flag(ProgramFlagConfiguration),         // 0
    ClosedForRequestsGracePeriodSlots(u32), // 1
    Usdc2zMaxSlippageBps(u16),              // 2
    Oracle(Pubkey),                         // 3
    Usdc2zOracle(Pubkey),                   // 4
    UsdcMint(Pubkey),                       // 5
}

#[derive(BorshSerialize)]
pub enum ProgramFlagConfiguration {
    IsPaused(bool),
}

// ---------------------------------------------------------------------------
// Account builder structs
// ---------------------------------------------------------------------------

pub struct InitializeProgramAccounts {
    payer_key: Pubkey,
    program_config_key: Pubkey,
    execution_controller_key: Pubkey,
}

impl InitializeProgramAccounts {
    pub fn new(payer_key: &Pubkey) -> Self {
        Self {
            payer_key: *payer_key,
            program_config_key: find_program_config_address().0,
            execution_controller_key: find_execution_controller_address().0,
        }
    }
}

impl From<InitializeProgramAccounts> for Vec<AccountMeta> {
    fn from(a: InitializeProgramAccounts) -> Self {
        vec![
            AccountMeta::new(a.payer_key, true),
            AccountMeta::new(a.program_config_key, false),
            AccountMeta::new(a.execution_controller_key, false),
            AccountMeta::new_readonly(solana_sdk::system_program::ID, false),
        ]
    }
}

pub struct SetAdminAccounts {
    program_data_key: Pubkey,
    upgrade_authority_key: Pubkey,
    program_config_key: Pubkey,
}

impl SetAdminAccounts {
    pub fn new(program_id: &Pubkey, upgrade_authority_key: &Pubkey) -> Self {
        Self {
            program_data_key: get_program_data_address(program_id),
            upgrade_authority_key: *upgrade_authority_key,
            program_config_key: find_program_config_address().0,
        }
    }
}

impl From<SetAdminAccounts> for Vec<AccountMeta> {
    fn from(a: SetAdminAccounts) -> Self {
        vec![
            AccountMeta::new_readonly(a.program_data_key, false),
            AccountMeta::new_readonly(a.upgrade_authority_key, true),
            AccountMeta::new(a.program_config_key, false),
        ]
    }
}

pub struct ConfigureProgramAccounts {
    program_config_key: Pubkey,
    admin_key: Pubkey,
}

impl ConfigureProgramAccounts {
    pub fn new(admin_key: &Pubkey) -> Self {
        Self {
            program_config_key: find_program_config_address().0,
            admin_key: *admin_key,
        }
    }
}

impl From<ConfigureProgramAccounts> for Vec<AccountMeta> {
    fn from(a: ConfigureProgramAccounts) -> Self {
        vec![
            AccountMeta::new(a.program_config_key, false),
            AccountMeta::new_readonly(a.admin_key, true),
        ]
    }
}

pub struct InitializeMetroHistoryAccounts {
    program_config_key: Pubkey,
    oracle_key: Pubkey,
    execution_controller_key: Pubkey,
    payer_key: Pubkey,
    new_metro_history_key: Pubkey,
}

impl InitializeMetroHistoryAccounts {
    pub fn new(oracle_key: &Pubkey, payer_key: &Pubkey, exchange_key: &Pubkey) -> Self {
        Self {
            program_config_key: find_program_config_address().0,
            oracle_key: *oracle_key,
            execution_controller_key: find_execution_controller_address().0,
            payer_key: *payer_key,
            new_metro_history_key: find_metro_history_address(exchange_key).0,
        }
    }
}

impl From<InitializeMetroHistoryAccounts> for Vec<AccountMeta> {
    fn from(a: InitializeMetroHistoryAccounts) -> Self {
        vec![
            AccountMeta::new_readonly(a.program_config_key, false),
            AccountMeta::new_readonly(a.oracle_key, true),
            AccountMeta::new(a.execution_controller_key, false),
            AccountMeta::new(a.payer_key, true),
            AccountMeta::new(a.new_metro_history_key, false),
            AccountMeta::new_readonly(solana_sdk::system_program::ID, false),
        ]
    }
}

pub struct InitializeDeviceHistoryAccounts {
    program_config_key: Pubkey,
    oracle_key: Pubkey,
    execution_controller_key: Pubkey,
    metro_history_key: Pubkey,
    payer_key: Pubkey,
    new_device_history_key: Pubkey,
    new_device_history_usdc_token_pda_key: Pubkey,
    usdc_mint_key: Pubkey,
}

impl InitializeDeviceHistoryAccounts {
    pub fn new(
        oracle_key: &Pubkey,
        payer_key: &Pubkey,
        exchange_key: &Pubkey,
        device_key: &Pubkey,
        usdc_mint_key: &Pubkey,
    ) -> Self {
        let new_device_history_key = find_device_history_address(device_key).0;
        Self {
            program_config_key: find_program_config_address().0,
            oracle_key: *oracle_key,
            execution_controller_key: find_execution_controller_address().0,
            metro_history_key: find_metro_history_address(exchange_key).0,
            payer_key: *payer_key,
            new_device_history_key,
            new_device_history_usdc_token_pda_key: find_token_pda_address(
                &new_device_history_key,
                usdc_mint_key,
            )
            .0,
            usdc_mint_key: *usdc_mint_key,
        }
    }
}

impl From<InitializeDeviceHistoryAccounts> for Vec<AccountMeta> {
    fn from(a: InitializeDeviceHistoryAccounts) -> Self {
        vec![
            AccountMeta::new_readonly(a.program_config_key, false),
            AccountMeta::new_readonly(a.oracle_key, true),
            AccountMeta::new(a.execution_controller_key, false),
            AccountMeta::new(a.metro_history_key, false),
            AccountMeta::new(a.payer_key, true),
            AccountMeta::new(a.new_device_history_key, false),
            AccountMeta::new(a.new_device_history_usdc_token_pda_key, false),
            AccountMeta::new_readonly(a.usdc_mint_key, false),
            AccountMeta::new_readonly(spl_token_interface::ID, false),
            AccountMeta::new_readonly(solana_sdk::system_program::ID, false),
        ]
    }
}

pub struct SetDeviceEnabledAccounts {
    program_config_key: Pubkey,
    admin_key: Pubkey,
    device_history_key: Pubkey,
    execution_controller_key: Pubkey,
}

impl SetDeviceEnabledAccounts {
    pub fn new(admin_key: &Pubkey, device_key: &Pubkey) -> Self {
        Self {
            program_config_key: find_program_config_address().0,
            admin_key: *admin_key,
            device_history_key: find_device_history_address(device_key).0,
            execution_controller_key: find_execution_controller_address().0,
        }
    }
}

impl From<SetDeviceEnabledAccounts> for Vec<AccountMeta> {
    fn from(a: SetDeviceEnabledAccounts) -> Self {
        vec![
            AccountMeta::new_readonly(a.program_config_key, false),
            AccountMeta::new_readonly(a.admin_key, true),
            AccountMeta::new(a.device_history_key, false),
            AccountMeta::new(a.execution_controller_key, false),
        ]
    }
}

pub struct TestSetupAccounts {
    program_config_key: Pubkey,
    admin_key: Pubkey,
    execution_controller_key: Pubkey,
    metro_history_key: Pubkey,
    device_history_key: Pubkey,
}

impl TestSetupAccounts {
    pub fn new(admin_key: &Pubkey, exchange_key: &Pubkey, device_key: &Pubkey) -> Self {
        Self {
            program_config_key: find_program_config_address().0,
            admin_key: *admin_key,
            execution_controller_key: find_execution_controller_address().0,
            metro_history_key: find_metro_history_address(exchange_key).0,
            device_history_key: find_device_history_address(device_key).0,
        }
    }
}

impl From<TestSetupAccounts> for Vec<AccountMeta> {
    fn from(a: TestSetupAccounts) -> Self {
        vec![
            AccountMeta::new_readonly(a.program_config_key, false),
            AccountMeta::new_readonly(a.admin_key, true),
            AccountMeta::new(a.execution_controller_key, false),
            AccountMeta::new(a.metro_history_key, false),
            AccountMeta::new(a.device_history_key, false),
        ]
    }
}
