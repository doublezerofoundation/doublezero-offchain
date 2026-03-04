use std::{cell::RefMut, net::Ipv4Addr};

use borsh::BorshDeserialize;
use bytemuck::Pod;
use doublezero_program_tools::{
    account_info::{
        try_next_enumerated_account, NextAccountOptions, TryNextAccounts, UpgradeAuthority,
    },
    recipe::Invoker,
    zero_copy::{self, ZeroCopyAccount, ZeroCopyMutAccount},
    Discriminator, PrecomputedDiscriminator, DISCRIMINATOR_LEN,
};
use doublezero_revenue_distribution::types::UnitShare16;
use solana_account_info::{AccountInfo, MAX_PERMITTED_DATA_INCREASE};
use solana_cpi::invoke_signed;
use solana_msg::msg;
use solana_program_error::{ProgramError, ProgramResult};
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_sysvar::{clock::Clock, epoch_schedule::EpochSchedule, rent::Rent, Sysvar};

use crate::{
    instruction::{
        ProgramConfiguration, ProgramFlagConfiguration, ReservationInstructionData,
    },
    state::{
        ClientSeat, DeviceHistory, ExecutionController, ExecutionPhase, MetroHistory,
        PaymentEscrow, ProgramConfig, find_token_pda_address,
    },
    ID,
};

solana_program_entrypoint::entrypoint!(try_process_instruction);

fn try_process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    if program_id != &ID {
        return Err(ProgramError::IncorrectProgramId);
    }

    // Read discriminator from raw bytes.
    let discriminator = Discriminator::<DISCRIMINATOR_LEN>::deserialize_reader(&mut &*data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    // User-facing instructions — matched by raw discriminator so the enum
    // doesn't need to carry these variants (they live in the offchain SDK).
    if discriminator == ReservationInstructionData::INITIALIZE_CLIENT_SEAT {
        let mut reader = &data[DISCRIMINATOR_LEN..];
        let ip_bits: u32 = BorshDeserialize::deserialize_reader(&mut reader)
            .map_err(|_| ProgramError::InvalidInstructionData)?;
        return try_initialize_client_seat(accounts, Ipv4Addr::from(ip_bits));
    }
    if discriminator == ReservationInstructionData::INITIALIZE_PAYMENT_ESCROW {
        return try_initialize_payment_escrow(accounts);
    }
    if discriminator == ReservationInstructionData::CLOSE_PAYMENT_ESCROW {
        return try_close_payment_escrow(accounts);
    }
    if discriminator == ReservationInstructionData::FUND_PAYMENT_ESCROW_USDC {
        let mut reader = &data[DISCRIMINATOR_LEN..];
        let amount: u64 = BorshDeserialize::deserialize_reader(&mut reader)
            .map_err(|_| ProgramError::InvalidInstructionData)?;
        return try_fund_payment_escrow_usdc(accounts, amount);
    }
    if discriminator == ReservationInstructionData::DEDUCT_SUBSCRIPTION_FEE {
        return try_deduct_subscription_fee(accounts);
    }

    // Admin/oracle instructions — go through the enum.
    let ix_data =
        BorshDeserialize::try_from_slice(data).map_err(|_| ProgramError::InvalidInstructionData)?;

    match ix_data {
        ReservationInstructionData::InitializeProgram => try_initialize_program(accounts),
        ReservationInstructionData::SetAdmin(admin_key) => try_set_admin(accounts, admin_key),
        ReservationInstructionData::ConfigureProgram(setting) => {
            try_configure_program(accounts, setting)
        }
        ReservationInstructionData::InitializeMetroHistory(exchange_key) => {
            try_initialize_metro_history(accounts, exchange_key)
        }
        ReservationInstructionData::InitializeDeviceHistory(device_key) => {
            try_initialize_device_history(accounts, device_key)
        }
        ReservationInstructionData::SetDeviceEnabled(should_enable) => {
            try_set_device_enabled(accounts, should_enable)
        }
        ReservationInstructionData::AdvanceExecutionPhase => {
            try_advance_execution_phase(accounts)
        }
        ReservationInstructionData::UpdateMetroUsdcPrice(metro_price) => {
            try_update_metro_usdc_price(accounts, metro_price)
        }
        ReservationInstructionData::TestSetup {
            subscription_epoch,
            metro_usdc_price,
            device_premium,
        } => try_test_setup(accounts, subscription_epoch, metro_usdc_price, device_premium),
    }
}

fn try_initialize_program(accounts: &[AccountInfo]) -> ProgramResult {
    msg!("Initialize program");

    // We expect the following accounts for this instruction:
    // - 0: Payer.
    // - 1: New program config.
    // - 2: New execution controller.
    // - 3: System program.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be a signer and writable because it will send lamports to
    // the new config account and reserve 2Z account. We do not check these
    // fields because the create-account workflow requires that this account is
    // writable and a signer.
    let (_, payer_info) = try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    // Account 1 must be the new program config account. The create-account
    // workflow requires that this account does not exist yet and is writable.
    let (account_index, new_program_config_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    let (expected_program_config_key, program_config_bump) = ProgramConfig::find_address();

    // Enforce this account location and seed validity.
    if new_program_config_info.key != &expected_program_config_key {
        msg!(
            "Invalid seeds for program config (account {})",
            account_index
        );
        return Err(ProgramError::InvalidSeeds);
    }

    // Account 2 must be the new execution controller account. The
    // create-account workflow requires that this account does not exist yet
    // and is writable.
    let (account_index, new_execution_controller_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    let (expected_execution_controller_key, execution_controller_bump) =
        ExecutionController::find_address();

    // Enforce this account location and seed validity.
    if new_execution_controller_info.key != &expected_execution_controller_key {
        msg!(
            "Invalid seeds for execution controller (account {})",
            account_index
        );
        return Err(ProgramError::InvalidSeeds);
    }

    // Rent sysvar will be used to create the new program config and execution
    // controller accounts.
    let rent_sysvar = Rent::get().unwrap();

    // The program config account is created with the maximum data length
    // allowed (10kb) in case other fields are added in the future.
    let mut program_config = try_create_account::<ProgramConfig>(
        payer_info.key,
        new_program_config_info,
        &[ProgramConfig::SEED_PREFIX, &[program_config_bump]],
        accounts,
        CreateAccountOptions {
            data_len: Some(MAX_PERMITTED_DATA_INCREASE),
            rent_sysvar: Some(&rent_sysvar),
            ..Default::default()
        },
    )?;

    // Set the program in paused state by default.
    msg!("Pause program");
    program_config.set_is_paused(true);

    // The execution controller account is also created with the maximum data
    // length allowed (10kb) in case other fields are added in the future.
    let mut execution_controller = try_create_account::<ExecutionController>(
        payer_info.key,
        new_execution_controller_info,
        &[
            ExecutionController::SEED_PREFIX,
            &[execution_controller_bump],
        ],
        accounts,
        CreateAccountOptions {
            data_len: Some(MAX_PERMITTED_DATA_INCREASE),
            rent_sysvar: Some(&rent_sysvar),
            ..Default::default()
        },
    )?;

    let next_solana_epoch = Clock::get().unwrap().epoch + 1;
    msg!(
        "Only allowed to update prices for next Solana epoch: {}",
        next_solana_epoch
    );

    execution_controller.next_subscription_epoch = next_solana_epoch;

    Ok(())
}

fn try_set_admin(accounts: &[AccountInfo], admin_key: Pubkey) -> ProgramResult {
    msg!("Set admin");

    // We expect the following accounts for this instruction:
    // - 0: Program data.
    // - 1: Upgrade authority.
    // - 2: Program config.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be the program data belonging to this program.
    // Account 1 must be the upgrade authority.
    //
    // This call ensures that the upgrade authority is a signer and is the
    // same authority encoded in the program data.
    UpgradeAuthority::try_next_accounts(&mut accounts_iter, &ID)?;

    // Account 2 must be the program config. Ensure it is writable so we can
    // update the admin key.
    let mut program_config =
        ZeroCopyMutAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    msg!("admin_key: {}", admin_key);
    program_config.admin_key = admin_key;

    Ok(())
}

fn try_configure_program(accounts: &[AccountInfo], setting: ProgramConfiguration) -> ProgramResult {
    msg!("Configure program");

    // We expect the following accounts for this instruction:
    // - 0: Program config.
    // - 1: Admin.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be the program config.
    let mut program_config =
        ZeroCopyMutAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    // Account 1 must be the admin.
    let (index, admin_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if admin_info.key != &program_config.admin_key {
        msg!("Unauthorized admin (account {})", index);
        return Err(ProgramError::InvalidAccountData);
    }

    match setting {
        ProgramConfiguration::Flag(configure_flag) => {
            msg!("Set flag");
            match configure_flag {
                ProgramFlagConfiguration::IsPaused(should_pause) => {
                    msg!("is_paused: {}", should_pause);
                    program_config.set_is_paused(should_pause);
                }
            };
        }
        ProgramConfiguration::ClosedForRequestsGracePeriodSlots(grace_period_slots) => {
            if grace_period_slots == 0 {
                msg!("Closed for requests grace period slots is zero");
                return Err(ProgramError::InvalidInstructionData);
            }

            let half_epoch_slots = EpochSchedule::get().unwrap().slots_per_epoch / 2;
            if u64::from(grace_period_slots) > half_epoch_slots {
                msg!(
                    "Settlement grace period slots cannot exceed half an epoch ({} slots)",
                    half_epoch_slots
                );
                return Err(ProgramError::InvalidInstructionData);
            }

            msg!(
                "Set closed_for_requests_grace_period_slots: {}",
                grace_period_slots
            );
            program_config.closed_for_requests_grace_period_slots = grace_period_slots;
        }
        ProgramConfiguration::Usdc2zMaxSlippageBps(max_slippage_bps) => {
            let max_slippage = UnitShare16::new(max_slippage_bps).ok_or_else(|| {
                msg!("Invalid max slippage: {}", max_slippage_bps);
                ProgramError::InvalidInstructionData
            })?;

            msg!("Set usdc_2z_max_slippage: {}", max_slippage);
            program_config.usdc_2z_max_slippage_bps = max_slippage;
        }
        ProgramConfiguration::Oracle(oracle_key) => {
            msg!("Set oracle_key: {}", oracle_key);
            program_config.oracle_key = oracle_key;
        }
        ProgramConfiguration::Usdc2zOracle(usdc_2z_oracle_key) => {
            msg!("Set usdc_2z_oracle_key: {}", usdc_2z_oracle_key);
            program_config.usdc_2z_oracle_key = usdc_2z_oracle_key;
        }
        ProgramConfiguration::UsdcMint(usdc_mint_key) => {
            msg!("Set usdc_mint_key: {}", usdc_mint_key);
            program_config.usdc_mint_key = usdc_mint_key;
        }
    }

    Ok(())
}

fn try_initialize_metro_history(accounts: &[AccountInfo], exchange_key: Pubkey) -> ProgramResult {
    msg!("Initialize metro history");

    // We expect the following accounts for this instruction:
    // - 0: Program config.
    // - 1: Oracle.
    // - 2: Execution controller.
    // - 3: Payer.
    // - 4: New metro history.
    // - 5: System program.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be the program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1 must be the oracle.
    let (index, oracle_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if oracle_info.key != &program_config.oracle_key {
        msg!("Unauthorized oracle (account {})", index);
        return Err(ProgramError::InvalidAccountData);
    }

    // Account 2 must be the exchange controller.
    let mut execution_controller = ZeroCopyMutAccount::<ExecutionController>::try_next_accounts(
        &mut accounts_iter,
        Some(&ID),
    )?;
    execution_controller.total_metros = execution_controller
        .total_metros
        .checked_add(1)
        .ok_or_else(|| {
            msg!("Too many metro exchanges");
            ProgramError::InvalidAccountData
        })?;

    // Account 3 must be the payer.
    let (_, payer_info) = try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    // Account 4 must be the new metro history.
    let (account_index, new_metro_history_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    let (expected_metro_history_key, metro_history_bump) =
        MetroHistory::find_address(&exchange_key);

    // Enforce this account location and seed validity.
    if new_metro_history_info.key != &expected_metro_history_key {
        msg!(
            "Invalid seeds for metro history (account {})",
            account_index
        );
        return Err(ProgramError::InvalidSeeds);
    }

    let mut metro_history = try_create_account::<MetroHistory>(
        payer_info.key,
        new_metro_history_info,
        &[
            MetroHistory::SEED_PREFIX,
            exchange_key.as_ref(),
            &[metro_history_bump],
        ],
        accounts,
        Default::default(),
    )?;

    metro_history.exchange_key = exchange_key;
    msg!("History initialized for metro exchange: {}", exchange_key);

    Ok(())
}

fn try_initialize_device_history(accounts: &[AccountInfo], device_key: Pubkey) -> ProgramResult {
    msg!("Initialize device history");

    // We expect the following accounts for this instruction:
    // - 0: Program config.
    // - 1: Oracle.
    // - 2: Execution controller.
    // - 3: Metro history.
    // - 4: Payer.
    // - 5: New device history.
    // - 6: System program.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be the program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1 must be the oracle.
    let (index, oracle_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if oracle_info.key != &program_config.oracle_key {
        msg!("Unauthorized oracle (account {})", index);
        return Err(ProgramError::InvalidAccountData);
    }

    // Account 2 must be the execution controller.
    let mut execution_controller = ZeroCopyMutAccount::<ExecutionController>::try_next_accounts(
        &mut accounts_iter,
        Some(&ID),
    )?;
    execution_controller.total_enabled_devices += 1;

    // Account 3 must be the metro history.
    let mut metro_history =
        ZeroCopyMutAccount::<MetroHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;
    metro_history.total_initialized_devices = metro_history
        .total_initialized_devices
        .checked_add(1)
        .ok_or_else(|| {
            msg!("Too many initialized devices");
            ProgramError::InvalidAccountData
        })?;

    // Account 4 must be the payer.
    let (_, payer_info) = try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    // Account 5 must be the new device history.
    let (account_index, new_device_history_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    let (expected_device_history_key, device_history_bump) =
        DeviceHistory::find_address(&device_key);

    // Enforce this account location and seed validity.
    if new_device_history_info.key != &expected_device_history_key {
        msg!(
            "Invalid seeds for device history (account {})",
            account_index
        );
        return Err(ProgramError::InvalidSeeds);
    }

    let mut device_history = try_create_account::<DeviceHistory>(
        payer_info.key,
        new_device_history_info,
        &[
            DeviceHistory::SEED_PREFIX,
            device_key.as_ref(),
            &[device_history_bump],
        ],
        accounts,
        Default::default(),
    )?;

    device_history.device_key = device_key;
    device_history.metro_exchange_key = metro_history.exchange_key;
    device_history.set_is_enabled(true);
    msg!("History initialized and enabled for device: {}", device_key);

    Ok(())
}

fn try_set_device_enabled(accounts: &[AccountInfo], should_enable: bool) -> ProgramResult {
    msg!("Set device enabled");

    // We expect the following accounts for this instruction:
    // - 0: Program config.
    // - 1: Admin.
    // - 2: Device history.
    // - 3: Execution controller.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be the program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    // Account 1 must be the admin.
    let (index, admin_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if admin_info.key != &program_config.admin_key {
        msg!("Unauthorized admin (account {})", index);
        return Err(ProgramError::InvalidAccountData);
    }

    // Account 2 must be the device history.
    let mut device_history =
        ZeroCopyMutAccount::<DeviceHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;
    let enabled_str = if should_enable { "enabled" } else { "disabled" };

    if should_enable == device_history.is_enabled() {
        msg!("Device is already {}", enabled_str);
        return Err(ProgramError::InvalidInstructionData);
    }

    device_history.set_is_enabled(should_enable);
    msg!("Device now {}", enabled_str);

    // Account 3 must be the execution controller.
    let mut execution_controller = ZeroCopyMutAccount::<ExecutionController>::try_next_accounts(
        &mut accounts_iter,
        Some(&ID),
    )?;
    if should_enable {
        execution_controller.total_enabled_devices += 1;
    } else {
        execution_controller.total_enabled_devices -= 1;
    }

    // TODO: Depending on the execution phase, we may not be able to disable a
    // device yet. For example, if seat requests are open, we should not disable
    // this device. Or if we do, maybe allow disabled if there are no pending
    // requests.
    //
    // Implement when phase changes are added (if needed).

    Ok(())
}


fn try_advance_execution_phase(accounts: &[AccountInfo]) -> ProgramResult {
    msg!("Advance execution phase");

    // We expect the following accounts for this instruction:
    // - 0: Program config.
    // - 1: Oracle.
    // - 2: Execution controller.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be the program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1 must be the oracle.
    let (index, oracle_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if oracle_info.key != &program_config.oracle_key {
        msg!("Unauthorized oracle (account {})", index);
        return Err(ProgramError::InvalidAccountData);
    }

    // Account 2 must be the execution controller.
    let mut execution_controller = ZeroCopyMutAccount::<ExecutionController>::try_next_accounts(
        &mut accounts_iter,
        Some(&ID),
    )?;

    let clock_sysvar = Clock::get().unwrap();

    match execution_controller.next_phase() {
        ExecutionPhase::Settled => {
            execution_controller.next_subscription_epoch += 1;

            // Reset the settled devices count. Once we are in the settled
            // phase, each settled device will be counted again.
            execution_controller.settled_devices_count = 0;
            execution_controller.last_settled_slot = clock_sysvar.slot;
        }
        ExecutionPhase::UpdatingPrices => {
            if !execution_controller.are_all_devices_settled() {
                msg!(
                    "Not all devices are settled yet. {} / {} settled",
                    execution_controller.settled_devices_count,
                    execution_controller.total_enabled_devices
                );
                return Err(ProgramError::InvalidAccountData);
            }

            let last_settled_epoch = execution_controller.last_settled_epoch().unwrap();

            // Cannot update prices until the next subscription epoch.
            if clock_sysvar.epoch < last_settled_epoch {
                msg!("Not the last settled epoch: {}", last_settled_epoch);
                return Err(ProgramError::InvalidAccountData);
            }

            // Reset the updated device prices count. Once we are in the
            // updating prices phase, each updated device will be counted again.
            execution_controller.updated_device_prices_count = 0;
            execution_controller.last_updating_prices_slot = clock_sysvar.slot;
        }
        ExecutionPhase::OpenForRequests => {
            if !execution_controller.are_all_devices_updated() {
                msg!(
                    "Not all devices are updated yet. {} / {} updated",
                    execution_controller.updated_device_prices_count,
                    execution_controller.total_enabled_devices
                );
                return Err(ProgramError::InvalidAccountData);
            }

            execution_controller.last_open_for_requests_slot = clock_sysvar.slot;
        }
        ExecutionPhase::ClosedForRequests => {
            let grace_period_slots = program_config
                .closed_for_requests_grace_period_slots()
                .ok_or_else(|| {
                    msg!("Closed for requests grace period slots is not set");
                    ProgramError::InvalidAccountData
                })?;

            let epoch_schedule_sysvar = EpochSchedule::get().unwrap();

            let valid_settlement_slot = epoch_schedule_sysvar
                .get_first_slot_in_epoch(execution_controller.next_subscription_epoch)
                .saturating_sub(grace_period_slots);

            if clock_sysvar.slot < valid_settlement_slot {
                msg!(
                    "Not within the settlement grace period. Slot {} < {}",
                    clock_sysvar.slot,
                    valid_settlement_slot
                );
                return Err(ProgramError::InvalidAccountData);
            }

            execution_controller.last_closed_for_requests_slot = clock_sysvar.slot;
        }
    }

    execution_controller.set_next_phase();
    msg!(
        "Epoch {}. Now {} for epoch {}",
        clock_sysvar.epoch,
        execution_controller.phase(),
        execution_controller.current_subscription_epoch().unwrap()
    );

    Ok(())
}

fn try_update_metro_usdc_price(accounts: &[AccountInfo], metro_price: u16) -> ProgramResult {
    msg!("Update metro USDC price");

    // We expect the following accounts for this instruction:
    // - 0: Program config.
    // - 1: Oracle.
    // - 2: Execution controller.
    // - 3: Metro history.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0 must be the program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1 must be the oracle.
    let (index, oracle_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if oracle_info.key != &program_config.oracle_key {
        msg!("Unauthorized oracle (account {})", index);
        return Err(ProgramError::InvalidAccountData);
    }

    // Account 2 must be the execution controller.
    let execution_controller =
        ZeroCopyAccount::<ExecutionController>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    if execution_controller.phase() != ExecutionPhase::UpdatingPrices {
        msg!("Invalid execution phase: {}", execution_controller.phase());
        return Err(ProgramError::InvalidAccountData);
    }

    // Account 3 must be the metro history.
    let mut metro_history =
        ZeroCopyMutAccount::<MetroHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    let next_subscription_epoch = execution_controller.next_subscription_epoch;

    let entry = metro_history
        .prices
        .advance_mut(next_subscription_epoch)
        .ok_or_else(|| {
            msg!(
                "Metro price epoch {} is not monotonically increasing",
                next_subscription_epoch
            );
            ProgramError::InvalidInstructionData
        })?;
    entry.data.usdc_price = metro_price;
    msg!(
        "Set metro price for epoch {}: {}",
        next_subscription_epoch,
        metro_price
    );

    Ok(())
}

/// Test-only: force execution controller into `OpenForRequests` and write
/// price entries into metro/device history ring buffers. Equivalent to the
/// raw-byte patching done via `ProgramTest::set_account()` in unit tests.
fn try_test_setup(
    accounts: &[AccountInfo],
    subscription_epoch: u64,
    metro_usdc_price: u16,
    device_premium: i16,
) -> ProgramResult {
    msg!("Test setup");

    // Accounts:
    // 0: Program config (read).
    // 1: Admin (signer).
    // 2: Execution controller (mut).
    // 3: Metro history (mut).
    // 4: Device history (mut).
    let mut accounts_iter = accounts.iter().enumerate();

    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    let (index, admin_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if admin_info.key != &program_config.admin_key {
        msg!("Unauthorized admin (account {})", index);
        return Err(ProgramError::InvalidAccountData);
    }

    // Force execution controller into OpenForRequests.
    let mut execution_controller = ZeroCopyMutAccount::<ExecutionController>::try_next_accounts(
        &mut accounts_iter,
        Some(&ID),
    )?;
    execution_controller.set_phase(ExecutionPhase::OpenForRequests);
    execution_controller.next_subscription_epoch = subscription_epoch;
    msg!(
        "Execution controller set to OpenForRequests, epoch={}",
        subscription_epoch
    );

    // Write metro price entry.
    let mut metro_history =
        ZeroCopyMutAccount::<MetroHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;
    let metro_entry = metro_history
        .prices
        .advance_mut(subscription_epoch)
        .ok_or_else(|| {
            msg!("Failed to advance metro history ring buffer");
            ProgramError::InvalidArgument
        })?;
    metro_entry.data.usdc_price = metro_usdc_price;
    msg!("Metro price set to {} USDC", metro_usdc_price);

    // Write device subscription entry.
    let mut device_history =
        ZeroCopyMutAccount::<DeviceHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;
    let device_entry = device_history
        .history
        .advance_mut(subscription_epoch)
        .ok_or_else(|| {
            msg!("Failed to advance device history ring buffer");
            ProgramError::InvalidArgument
        })?;
    device_entry.data.usdc_price_premium = device_premium;
    msg!("Device premium set to {}", device_premium);

    Ok(())
}

/// Initialize a client seat and make the first deposit. Creates the
/// ClientSeat PDA and the per-device escrow ATA, transfers USDC from the
/// payer to the device escrow.
///
/// The program enforces `amount >= current epoch price` to prevent spam
/// (seats with dust amounts that the oracle would need to process and close).
fn try_initialize_client_seat(
    accounts: &[AccountInfo],
    client_ip: Ipv4Addr,
) -> ProgramResult {
    msg!("Initialize client seat");

    // Accounts:
    // - 0: Program config.
    // - 1: Execution controller.
    // - 2: Device history.
    // - 3: Payer (signer, writable).
    // - 4: Client seat (must not exist yet).
    // - 5: Client seat USDC token PDA (new).
    // - 6: USDC mint.
    // - 7: SPL token program.
    // - 8: System program.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0: Program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1: Execution controller (phase check).
    let execution_controller =
        ZeroCopyAccount::<ExecutionController>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    let phase = execution_controller.phase();
    if phase != ExecutionPhase::OpenForRequests {
        msg!("Phase is not OpenForRequests (current: {})", phase);
        return Err(ProgramError::InvalidInstructionData);
    }

    // Account 2: Device history.
    let device_history =
        ZeroCopyAccount::<DeviceHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    if !device_history.is_enabled() {
        msg!("Device is not enabled");
        return Err(ProgramError::InvalidAccountData);
    }

    // Account 3: Payer (signer, writable).
    let (_, payer_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    // Account 4: Client seat PDA — must not exist yet.
    let client_ip_bits = u32::from(client_ip);
    let (account_index, client_seat_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    let (expected_client_seat_key, client_seat_bump) =
        ClientSeat::find_address(&device_history.device_key, client_ip_bits);

    if client_seat_info.key != &expected_client_seat_key {
        msg!("Invalid seeds for client seat (account {})", account_index);
        return Err(ProgramError::InvalidSeeds);
    }

    if client_seat_info.data_len() != 0 {
        msg!("Client seat already exists");
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    // Account 5: Client seat USDC token PDA.
    let (account_index, token_pda_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    // Account 6: USDC mint.
    let (_, usdc_mint_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    let (expected_token_pda, token_pda_bump) =
        find_token_pda_address(&expected_client_seat_key, usdc_mint_info.key);

    if token_pda_info.key != &expected_token_pda {
        msg!("Invalid token PDA (account {})", account_index);
        return Err(ProgramError::InvalidSeeds);
    }

    // Create client seat account.
    let mut client_seat = try_create_account::<ClientSeat>(
        payer_info.key,
        client_seat_info,
        &[
            ClientSeat::SEED_PREFIX,
            device_history.device_key.as_ref(),
            &client_ip_bits.to_le_bytes(),
            &[client_seat_bump],
        ],
        accounts,
        Default::default(),
    )?;

    client_seat.device_key = device_history.device_key;
    client_seat.client_ip_bits = client_ip_bits;
    client_seat.bump_seed = client_seat_bump;
    client_seat.usdc_token_pda_bump_seed = token_pda_bump;

    msg!(
        "Created seat for device {} ip {}",
        device_history.device_key,
        client_ip
    );

    // Create USDC token account as a PDA owned by the ClientSeat PDA.
    let rent = Rent::get().unwrap();
    let token_account_len = spl_token_interface::state::Account::LEN;
    let lamports = rent.minimum_balance(token_account_len);

    let client_seat_key_bytes = expected_client_seat_key.to_bytes();
    let usdc_mint_key_bytes = usdc_mint_info.key.to_bytes();
    let token_pda_seeds: &[&[u8]] = &[
        crate::state::TOKEN_PDA_SEED_PREFIX,
        &client_seat_key_bytes,
        &usdc_mint_key_bytes,
        &[token_pda_bump],
    ];

    let create_account_ix = solana_system_interface::instruction::create_account(
        payer_info.key,
        token_pda_info.key,
        lamports,
        token_account_len as u64,
        &spl_token_interface::ID,
    );

    invoke_signed(
        &create_account_ix,
        &[payer_info.clone(), token_pda_info.clone()],
        &[token_pda_seeds],
    )?;

    // Initialize the token account with the ClientSeat PDA as owner.
    let init_token_ix = spl_token_interface::instruction::initialize_account3(
        &spl_token_interface::ID,
        token_pda_info.key,
        usdc_mint_info.key,
        &expected_client_seat_key,
    )
    .map_err(|_| ProgramError::InvalidInstructionData)?;

    solana_cpi::invoke(
        &init_token_ix,
        &[token_pda_info.clone(), usdc_mint_info.clone()],
    )?;

    msg!("Created token PDA for seat");

    Ok(())
}

/// Initialize a payment escrow for a (seat, withdraw_authority) pair.
fn try_initialize_payment_escrow(accounts: &[AccountInfo]) -> ProgramResult {
    msg!("Initialize payment escrow");

    // Accounts:
    // - 0: Program config.
    // - 1: Client seat (must exist).
    // - 2: Withdraw authority (signer, writable — funds rent).
    // - 3: Payment escrow PDA (new).
    // - 4: System program.
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0: Program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1: Client seat (must exist).
    let (account_index, client_seat_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    if client_seat_info.owner != &ID {
        msg!("Client seat not owned by program (account {})", account_index);
        return Err(ProgramError::InvalidAccountOwner);
    }
    if client_seat_info.data_len() == 0 {
        msg!("Client seat does not exist (account {})", account_index);
        return Err(ProgramError::UninitializedAccount);
    }

    // Account 2: Withdraw authority (signer).
    let (_, withdraw_authority_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    // Account 3: Payment escrow PDA.
    let (account_index, escrow_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    let (expected_escrow_key, escrow_bump) =
        PaymentEscrow::find_address(client_seat_info.key, withdraw_authority_info.key);

    if escrow_info.key != &expected_escrow_key {
        msg!("Invalid payment escrow PDA (account {})", account_index);
        return Err(ProgramError::InvalidSeeds);
    }

    let mut payment_escrow = try_create_account::<PaymentEscrow>(
        withdraw_authority_info.key,
        escrow_info,
        &[
            PaymentEscrow::SEED_PREFIX,
            client_seat_info.key.as_ref(),
            withdraw_authority_info.key.as_ref(),
            &[escrow_bump],
        ],
        accounts,
        Default::default(),
    )?;

    payment_escrow.client_seat_key = *client_seat_info.key;
    payment_escrow.withdraw_authority_key = *withdraw_authority_info.key;

    msg!(
        "Created payment escrow for seat {} authority {}",
        client_seat_info.key,
        withdraw_authority_info.key
    );

    Ok(())
}

/// Close a payment escrow. If the escrow has a non-zero balance, refund
/// the USDC from the seat's token PDA to the refund account.
fn try_close_payment_escrow(accounts: &[AccountInfo]) -> ProgramResult {
    msg!("Close payment escrow");

    // Accounts:
    // - 0: Program config.
    // - 1: Execution controller (readonly).
    // - 2: Payment escrow (writable).
    // - 3: Withdraw authority (signer, writable).
    // - 4: Client seat (if balance > 0).
    // - 5: Client seat USDC token account (if balance > 0).
    // - 6: Refund USDC token account (if balance > 0).
    // - 7: SPL token program (if balance > 0).
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0: Program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1: Execution controller — forbid ClosedForRequests phase.
    let execution_controller =
        ZeroCopyAccount::<ExecutionController>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    if execution_controller.phase() == ExecutionPhase::ClosedForRequests {
        msg!("Cannot close payment escrow during ClosedForRequests phase");
        return Err(ProgramError::InvalidInstructionData);
    }

    // Account 2: Payment escrow.
    let payment_escrow =
        ZeroCopyMutAccount::<PaymentEscrow>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    // Account 3: Withdraw authority (signer).
    let (account_index, withdraw_authority_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    if withdraw_authority_info.key != &payment_escrow.withdraw_authority_key {
        msg!(
            "Signer is not the withdraw authority (account {})",
            account_index
        );
        return Err(ProgramError::InvalidAccountData);
    }

    let usdc_balance = payment_escrow.usdc_balance;

    if usdc_balance > 0 {
        // Account 4: Client seat.
        let client_seat =
            ZeroCopyAccount::<ClientSeat>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

        // Account 5: Client seat USDC token account.
        let (_, token_pda_info) =
            try_next_enumerated_account(&mut accounts_iter, Default::default())?;

        // Account 6: Refund USDC token account.
        let (_, refund_info) =
            try_next_enumerated_account(&mut accounts_iter, Default::default())?;

        // Transfer USDC from seat token PDA to refund account (client seat PDA signs).
        let transfer_ix = spl_token_interface::instruction::transfer(
            &spl_token_interface::ID,
            token_pda_info.key,
            refund_info.key,
            &payment_escrow.client_seat_key,
            &[],
            usdc_balance,
        )
        .map_err(|_| ProgramError::InvalidInstructionData)?;

        let device_key_bytes = client_seat.device_key.to_bytes();
        let ip_bytes = client_seat.client_ip_bits.to_le_bytes();
        let seat_seeds: &[&[u8]] = &[
            ClientSeat::SEED_PREFIX,
            &device_key_bytes,
            &ip_bytes,
            &[client_seat.bump_seed],
        ];

        invoke_signed(
            &transfer_ix,
            &[
                token_pda_info.clone(),
                refund_info.clone(),
                client_seat.info.clone(),
            ],
            &[seat_seeds],
        )?;

        msg!("Refunded {} USDC (atomic)", usdc_balance);
    }

    // Close payment escrow account: zero data, return rent to withdraw authority.
    let escrow_info = payment_escrow.info.clone();
    drop(payment_escrow);

    let escrow_lamports = escrow_info.lamports();
    **escrow_info.try_borrow_mut_lamports()? = 0;
    **withdraw_authority_info.try_borrow_mut_lamports()? = withdraw_authority_info
        .lamports()
        .checked_add(escrow_lamports)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let mut escrow_data = escrow_info.try_borrow_mut_data()?;
    let data_len = escrow_data.len();
    solana_program_memory::sol_memset(&mut escrow_data, 0, data_len);

    msg!("Closed payment escrow");

    Ok(())
}

fn try_fund_payment_escrow_usdc(accounts: &[AccountInfo], amount: u64) -> ProgramResult {
    msg!("Fund payment escrow USDC (amount={})", amount);

    // Accounts:
    // - 0: Program config (readonly).
    // - 1: Execution controller (writable).
    // - 2: Metro history (readonly).
    // - 3: Device history (readonly).
    // - 4: Client seat (writable).
    // - 5: Payment escrow (writable).
    // - 6: Client seat USDC token account (writable).
    // - 7: Source USDC token account (writable).
    // - 8: Transfer authority (signer, readonly).
    // - 9: SPL token program (readonly).
    let mut accounts_iter = accounts.iter().enumerate();

    // Account 0: Program config.
    let program_config =
        ZeroCopyAccount::<ProgramConfig>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    program_config.try_require_unpaused()?;

    // Account 1: Execution controller.
    let execution_controller =
        ZeroCopyAccount::<ExecutionController>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    let phase = execution_controller.phase();
    if phase != ExecutionPhase::OpenForRequests {
        msg!("Phase is not OpenForRequests (current: {})", phase);
        return Err(ProgramError::InvalidInstructionData);
    }

    let subscription_epoch = execution_controller.next_subscription_epoch;

    // Account 2: Metro history.
    let metro_history =
        ZeroCopyAccount::<MetroHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    let metro_price = metro_history
        .current_price()
        .ok_or_else(|| {
            msg!("No metro price available");
            ProgramError::InvalidAccountData
        })?;

    // Account 3: Device history.
    let device_history =
        ZeroCopyAccount::<DeviceHistory>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    if !device_history.is_enabled() {
        msg!("Device is not enabled");
        return Err(ProgramError::InvalidAccountData);
    }

    let device_sub = device_history.history.current_entry().ok_or_else(|| {
        msg!("No device subscription entry");
        ProgramError::InvalidAccountData
    })?;

    // Compute expected price: metro_usdc_price + device_premium (in whole USDC).
    let expected_price_usdc = (metro_price.usdc_price as i32 + device_sub.data.usdc_price_premium as i32)
        .max(0) as u64;
    // Convert to micro-USDC (6 decimals).
    let expected_amount = expected_price_usdc * 1_000_000;

    if amount < expected_amount {
        msg!(
            "Insufficient funding: {} < {} (expected)",
            amount,
            expected_amount
        );
        return Err(ProgramError::InsufficientFunds);
    }

    // Account 4: Client seat.
    let mut client_seat =
        ZeroCopyMutAccount::<ClientSeat>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    // Account 5: Payment escrow.
    let mut payment_escrow =
        ZeroCopyMutAccount::<PaymentEscrow>::try_next_accounts(&mut accounts_iter, Some(&ID))?;

    // Account 6: Client seat USDC token account (destination).
    let (_, token_pda_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    // Account 7: Source USDC token account.
    let (_, source_token_info) =
        try_next_enumerated_account(&mut accounts_iter, Default::default())?;

    // Account 8: Transfer authority (signer).
    let (_, transfer_authority_info) = try_next_enumerated_account(
        &mut accounts_iter,
        NextAccountOptions {
            must_be_signer: true,
            ..Default::default()
        },
    )?;

    // Transfer USDC from source to client seat token PDA.
    let transfer_ix = spl_token_interface::instruction::transfer(
        &spl_token_interface::ID,
        source_token_info.key,
        token_pda_info.key,
        transfer_authority_info.key,
        &[],
        amount,
    )
    .map_err(|_| ProgramError::InvalidInstructionData)?;

    solana_cpi::invoke(
        &transfer_ix,
        &[
            source_token_info.clone(),
            token_pda_info.clone(),
            transfer_authority_info.clone(),
        ],
    )?;

    // Update payment escrow balance.
    payment_escrow.usdc_balance = payment_escrow
        .usdc_balance
        .checked_add(amount)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    // Update client seat: set funded_epoch.
    client_seat.funded_epoch = subscription_epoch;

    msg!(
        "Funded escrow with {} USDC (atomic) for epoch {}",
        amount,
        subscription_epoch
    );

    Ok(())
}

/// Oracle-only: deduct subscription fee from a seat (stub — not yet adapted
/// to the per-seat token PDA model, will be updated when Karl's Deposit
/// instruction lands).
fn try_deduct_subscription_fee(_accounts: &[AccountInfo]) -> ProgramResult {
    msg!("Deduct subscription fee (stub)");
    Ok(())
}

//

#[derive(Default)]
struct CreateAccountOptions<'a> {
    pub data_len: Option<usize>,
    pub rent_sysvar: Option<&'a Rent>,
    pub additional_lamports: Option<u64>,
}

fn try_create_account<'a, T: Pod + PrecomputedDiscriminator + Default>(
    payer_key: &Pubkey,
    destination_account_info: &'a AccountInfo,
    destination_signer_seeds: &[&[u8]],
    account_infos: &[AccountInfo],
    options: CreateAccountOptions,
) -> Result<RefMut<'a, T>, ProgramError> {
    let CreateAccountOptions {
        data_len,
        rent_sysvar,
        additional_lamports,
    } = options;

    doublezero_program_tools::recipe::create_account::try_create_account(
        Invoker::Signer(payer_key),
        Invoker::Pda {
            key: destination_account_info.key,
            signer_seeds: destination_signer_seeds,
        },
        destination_account_info.lamports(),
        data_len.unwrap_or(zero_copy::data_end::<T>()),
        &ID,
        account_infos,
        doublezero_program_tools::recipe::create_account::CreateAccountOptions {
            rent_sysvar,
            additional_lamports,
        },
    )?;

    let (mucked_data, _) = zero_copy::try_initialize(destination_account_info)?;

    Ok(mucked_data)
}

impl ProgramConfig {
    #[inline(always)]
    fn try_require_unpaused(&self) -> ProgramResult {
        if self.is_paused() {
            msg!("Program is paused");
            return Err(ProgramError::InvalidAccountData);
        }

        Ok(())
    }
}
