use anyhow::{Context, Result};
use doublezero_serviceability::{
    instructions::DoubleZeroInstruction,
    pda::{get_accesspass_pda, get_globalstate_pda, get_user_pda},
    processors::{
        accesspass::set::SetAccessPassArgs,
        multicastgroup::allowlist::publisher::add::AddMulticastGroupPubAllowlistArgs,
        user::create_subscribe::UserCreateSubscribeArgs,
    },
    state::{
        accesspass::AccessPassType,
        user::{UserCYOA, UserType as SvcUserType},
    },
};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};

use super::dz_ledger_reader::DzUser;

/// The three instructions needed to create a multicast publisher onchain.
pub struct CreateMulticastPublisherInstructions {
    pub set_access_pass: Instruction,
    pub add_allowlist: Instruction,
    pub create_user: Instruction,
}

/// Build the three instructions needed to create a multicast publisher for a user.
pub fn build_create_multicast_publisher_instructions(
    program_id: &Pubkey,
    payer: &Pubkey,
    multicast_group_pk: &Pubkey,
    user: &DzUser,
) -> Result<CreateMulticastPublisherInstructions> {
    let (accesspass_pda, _) = get_accesspass_pda(program_id, &user.client_ip, payer);
    let (globalstate_pda, _) = get_globalstate_pda(program_id);

    // Step 1: set_access_pass
    let set_access_pass = build_instruction(
        program_id,
        DoubleZeroInstruction::SetAccessPass(SetAccessPassArgs {
            accesspass_type: AccessPassType::Prepaid,
            client_ip: user.client_ip,
            last_access_epoch: u64::MAX,
            allow_multiple_ip: false,
            tenant: Pubkey::default(),
        }),
        vec![
            AccountMeta::new(accesspass_pda, false),
            AccountMeta::new_readonly(globalstate_pda, false),
            AccountMeta::new(*payer, false),
            AccountMeta::new(*payer, true),
            AccountMeta::new_readonly(solana_sdk::system_program::ID, false),
        ],
    )?;

    // Step 2: add_multicast_publisher_allowlist
    let add_allowlist = build_instruction(
        program_id,
        DoubleZeroInstruction::AddMulticastGroupPubAllowlist(AddMulticastGroupPubAllowlistArgs {
            client_ip: user.client_ip,
            user_payer: *payer,
        }),
        vec![
            AccountMeta::new(*multicast_group_pk, false),
            AccountMeta::new(accesspass_pda, false),
            AccountMeta::new_readonly(globalstate_pda, false),
            AccountMeta::new(*payer, true),
            AccountMeta::new_readonly(solana_sdk::system_program::ID, false),
        ],
    )?;

    // Step 3: create_subscribe_user (as publisher)
    let (user_pda, _) = get_user_pda(program_id, &user.client_ip, SvcUserType::Multicast);
    let create_user = build_instruction(
        program_id,
        DoubleZeroInstruction::CreateSubscribeUser(UserCreateSubscribeArgs {
            user_type: SvcUserType::Multicast,
            cyoa_type: UserCYOA::GREOverDIA,
            client_ip: user.client_ip,
            publisher: true,
            subscriber: false,
        }),
        vec![
            AccountMeta::new(user_pda, false),
            AccountMeta::new(user.device_pk, false),
            AccountMeta::new(*multicast_group_pk, false),
            AccountMeta::new(accesspass_pda, false),
            AccountMeta::new_readonly(globalstate_pda, false),
            AccountMeta::new(*payer, true),
            AccountMeta::new_readonly(solana_sdk::system_program::ID, false),
        ],
    )?;

    Ok(CreateMulticastPublisherInstructions {
        set_access_pass,
        add_allowlist,
        create_user,
    })
}

fn build_instruction(
    program_id: &Pubkey,
    dz_ix: DoubleZeroInstruction,
    accounts: Vec<AccountMeta>,
) -> Result<Instruction> {
    let data = borsh::to_vec(&dz_ix).context("failed to serialize instruction")?;
    Ok(Instruction {
        program_id: *program_id,
        accounts,
        data,
    })
}
