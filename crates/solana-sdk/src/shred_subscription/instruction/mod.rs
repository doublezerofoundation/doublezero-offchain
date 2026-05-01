pub mod account;

use std::io;

use borsh::{BorshDeserialize, BorshSerialize};
use doublezero_program_tools::{DISCRIMINATOR_LEN, Discriminator};
use solana_sdk::pubkey::Pubkey;

/// Envelope for an offchain authorization produced by a validator operator
/// via `solana sign-offchain-message`. Carries the ed25519 signature plus
/// the cluster slot after which the authorization is no longer valid.
///
/// Wire-compatible with `ValidatorOffchainAuthorization` in the onchain
/// program — Borsh-serialized in the same field order.
#[derive(Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize)]
pub struct ValidatorOffchainAuthorization {
    pub deadline_slot: u64,
    pub signature: [u8; 64],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShredSubscriptionInstructionData {
    /// Initialize a client seat for a (device, client_ip) pair.
    InitializeClientSeat { client_ip: u32 },
    /// Initialize a payment escrow for a (seat, withdraw_authority) pair.
    InitializePaymentEscrow,
    /// Close a payment escrow and refund any remaining USDC.
    ClosePaymentEscrow,
    /// Fund a payment escrow with USDC.
    FundPaymentEscrowUsdc(u64),
    /// Request instant allocation for a funded seat (skips auction settlement).
    RequestInstantSeatAllocation,
    /// Request instant seat withdrawal.
    RequestInstantSeatWithdrawal,
    /// Request instant seat withdrawal with a prorated USDC refund based on
    /// the remaining slots in the epoch. Superset of
    /// `RequestInstantSeatWithdrawal` (more accounts).
    RequestProratedInstantSeatWithdrawal,
    /// Set the rewards proportion for a validator client.
    SetValidatorClientRewardsProportion(u16),
    /// Initialize the validator publisher rewards account for a node. Anyone
    /// can call this; the account is created with the canonical 2Z mint as
    /// the default reward token. Use `ConfigureValidatorPublisherRewards` to
    /// set the destination owner and (optionally) switch the mint.
    InitializeValidatorPublisherRewards { node_id: Pubkey },
    /// Set the reward token destination owner and mint on a previously
    /// initialized validator publisher rewards account. The mint is read
    /// from the `ShredRewardToken` account passed in, not from this struct;
    /// only protocol-registered mints are accepted.
    ///
    /// Authorization takes one of two forms:
    /// - `offchain_authorization = Some(_)`: the validator identity signed
    ///   the canonical authorization message via
    ///   `solana sign-offchain-message`. The transaction does not need the
    ///   node identity as a Solana signer.
    /// - `offchain_authorization = None`: the validator identity must be a
    ///   Solana signer on the transaction.
    ConfigureValidatorPublisherRewards {
        rewards_token_owner_key: Pubkey,
        offchain_authorization: Option<ValidatorOffchainAuthorization>,
    },
    /// Validates the provided CLI version against the onchain minimum.
    CheckCliVersion { major: u32, minor: u32, patch: u32 },
}

impl ShredSubscriptionInstructionData {
    pub const INITIALIZE_CLIENT_SEAT: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_client_seat");
    pub const INITIALIZE_PAYMENT_ESCROW: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_payment_escrow");
    pub const CLOSE_PAYMENT_ESCROW: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::close_payment_escrow");
    pub const FUND_PAYMENT_ESCROW_USDC: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::fund_payment_escrow_usdc");
    pub const REQUEST_INSTANT_SEAT_ALLOCATION: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::request_instant_seat_allocation");
    pub const REQUEST_INSTANT_SEAT_WITHDRAWAL: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::request_instant_seat_withdrawal");
    pub const REQUEST_PRORATED_INSTANT_SEAT_WITHDRAWAL: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::request_prorated_instant_seat_withdrawal");
    pub const SET_VALIDATOR_CLIENT_REWARDS_PROPORTION: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::set_validator_client_rewards_proportion");
    pub const INITIALIZE_VALIDATOR_PUBLISHER_REWARDS: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_validator_publisher_rewards");
    pub const CONFIGURE_VALIDATOR_PUBLISHER_REWARDS: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::configure_validator_publisher_rewards");
    pub const CHECK_CLI_VERSION: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::check_cli_version");
}

impl BorshSerialize for ShredSubscriptionInstructionData {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Self::InitializeClientSeat { client_ip } => {
                Self::INITIALIZE_CLIENT_SEAT.serialize(writer)?;
                client_ip.serialize(writer)
            }
            Self::InitializePaymentEscrow => Self::INITIALIZE_PAYMENT_ESCROW.serialize(writer),
            Self::ClosePaymentEscrow => Self::CLOSE_PAYMENT_ESCROW.serialize(writer),
            Self::FundPaymentEscrowUsdc(amount) => {
                Self::FUND_PAYMENT_ESCROW_USDC.serialize(writer)?;
                amount.serialize(writer)
            }
            Self::RequestInstantSeatAllocation => {
                Self::REQUEST_INSTANT_SEAT_ALLOCATION.serialize(writer)
            }
            Self::RequestInstantSeatWithdrawal => {
                Self::REQUEST_INSTANT_SEAT_WITHDRAWAL.serialize(writer)
            }
            Self::RequestProratedInstantSeatWithdrawal => {
                Self::REQUEST_PRORATED_INSTANT_SEAT_WITHDRAWAL.serialize(writer)
            }
            Self::SetValidatorClientRewardsProportion(proportion) => {
                Self::SET_VALIDATOR_CLIENT_REWARDS_PROPORTION.serialize(writer)?;
                proportion.serialize(writer)
            }
            Self::InitializeValidatorPublisherRewards { node_id } => {
                Self::INITIALIZE_VALIDATOR_PUBLISHER_REWARDS.serialize(writer)?;
                node_id.serialize(writer)
            }
            Self::ConfigureValidatorPublisherRewards {
                rewards_token_owner_key,
                offchain_authorization,
            } => {
                Self::CONFIGURE_VALIDATOR_PUBLISHER_REWARDS.serialize(writer)?;
                rewards_token_owner_key.serialize(writer)?;
                offchain_authorization.serialize(writer)
            }
            Self::CheckCliVersion {
                major,
                minor,
                patch,
            } => {
                Self::CHECK_CLI_VERSION.serialize(writer)?;
                major.serialize(writer)?;
                minor.serialize(writer)?;
                patch.serialize(writer)
            }
        }
    }
}

impl BorshDeserialize for ShredSubscriptionInstructionData {
    fn deserialize_reader<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        match Discriminator::deserialize_reader(reader)? {
            Self::INITIALIZE_CLIENT_SEAT => {
                let client_ip = u32::deserialize_reader(reader)?;
                Ok(Self::InitializeClientSeat { client_ip })
            }
            Self::INITIALIZE_PAYMENT_ESCROW => Ok(Self::InitializePaymentEscrow),
            Self::CLOSE_PAYMENT_ESCROW => Ok(Self::ClosePaymentEscrow),
            Self::FUND_PAYMENT_ESCROW_USDC => {
                let amount = u64::deserialize_reader(reader)?;
                Ok(Self::FundPaymentEscrowUsdc(amount))
            }
            Self::REQUEST_INSTANT_SEAT_ALLOCATION => Ok(Self::RequestInstantSeatAllocation),
            Self::REQUEST_INSTANT_SEAT_WITHDRAWAL => Ok(Self::RequestInstantSeatWithdrawal),
            Self::REQUEST_PRORATED_INSTANT_SEAT_WITHDRAWAL => {
                Ok(Self::RequestProratedInstantSeatWithdrawal)
            }
            Self::SET_VALIDATOR_CLIENT_REWARDS_PROPORTION => {
                let proportion = u16::deserialize_reader(reader)?;
                Ok(Self::SetValidatorClientRewardsProportion(proportion))
            }
            Self::INITIALIZE_VALIDATOR_PUBLISHER_REWARDS => {
                let node_id = Pubkey::deserialize_reader(reader)?;
                Ok(Self::InitializeValidatorPublisherRewards { node_id })
            }
            Self::CONFIGURE_VALIDATOR_PUBLISHER_REWARDS => {
                let rewards_token_owner_key = Pubkey::deserialize_reader(reader)?;
                let offchain_authorization =
                    Option::<ValidatorOffchainAuthorization>::deserialize_reader(reader)?;
                Ok(Self::ConfigureValidatorPublisherRewards {
                    rewards_token_owner_key,
                    offchain_authorization,
                })
            }
            Self::CHECK_CLI_VERSION => {
                let major = u32::deserialize_reader(reader)?;
                let minor = u32::deserialize_reader(reader)?;
                let patch = u32::deserialize_reader(reader)?;
                Ok(Self::CheckCliVersion {
                    major,
                    minor,
                    patch,
                })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid discriminator",
            )),
        }
    }
}
