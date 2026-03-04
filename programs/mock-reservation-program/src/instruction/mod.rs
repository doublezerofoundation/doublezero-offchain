pub mod account;

//

use std::io;

use borsh::{BorshDeserialize, BorshSerialize};
use doublezero_program_tools::{Discriminator, DISCRIMINATOR_LEN};
use solana_pubkey::Pubkey;

#[derive(Debug, BorshDeserialize, BorshSerialize, Clone, PartialEq, Eq)]
pub enum ProgramConfiguration {
    Flag(ProgramFlagConfiguration),
    ClosedForRequestsGracePeriodSlots(u32),
    Usdc2zMaxSlippageBps(u16),
    Oracle(Pubkey),
    Usdc2zOracle(Pubkey),
    UsdcMint(Pubkey),
}

#[derive(Debug, BorshDeserialize, BorshSerialize, Clone, PartialEq, Eq)]
pub enum ProgramFlagConfiguration {
    IsPaused(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservationInstructionData {
    InitializeProgram,
    /// Only the upgrade authority can set the admin.
    SetAdmin(Pubkey),
    /// Only the admin can configure the program. All settings are found in the
    /// program config account.
    ConfigureProgram(ProgramConfiguration),
    /// Initialize the metro history for a given exchange. Only the oracle
    /// can initialize this account.
    InitializeMetroHistory(Pubkey),
    /// Initialize the device history for a given device. Only the oracle
    /// can initialize this account.
    InitializeDeviceHistory(Pubkey),
    /// Only the admin can set whether a device is enabled or disabled.
    SetDeviceEnabled(bool),
    /// Advance the execution phase to the next phase. Only the oracle can
    /// call this instruction.
    AdvanceExecutionPhase,
    /// Only the oracle can update the USDC price for a given metro.
    UpdateMetroUsdcPrice(u16),
    /// Test-only: force execution controller into OpenForRequests and write
    /// price entries into metro/device history ring buffers. Equivalent to
    /// the raw-byte patching in SDK integration tests.
    TestSetup {
        subscription_epoch: u64,
        metro_usdc_price: u16,
        device_premium: i16,
    },
}

impl ReservationInstructionData {
    pub const INITIALIZE_PROGRAM: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_program");
    pub const SET_ADMIN: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::set_admin");
    pub const CONFIGURE_PROGRAM: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::configure_program");
    pub const INITIALIZE_METRO_HISTORY: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_metro_history");
    pub const INITIALIZE_DEVICE_HISTORY: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_device_history");
    pub const SET_DEVICE_ENABLED: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::set_device_enabled");
    pub const INITIALIZE_CLIENT_SEAT: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_client_seat");
    pub const ADVANCE_EXECUTION_PHASE: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::advance_execution_phase");
    pub const UPDATE_METRO_USDC_PRICE: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::update_metro_usdc_price");
    pub const INITIALIZE_PAYMENT_ESCROW: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::initialize_payment_escrow");
    pub const CLOSE_PAYMENT_ESCROW: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::close_payment_escrow");
    pub const FUND_PAYMENT_ESCROW_USDC: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::fund_payment_escrow_usdc");
    pub const DEDUCT_SUBSCRIPTION_FEE: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::deduct_subscription_fee");
    pub const TEST_SETUP: Discriminator<DISCRIMINATOR_LEN> =
        Discriminator::new_sha2(b"dz::ix::test_setup");
}

impl BorshDeserialize for ReservationInstructionData {
    fn deserialize_reader<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        match Discriminator::deserialize_reader(reader)? {
            Self::INITIALIZE_PROGRAM => Ok(Self::InitializeProgram),
            Self::SET_ADMIN => BorshDeserialize::deserialize_reader(reader).map(Self::SetAdmin),
            Self::CONFIGURE_PROGRAM => {
                BorshDeserialize::deserialize_reader(reader).map(Self::ConfigureProgram)
            }
            Self::INITIALIZE_METRO_HISTORY => {
                BorshDeserialize::deserialize_reader(reader).map(Self::InitializeMetroHistory)
            }
            Self::INITIALIZE_DEVICE_HISTORY => {
                BorshDeserialize::deserialize_reader(reader).map(Self::InitializeDeviceHistory)
            }
            Self::SET_DEVICE_ENABLED => {
                BorshDeserialize::deserialize_reader(reader).map(Self::SetDeviceEnabled)
            }
            Self::ADVANCE_EXECUTION_PHASE => Ok(Self::AdvanceExecutionPhase),
            Self::UPDATE_METRO_USDC_PRICE => {
                BorshDeserialize::deserialize_reader(reader).map(Self::UpdateMetroUsdcPrice)
            }
            Self::TEST_SETUP => {
                let subscription_epoch = BorshDeserialize::deserialize_reader(reader)?;
                let metro_usdc_price = BorshDeserialize::deserialize_reader(reader)?;
                let device_premium = BorshDeserialize::deserialize_reader(reader)?;
                Ok(Self::TestSetup {
                    subscription_epoch,
                    metro_usdc_price,
                    device_premium,
                })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid discriminator",
            )),
        }
    }
}

impl BorshSerialize for ReservationInstructionData {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Self::InitializeProgram => Self::INITIALIZE_PROGRAM.serialize(writer),
            Self::SetAdmin(admin_key) => {
                Self::SET_ADMIN.serialize(writer)?;
                admin_key.serialize(writer)
            }
            Self::ConfigureProgram(setting) => {
                Self::CONFIGURE_PROGRAM.serialize(writer)?;
                setting.serialize(writer)
            }
            Self::InitializeMetroHistory(exchange_key) => {
                Self::INITIALIZE_METRO_HISTORY.serialize(writer)?;
                exchange_key.serialize(writer)
            }
            Self::InitializeDeviceHistory(device_key) => {
                Self::INITIALIZE_DEVICE_HISTORY.serialize(writer)?;
                device_key.serialize(writer)
            }
            Self::SetDeviceEnabled(is_enabled) => {
                Self::SET_DEVICE_ENABLED.serialize(writer)?;
                is_enabled.serialize(writer)
            }
            Self::AdvanceExecutionPhase => Self::ADVANCE_EXECUTION_PHASE.serialize(writer),
            Self::UpdateMetroUsdcPrice(metro_price) => {
                Self::UPDATE_METRO_USDC_PRICE.serialize(writer)?;
                metro_price.serialize(writer)
            }
            Self::TestSetup {
                subscription_epoch,
                metro_usdc_price,
                device_premium,
            } => {
                Self::TEST_SETUP.serialize(writer)?;
                subscription_epoch.serialize(writer)?;
                metro_usdc_price.serialize(writer)?;
                device_premium.serialize(writer)
            }
        }
    }
}
