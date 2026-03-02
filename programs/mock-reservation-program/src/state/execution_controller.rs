use std::fmt;

use bytemuck::{Pod, Zeroable};
use doublezero_program_tools::{Discriminator, PrecomputedDiscriminator};
use solana_pubkey::Pubkey;

/// Representation of the execution phase of the program. Expected state flow is
/// defined as:
///
/// Settled -> UpdatingPrices -> OpenForRequests -> ClosedForRequests -> Settled
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum ExecutionPhase {
    #[default]
    Settled,
    UpdatingPrices,
    OpenForRequests,
    ClosedForRequests,
}

impl ExecutionPhase {
    pub const fn new(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Settled),
            1 => Some(Self::UpdatingPrices),
            2 => Some(Self::OpenForRequests),
            3 => Some(Self::ClosedForRequests),
            _ => None,
        }
    }

    pub const fn next_phase(&self) -> Self {
        match self {
            Self::Settled => Self::UpdatingPrices,
            Self::UpdatingPrices => Self::OpenForRequests,
            Self::OpenForRequests => Self::ClosedForRequests,
            Self::ClosedForRequests => Self::Settled,
        }
    }

    pub const fn static_str(&self) -> &'static str {
        match self {
            Self::Settled => "settled",
            Self::UpdatingPrices => "updating prices",
            Self::OpenForRequests => "open for requests",
            Self::ClosedForRequests => "closed for requests",
        }
    }
}

impl fmt::Display for ExecutionPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.static_str())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ExecutionPhaseField(u8);

impl ExecutionPhaseField {
    pub const fn new(phase: ExecutionPhase) -> Self {
        Self(phase as u8)
    }

    pub fn get(&self) -> Option<ExecutionPhase> {
        ExecutionPhase::new(self.0)
    }

    pub fn set(&mut self, phase: ExecutionPhase) {
        self.0 = phase as u8;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct ExecutionController {
    phase_field: ExecutionPhaseField,
    _padding_0: [u8; 3],

    pub total_metros: u16,
    pub total_enabled_devices: u16,

    pub next_subscription_epoch: u64,

    pub updated_device_prices_count: u16,
    pub settled_devices_count: u16,
    _padding_1: [u8; 4],

    pub last_settled_slot: u64,
    pub last_updating_prices_slot: u64,
    pub last_open_for_requests_slot: u64,
    pub last_closed_for_requests_slot: u64,
}

impl PrecomputedDiscriminator for ExecutionController {
    const DISCRIMINATOR: Discriminator<8> =
        Discriminator::new_sha2(b"dz::account::execution_controller");
}

impl ExecutionController {
    pub const SEED_PREFIX: &'static [u8] = b"execution_controller";

    pub fn find_address() -> (Pubkey, u8) {
        Pubkey::find_program_address(&[Self::SEED_PREFIX], &crate::ID)
    }

    #[inline]
    pub fn phase(&self) -> ExecutionPhase {
        self.phase_field.get().unwrap()
    }

    #[inline]
    pub fn next_phase(&self) -> ExecutionPhase {
        self.phase().next_phase()
    }

    #[inline]
    pub fn set_next_phase(&mut self) {
        self.phase_field.set(self.next_phase());
    }

    #[inline]
    pub fn set_phase(&mut self, phase: ExecutionPhase) {
        self.phase_field.set(phase);
    }

    pub fn last_settled_epoch(&self) -> Option<u64> {
        self.next_subscription_epoch.checked_sub(1)
    }

    pub fn current_subscription_epoch(&self) -> Option<u64> {
        if self.phase() == ExecutionPhase::Settled {
            self.last_settled_epoch()
        } else {
            Some(self.next_subscription_epoch)
        }
    }

    pub fn are_all_devices_updated(&self) -> bool {
        self.total_enabled_devices
            .saturating_sub(self.updated_device_prices_count)
            == 0
    }

    pub fn are_all_devices_settled(&self) -> bool {
        self.total_enabled_devices
            .saturating_sub(self.settled_devices_count)
            == 0
    }
}
