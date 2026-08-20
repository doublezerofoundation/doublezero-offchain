//! Epoch calculation utilities for mapping timestamps to Solana epochs
//!
//! This module provides functionality to:
//! - Estimate slots from timestamps
//! - Find epochs corresponding to specific timestamps

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use backon::{ExponentialBuilder, Retryable};
use chrono::Utc;
use doublezero_solana_client_tools::rpc::DoubleZeroLedgerConnection;
use serde::{Deserialize, Serialize};
use solana_client::{
    client_error::{ClientError as SolanaClientError, ClientErrorKind},
    nonblocking::rpc_client::RpcClient,
    rpc_custom_error::{
        JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP, JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED, JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
    },
    rpc_request::RpcError,
};
use solana_sdk::epoch_schedule::EpochSchedule;
use tracing::{debug, info};

use crate::cli::{
    common::{OutputFormat, to_json_string},
    traits::Exportable,
};

// Seed slot duration for the epoch search in `find_epoch_at_timestamp`. Solana's
// real slot duration is drifting: mainnet-beta moves 400ms to 350ms at the start
// of epoch 1020 (2026-08-21) and SIMD-0525 continues stepping it down to 200ms,
// while testnet is already at 200ms. So this number is wrong on some cluster at
// any given time, and during the rollout the real rate changes inside a single
// lookback window. It only decides how many RPC round trips the search needs to
// converge, never which epoch the search returns.
const SEED_SLOT_DURATION_US: u64 = 350_000;

// The first slot of an epoch is frequently skipped, so the epoch start lookup
// walks forward from it until a confirmed block turns up. A boundary that needs
// more than this many slots to produce one block is not a run of skipped slots,
// it is a stalled cluster, and guessing through that would be worse than an
// error. The same bound caps the backward walk for the chain tip's block time.
const MAX_BOUNDARY_SEARCH_SLOTS: u64 = 128;

// Each search step moves the candidate by one epoch. The seed is normally within
// an epoch or two of correct, so this cap exists only to bound a pathological
// seed rather than loop forever.
const MAX_EPOCH_SEARCH_STEPS: usize = 16;

// key: validator_pk, val: slot count
pub type LeaderScheduleMap = BTreeMap<String, usize>;

// Wrapper struct for leader scheduler
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderSchedule {
    pub solana_epoch: u64,
    pub schedule_map: LeaderScheduleMap,
}

impl Exportable for LeaderSchedule {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => {
                bail!("CSV export not supported for leader schedule. Use JSON format instead.")
            }
            OutputFormat::Json => to_json_string(&self, false),
            OutputFormat::JsonPretty => to_json_string(&self, true),
        }
    }
}

/// Report whether an RPC error means the slot has no block to report a time for,
/// as opposed to the request itself failing.
///
/// `getBlockTime` says this in more than one way depending on what the endpoint
/// has behind it. A validator with long term storage answers with a coded
/// skipped-slot error, one without it answers with a JSON `null` that
/// `RpcClient::get_block_time` turns into `RpcError::ForUser("Block Not
/// Found: ...")`, and a slot the endpoint has not rooted yet is reported as
/// block-not-available. All three mean the same thing to the callers here: move
/// on to the next slot.
///
/// `JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP` is deliberately excluded. A pruned
/// ledger means the answer is unknowable rather than "this slot produced no
/// block", so it has to fail the search instead of sending the walk forward over
/// slots whose block times are equally gone.
fn is_block_unavailable(err: &SolanaClientError) -> bool {
    match err.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) => matches!(
            *code,
            JSON_RPC_SERVER_ERROR_SLOT_SKIPPED
                | JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED
                | JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE
        ),
        ClientErrorKind::RpcError(RpcError::ForUser(message)) => {
            message.starts_with("Block Not Found")
        }
        _ => false,
    }
}

/// Report whether an RPC error means the ledger no longer holds the slot.
///
/// Kept apart from [`is_block_unavailable`] because this one has to fail the
/// search rather than advance it, but it is just as settled, so it is not worth
/// retrying either.
fn is_block_cleaned_up(err: &SolanaClientError) -> bool {
    matches!(
        err.kind(),
        ClientErrorKind::RpcError(RpcError::RpcResponseError {
            code: JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP,
            ..
        })
    )
}

#[derive(Debug, PartialEq, Eq)]
enum EpochSearchStep {
    Earlier,
    Later,
    Found,
}

/// Decide which way the epoch search should move from its current candidate.
///
/// `epoch_start_time` and `next_epoch_start_time` are the block times of the
/// first block in the candidate epoch and in the epoch after it, in seconds.
/// `None` means that epoch has produced no block yet, so it bounds nothing: for
/// the candidate that rules the candidate out, and for the next epoch it means
/// the candidate is the newest epoch the cluster has produced a block in, so
/// there is no upper bound to compare against. Accepting the candidate in that
/// second case is only sound because the caller has already established that the
/// target is at or before the chain tip.
///
/// The lower bound is inclusive and the upper bound is exclusive, so a timestamp
/// falling exactly on an epoch's first block time belongs to that epoch.
fn decide_epoch_search_step(
    target_time: i64,
    epoch_start_time: Option<i64>,
    next_epoch_start_time: Option<i64>,
) -> EpochSearchStep {
    let Some(epoch_start_time) = epoch_start_time else {
        return EpochSearchStep::Earlier;
    };

    if target_time < epoch_start_time {
        return EpochSearchStep::Earlier;
    }

    match next_epoch_start_time {
        Some(next_epoch_start_time) if target_time >= next_epoch_start_time => {
            EpochSearchStep::Later
        }
        _ => EpochSearchStep::Found,
    }
}

/// Estimate the slot at a given timestamp based on current slot and time
///
/// Returns an error if the timestamp is in the future or too far in the past.
pub fn estimate_slot_from_timestamp(
    timestamp_us: u64,
    current_slot: u64,
    current_time_us: u64,
) -> Result<u64> {
    if timestamp_us > current_time_us {
        bail!("Timestamp {timestamp_us} is in the future");
    }

    // Calculate approximate slot at the given timestamp
    let time_diff_us = current_time_us - timestamp_us;
    let slots_ago = time_diff_us / SEED_SLOT_DURATION_US;

    if slots_ago > current_slot {
        bail!("Timestamp {timestamp_us} is too far in the past");
    }

    Ok(current_slot - slots_ago)
}

/// Helper for finding epochs at specific timestamps
///
/// This struct manages the epoch schedule and provides methods for
/// converting between timestamps and epochs. It caches the epoch schedule
/// to avoid redundant RPC calls but ONLY within a single execution context.
///
/// The struct takes explicit RPC clients to make it clear which network
/// is being queried for epoch calculations.
pub struct EpochFinder {
    /// DZ network RPC client for getting current slot and timestamps
    dz_rpc_client: Arc<DoubleZeroLedgerConnection>,
    /// Solana network RPC client for getting leader schedules
    solana_read_client: Arc<RpcClient>,
    /// Cached DZ epoch schedule
    dz_schedule: Option<EpochSchedule>,
    /// Cached Solana epoch schedule
    solana_schedule: Option<EpochSchedule>,
}

impl EpochFinder {
    /// Create a new EpochFinder with explicit RPC clients
    ///
    /// # Arguments
    /// * `dz_rpc_client` - RPC client for the DZ network (for timestamps and current slot)
    /// * `solana_read_client` - RPC client for Solana network (for leader schedules)
    pub fn new(
        dz_rpc_client: Arc<DoubleZeroLedgerConnection>,
        solana_read_client: Arc<RpcClient>,
    ) -> Self {
        Self {
            dz_rpc_client,
            solana_read_client,
            dz_schedule: None,
            solana_schedule: None,
        }
    }

    /// Get the DZ epoch schedule, fetching it if not already cached
    pub async fn get_dz_schedule(&mut self) -> Result<&EpochSchedule> {
        if self.dz_schedule.is_none() {
            let schedule = (|| async { self.dz_rpc_client.get_epoch_schedule().await })
                .retry(&ExponentialBuilder::default().with_jitter())
                .notify(|err: &SolanaClientError, dur: Duration| {
                    info!(
                        "retrying get_epoch_schedule error: {:?} with sleeping {:?}",
                        err, dur
                    )
                })
                .await?;
            self.dz_schedule = Some(schedule);
        }

        Ok(self
            .dz_schedule
            .as_ref()
            .expect("dz_schedule cannot be none"))
    }

    /// Get the Solana epoch schedule, fetching it if not already cached
    pub async fn get_solana_schedule(&mut self) -> Result<&EpochSchedule> {
        if self.solana_schedule.is_none() {
            let schedule = (|| async { self.solana_read_client.get_epoch_schedule().await })
                .retry(&ExponentialBuilder::default().with_jitter())
                .notify(|err: &SolanaClientError, dur: Duration| {
                    info!(
                        "retrying get_epoch_schedule error: {:?} with sleeping {:?}",
                        err, dur
                    )
                })
                .await?;
            self.solana_schedule = Some(schedule);
        }

        Ok(self
            .solana_schedule
            .as_ref()
            .expect("solana_schedule cannot be none"))
    }

    /// Get a slot's block time in seconds, or `Ok(None)` when the slot has no
    /// block to report a time for.
    ///
    /// Transport failures are retried, but a slot that produced no block is
    /// returned immediately: that is a settled fact rather than a transient
    /// error, so retrying it only burns the backoff schedule before arriving at
    /// the same answer. A pruned ledger is settled in the same way but is an
    /// error rather than a `None`, so it is not retried either.
    async fn try_get_block_time(&self, slot: u64) -> Result<Option<i64>> {
        let block_time = (|| async { self.solana_read_client.get_block_time(slot).await })
            .retry(&ExponentialBuilder::default().with_jitter())
            .when(|err: &SolanaClientError| !is_block_unavailable(err) && !is_block_cleaned_up(err))
            .notify(|err: &SolanaClientError, dur: Duration| {
                info!(
                    "retrying get_block_time error: {:?} with sleeping {:?}",
                    err, dur
                )
            })
            .await;

        match block_time {
            Ok(block_time) => Ok(Some(block_time)),
            Err(err) if is_block_unavailable(&err) => Ok(None),
            Err(err) => {
                Err(err).with_context(|| format!("Failed to get block time for Solana slot {slot}"))
            }
        }
    }

    /// Find the block time of the first block at or after `first_slot`, in
    /// seconds.
    ///
    /// Returns `Ok(None)` when no block exists between `first_slot` and
    /// `current_slot`, which means the epoch starting at `first_slot` has not
    /// produced a block yet and so bounds nothing.
    ///
    /// The block time that is found is returned as is, with no back estimation of
    /// the skipped slots that preceded it. For deciding which epoch a timestamp
    /// falls in, the first block's time is the epoch's effective start, so
    /// subtracting an estimate would only add error. This is a deliberate
    /// difference from `estimate_block_time_for_skipped_slot` in
    /// `validator-debt/src/rpc.rs`, which does subtract one.
    async fn try_epoch_start_block_time(
        &self,
        epoch: u64,
        first_slot: u64,
        current_slot: u64,
    ) -> Result<Option<i64>> {
        for slot in first_slot..first_slot + MAX_BOUNDARY_SEARCH_SLOTS {
            if slot > current_slot {
                return Ok(None);
            }

            match self.try_get_block_time(slot).await? {
                Some(block_time) => return Ok(Some(block_time)),
                None => debug!(
                    "Solana slot {} has no block, searching forward for the start of epoch {}",
                    slot, epoch
                ),
            }
        }

        bail!(
            "No block within {MAX_BOUNDARY_SEARCH_SLOTS} slots of slot {first_slot}, \
             the first slot of Solana epoch {epoch}"
        )
    }

    /// Find the block time of the newest block at or before `current_slot`, in
    /// seconds.
    ///
    /// The walk runs backward because `getSlot` can name a slot that has no block
    /// yet, either because it was skipped or because the endpoint has not caught
    /// up to it.
    async fn try_chain_tip_block_time(&self, current_slot: u64) -> Result<i64> {
        let oldest_slot_to_search = current_slot.saturating_sub(MAX_BOUNDARY_SEARCH_SLOTS - 1);

        for slot in (oldest_slot_to_search..=current_slot).rev() {
            if let Some(block_time) = self.try_get_block_time(slot).await? {
                return Ok(block_time);
            }
        }

        bail!(
            "No block within {MAX_BOUNDARY_SEARCH_SLOTS} slots at or before the current slot \
             {current_slot}"
        )
    }

    /// Find the Solana epoch that was active at a given timestamp
    ///
    /// The timestamp is mapped to a seed slot by dividing wall clock elapsed by
    /// `SEED_SLOT_DURATION_US`, and the epoch that seed lands in is then verified
    /// against real block times. The verification is what makes the answer
    /// correct: the seed drifts by thousands of slots over a day of lookback, and
    /// no fixed slot duration survives the SIMD-0525 rollout, so a seeded guess
    /// alone selects the wrong epoch near a boundary. Since the epoch chooses the
    /// leader schedule that contributor rewards are computed against, a wrong
    /// answer here corrupts rewards, and an error is the better outcome.
    ///
    /// A second chain verified epoch search lives in
    /// `validator-debt/src/rpc.rs` (`find_solana_epoch_before_timestamp`). That
    /// one walks backward only and threads a `leaky-bucket` rate limiter, so the
    /// two are not yet worth unifying, but a fix to the skipped slot or boundary
    /// handling here probably belongs there too.
    pub async fn find_epoch_at_timestamp(&mut self, timestamp_us: u64) -> Result<u64> {
        // Get current slot from Solana
        let current_slot = (|| async { self.solana_read_client.get_slot().await })
            .retry(&ExponentialBuilder::default().with_jitter())
            .notify(|err: &SolanaClientError, dur: Duration| {
                info!("retrying get_slot error: {:?} with sleeping {:?}", err, dur)
            })
            .await?;

        let current_time_us = Utc::now().timestamp_micros() as u64;

        // Seed the search. This also rejects a future or unreachably old
        // timestamp before any RPC calls are spent on it.
        let estimated_slot =
            estimate_slot_from_timestamp(timestamp_us, current_slot, current_time_us)?;

        // Copied out of the cache rather than borrowed, because the borrow that
        // get_solana_schedule hands back is tied to its &mut self, which the
        // block time lookups below also need.
        let schedule = self.get_solana_schedule().await?.clone();

        let mut candidate_epoch = schedule.get_epoch(estimated_slot);
        let target_time = (timestamp_us / 1_000_000) as i64;

        // estimate_slot_from_timestamp only checked the timestamp against the
        // local clock, which says nothing about how far the read endpoint has
        // caught up. The search below accepts a candidate with no upper bound as
        // the answer, on the grounds that the next epoch has produced no block
        // yet, so a timestamp past the chain tip would resolve to whatever epoch
        // a lagging endpoint happens to be sitting in. Reject it here instead.
        let chain_tip_time = self.try_chain_tip_block_time(current_slot).await?;
        if target_time > chain_tip_time {
            bail!(
                "Timestamp {timestamp_us} is ahead of the Solana chain tip at slot \
                 {current_slot} (block time {chain_tip_time}), so the epoch containing it is \
                 not yet determined"
            );
        }

        for _ in 0..MAX_EPOCH_SEARCH_STEPS {
            let epoch_start_time = self
                .try_epoch_start_block_time(
                    candidate_epoch,
                    schedule.get_first_slot_in_epoch(candidate_epoch),
                    current_slot,
                )
                .await?;
            let next_epoch_start_time = self
                .try_epoch_start_block_time(
                    candidate_epoch + 1,
                    schedule.get_first_slot_in_epoch(candidate_epoch + 1),
                    current_slot,
                )
                .await?;

            match decide_epoch_search_step(target_time, epoch_start_time, next_epoch_start_time) {
                EpochSearchStep::Found => {
                    debug!(
                        "Mapped timestamp {} to Solana epoch {}",
                        timestamp_us, candidate_epoch
                    );
                    return Ok(candidate_epoch);
                }
                EpochSearchStep::Earlier => {
                    candidate_epoch = candidate_epoch.checked_sub(1).with_context(|| {
                        format!("Timestamp {timestamp_us} precedes the first Solana epoch")
                    })?;
                }
                EpochSearchStep::Later => candidate_epoch += 1,
            }
        }

        bail!(
            "Could not resolve timestamp {timestamp_us} to a Solana epoch within \
             {MAX_EPOCH_SEARCH_STEPS} steps, last candidate was epoch {candidate_epoch}"
        )
    }

    /// Fetch leader schedule for a DZ epoch
    ///
    /// This method:
    /// 1. Takes a DZ epoch and timestamp as input
    /// 2. Maps it to a Solana epoch
    /// 3. Gets the first slot of that Solana epoch
    /// 4. Fetches the leader schedule using the slot number
    ///
    /// Returns the leader schedule as a map of validator pubkey to slot count
    pub async fn fetch_leader_schedule(
        &mut self,
        dz_epoch: u64,
        timestamp_us: u64,
    ) -> Result<LeaderSchedule> {
        info!("Fetching leader schedule for DZ epoch {}", dz_epoch);

        // Find the corresponding Solana epoch for this timestamp
        let solana_epoch = self.find_epoch_at_timestamp(timestamp_us).await?;

        info!(
            "DZ epoch {} corresponds to Solana epoch {} (based on timestamp {})",
            dz_epoch, solana_epoch, timestamp_us
        );

        // Get Solana epoch schedule
        let solana_schedule = self.get_solana_schedule().await?;

        // Get the first slot of the Solana epoch
        let first_slot_of_epoch = solana_schedule.get_first_slot_in_epoch(solana_epoch);

        debug!(
            "Fetching leader schedule for Solana epoch {} using slot {}",
            solana_epoch, first_slot_of_epoch
        );

        // Get leader schedule using slot number (not epoch number)
        let leader_schedule = (|| async {
            self.solana_read_client
                .get_leader_schedule(Some(first_slot_of_epoch))
                .await
        })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!(
                "retrying get_leader_schedule error: {:?} with sleeping {:?}",
                err, dur
            )
        })
        .await?
        .ok_or_else(|| anyhow!("No leader schedule found for Solana epoch {solana_epoch}"))?;

        // Convert leader schedule to map of validator -> slot count
        let schedule_map: LeaderScheduleMap = leader_schedule
            .into_iter()
            .map(|(pk, schedule)| (pk, schedule.len()))
            .collect();

        info!(
            "Retrieved leader schedule with {} validators",
            schedule_map.len()
        );

        Ok(LeaderSchedule {
            solana_epoch,
            schedule_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use solana_client::rpc_request::RpcResponseErrorData;

    use super::*;

    #[test]
    fn test_estimate_slot_from_timestamp() {
        let current_slot = 1000000;
        let current_time_us = 1_000_000_000_000; // 1 million seconds in microseconds

        // Test normal case - 350 seconds ago (350_000_000 us / 350_000 us per
        // slot = 1000 slots)
        let timestamp_us = current_time_us - 350_000_000;
        let result = estimate_slot_from_timestamp(timestamp_us, current_slot, current_time_us);
        assert_eq!(result.unwrap(), 999000);

        // Test future timestamp. find_epoch_at_timestamp seeds its search with
        // this call, so this guard is what keeps a future timestamp out of the
        // search entirely.
        let future_timestamp = current_time_us + 1000;
        let result = estimate_slot_from_timestamp(future_timestamp, current_slot, current_time_us);
        assert!(result.is_err());

        // Test too far in the past
        let ancient_timestamp = 0;
        let result = estimate_slot_from_timestamp(ancient_timestamp, current_slot, current_time_us);
        assert!(result.is_err());
    }

    // The epoch's first confirmed block time is the inclusive lower bound, so a
    // timestamp landing exactly on it belongs to the candidate epoch.
    #[test]
    fn test_decide_epoch_search_step_at_epoch_start_is_found() {
        assert_eq!(
            decide_epoch_search_step(1_700_000_000, Some(1_700_000_000), Some(1_700_100_000)),
            EpochSearchStep::Found
        );
    }

    // One second earlier belongs to the previous epoch. This is the boundary case
    // that a seeded estimate alone got wrong.
    #[test]
    fn test_decide_epoch_search_step_before_epoch_start_steps_earlier() {
        assert_eq!(
            decide_epoch_search_step(1_699_999_999, Some(1_700_000_000), Some(1_700_100_000)),
            EpochSearchStep::Earlier
        );
    }

    // The next epoch's first confirmed block time is the exclusive upper bound, so
    // a timestamp landing exactly on it belongs to the next epoch, not this one.
    #[test]
    fn test_decide_epoch_search_step_at_next_epoch_start_steps_later() {
        assert_eq!(
            decide_epoch_search_step(1_700_100_000, Some(1_700_000_000), Some(1_700_100_000)),
            EpochSearchStep::Later
        );
    }

    // The current epoch has no next epoch to bound it, because
    // get_first_slot_in_epoch for the epoch after it names a future slot with no
    // block time. Without this the search would fail on every recent timestamp.
    #[test]
    fn test_decide_epoch_search_step_current_epoch_has_no_upper_bound() {
        assert_eq!(
            decide_epoch_search_step(1_700_100_000, Some(1_700_000_000), None),
            EpochSearchStep::Found
        );
    }

    // An epoch that has produced no block cannot contain a past timestamp. The
    // caller feeds this step into a checked_sub, so a timestamp older than the
    // earliest available block walks the candidate down and then errors rather
    // than underflowing or silently returning epoch 0.
    #[test]
    fn test_decide_epoch_search_step_unstarted_epoch_steps_earlier() {
        assert_eq!(
            decide_epoch_search_step(1_700_000_000, None, None),
            EpochSearchStep::Earlier
        );
    }

    fn rpc_response_error(code: i64) -> SolanaClientError {
        RpcError::RpcResponseError {
            code,
            message: "test".to_string(),
            data: RpcResponseErrorData::Empty,
        }
        .into()
    }

    // Every shape `getBlockTime` uses to say "this slot has no block" has to be
    // classified the same way, or the forward walk over a skipped epoch boundary
    // never happens and the search aborts instead. Which shape arrives depends on
    // what the endpoint has behind it, so an endpoint without long-term storage
    // would otherwise fail at most epoch boundaries.
    #[test]
    fn test_is_block_unavailable_covers_every_absent_block_shape() {
        assert!(is_block_unavailable(&rpc_response_error(
            JSON_RPC_SERVER_ERROR_SLOT_SKIPPED
        )));
        assert!(is_block_unavailable(&rpc_response_error(
            JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED
        )));
        assert!(is_block_unavailable(&rpc_response_error(
            JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE
        )));
        // What RpcClient::get_block_time synthesizes from a JSON null response.
        assert!(is_block_unavailable(
            &RpcError::ForUser("Block Not Found: slot=123".to_string()).into()
        ));
    }

    // A pruned ledger has to fail the search rather than send the walk forward,
    // because the block times it would walk over are equally gone. Neither
    // classifier may retry it, since the answer will not change.
    #[test]
    fn test_is_block_cleaned_up_is_not_an_absent_block() {
        let err = rpc_response_error(JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP);
        assert!(!is_block_unavailable(&err));
        assert!(is_block_cleaned_up(&err));
    }

    // A transport failure is neither, so it stays retryable.
    #[test]
    fn test_transport_error_is_retryable() {
        let err = RpcError::RpcRequestError("connection reset".to_string()).into();
        assert!(!is_block_unavailable(&err));
        assert!(!is_block_cleaned_up(&err));
    }
}
