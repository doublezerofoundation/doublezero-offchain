use std::{path::PathBuf, sync::Arc, time::Duration};

use doublezero_serviceability::state::tenant::{
    FlatPerEpochConfig, TenantBillingConfig, TenantPaymentStatus,
};
use retainer::Cache;
use solana_sdk::pubkey::Pubkey;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    TenantBillingInfo,
    client::{doublezero_ledger::DzRpcClientType, solana::SolRpcClientType},
};

// cache ttl: 10 minutes
const BILLING_CACHE_TTL: Duration = Duration::from_secs(600);
// cache monitoring interval, every 2 minutes
const BILLING_CACHE_MONITOR_INTERVAL: Duration = Duration::from_secs(120);

/// Tracks the two-phase state of a billing deduction to prevent double-charges
/// when the epoch update fails after a successful SPL transfer.
#[derive(Clone, Debug)]
enum DeductionState {
    /// SPL transfer succeeded; epoch update still pending.
    Transferred(u64),
    /// Both transfer and epoch update completed.
    Completed(u64),
    /// Transfer failed; suppress retries until TTL expires.
    Failed(u64),
}

pub struct BillingConfig {
    pub poll_interval_secs: u64,
    pub minimum_balance: Option<u64>,
    pub journal_ata: Pubkey,
    pub mint: Pubkey,
    pub decimals: u8,
    pub pending_dir: PathBuf,
}

pub struct BillingSentinel<D: DzRpcClientType, S: SolRpcClientType> {
    dz_rpc_client: D,
    sol_rpc_client: S,
    status_cache: Arc<Cache<Pubkey, TenantPaymentStatus>>,
    deduction_cache: Arc<Cache<Pubkey, DeductionState>>,
    poll_interval: Duration,
    minimum_balance: u64,
    journal_ata: Pubkey,
    mint: Pubkey,
    decimals: u8,
    pending_dir: PathBuf,
}

impl<D: DzRpcClientType, S: SolRpcClientType> BillingSentinel<D, S> {
    pub async fn new(dz_rpc_client: D, sol_rpc_client: S, config: BillingConfig) -> Self {
        std::fs::create_dir_all(&config.pending_dir)
            .expect("failed to create billing state directory");

        let status_cache = Arc::new(Cache::new());
        let deduction_cache = Arc::new(Cache::new());

        // Spawn background tasks to monitor caches
        let status_clone = status_cache.clone();
        tokio::spawn(async move {
            status_clone
                .monitor(5, 0.25, BILLING_CACHE_MONITOR_INTERVAL)
                .await;
        });
        let deduction_clone = deduction_cache.clone();
        tokio::spawn(async move {
            deduction_clone
                .monitor(5, 0.25, BILLING_CACHE_MONITOR_INTERVAL)
                .await;
        });

        Self {
            dz_rpc_client,
            sol_rpc_client,
            status_cache,
            deduction_cache,
            poll_interval: Duration::from_secs(config.poll_interval_secs),
            minimum_balance: config.minimum_balance.unwrap_or(1),
            journal_ata: config.journal_ata,
            mint: config.mint,
            decimals: config.decimals,
            pending_dir: config.pending_dir,
        }
    }

    pub async fn run(&mut self, shutdown_listener: CancellationToken) -> crate::Result<()> {
        let mut poll_timer = interval(self.poll_interval);

        loop {
            tokio::select! {
                biased;
                _ = shutdown_listener.cancelled() => {
                    info!("billing sentinel: shutdown signal received");
                    break;
                }
                _ = poll_timer.tick() => {
                    if let Err(err) = self.poll_cycle().await {
                        error!(?err, "billing poll cycle failed; will retry in next cycle");
                        metrics::counter!("doublezero_sentinel_billing_poll_failed").increment(1);
                    }
                }
            }
        }

        Ok(())
    }

    async fn poll_cycle(&self) -> crate::Result<()> {
        let start = std::time::Instant::now();

        // Fetch current DZ epoch for deduction processing. If this fails,
        // we can still run legacy balance checks but skip deductions.
        let current_epoch = match self.dz_rpc_client.get_current_dz_epoch().await {
            Ok(epoch) => Some(epoch),
            Err(err) => {
                warn!(
                    ?err,
                    "billing: failed to fetch current DZ epoch; skipping deductions"
                );
                None
            }
        };

        let tenants = self.dz_rpc_client.get_tenants_with_token_accounts().await?;

        info!(count = tenants.len(), "billing: checking tenant balances");
        metrics::gauge!("doublezero_sentinel_billing_tenants_checked").set(tenants.len() as f64);

        for tenant in tenants {
            let TenantBillingConfig::FlatPerEpoch(ref config) = tenant.billing;

            let result = if config.rate > 0 {
                if let Some(epoch) = current_epoch {
                    self.deduct_tenant(&tenant, config, epoch).await
                } else {
                    // Can't process deductions without knowing the current epoch
                    continue;
                }
            } else {
                // Legacy balance-check path (rate == 0)
                self.check_and_update_tenant(&tenant).await
            };

            if let Err(err) = result {
                warn!(
                    tenant = %tenant.tenant_pda,
                    ?err,
                    "billing: failed to process tenant"
                );
            }
        }

        let elapsed = start.elapsed();
        metrics::histogram!("doublezero_sentinel_billing_cycle_duration_seconds")
            .record(elapsed.as_secs_f64());

        Ok(())
    }

    async fn deduct_tenant(
        &self,
        tenant: &TenantBillingInfo,
        config: &FlatPerEpochConfig,
        current_epoch: u64,
    ) -> crate::Result<()> {
        // Already current — nothing to deduct
        if config.last_deduction_dz_epoch >= current_epoch {
            return Ok(());
        }

        let target_epoch = config.last_deduction_dz_epoch + 1;

        // Check in-memory deduction cache (fast path)
        if let Some(state) = self.deduction_cache.get(&tenant.tenant_pda).await {
            match *state {
                DeductionState::Completed(epoch) if epoch >= target_epoch => return Ok(()),
                DeductionState::Transferred(epoch) if epoch >= target_epoch => {
                    // Transfer already succeeded — retry only the epoch update
                    info!(
                        tenant = %tenant.tenant_pda,
                        target_epoch,
                        "billing: retrying epoch update for completed transfer"
                    );
                    return self
                        .complete_deduction(&tenant.tenant_pda, target_epoch)
                        .await;
                }
                DeductionState::Failed(epoch) if epoch >= target_epoch => return Ok(()),
                _ => {}
            }
        }

        // Check persistent marker (survives restarts — guards against
        // double-charge when the process crashed between transfer and epoch update)
        if self.has_pending_transfer(&tenant.tenant_pda, target_epoch) {
            info!(
                tenant = %tenant.tenant_pda,
                target_epoch,
                "billing: found pending transfer marker, retrying epoch update only"
            );
            return self
                .complete_deduction(&tenant.tenant_pda, target_epoch)
                .await;
        }

        info!(
            tenant = %tenant.tenant_pda,
            rate = config.rate,
            target_epoch,
            current_epoch,
            "billing: deducting tenant"
        );

        // Attempt the SPL token transfer
        match self
            .sol_rpc_client
            .transfer_spl_token(
                &tenant.token_account,
                &self.journal_ata,
                config.rate,
                &self.mint,
                self.decimals,
            )
            .await
        {
            Ok(signature) => {
                info!(
                    tenant = %tenant.tenant_pda,
                    %signature,
                    target_epoch,
                    "billing: transfer successful"
                );
                // Persist marker BEFORE epoch update so a crash between
                // these two steps can be recovered on restart
                self.mark_transfer_pending(&tenant.tenant_pda, target_epoch);
                self.complete_deduction(&tenant.tenant_pda, target_epoch)
                    .await
            }
            Err(err) => {
                warn!(
                    tenant = %tenant.tenant_pda,
                    ?err,
                    "billing: deduction transfer failed, checking balance"
                );

                // Cache to suppress retries until TTL expires
                self.deduction_cache
                    .insert(
                        tenant.tenant_pda,
                        DeductionState::Failed(target_epoch),
                        BILLING_CACHE_TTL,
                    )
                    .await;

                // Check whether the failure is due to insufficient balance
                let balance = self
                    .sol_rpc_client
                    .get_token_account_balance(&tenant.token_account)
                    .await?;

                if balance < config.rate {
                    info!(
                        tenant = %tenant.tenant_pda,
                        balance,
                        rate = config.rate,
                        "billing: insufficient balance, marking delinquent"
                    );
                    self.dz_rpc_client
                        .update_tenant_payment_status(
                            &tenant.tenant_pda,
                            TenantPaymentStatus::Delinquent,
                        )
                        .await?;
                    self.status_cache
                        .insert(
                            tenant.tenant_pda,
                            TenantPaymentStatus::Delinquent,
                            BILLING_CACHE_TTL,
                        )
                        .await;
                    metrics::counter!("doublezero_sentinel_billing_status_delinquent").increment(1);
                }
                // If balance >= rate, it was a transient error — will retry next cycle
                Ok(())
            }
        }
    }

    /// Attempt to bump the tenant's billing epoch on-chain. On success, caches
    /// `Completed`. On failure, caches/refreshes `Transferred` so the next cycle
    /// retries only this step — never re-submitting the SPL transfer.
    async fn complete_deduction(
        &self,
        tenant_pda: &Pubkey,
        target_epoch: u64,
    ) -> crate::Result<()> {
        match self
            .dz_rpc_client
            .update_tenant_billing_epoch(tenant_pda, target_epoch)
            .await
        {
            Ok(signature) => {
                info!(
                    tenant = %tenant_pda,
                    %signature,
                    target_epoch,
                    "billing: epoch update successful"
                );
                self.deduction_cache
                    .insert(
                        *tenant_pda,
                        DeductionState::Completed(target_epoch),
                        BILLING_CACHE_TTL,
                    )
                    .await;
                self.status_cache
                    .insert(*tenant_pda, TenantPaymentStatus::Paid, BILLING_CACHE_TTL)
                    .await;
                self.clear_pending_transfer(tenant_pda, target_epoch);
                metrics::counter!("doublezero_sentinel_billing_deduction_success").increment(1);
                Ok(())
            }
            Err(err) => {
                warn!(
                    tenant = %tenant_pda,
                    target_epoch,
                    ?err,
                    "billing: epoch update failed; will retry next cycle"
                );
                // Refresh Transferred state to extend TTL — prevents cache
                // expiry from causing a double-transfer during prolonged outages
                self.deduction_cache
                    .insert(
                        *tenant_pda,
                        DeductionState::Transferred(target_epoch),
                        BILLING_CACHE_TTL,
                    )
                    .await;
                Err(err)
            }
        }
    }

    // ── Persistent marker helpers ──────────────────────────────────────

    fn pending_path(&self, tenant_pda: &Pubkey, epoch: u64) -> PathBuf {
        self.pending_dir.join(format!("{tenant_pda}_{epoch}"))
    }

    fn has_pending_transfer(&self, tenant_pda: &Pubkey, epoch: u64) -> bool {
        self.pending_path(tenant_pda, epoch).exists()
    }

    fn mark_transfer_pending(&self, tenant_pda: &Pubkey, epoch: u64) {
        if let Err(e) = std::fs::write(self.pending_path(tenant_pda, epoch), []) {
            warn!(
                tenant = %tenant_pda,
                epoch,
                ?e,
                "billing: failed to persist pending deduction marker"
            );
        }
    }

    fn clear_pending_transfer(&self, tenant_pda: &Pubkey, epoch: u64) {
        let _ = std::fs::remove_file(self.pending_path(tenant_pda, epoch));
    }

    /// Legacy balance-check path for tenants with rate == 0.
    async fn check_and_update_tenant(&self, tenant: &TenantBillingInfo) -> crate::Result<()> {
        let balance = self
            .sol_rpc_client
            .get_token_account_balance(&tenant.token_account)
            .await?;

        let new_status = self.derive_status(balance);

        // Check cache — skip if status hasn't changed
        if let Some(cached_status) = self.status_cache.get(&tenant.tenant_pda).await
            && *cached_status == new_status
        {
            return Ok(());
        }

        let current_onchain = tenant.current_payment_status;
        if current_onchain == new_status {
            // Cache the current status so we don't re-check until TTL
            self.status_cache
                .insert(tenant.tenant_pda, new_status, BILLING_CACHE_TTL)
                .await;
            return Ok(());
        }

        info!(
            tenant = %tenant.tenant_pda,
            old_status = ?current_onchain,
            new_status = ?new_status,
            balance,
            "billing: updating tenant payment status"
        );

        self.dz_rpc_client
            .update_tenant_payment_status(&tenant.tenant_pda, new_status)
            .await?;

        // Update cache
        self.status_cache
            .insert(tenant.tenant_pda, new_status, BILLING_CACHE_TTL)
            .await;

        // Emit status metrics
        match new_status {
            TenantPaymentStatus::Paid => {
                metrics::counter!("doublezero_sentinel_billing_status_paid").increment(1);
            }
            TenantPaymentStatus::Delinquent => {
                metrics::counter!("doublezero_sentinel_billing_status_delinquent").increment(1);
            }
        }

        Ok(())
    }

    fn derive_status(&self, balance: u64) -> TenantPaymentStatus {
        if balance >= self.minimum_balance {
            TenantPaymentStatus::Paid
        } else {
            TenantPaymentStatus::Delinquent
        }
    }
}

#[cfg(test)]
mod tests {
    use mockall::predicate;
    use solana_sdk::signature::Signature;

    use super::*;
    use crate::client::{doublezero_ledger::MockDzRpcClientType, solana::MockSolRpcClientType};

    const TEST_MINT: Pubkey = Pubkey::new_from_array([99; 32]);
    const TEST_JOURNAL_ATA: Pubkey = Pubkey::new_from_array([88; 32]);
    const TEST_DECIMALS: u8 = 8;

    fn make_tenant(pda_byte: u8, token_byte: u8, status: u8) -> TenantBillingInfo {
        make_tenant_with_billing(pda_byte, token_byte, status, TenantBillingConfig::default())
    }

    fn make_tenant_with_billing(
        pda_byte: u8,
        token_byte: u8,
        status: u8,
        billing: TenantBillingConfig,
    ) -> TenantBillingInfo {
        let mut pda_bytes = [0u8; 32];
        pda_bytes[0] = pda_byte;
        let mut token_bytes = [0u8; 32];
        token_bytes[0] = token_byte;
        TenantBillingInfo {
            tenant_pda: Pubkey::new_from_array(pda_bytes),
            token_account: Pubkey::new_from_array(token_bytes),
            current_payment_status: status.into(),
            billing,
        }
    }

    fn billing_config(rate: u64, last_epoch: u64) -> TenantBillingConfig {
        TenantBillingConfig::FlatPerEpoch(FlatPerEpochConfig {
            rate,
            last_deduction_dz_epoch: last_epoch,
        })
    }

    fn test_pending_dir() -> PathBuf {
        std::env::temp_dir().join("sentinel-billing-test")
    }

    async fn new_sentinel(
        dz: MockDzRpcClientType,
        sol: MockSolRpcClientType,
    ) -> BillingSentinel<MockDzRpcClientType, MockSolRpcClientType> {
        new_sentinel_with_dir(dz, sol, test_pending_dir()).await
    }

    async fn new_sentinel_with_dir(
        dz: MockDzRpcClientType,
        sol: MockSolRpcClientType,
        pending_dir: PathBuf,
    ) -> BillingSentinel<MockDzRpcClientType, MockSolRpcClientType> {
        BillingSentinel::new(
            dz,
            sol,
            BillingConfig {
                poll_interval_secs: 60,
                minimum_balance: Some(1000),
                journal_ata: TEST_JOURNAL_ATA,
                mint: TEST_MINT,
                decimals: TEST_DECIMALS,
                pending_dir,
            },
        )
        .await
    }

    // ── Legacy balance-check tests (rate == 0) ──────────────────────────

    #[tokio::test]
    async fn test_derive_status_paid() {
        let dz = MockDzRpcClientType::new();
        let sol = MockSolRpcClientType::new();

        let sentinel = new_sentinel(dz, sol).await;
        assert_eq!(sentinel.derive_status(1000), TenantPaymentStatus::Paid);
        assert_eq!(sentinel.derive_status(9999), TenantPaymentStatus::Paid);
    }

    #[tokio::test]
    async fn test_derive_status_delinquent() {
        let dz = MockDzRpcClientType::new();
        let sol = MockSolRpcClientType::new();

        let sentinel = new_sentinel(dz, sol).await;
        assert_eq!(sentinel.derive_status(999), TenantPaymentStatus::Delinquent);
        assert_eq!(sentinel.derive_status(1), TenantPaymentStatus::Delinquent);
    }

    #[tokio::test]
    async fn test_cache_prevents_redundant_writes() {
        let tenant = make_tenant(1, 2, 0); // current onchain = Unknown
        let tenant_pda = tenant.tenant_pda;
        let token_account = tenant.token_account;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        dz.expect_get_tenants_with_token_accounts()
            .returning(move || Ok(vec![make_tenant(1, 2, 0)]));

        // Balance check happens every call (cache only prevents DZ writes)
        sol.expect_get_token_account_balance()
            .with(predicate::eq(token_account))
            .times(2)
            .returning(|_| Ok(5000));

        // DZ write should only happen once — second call is skipped by cache
        dz.expect_update_tenant_payment_status()
            .with(
                predicate::eq(tenant_pda),
                predicate::eq(TenantPaymentStatus::Paid),
            )
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        let sentinel = new_sentinel(dz, sol).await;

        // First check — should trigger update
        sentinel.check_and_update_tenant(&tenant).await.unwrap();

        // Second check — cache hit, no DZ write (mockall would panic if update called again)
        sentinel.check_and_update_tenant(&tenant).await.unwrap();
    }

    #[tokio::test]
    async fn test_onchain_status_matches_skips_write() {
        // Tenant already has Paid status onchain; balance still high.
        // Should NOT write to DZ Ledger, but should populate cache.
        let tenant = make_tenant(1, 2, 1); // current onchain = Paid
        let token_account = tenant.token_account;

        let dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        sol.expect_get_token_account_balance()
            .with(predicate::eq(token_account))
            .times(1)
            .returning(|_| Ok(5000));

        // No update expected — status already matches
        // (mockall will panic if update_tenant_payment_status is called)

        let sentinel = new_sentinel(dz, sol).await;
        sentinel.check_and_update_tenant(&tenant).await.unwrap();
    }

    #[tokio::test]
    async fn test_paid_to_delinquent_transition() {
        // Tenant is Paid onchain but balance has dropped below threshold
        let tenant = make_tenant(1, 2, 1); // current onchain = Paid
        let tenant_pda = tenant.tenant_pda;
        let token_account = tenant.token_account;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        sol.expect_get_token_account_balance()
            .with(predicate::eq(token_account))
            .times(1)
            .returning(|_| Ok(0));

        dz.expect_update_tenant_payment_status()
            .with(
                predicate::eq(tenant_pda),
                predicate::eq(TenantPaymentStatus::Delinquent),
            )
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        let sentinel = new_sentinel(dz, sol).await;
        sentinel.check_and_update_tenant(&tenant).await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_tenants_tracked_independently() {
        let tenant_a = make_tenant(1, 10, 0); // Delinquent -> should become Paid
        let tenant_b = make_tenant(2, 20, 0); // Delinquent -> should remain Delinquent

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        // Tenant A: high balance
        sol.expect_get_token_account_balance()
            .with(predicate::eq(tenant_a.token_account))
            .returning(|_| Ok(5000));

        // Tenant B: zero balance
        sol.expect_get_token_account_balance()
            .with(predicate::eq(tenant_b.token_account))
            .returning(|_| Ok(0));

        dz.expect_update_tenant_payment_status()
            .with(
                predicate::eq(tenant_a.tenant_pda),
                predicate::eq(TenantPaymentStatus::Paid),
            )
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        // No update expected for tenant_b — already Delinquent and stays Delinquent
        // (mockall will panic if update_tenant_payment_status is called with tenant_b)

        let sentinel = new_sentinel(dz, sol).await;

        sentinel.check_and_update_tenant(&tenant_a).await.unwrap();
        sentinel.check_and_update_tenant(&tenant_b).await.unwrap();
    }

    // ── Deduction path tests (rate > 0) ─────────────────────────────────

    #[tokio::test]
    async fn test_deduction_happy_path() {
        // Tenant with rate > 0, epoch behind current → should deduct
        let tenant = make_tenant_with_billing(1, 2, 1, billing_config(1_000_000, 5));
        let tenant_pda = tenant.tenant_pda;
        let token_account = tenant.token_account;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        // Transfer succeeds
        sol.expect_transfer_spl_token()
            .with(
                predicate::eq(token_account),
                predicate::eq(TEST_JOURNAL_ATA),
                predicate::eq(1_000_000),
                predicate::eq(TEST_MINT),
                predicate::eq(TEST_DECIMALS),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(Signature::new_unique()));

        // Epoch bump (also sets Paid)
        dz.expect_update_tenant_billing_epoch()
            .with(predicate::eq(tenant_pda), predicate::eq(6))
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        let sentinel = new_sentinel(dz, sol).await;
        let config = FlatPerEpochConfig {
            rate: 1_000_000,
            last_deduction_dz_epoch: 5,
        };
        sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();
    }

    #[tokio::test]
    async fn test_deduction_insufficient_balance() {
        // Transfer fails, balance < rate → Delinquent
        let tenant = make_tenant_with_billing(1, 2, 1, billing_config(1_000_000, 5));
        let tenant_pda = tenant.tenant_pda;
        let token_account = tenant.token_account;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        // Transfer fails
        sol.expect_transfer_spl_token()
            .times(1)
            .returning(|_, _, _, _, _| Err(crate::Error::Deserialize("insufficient funds".into())));

        // Balance check reveals insufficient funds
        sol.expect_get_token_account_balance()
            .with(predicate::eq(token_account))
            .times(1)
            .returning(|_| Ok(500_000)); // less than rate

        // Should mark Delinquent
        dz.expect_update_tenant_payment_status()
            .with(
                predicate::eq(tenant_pda),
                predicate::eq(TenantPaymentStatus::Delinquent),
            )
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        let sentinel = new_sentinel(dz, sol).await;
        let config = FlatPerEpochConfig {
            rate: 1_000_000,
            last_deduction_dz_epoch: 5,
        };
        sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();
    }

    #[tokio::test]
    async fn test_deduction_already_current() {
        // last_deduction_dz_epoch >= current_epoch → no calls
        let tenant = make_tenant_with_billing(1, 2, 1, billing_config(1_000_000, 10));

        let dz = MockDzRpcClientType::new();
        let sol = MockSolRpcClientType::new();
        // No expectations — mockall panics if any method is called

        let sentinel = new_sentinel(dz, sol).await;
        let config = FlatPerEpochConfig {
            rate: 1_000_000,
            last_deduction_dz_epoch: 10,
        };
        sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();
    }

    #[tokio::test]
    async fn test_rate_zero_uses_legacy_path() {
        // rate == 0 → poll_cycle routes to check_and_update_tenant, NOT deduct_tenant
        let tenant = make_tenant_with_billing(1, 2, 0, billing_config(0, 0));
        let token_account = tenant.token_account;
        let tenant_pda = tenant.tenant_pda;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        dz.expect_get_current_dz_epoch().returning(|| Ok(10));

        dz.expect_get_tenants_with_token_accounts()
            .returning(move || {
                Ok(vec![make_tenant_with_billing(
                    1,
                    2,
                    0,
                    billing_config(0, 0),
                )])
            });

        // Legacy balance check path
        sol.expect_get_token_account_balance()
            .with(predicate::eq(token_account))
            .times(1)
            .returning(|_| Ok(5000));

        dz.expect_update_tenant_payment_status()
            .with(
                predicate::eq(tenant_pda),
                predicate::eq(TenantPaymentStatus::Paid),
            )
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        // transfer_spl_token should NOT be called (mockall panics if it is)

        let sentinel = new_sentinel(dz, sol).await;
        sentinel.poll_cycle().await.unwrap();
    }

    #[tokio::test]
    async fn test_deduction_cache_prevents_duplicate() {
        // Second call for the same epoch → deduction not re-attempted
        let tenant = make_tenant_with_billing(1, 2, 1, billing_config(1_000_000, 5));
        let tenant_pda = tenant.tenant_pda;
        let token_account = tenant.token_account;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        // Transfer succeeds ONCE
        sol.expect_transfer_spl_token()
            .with(
                predicate::eq(token_account),
                predicate::eq(TEST_JOURNAL_ATA),
                predicate::eq(1_000_000),
                predicate::eq(TEST_MINT),
                predicate::eq(TEST_DECIMALS),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(Signature::new_unique()));

        // Epoch bump called ONCE
        dz.expect_update_tenant_billing_epoch()
            .with(predicate::eq(tenant_pda), predicate::eq(6))
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        let sentinel = new_sentinel(dz, sol).await;
        let config = FlatPerEpochConfig {
            rate: 1_000_000,
            last_deduction_dz_epoch: 5,
        };

        // First call — deducts
        sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();

        // Second call — cache hit, no deduction (mockall panics if transfer called again)
        sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();
    }

    #[tokio::test]
    async fn test_deduction_catches_up_one_epoch() {
        // Tenant is 3 epochs behind (last=5, current=8) → deducts only epoch 6
        let tenant = make_tenant_with_billing(1, 2, 1, billing_config(1_000_000, 5));
        let token_account = tenant.token_account;
        let tenant_pda = tenant.tenant_pda;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        // Should transfer for epoch 6 only (last + 1)
        sol.expect_transfer_spl_token()
            .with(
                predicate::eq(token_account),
                predicate::eq(TEST_JOURNAL_ATA),
                predicate::eq(1_000_000),
                predicate::eq(TEST_MINT),
                predicate::eq(TEST_DECIMALS),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(Signature::new_unique()));

        // Should bump to epoch 6, NOT 7 or 8
        dz.expect_update_tenant_billing_epoch()
            .with(predicate::eq(tenant_pda), predicate::eq(6))
            .times(1)
            .returning(|_, _| Ok(Signature::new_unique()));

        let sentinel = new_sentinel(dz, sol).await;
        let config = FlatPerEpochConfig {
            rate: 1_000_000,
            last_deduction_dz_epoch: 5,
        };
        sentinel.deduct_tenant(&tenant, &config, 8).await.unwrap();
    }

    #[tokio::test]
    async fn test_transfer_not_retried_after_epoch_update_failure() {
        // Transfer succeeds but epoch update fails → second call retries
        // only the epoch update, NOT the transfer (prevents double-charge)
        let tenant = make_tenant_with_billing(1, 2, 1, billing_config(1_000_000, 5));
        let tenant_pda = tenant.tenant_pda;
        let token_account = tenant.token_account;

        let mut dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        // Transfer called exactly ONCE — must not be retried
        sol.expect_transfer_spl_token()
            .with(
                predicate::eq(token_account),
                predicate::eq(TEST_JOURNAL_ATA),
                predicate::eq(1_000_000),
                predicate::eq(TEST_MINT),
                predicate::eq(TEST_DECIMALS),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(Signature::new_unique()));

        // Epoch update: first call fails, second succeeds
        let mut seq = mockall::Sequence::new();

        dz.expect_update_tenant_billing_epoch()
            .with(predicate::eq(tenant_pda), predicate::eq(6))
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Err(crate::Error::Deserialize("network error".into())));

        dz.expect_update_tenant_billing_epoch()
            .with(predicate::eq(tenant_pda), predicate::eq(6))
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Ok(Signature::new_unique()));

        let sentinel = new_sentinel(dz, sol).await;
        let config = FlatPerEpochConfig {
            rate: 1_000_000,
            last_deduction_dz_epoch: 5,
        };

        // First call: transfer OK, epoch update fails → error propagated
        assert!(sentinel.deduct_tenant(&tenant, &config, 10).await.is_err());

        // Second call: cache has Transferred(6) → retries only epoch update → succeeds
        // (mockall would panic if transfer_spl_token were called again)
        sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();
    }

    #[tokio::test]
    async fn test_deduction_transient_error_does_not_set_delinquent() {
        // Transfer fails but balance >= rate → transient error, no status change
        let tenant = make_tenant_with_billing(1, 2, 1, billing_config(1_000_000, 5));
        let token_account = tenant.token_account;

        let dz = MockDzRpcClientType::new();
        let mut sol = MockSolRpcClientType::new();

        // Transfer fails
        sol.expect_transfer_spl_token()
            .times(1)
            .returning(|_, _, _, _, _| Err(crate::Error::Deserialize("timeout".into())));

        // Balance is sufficient — transient error
        sol.expect_get_token_account_balance()
            .with(predicate::eq(token_account))
            .times(1)
            .returning(|_| Ok(5_000_000));

        // update_tenant_payment_status should NOT be called
        // (mockall panics if it is)

        let sentinel = new_sentinel(dz, sol).await;
        let config = FlatPerEpochConfig {
            rate: 1_000_000,
            last_deduction_dz_epoch: 5,
        };
        sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();
    }

    #[tokio::test]
    async fn test_pending_marker_prevents_double_transfer_after_restart() {
        // Simulates: transfer OK → epoch update fails → process restart
        // (fresh sentinel, no cache) → should NOT re-transfer
        let dir = std::env::temp_dir().join("sentinel-test-restart");

        // Use unique tenant bytes (3, 4) to avoid collision with other tests
        let tenant = make_tenant_with_billing(3, 4, 1, billing_config(1_000_000, 5));
        let tenant_pda = tenant.tenant_pda;
        let token_account = tenant.token_account;

        // --- First "run": transfer OK, epoch update fails ---
        {
            let mut dz = MockDzRpcClientType::new();
            let mut sol = MockSolRpcClientType::new();

            sol.expect_transfer_spl_token()
                .with(
                    predicate::eq(token_account),
                    predicate::eq(TEST_JOURNAL_ATA),
                    predicate::eq(1_000_000),
                    predicate::eq(TEST_MINT),
                    predicate::eq(TEST_DECIMALS),
                )
                .times(1)
                .returning(|_, _, _, _, _| Ok(Signature::new_unique()));

            dz.expect_update_tenant_billing_epoch()
                .with(predicate::eq(tenant_pda), predicate::eq(6))
                .times(1)
                .returning(|_, _| Err(crate::Error::Deserialize("network error".into())));

            let sentinel = new_sentinel_with_dir(dz, sol, dir.clone()).await;
            let config = FlatPerEpochConfig {
                rate: 1_000_000,
                last_deduction_dz_epoch: 5,
            };
            assert!(sentinel.deduct_tenant(&tenant, &config, 10).await.is_err());
        }
        // sentinel dropped — cache gone, but marker file persists

        // --- Second "run" (simulating restart): fresh sentinel, same dir ---
        {
            let mut dz = MockDzRpcClientType::new();
            let sol = MockSolRpcClientType::new();
            // NO transfer expected — mockall panics if transfer_spl_token is called

            dz.expect_update_tenant_billing_epoch()
                .with(predicate::eq(tenant_pda), predicate::eq(6))
                .times(1)
                .returning(|_, _| Ok(Signature::new_unique()));

            let sentinel = new_sentinel_with_dir(dz, sol, dir.clone()).await;
            let config = FlatPerEpochConfig {
                rate: 1_000_000,
                last_deduction_dz_epoch: 5,
            };
            sentinel.deduct_tenant(&tenant, &config, 10).await.unwrap();
        }

        // Verify marker was cleaned up after successful epoch update
        let marker = dir.join(format!("{tenant_pda}_6"));
        assert!(
            !marker.exists(),
            "marker file should be removed after success"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
