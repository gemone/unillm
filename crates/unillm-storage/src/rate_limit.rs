//! Rate limiting & concurrency (`DESIGN.md` §12). In-memory now; Redis is the production primary
//! (trait-pluggable, wired later). Per-key limits come from `virtual_keys`.
//!
//! Token-based limits (TPM, daily budget) are checked pre-call against an estimate and accumulated
//! post-call from actual usage — a best-effort model appropriate to an in-memory, per-instance
//! limiter (§12.4 fail-open philosophy). A shared Redis backend would use atomic check-and-increment.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use uuid::Uuid;

use crate::model::VirtualKey;

const MINUTE: Duration = Duration::from_secs(60);
const DAY: Duration = Duration::from_secs(86_400);

/// Per-key limits (`virtual_keys`). `None` = unlimited on that dimension.
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyLimits {
    pub rpm: Option<u32>,
    pub tpm: Option<u64>,
    pub budget_daily_tokens: Option<i64>,
    pub max_concurrency: Option<u32>,
}

impl KeyLimits {
    /// Project a key's limit columns into the limiter's view.
    pub fn from_key(key: &VirtualKey) -> Self {
        Self {
            rpm: key.rpm.map(|v| v as u32),
            tpm: key.tpm.map(|v| v as u64),
            budget_daily_tokens: key.budget_daily_tokens,
            max_concurrency: key.max_concurrency.map(|v| v as u32),
        }
    }

    /// `true` if no limit is configured (the limiter is a no-op for this key).
    pub fn is_unlimited(&self) -> bool {
        self.rpm.is_none()
            && self.tpm.is_none()
            && self.budget_daily_tokens.is_none()
            && self.max_concurrency.is_none()
    }
}

/// Pre-call token estimate (`DESIGN.md` §12.1: prompt tokens + max output).
#[derive(Debug, Clone, Copy)]
pub struct TokenEstimate {
    pub prompt: u64,
    pub max_output: u64,
}

/// Actual usage from the response (for post-call reconciliation). `None` when unknown (e.g. a stream
/// that didn't reach `Completed`).
#[derive(Debug, Clone, Copy, Default)]
pub struct TokenActual {
    pub input: u64,
    pub output: u64,
}

impl TokenActual {
    pub fn total(&self) -> u64 {
        self.input + self.output
    }
}

/// `X-Unillm-RateLimit-*` header values (`DESIGN.md` §12.3).
#[derive(Debug, Clone, Copy, Default)]
pub struct RateHeaders {
    pub limit: u64,
    pub remaining: u64,
    pub reset_seconds: u64,
}

/// Why a request was denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenyReason {
    /// Requests-per-minute exceeded.
    Rpm,
    /// Tokens-per-minute exceeded.
    Tpm,
    /// Daily token budget exceeded.
    Budget,
    /// Too many concurrent in-flight requests.
    Concurrency,
}

/// A limiter decision. On `Allow`, a concurrency slot is held until `RateLimiter::release`.
#[derive(Debug, Clone)]
pub enum RateDecision {
    Allow(RateHeaders),
    Deny {
        reason: DenyReason,
        retry_after: Duration,
        headers: RateHeaders,
    },
}

/// Pluggable rate limiter (`DESIGN.md` §11.1, §12).
#[async_trait]
pub trait RateLimiter: Send + Sync {
    /// Try to admit a request under the key's limits. On `Allow`, a concurrency slot (if limited)
    /// is held until `release`.
    async fn acquire(
        &self,
        key_id: Uuid,
        limits: &KeyLimits,
        estimate: TokenEstimate,
    ) -> RateDecision;

    /// Release the held concurrency slot; reconcile token usage when `actual` is known.
    async fn release(&self, key_id: Uuid, limits: &KeyLimits, actual: Option<TokenActual>);
}

// -------------------------------------------------------------------------------------------------
// In-memory implementation
// -------------------------------------------------------------------------------------------------

struct KeyState {
    /// Request timestamps within the rolling minute (RPM).
    rpm_window: VecDeque<Instant>,
    /// (minute-start, actual tokens accumulated this minute) — TPM.
    tpm_window: (Instant, u64),
    /// (day-start, actual tokens accumulated today) — daily budget.
    budget: (Instant, i64),
    /// In-flight request count (concurrency).
    in_flight: u32,
}

impl Default for KeyState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            rpm_window: VecDeque::new(),
            tpm_window: (now, 0),
            budget: (now, 0),
            in_flight: 0,
        }
    }
}

/// A per-instance, in-memory rate limiter (`DESIGN.md` §11.2 dev/fallback backend).
#[derive(Default)]
pub struct InMemoryRateLimiter {
    keys: Mutex<HashMap<Uuid, KeyState>>,
}

impl InMemoryRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RateLimiter for InMemoryRateLimiter {
    async fn acquire(
        &self,
        key_id: Uuid,
        limits: &KeyLimits,
        estimate: TokenEstimate,
    ) -> RateDecision {
        let now = Instant::now();
        let est_total = estimate.prompt + estimate.max_output;
        let mut keys = self.keys.lock().unwrap();
        let st = keys.entry(key_id).or_default();

        // Roll windows forward.
        while let Some(t) = st.rpm_window.front() {
            if now.duration_since(*t) >= MINUTE {
                st.rpm_window.pop_front();
            } else {
                break;
            }
        }
        if now.duration_since(st.tpm_window.0) >= MINUTE {
            st.tpm_window = (now, 0);
        }
        if now.duration_since(st.budget.0) >= DAY {
            st.budget = (now, 0);
        }

        let deny = |reason, reset: Duration, limit: u64| RateDecision::Deny {
            reason,
            retry_after: reset,
            headers: RateHeaders {
                limit,
                remaining: 0,
                reset_seconds: reset.as_secs(),
            },
        };

        if let Some(max) = limits.max_concurrency
            && st.in_flight >= max
        {
            return deny(DenyReason::Concurrency, Duration::ZERO, max as u64);
        }
        if let Some(rpm) = limits.rpm
            && st.rpm_window.len() >= rpm as usize
        {
            let reset = MINUTE.saturating_sub(now.duration_since(st.rpm_window[0]));
            return deny(DenyReason::Rpm, reset, rpm as u64);
        }
        if let Some(tpm) = limits.tpm
            && st.tpm_window.1.saturating_add(est_total) > tpm
        {
            let reset = MINUTE.saturating_sub(now.duration_since(st.tpm_window.0));
            return deny(DenyReason::Tpm, reset, tpm);
        }
        if let Some(budget) = limits.budget_daily_tokens
            && st.budget.1.saturating_add(est_total as i64) > budget
        {
            let reset = DAY.saturating_sub(now.duration_since(st.budget.0));
            return deny(DenyReason::Budget, reset, budget as u64);
        }

        // Admit: count the request (RPM) and hold a concurrency slot. Token windows accumulate on
        // `release` from actual usage, so they are not adjusted here.
        st.rpm_window.push_back(now);
        st.in_flight += 1;

        let headers = if let Some(rpm) = limits.rpm {
            RateHeaders {
                limit: rpm as u64,
                remaining: (rpm as usize).saturating_sub(st.rpm_window.len()) as u64,
                reset_seconds: MINUTE.as_secs(),
            }
        } else if let Some(max) = limits.max_concurrency {
            RateHeaders {
                limit: max as u64,
                remaining: max.saturating_sub(st.in_flight) as u64,
                reset_seconds: 0,
            }
        } else {
            RateHeaders::default()
        };
        RateDecision::Allow(headers)
    }

    async fn release(&self, key_id: Uuid, _limits: &KeyLimits, actual: Option<TokenActual>) {
        let mut keys = self.keys.lock().unwrap();
        let Some(st) = keys.get_mut(&key_id) else {
            return;
        };
        if st.in_flight > 0 {
            st.in_flight -= 1;
        }
        if let Some(a) = actual {
            st.tpm_window.1 = st.tpm_window.1.saturating_add(a.total());
            st.budget.1 = st.budget.1.saturating_add(a.total() as i64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lim(rpm: Option<u32>, conc: Option<u32>, budget: Option<i64>) -> KeyLimits {
        KeyLimits {
            rpm,
            tpm: None,
            budget_daily_tokens: budget,
            max_concurrency: conc,
        }
    }

    fn est(n: u64) -> TokenEstimate {
        TokenEstimate {
            prompt: n,
            max_output: 0,
        }
    }

    #[tokio::test]
    async fn rpm_denies_third_in_minute() {
        let rl = InMemoryRateLimiter::new();
        let k = Uuid::new_v4();
        let lim = lim(Some(2), None, None);
        assert!(matches!(
            rl.acquire(k, &lim, est(0)).await,
            RateDecision::Allow(_)
        ));
        assert!(matches!(
            rl.acquire(k, &lim, est(0)).await,
            RateDecision::Allow(_)
        ));
        match rl.acquire(k, &lim, est(0)).await {
            RateDecision::Deny {
                reason,
                retry_after,
                ..
            } => {
                assert_eq!(reason, DenyReason::Rpm);
                assert!(retry_after <= MINUTE && retry_after > Duration::ZERO);
            }
            _ => panic!("expected Deny"),
        }
    }

    #[tokio::test]
    async fn concurrency_releases_on_release() {
        let rl = InMemoryRateLimiter::new();
        let k = Uuid::new_v4();
        let lim = lim(None, Some(1), None);
        assert!(matches!(
            rl.acquire(k, &lim, est(0)).await,
            RateDecision::Allow(_)
        ));
        // Slot held → second is denied.
        assert!(matches!(
            rl.acquire(k, &lim, est(0)).await,
            RateDecision::Deny {
                reason: DenyReason::Concurrency,
                ..
            }
        ));
        rl.release(k, &lim, None).await;
        // Slot freed → next is allowed.
        assert!(matches!(
            rl.acquire(k, &lim, est(0)).await,
            RateDecision::Allow(_)
        ));
    }

    #[tokio::test]
    async fn budget_denies_when_estimated_total_exceeds() {
        let rl = InMemoryRateLimiter::new();
        let k = Uuid::new_v4();
        let lim = lim(None, None, Some(100));
        // First request: estimate 60 fits (used 0 + 60 ≤ 100) — but acquire only checks; it does not
        // accumulate. So a second estimate-60 also "fits" until release records actual. After a real
        // release of 60, the next estimate-60 is denied (60 + 60 > 100).
        assert!(matches!(
            rl.acquire(k, &lim, est(60)).await,
            RateDecision::Allow(_)
        ));
        rl.release(
            k,
            &lim,
            Some(TokenActual {
                input: 60,
                output: 0,
            }),
        )
        .await;
        assert!(matches!(
            rl.acquire(k, &lim, est(60)).await,
            RateDecision::Deny {
                reason: DenyReason::Budget,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn unlimited_key_is_always_allowed() {
        let rl = InMemoryRateLimiter::new();
        let k = Uuid::new_v4();
        let lim = KeyLimits::default();
        assert!(lim.is_unlimited());
        for _ in 0..50 {
            assert!(matches!(
                rl.acquire(k, &lim, est(9999)).await,
                RateDecision::Allow(_)
            ));
        }
    }
}
