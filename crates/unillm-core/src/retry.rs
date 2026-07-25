//! Retry policy (`DESIGN.md` §15.2).
//!
//! Retries transport faults and retriable upstream statuses (5xx, 429); never retries client
//! errors (4xx except 429), auth, validation, or not-found. Uses exponential backoff without
//! jitter (no `rand` dependency); the proxy/SDK may layer jitter on top.

use std::time::Duration;

use crate::error::CoreError;

/// How aggressively to retry a request (`DESIGN.md` §15.2).
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            base_delay: Duration::from_millis(500),
        }
    }
}

impl RetryPolicy {
    /// No retries — fail on the first error.
    pub const fn none() -> Self {
        Self {
            max_retries: 0,
            base_delay: Duration::ZERO,
        }
    }

    /// Whether `err` is worth retrying (`DESIGN.md` §15.2).
    pub fn should_retry(&self, err: &CoreError) -> bool {
        match err {
            CoreError::Io { .. } | CoreError::Stream { .. } => true,
            CoreError::ProviderError { status, .. } => *status >= 500 || *status == 429,
            _ => false,
        }
    }

    /// Backoff before the `attempt`-th retry (0-indexed): `base_delay * 2^attempt`.
    pub fn delay(&self, attempt: u32) -> Duration {
        self.base_delay * 2u32.saturating_pow(attempt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreError;

    #[test]
    fn retries_io_stream_5xx_429() {
        let p = RetryPolicy::default();
        assert!(p.should_retry(&CoreError::Io {
            message: "x".into()
        }));
        assert!(p.should_retry(&CoreError::Stream {
            message: "x".into()
        }));
        assert!(p.should_retry(&CoreError::ProviderError {
            status: 500,
            message: "x".into(),
            raw: None
        }));
        assert!(p.should_retry(&CoreError::ProviderError {
            status: 429,
            message: "x".into(),
            raw: None
        }));
    }

    #[test]
    fn does_not_retry_client_errors() {
        let p = RetryPolicy::default();
        assert!(!p.should_retry(&CoreError::InvalidRequest {
            message: "x".into()
        }));
        assert!(!p.should_retry(&CoreError::Unauthorized {
            message: "x".into()
        }));
        assert!(!p.should_retry(&CoreError::NotFound {
            message: "x".into()
        }));
        assert!(!p.should_retry(&CoreError::ProviderError {
            status: 400,
            message: "x".into(),
            raw: None
        }));
    }

    #[test]
    fn backoff_grows() {
        let p = RetryPolicy::default();
        assert_eq!(p.delay(0), Duration::from_millis(500));
        assert_eq!(p.delay(1), Duration::from_millis(1000));
        assert_eq!(p.delay(2), Duration::from_millis(2000));
    }
}
