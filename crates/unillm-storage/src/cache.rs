//! Exact-hash response cache (`DESIGN.md` §7.4, §11.1 `cache`).
//!
//! In-memory now (TTL); Redis and the `response_cache` table (`DESIGN.md` §11.2/§11.3) are the
//! production primaries behind this same trait. The cache holds **opaque bytes** — a serialized
//! canonical `Response` — keyed by `(scope, key_hash)`. The proxy derives the key (canonical request
//! minus `metadata` + virtual-key scope) and (de)serializes the value, so this crate stays free of
//! the core IR. §16 is honored structurally: nothing but the caller-supplied bytes is stored, and the
//! caller stores the response only (no prompt/request body).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;

/// Pluggable exact-hash response cache (`DESIGN.md` §11.1 `cache`, §7.4).
#[async_trait]
pub trait CacheStore: Send + Sync {
    /// Look up a cached value by `(scope, key_hash)`. Returns the stored bytes if present and
    /// unexpired; `None` otherwise (implementations evict stale entries lazily on read).
    async fn get(&self, scope: &str, key_hash: &str) -> Option<Vec<u8>>;

    /// Store `value` under `(scope, key_hash)` with a TTL, overwriting any prior entry.
    async fn put(&self, scope: &str, key_hash: &str, value: Vec<u8>, ttl: Duration);

    /// Invalidate entries: both `None` flushes everything; `scope` only flushes that scope;
    /// `key_hash` only flushes that hash across all scopes; both flush the single matching entry.
    /// Returns the count removed (`DESIGN.md` §7.4 invalidation, §10.6 `POST /admin/cache/invalidate`).
    async fn invalidate(&self, scope: Option<&str>, key_hash: Option<&str>) -> u64;
}

// --- in-memory implementation ---------------------------------------------------

struct Entry {
    value: Vec<u8>,
    expires_at: Instant,
}

/// A per-instance, in-memory exact-hash cache with TTL eviction (`DESIGN.md` §11.2 dev/fallback
/// backend). Entries expire lazily on read; `put` also runs an amortized sweep (every `SWEEP_PERIOD`
/// inserts) so entries that are written but never re-read are still reaped — without it the map would
/// grow without bound on a long-running proxy. Not shared across instances (documented): the
/// Redis/DB backends serve HA; a size-bounded LRU is the production upgrade path.
#[derive(Default)]
pub struct InMemoryCache {
    entries: Mutex<HashMap<(String, String), Entry>>,
}

/// Run a full expired-entry sweep every this many `put`s (amortizes the cost; bounds stale entries).
const SWEEP_PERIOD: usize = 256;

impl InMemoryCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Remove every expired entry. Called on a schedule from `put` to bound growth.
    fn sweep(entries: &mut HashMap<(String, String), Entry>) {
        let now = Instant::now();
        entries.retain(|_, e| e.expires_at > now);
    }
}

#[async_trait]
impl CacheStore for InMemoryCache {
    async fn get(&self, scope: &str, key_hash: &str) -> Option<Vec<u8>> {
        let mut entries = self.entries.lock().unwrap();
        let key = (scope.to_string(), key_hash.to_string());
        let expired = matches!(entries.get(&key), Some(e) if e.expires_at <= Instant::now());
        if expired {
            entries.remove(&key);
            return None;
        }
        entries.get(&key).map(|e| e.value.clone())
    }

    async fn put(&self, scope: &str, key_hash: &str, value: Vec<u8>, ttl: Duration) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() % SWEEP_PERIOD == 0 {
            Self::sweep(&mut entries);
        }
        entries.insert(
            (scope.to_string(), key_hash.to_string()),
            Entry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
    }

    async fn invalidate(&self, scope: Option<&str>, key_hash: Option<&str>) -> u64 {
        let mut entries = self.entries.lock().unwrap();
        let before = entries.len();
        entries.retain(|(s, h), _| {
            let matches = scope.is_none_or(|sc| sc == s.as_str())
                && key_hash.is_none_or(|kh| kh == h.as_str());
            !matches
        });
        (before - entries.len()) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_round_trip() {
        let cache = InMemoryCache::new();
        assert!(cache.get("k1", "h1").await.is_none());
        cache
            .put("k1", "h1", b"hello".to_vec(), Duration::from_secs(60))
            .await;
        assert_eq!(
            cache.get("k1", "h1").await.as_deref(),
            Some(b"hello".as_slice())
        );
        // Different scope or hash → different entry (no cross-key leakage).
        assert!(cache.get("k1", "h2").await.is_none());
        assert!(cache.get("k2", "h1").await.is_none());
    }

    #[tokio::test]
    async fn expired_entry_is_evicted_on_read() {
        let cache = InMemoryCache::new();
        cache
            .put("k", "h", b"v".to_vec(), Duration::from_millis(1))
            .await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(cache.get("k", "h").await.is_none());
    }

    #[tokio::test]
    async fn invalidate_variants() {
        let cache = InMemoryCache::new();
        cache
            .put("a", "h1", b"1".to_vec(), Duration::from_secs(60))
            .await;
        cache
            .put("a", "h2", b"2".to_vec(), Duration::from_secs(60))
            .await;
        cache
            .put("b", "h1", b"3".to_vec(), Duration::from_secs(60))
            .await;

        // Single entry by (scope, hash).
        assert_eq!(cache.invalidate(Some("a"), Some("h1")).await, 1);
        assert_eq!(cache.invalidate(Some("a"), Some("h1")).await, 0); // already gone

        // Whole scope.
        assert_eq!(cache.invalidate(Some("a"), None).await, 1); // only a/h2 remains under a
        assert!(cache.get("a", "h2").await.is_none());
        assert!(cache.get("b", "h1").await.is_some());

        // One hash across all scopes.
        assert_eq!(cache.invalidate(None, Some("h1")).await, 1);

        // Flush all.
        cache
            .put("c", "h9", b"z".to_vec(), Duration::from_secs(60))
            .await;
        assert_eq!(cache.invalidate(None, None).await, 1);
        assert!(cache.get("c", "h9").await.is_none());
    }

    #[tokio::test]
    async fn put_sweeps_expired_entries() {
        // An entry that is written but never re-read must still be reaped by the amortized sweep —
        // otherwise the map grows without bound. We observe the sweep via `invalidate`'s count.
        let cache = InMemoryCache::new();
        cache
            .put("s", "expired", b"old".to_vec(), Duration::from_millis(1))
            .await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Add SWEEP_PERIOD live entries; the 256th lands on len % SWEEP_PERIOD == 0 → sweep.
        for i in 0..SWEEP_PERIOD {
            cache
                .put(
                    "s",
                    &format!("live{i}"),
                    b"v".to_vec(),
                    Duration::from_secs(60),
                )
                .await;
        }
        // Only the SWEEP_PERIOD live entries remain — the expired one was swept (not just lazily
        // evicted on a get, which never happened for it).
        assert_eq!(cache.invalidate(None, None).await, SWEEP_PERIOD as u64);
    }
}
