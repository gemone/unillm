//! Request/usage logging (`DESIGN.md` §10.3 step 9, §16 PII hygiene).
//!
//! Writes are **fire-and-forget**: the data plane spawns the insert and never blocks or fails a
//! request on a logging error. Per §16, only metadata + token sizes are recorded — request and
//! response bodies are never stored, and this is enforced structurally ([`NewRequestLog`] has no
//! body field). Cost is computed best-effort from the model catalog when the caller does not supply
//! one.

use std::sync::Arc;
use std::time::Instant;

use uuid::Uuid;

use unillm_core::Usage;
use unillm_storage::{LogStore, ModelStore, NewRequestLog, NewUsage};

use crate::inbound::Format;
use crate::metrics::{CacheOutcome, Metrics};

/// What the data plane knows about a request once it has authenticated, rate-limited, and resolved
/// a route — enough to write a request log without ever touching the body. `started` carries the
/// request's start instant for latency.
#[derive(Clone)]
pub struct LogContext {
    pub request_id: String,
    pub virtual_key_id: Uuid,
    pub tenant_id: Uuid,
    pub provider: String,
    pub model: String,
    pub inbound_format: String,
    pub outbound_format: String,
    pub started: Instant,
}

impl LogContext {
    /// Build a context for a request once it has authenticated and knows its `(provider, model)`:
    /// a fresh request id, the key/tenant, the inbound/outbound wire names, and the start instant.
    /// Used by both the upstream path (provider/model from the resolved route) and the cache-hit
    /// path (from the cached response) so the field set lives in one place.
    pub fn for_request(
        virtual_key_id: Uuid,
        tenant_id: Uuid,
        provider: impl Into<String>,
        model: impl Into<String>,
        inbound: Format,
        outbound: Format,
        started: Instant,
    ) -> Self {
        Self {
            request_id: Uuid::new_v4().to_string(),
            virtual_key_id,
            tenant_id,
            provider: provider.into(),
            model: model.into(),
            inbound_format: inbound.wire_name().to_string(),
            outbound_format: outbound.wire_name().to_string(),
            started,
        }
    }

    /// Build the storage record for this request at `status`, with measured latency. `cached` is
    /// `false` — cache hits build their record separately (the provider/model come from the cached
    /// response, not the resolved route).
    pub fn new_request_log(&self, status: i16) -> NewRequestLog {
        NewRequestLog {
            request_id: self.request_id.clone(),
            virtual_key_id: self.virtual_key_id,
            tenant_id: self.tenant_id,
            provider: self.provider.clone(),
            model: self.model.clone(),
            inbound_format: self.inbound_format.clone(),
            outbound_format: self.outbound_format.clone(),
            status,
            cached: false,
            latency_ms: Some(latency(self.started)),
        }
    }
}

/// Elapsed milliseconds since `started`, clamped to `i32` (`DESIGN.md` §11.3 `latency_ms`).
fn latency(started: Instant) -> i32 {
    started.elapsed().as_millis().min(i32::MAX as u128) as i32
}

/// Map a canonical [`Usage`] to the storage usage record.
pub fn usage_from(u: &Usage) -> NewUsage {
    NewUsage {
        input_tokens: u.input_tokens as i64,
        output_tokens: u.output_tokens as i64,
        cache_read: u.cache_read as i64,
        cache_creation: u.cache_creation as i64,
        cost_usd: u.cost_usd,
    }
}

/// Best-effort USD cost from the catalog price (per 1M tokens, `DESIGN.md` §13.5). `None` if the
/// model is unpriced or the lookup fails — logging never fails the request.
async fn compute_cost(
    models: &dyn ModelStore,
    provider: &str,
    model: &str,
    u: &NewUsage,
) -> Option<f64> {
    let m = models.get_model(provider, model).await.ok()??;
    let price_in = m.price_in?;
    let price_out = m.price_out?;
    let price_cache_read = m.price_cache_read.unwrap_or(0.0);
    Some(
        (u.input_tokens as f64 / 1_000_000.0) * price_in
            + (u.output_tokens as f64 / 1_000_000.0) * price_out
            + (u.cache_read as f64 / 1_000_000.0) * price_cache_read,
    )
}

/// Fire-and-forget a request log + optional usage write. Computes cost from the catalog when the
/// caller did not supply one (`DESIGN.md` §10.3 step 9: non-blocking).
pub fn spawn_log(
    logs: Arc<dyn LogStore>,
    models: Arc<dyn ModelStore>,
    log: NewRequestLog,
    mut usage: Option<NewUsage>,
) {
    tokio::spawn(async move {
        if let Some(u) = usage.as_mut()
            && u.cost_usd.is_none()
        {
            u.cost_usd = compute_cost(&*models, &log.provider, &log.model, u).await;
        }
        if let Err(e) = logs.insert_request_log(log, usage).await {
            eprintln!("WARNING: request log write failed: {e}");
        }
    });
}

/// Streaming log collector: observes events as they flow to capture the terminal usage, then writes
/// one request log (status 200, since the stream committed) when the stream completes. A dropped
/// stream (client disconnect) is not logged — best-effort, like the rate-limit slot release. The
/// metrics registry is also updated at completion so stream token totals reach Prometheus.
pub struct StreamLogger {
    logs: Arc<dyn LogStore>,
    models: Arc<dyn ModelStore>,
    metrics: Arc<Metrics>,
    ctx: LogContext,
    usage: Option<NewUsage>,
}

impl StreamLogger {
    pub fn new(
        logs: Arc<dyn LogStore>,
        models: Arc<dyn ModelStore>,
        metrics: Arc<Metrics>,
        ctx: LogContext,
    ) -> Self {
        Self {
            logs,
            models,
            metrics,
            ctx,
            usage: None,
        }
    }

    /// Record usage from a `Completed` event if one flows.
    pub fn observe(&mut self, ev: &unillm_core::stream::StreamEvent) {
        if let unillm_core::stream::StreamEvent::Completed { response } = ev {
            self.usage = Some(usage_from(&response.usage));
        }
    }

    /// Fire-and-forget the accumulated log + metrics (called at stream completion).
    pub fn finish(self) {
        let latency = self.ctx.started.elapsed();
        self.metrics.record(
            &self.ctx.provider,
            &self.ctx.model,
            200,
            CacheOutcome::None,
            latency,
            self.usage.as_ref(),
        );
        spawn_log(
            self.logs,
            self.models,
            self.ctx.new_request_log(200),
            self.usage,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inbound::Format;

    #[test]
    fn usage_from_maps_all_fields() {
        let u = Usage {
            input_tokens: 10,
            output_tokens: 5,
            cache_read: 3,
            cache_creation: 1,
            cost_usd: Some(0.1),
        };
        let n = usage_from(&u);
        assert_eq!(n.input_tokens, 10);
        assert_eq!(n.output_tokens, 5);
        assert_eq!(n.cache_read, 3);
        assert_eq!(n.cache_creation, 1);
        assert_eq!(n.cost_usd, Some(0.1));
    }

    #[test]
    fn log_context_request_log_shape() {
        let ctx = LogContext::for_request(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "openai",
            "gpt-4o",
            Format::OpenaiChat,
            Format::OpenaiChat,
            Instant::now(),
        );
        let rec = ctx.new_request_log(200);
        assert_eq!(rec.provider, "openai");
        assert_eq!(rec.model, "gpt-4o");
        assert_eq!(rec.inbound_format, "openai_chat");
        assert_eq!(rec.outbound_format, "openai_chat");
        assert_eq!(rec.status, 200);
        assert!(!rec.cached);
        assert!(rec.latency_ms.is_some());
        assert!(!rec.request_id.is_empty());
    }
}
