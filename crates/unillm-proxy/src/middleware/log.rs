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
    /// Build the storage record for this request at `status`, with measured latency.
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
/// stream (client disconnect) is not logged — best-effort, like the rate-limit slot release.
pub struct StreamLogger {
    logs: Arc<dyn LogStore>,
    models: Arc<dyn ModelStore>,
    ctx: LogContext,
    usage: Option<NewUsage>,
}

impl StreamLogger {
    pub fn new(logs: Arc<dyn LogStore>, models: Arc<dyn ModelStore>, ctx: LogContext) -> Self {
        Self {
            logs,
            models,
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

    /// Fire-and-forget the accumulated log (called at stream completion).
    pub fn finish(self) {
        spawn_log(
            self.logs,
            self.models,
            self.ctx.new_request_log(200),
            self.usage,
        );
    }
}
