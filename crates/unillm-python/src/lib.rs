//! Native (PyO3) bindings for the `unillm` Python SDK.
//!
//! Layers over [`unillm_core`]: a `Client` PyO3 class wraps the Rust client. Type crossing happens
//! at the JSON boundary — the pure-Python facade (de)serializes `Request`/`Response` — so the IR
//! types are not mirrored here. The tokio runtime is provided by `pyo3_async_runtimes`.

use std::sync::Arc;
use std::time::Duration;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use unillm_core::{Client as CoreClient, CoreError, ProviderConfig, ProviderId, Request};

create_exception!(_native, UnillmError, PyException);

/// Map a `CoreError` onto the `UnillmError` Python exception (kind + message).
fn map_core_err(e: CoreError) -> PyErr {
    UnillmError::new_err(format!("{}: {e}", core_kind(&e)))
}

fn core_kind(e: &CoreError) -> &'static str {
    match e {
        CoreError::InvalidRequest { .. } => "invalid_request",
        CoreError::Unauthorized { .. } => "unauthorized",
        CoreError::NotFound { .. } => "not_found",
        CoreError::RateLimited { .. } => "rate_limited",
        CoreError::ProviderError { .. } => "provider_error",
        CoreError::Io { .. } => "io",
        CoreError::Stream { .. } => "stream",
        CoreError::Serde { .. } => "serde",
        CoreError::Other { .. } => "other",
    }
}

fn parse_provider(s: &str) -> Result<ProviderId, PyErr> {
    match s.to_ascii_lowercase().as_str() {
        "openai" => Ok(ProviderId::Openai),
        "anthropic" => Ok(ProviderId::Anthropic),
        "openrouter" => Ok(ProviderId::Openrouter),
        "deepseek" => Ok(ProviderId::Deepseek),
        other => Err(UnillmError::new_err(format!("unknown provider: {other}"))),
    }
}

/// A direct-to-provider client. Wraps the Rust [`unillm_core::Client`].
#[pyclass(name = "Client")]
struct Client {
    core: Arc<CoreClient>,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (provider, api_key, base_url=None, timeout=None))]
    fn new(
        provider: &str,
        api_key: &str,
        base_url: Option<String>,
        timeout: Option<f64>,
    ) -> PyResult<Self> {
        let pid = parse_provider(provider)?;
        let mut cfg = ProviderConfig::new(pid, api_key);
        if let Some(base) = base_url {
            cfg.base_url = base;
        }
        if let Some(secs) = timeout {
            cfg.request_timeout = Some(Duration::from_secs_f64(secs));
        }
        let core = CoreClient::new(cfg).map_err(map_core_err)?;
        Ok(Self {
            core: Arc::new(core),
        })
    }

    /// Non-streaming request. `request_json` is a canonical `Request` as JSON; returns a canonical
    /// `Response` as JSON. Awaitable.
    fn create<'py>(&self, py: Python<'py>, request_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let req: Request = serde_json::from_str(request_json)
            .map_err(|e| UnillmError::new_err(format!("invalid request json: {e}")))?;
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let resp = core.create(&req).await.map_err(map_core_err)?;
            serde_json::to_string(&resp)
                .map_err(|e| UnillmError::new_err(format!("encode error: {e}")))
        })
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Client>()?;
    m.add("UnillmError", m.py().get_type::<UnillmError>())?;
    Ok(())
}
