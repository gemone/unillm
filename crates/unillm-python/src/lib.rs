//! Native (PyO3) bindings for the `unillm` Python SDK.
//!
//! Layers over [`unillm_core`]: a `Client` PyO3 class wraps the Rust client, and an `EventStream`
//! exposes streaming as a Python async iterator. Type crossing happens at the JSON boundary — the
//! pure-Python facade (de)serializes `Request`/`Response`/`StreamEvent` — so the IR types are not
//! mirrored here. The tokio runtime is provided by `pyo3_async_runtimes`.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyStopAsyncIteration};
use pyo3::prelude::*;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

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

fn parse_request(request_json: &str) -> Result<Request, PyErr> {
    serde_json::from_str(request_json)
        .map_err(|e| UnillmError::new_err(format!("invalid request json: {e}")))
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
        let req = parse_request(request_json)?;
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let resp = core.create(&req).await.map_err(map_core_err)?;
            serde_json::to_string(&resp)
                .map_err(|e| UnillmError::new_err(format!("encode error: {e}")))
        })
    }

    /// Streaming request. Returns an [`EventStream`] (async iterator of `StreamEvent` JSON).
    fn stream(&self, request_json: &str) -> PyResult<EventStream> {
        let req = parse_request(request_json)?;
        Ok(EventStream::new(self.core.clone(), req))
    }
}

/// A running producer: the drain task plus the channel receiver.
struct Running {
    task: JoinHandle<()>,
    rx: mpsc::Receiver<Result<String, PyErr>>,
}

/// A Python async iterator over canonical stream events (`DESIGN.md` §6.6).
///
/// A background tokio task drains the upstream `BoxStream` into a bounded channel (cap 64), giving
/// backpressure. The producer starts lazily on the first `__anext__`. Dropping the Python object
/// aborts the task, which drops the upstream response and closes the connection (no leak).
#[pyclass(name = "EventStream")]
struct EventStream {
    core: Arc<CoreClient>,
    req: Request,
    running: Arc<Mutex<Option<Running>>>,
}

impl EventStream {
    fn new(core: Arc<CoreClient>, req: Request) -> Self {
        Self {
            core,
            req,
            running: Arc::new(Mutex::new(None)),
        }
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        // Best-effort: abort the producer so the upstream connection is dropped promptly.
        if let Ok(mut guard) = self.running.try_lock() {
            if let Some(running) = guard.take() {
                running.task.abort();
            }
        }
    }
}

#[pymethods]
impl EventStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let running = self.running.clone();
        let core = self.core.clone();
        let req = self.req.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = running.lock().await;
            if guard.is_none() {
                let (tx, rx) = mpsc::channel::<Result<String, PyErr>>(64);
                let core = core.clone();
                let req = req.clone();
                let task = tokio::spawn(drain(core, req, tx));
                *guard = Some(Running { task, rx });
            }
            let running = guard.as_mut().expect("producer initialized above");
            match running.rx.recv().await {
                Some(Ok(json)) => Ok(json),
                Some(Err(err)) => Err(err),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

/// Drain the upstream stream into the channel. Exits when the stream ends, errors, or the receiver
/// is dropped (consumer went away) — closing the upstream connection.
async fn drain(core: Arc<CoreClient>, req: Request, tx: mpsc::Sender<Result<String, PyErr>>) {
    let mut stream = match core.stream(&req).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(map_core_err(e))).await;
            return;
        }
    };
    while let Some(item) = stream.next().await {
        match item {
            Ok(ev) => {
                let json = match serde_json::to_string(&ev) {
                    Ok(j) => j,
                    Err(e) => {
                        let _ = tx
                            .send(Err(UnillmError::new_err(format!("encode error: {e}"))))
                            .await;
                        break;
                    }
                };
                if tx.send(Ok(json)).await.is_err() {
                    break; // receiver dropped → consumer gone
                }
            }
            Err(e) => {
                let _ = tx.send(Err(map_core_err(e))).await;
                break;
            }
        }
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Client>()?;
    m.add_class::<EventStream>()?;
    m.add("UnillmError", m.py().get_type::<UnillmError>())?;
    Ok(())
}
