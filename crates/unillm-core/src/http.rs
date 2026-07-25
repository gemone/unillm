//! HTTP transport: drives a [`Provider`] over reqwest/rustls (`DESIGN.md` §5.1, §6.1, §15).
//!
//! [`Client`] owns a provider adapter, its [`ProviderConfig`], and an HTTP client. `create` does a
//! non-streaming round trip (build → POST → parse) with retry; `stream` returns a boxed stream of
//! canonical events by piping the upstream byte stream through the SSE codec and the dialect decoder.

use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;

use crate::error::CoreError;
use crate::ir::{ProviderId, Request, Response};
use crate::provider::{Dialect, Provider, ProviderConfig};
use crate::retry::RetryPolicy;
use crate::sse::SseParser;
use crate::stream::StreamEvent;
use crate::stream_decode::{AnthropicDecoder, CcDecoder, StreamDecoder};

/// A direct-to-provider client.
pub struct Client {
    config: ProviderConfig,
    provider: Box<dyn Provider>,
    http: reqwest::Client,
    retry: RetryPolicy,
}

fn io_err(e: reqwest::Error) -> CoreError {
    CoreError::Io {
        message: e.to_string(),
    }
}

impl Client {
    /// Construct from a config, selecting the adapter by dialect (`DESIGN.md` §5.6).
    pub fn new(config: ProviderConfig) -> Result<Self, CoreError> {
        let provider: Box<dyn Provider> = match config.dialect {
            Dialect::ChatCompletions => {
                Box::new(crate::providers::ChatCompletions::new(config.provider))
            }
            Dialect::Anthropic => Box::new(crate::providers::Anthropic::new()),
            Dialect::Responses => {
                return Err(CoreError::Other {
                    message: "Responses dialect is not implemented in v1".into(),
                });
            }
        };
        let mut builder = reqwest::Client::builder().use_rustls_tls();
        if let Some(t) = config.request_timeout {
            builder = builder.timeout(t);
        }
        let http = builder.build().map_err(io_err)?;
        Ok(Self {
            config,
            provider,
            http,
            retry: RetryPolicy::default(),
        })
    }

    /// Override the retry policy (builder style).
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn provider_id(&self) -> ProviderId {
        self.config.provider
    }

    fn endpoint(&self) -> String {
        let path = match self.config.dialect {
            Dialect::ChatCompletions => "/chat/completions",
            Dialect::Anthropic => "/messages",
            Dialect::Responses => "/responses",
        };
        format!("{}{}", self.config.base_url.trim_end_matches('/'), path)
    }

    fn build_request(&self, body: &Value) -> reqwest::RequestBuilder {
        let mut rb = self.http.post(self.endpoint()).json(body);
        match self.config.dialect {
            Dialect::Anthropic => {
                rb = rb.header("x-api-key", &self.config.api_key);
            }
            _ => {
                rb = rb.header("authorization", format!("Bearer {}", self.config.api_key));
            }
        }
        for (k, v) in &self.config.default_headers {
            rb = rb.header(k, v);
        }
        rb
    }

    /// Non-streaming request: build → POST → parse, with retry (`DESIGN.md` §15.2).
    pub async fn create(&self, req: &Request) -> Result<Response, CoreError> {
        let mut req2 = req.clone();
        req2.stream = false;
        let body = self.provider.build_payload(&req2);

        let mut attempt = 0;
        loop {
            match self.do_create(&body).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    if attempt >= self.retry.max_retries || !self.retry.should_retry(&e) {
                        return Err(e);
                    }
                    tokio::time::sleep(self.retry.delay(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn do_create(&self, body: &Value) -> Result<Response, CoreError> {
        let resp = self.build_request(body).send().await.map_err(io_err)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_error(status, resp).await);
        }
        let json: Value = resp.json().await.map_err(|e| CoreError::Serde {
            message: e.to_string(),
        })?;
        self.provider.parse_response(&json)
    }

    /// Streaming request → a boxed stream of canonical events (`DESIGN.md` §6.1, §6.6).
    ///
    /// The upstream byte stream is fed through [`SseParser`] and the dialect [`StreamDecoder`];
    /// transport faults mid-stream surface as a single `Err` item.
    pub async fn stream(
        &self,
        req: &Request,
    ) -> Result<BoxStream<'static, Result<StreamEvent, CoreError>>, CoreError> {
        let mut req2 = req.clone();
        req2.stream = true;
        let body = self.provider.build_payload(&req2);

        let resp = self.build_request(&body).send().await.map_err(io_err)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_error(status, resp).await);
        }

        let mut decoder: Box<dyn StreamDecoder> = match self.config.dialect {
            Dialect::ChatCompletions => Box::new(CcDecoder::new(self.config.provider)),
            Dialect::Anthropic => Box::new(AnthropicDecoder::new()),
            Dialect::Responses => unreachable!("guarded by Client::new"),
        };
        let mut parser = SseParser::new();
        let mut bytes = resp.bytes_stream();

        let s = async_stream::stream! {
            while let Some(chunk) = bytes.next().await {
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        yield Err(CoreError::Io { message: e.to_string() });
                        break;
                    }
                };
                for frame in parser.feed(chunk.as_ref()) {
                    for ev in decoder.feed_frame(&frame) {
                        yield Ok(ev);
                    }
                }
            }
            // Flush any trailing unterminated frame, then the terminal `completed`.
            for frame in parser.finish() {
                for ev in decoder.feed_frame(&frame) {
                    yield Ok(ev);
                }
            }
            for ev in decoder.finish() {
                yield Ok(ev);
            }
        };
        Ok(Box::pin(s))
    }
}

/// Map a non-2xx upstream response onto a [`CoreError`] (`DESIGN.md` §15.1).
async fn map_error(status: reqwest::StatusCode, resp: reqwest::Response) -> CoreError {
    let raw = resp.json::<Value>().await.ok();
    let message = raw.as_ref().and_then(error_message).unwrap_or_else(|| {
        status
            .canonical_reason()
            .unwrap_or("upstream error")
            .to_string()
    });
    match status.as_u16() {
        400 => CoreError::InvalidRequest { message },
        401 | 403 => CoreError::Unauthorized { message },
        404 => CoreError::NotFound { message },
        429 => CoreError::RateLimited { message },
        other => CoreError::ProviderError {
            status: other,
            message,
            raw,
        },
    }
}

fn error_message(v: &Value) -> Option<String> {
    v.get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .map(String::from)
        .or_else(|| v.get("message").and_then(|m| m.as_str()).map(String::from))
}
