//! Admin CLI (`DESIGN.md` §13.4). Mirrors the `/admin/*` REST API against a running proxy. The
//! target URL and admin token come from `UNILLM_PROXY_URL` (default `http://127.0.0.1:8080`) and
//! `UNILLM_ADMIN_TOKEN`. Responses are printed as pretty JSON to stdout.

use std::env;

use clap::{Parser, Subcommand};
use serde_json::{Value, json};

/// Top-level CLI. With no subcommand (or `serve`), the proxy server runs.
#[derive(Parser)]
#[command(
    name = "unillm-proxy",
    about = "Universal bidirectional LLM translator proxy"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<TopCmd>,
}

#[derive(Subcommand)]
pub enum TopCmd {
    /// Run the proxy server.
    Serve,
    /// Admin operations against a running proxy.
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
}

#[derive(Subcommand)]
pub enum AdminCmd {
    /// Virtual-key management.
    Keys {
        #[command(subcommand)]
        action: KeysCmd,
    },
    /// Model-catalog management.
    Models {
        #[command(subcommand)]
        action: CrudCmd,
    },
    /// Routing-rule management.
    Routes {
        #[command(subcommand)]
        action: CrudCmd,
    },
    /// Aggregated usage/cost.
    Usage {
        #[arg(long)]
        key_id: Option<String>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        group_by: Option<String>,
    },
    /// Recent request logs.
    Logs {
        #[arg(long)]
        key_id: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// Flush the response cache (`DESIGN.md` §7.4, §10.6).
    Cache {
        /// Flush only this scope (virtual key id); omit for all scopes.
        #[arg(long)]
        scope: Option<String>,
        /// Flush only this cache key hash; omit for all hashes.
        #[arg(long)]
        key_hash: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum KeysCmd {
    /// Create a virtual key (the secret is printed once).
    Create {
        #[arg(long)]
        tenant: String,
        #[arg(long = "scope")]
        scopes: Vec<String>,
        #[arg(long)]
        rpm: Option<i32>,
    },
    /// List keys (no secrets).
    List {
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Revoke a key by id.
    Revoke { id: String },
}

/// Shared create/list/delete shape for models and routes.
#[derive(Subcommand)]
pub enum CrudCmd {
    /// List all rows.
    List,
    /// Create/update from a JSON body (`--json '{...}'`).
    Create { json: String },
    /// Delete by identifier(s): models take `<provider> <native_model>`, routes take `<alias>`.
    Delete {
        #[arg(num_args = 1..)]
        args: Vec<String>,
    },
}

/// Run an admin command against the proxy at `UNILLM_PROXY_URL`, authenticating with
/// `UNILLM_ADMIN_TOKEN`.
pub async fn run_admin(cmd: AdminCmd) -> Result<(), Box<dyn std::error::Error>> {
    let base = env::var("UNILLM_PROXY_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let token =
        env::var("UNILLM_ADMIN_TOKEN").map_err(|_| "UNILLM_ADMIN_TOKEN is required for admin")?;
    run_admin_at(cmd, &base, &token).await
}

/// Run an admin command against `base` with `token` (the testable core; `run_admin` reads env then
/// delegates here).
pub async fn run_admin_at(
    cmd: AdminCmd,
    base: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let auth = format!("Bearer {token}");

    match cmd {
        AdminCmd::Keys { action } => match action {
            KeysCmd::Create {
                tenant,
                scopes,
                rpm,
            } => {
                let body = json!({"tenant_id": tenant, "scopes": scopes, "rpm": rpm});
                let r = client
                    .post(format!("{base}/admin/keys"))
                    .header("authorization", &auth)
                    .json(&body)
                    .send()
                    .await?;
                print_response(r).await?;
            }
            KeysCmd::List { tenant } => {
                let mut req = client
                    .get(format!("{base}/admin/keys"))
                    .header("authorization", &auth);
                if let Some(t) = tenant {
                    req = req.query(&[("tenant_id", t)]);
                }
                print_response(req.send().await?).await?;
            }
            KeysCmd::Revoke { id } => {
                print_response(
                    client
                        .delete(format!("{base}/admin/keys/{id}"))
                        .header("authorization", &auth)
                        .send()
                        .await?,
                )
                .await?;
            }
        },
        AdminCmd::Models { action } => match action {
            CrudCmd::List => {
                get_list(&client, base, &auth, "/admin/models").await?;
            }
            CrudCmd::Create { json } => {
                let body: Value = serde_json::from_str(&json)?;
                upsert(&client, base, &auth, "/admin/models", body).await?;
            }
            CrudCmd::Delete { args } => {
                let (provider, native_model) =
                    two(&args, "models delete <provider> <native_model>")?;
                delete(
                    &client,
                    base,
                    &auth,
                    &format!("/admin/models/{provider}/{native_model}"),
                )
                .await?;
            }
        },
        AdminCmd::Routes { action } => match action {
            CrudCmd::List => {
                get_list(&client, base, &auth, "/admin/routes").await?;
            }
            CrudCmd::Create { json } => {
                let body: Value = serde_json::from_str(&json)?;
                upsert(&client, base, &auth, "/admin/routes", body).await?;
            }
            CrudCmd::Delete { args } => {
                let alias = one(&args, "routes delete <alias>")?;
                delete(&client, base, &auth, &format!("/admin/routes/{alias}")).await?;
            }
        },
        AdminCmd::Usage {
            key_id,
            model,
            group_by,
        } => {
            let mut req = client
                .get(format!("{base}/admin/usage"))
                .header("authorization", &auth);
            if let Some(v) = key_id {
                req = req.query(&[("key_id", v)]);
            }
            if let Some(v) = model {
                req = req.query(&[("model", v)]);
            }
            if let Some(v) = group_by {
                req = req.query(&[("group_by", v)]);
            }
            print_response(req.send().await?).await?;
        }
        AdminCmd::Logs { key_id, limit } => {
            let mut req = client
                .get(format!("{base}/admin/logs"))
                .header("authorization", &auth)
                .query(&[("limit", limit.to_string())]);
            if let Some(v) = key_id {
                req = req.query(&[("key_id", v)]);
            }
            print_response(req.send().await?).await?;
        }
        AdminCmd::Cache { scope, key_hash } => {
            let mut body = json!({});
            if let Some(v) = scope {
                body["scope"] = v.into();
            }
            if let Some(v) = key_hash {
                body["key_hash"] = v.into();
            }
            print_response(
                client
                    .post(format!("{base}/admin/cache/invalidate"))
                    .header("authorization", &auth)
                    .json(&body)
                    .send()
                    .await?,
            )
            .await?;
        }
    }
    Ok(())
}

async fn get_list(
    client: &reqwest::Client,
    base: &str,
    auth: &str,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    print_response(
        client
            .get(format!("{base}{path}"))
            .header("authorization", auth)
            .send()
            .await?,
    )
    .await
}

async fn upsert(
    client: &reqwest::Client,
    base: &str,
    auth: &str,
    path: &str,
    body: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    print_response(
        client
            .post(format!("{base}{path}"))
            .header("authorization", auth)
            .json(&body)
            .send()
            .await?,
    )
    .await
}

async fn delete(
    client: &reqwest::Client,
    base: &str,
    auth: &str,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    print_response(
        client
            .delete(format!("{base}{path}"))
            .header("authorization", auth)
            .send()
            .await?,
    )
    .await
}

fn one(args: &[String], usage: &str) -> Result<String, String> {
    if args.len() != 1 {
        return Err(usage.into());
    }
    Ok(args[0].clone())
}

fn two(args: &[String], usage: &str) -> Result<(String, String), String> {
    if args.len() != 2 {
        return Err(usage.into());
    }
    Ok((args[0].clone(), args[1].clone()))
}

/// Print the response status + body (pretty JSON if the body is JSON, else raw text).
async fn print_response(r: reqwest::Response) -> Result<(), Box<dyn std::error::Error>> {
    let status = r.status();
    let text = r.text().await?;
    if status.is_success() {
        if let Ok(v) = serde_json::from_str::<Value>(&text) {
            println!("{}", serde_json::to_string_pretty(&v)?);
        } else {
            println!("{text}");
        }
        Ok(())
    } else {
        Err(format!("HTTP {status}: {text}").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::middleware::cache::CacheConfig;
    use crate::server::{AppState, Stores, build_app};
    use std::collections::HashMap;
    use std::sync::Arc;
    use unillm_storage::{
        InMemoryCache, InMemoryRateLimiter, KeyStore, LogStore, ModelStore, RouteStore, SqliteStore,
    };
    use uuid::Uuid;

    async fn spawn_admin_proxy(admin_token: &str) -> (String, Arc<SqliteStore>) {
        let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
        let keys: Arc<dyn KeyStore> = store.clone();
        let routes: Arc<dyn RouteStore> = store.clone();
        let models: Arc<dyn ModelStore> = store.clone();
        let logs: Arc<dyn LogStore> = store.clone();
        let stores = Stores {
            keys,
            routes,
            models,
            logs,
            rate_limiter: Arc::new(InMemoryRateLimiter::new()),
            cache: Arc::new(InMemoryCache::new()),
        };
        let app = build_app(AppState::new(
            HashMap::new(),
            stores,
            "pepper".into(),
            Some(admin_token.into()),
            crate::config::RequestLimits {
                max_input_items: 1000,
                max_tools: 128,
                max_output_tokens: 16_384,
            },
            CacheConfig::disabled(),
            Arc::new(Metrics::new()),
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), store)
    }

    /// Round-trip: the CLI creates a key against a live proxy, then lists it.
    #[tokio::test]
    async fn cli_creates_key_round_trip() {
        let (base, _store) = spawn_admin_proxy("admin-secret").await;
        run_admin_at(
            AdminCmd::Keys {
                action: KeysCmd::Create {
                    tenant: Uuid::new_v4().to_string(),
                    scopes: vec!["data".into()],
                    rpm: None,
                },
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("cli create succeeds");

        run_admin_at(
            AdminCmd::Keys {
                action: KeysCmd::List { tenant: None },
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("cli list succeeds");
    }

    /// A wrong admin token surfaces as an error (the server returns 401).
    #[tokio::test]
    async fn cli_wrong_token_errors() {
        let (base, _store) = spawn_admin_proxy("admin-secret").await;
        let err = run_admin_at(
            AdminCmd::Usage {
                key_id: None,
                model: None,
                group_by: None,
            },
            &base,
            "wrong-token",
        )
        .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn cli_models_create_list_delete() {
        let (base, _store) = spawn_admin_proxy("admin-secret").await;
        run_admin_at(
            AdminCmd::Models {
                action: CrudCmd::Create {
                    json: json!({"provider":"openai","native_model":"gpt-test","display_name":"T"})
                        .to_string(),
                },
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("models create");
        run_admin_at(
            AdminCmd::Models {
                action: CrudCmd::List,
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("models list");
        run_admin_at(
            AdminCmd::Models {
                action: CrudCmd::Delete {
                    args: vec!["openai".into(), "gpt-test".into()],
                },
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("models delete");
    }

    #[tokio::test]
    async fn cli_routes_usage_logs() {
        let (base, _store) = spawn_admin_proxy("admin-secret").await;
        run_admin_at(
            AdminCmd::Routes {
                action: CrudCmd::Create {
                    json: json!({"alias":"gpt-4o","provider":"openai","native_model":"gpt-4o"})
                        .to_string(),
                },
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("routes create");
        run_admin_at(
            AdminCmd::Routes {
                action: CrudCmd::List,
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("routes list");
        run_admin_at(
            AdminCmd::Usage {
                key_id: None,
                model: None,
                group_by: Some("model".into()),
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("usage");
        run_admin_at(
            AdminCmd::Logs {
                key_id: None,
                limit: 10,
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("logs");
    }

    #[tokio::test]
    async fn cli_cache_invalidate() {
        let (base, _store) = spawn_admin_proxy("admin-secret").await;
        // Flush-all on an empty cache → 0 invalidated, but the command wiring must succeed.
        run_admin_at(
            AdminCmd::Cache {
                scope: None,
                key_hash: None,
            },
            &base,
            "admin-secret",
        )
        .await
        .expect("cache invalidate");
    }
}
