//! M4.1 gate: CRUD round-trips on in-proc SQLite (keys, models, routes).

use chrono::Utc;
use uuid::Uuid;

use unillm_storage::model::{FallbackTarget, NewModel, NewRoute, NewVirtualKey};
use unillm_storage::{KeyStore, ModelStore, RouteStore, SqliteStore};

async fn mem() -> SqliteStore {
    SqliteStore::connect("sqlite::memory:").await.unwrap()
}

#[tokio::test]
async fn key_create_find_list_revoke() {
    let store = mem().await;
    let tenant = Uuid::new_v4();
    let k = store
        .create_key(NewVirtualKey {
            key_hash: "h1".into(),
            key_prefix: "sk-uni".into(),
            tenant_id: tenant,
            scopes: vec!["data".into()],
            model_allowlist: Some(vec!["gpt-4o".into()]),
            budget_daily_tokens: Some(1000),
            rpm: Some(10),
            tpm: None,
            max_concurrency: Some(2),
            expires_at: None,
        })
        .await
        .unwrap();
    assert!(k.is_active(Utc::now()));

    let found = store.find_by_hash("h1").await.unwrap().unwrap();
    assert_eq!(found.id, k.id);
    assert_eq!(found.scopes, vec!["data".to_string()]);
    assert_eq!(found.model_allowlist, Some(vec!["gpt-4o".to_string()]));
    assert_eq!(found.rpm, Some(10));
    assert_eq!(found.max_concurrency, Some(2));
    assert_eq!(found.budget_daily_tokens, Some(1000));
    assert!(found.revoked_at.is_none());

    assert!(store.find_by_hash("missing").await.unwrap().is_none());
    assert_eq!(store.list_keys(Some(tenant)).await.unwrap().len(), 1);
    assert_eq!(store.list_keys(None).await.unwrap().len(), 1);

    store.revoke_key(k.id).await.unwrap();
    let revoked = store.find_by_hash("h1").await.unwrap().unwrap();
    assert!(!revoked.is_active(Utc::now()));
    assert!(revoked.revoked_at.is_some());

    assert!(matches!(
        store.revoke_key(Uuid::new_v4()).await,
        Err(unillm_storage::StoreError::NotFound(_))
    ));
}

#[tokio::test]
async fn model_upsert_get_list() {
    let store = mem().await;
    store
        .upsert_model(NewModel {
            provider: "openai".into(),
            native_model: "gpt-4o".into(),
            display_name: "GPT-4o".into(),
            context_window: Some(128_000),
            max_output: Some(16_384),
            price_in: Some(2.5),
            price_out: Some(10.0),
            price_cache_read: Some(1.25),
            enabled: true,
        })
        .await
        .unwrap();

    let m = store.get_model("openai", "gpt-4o").await.unwrap().unwrap();
    assert_eq!(m.display_name, "GPT-4o");
    assert_eq!(m.context_window, Some(128_000));
    assert_eq!(m.price_out, Some(10.0));
    assert!(m.enabled);

    assert!(
        store
            .get_model("openai", "missing")
            .await
            .unwrap()
            .is_none()
    );

    // Upsert updates the same (provider, native_model) row.
    store
        .upsert_model(NewModel {
            provider: "openai".into(),
            native_model: "gpt-4o".into(),
            display_name: "GPT-4o (2024)".into(),
            context_window: None,
            max_output: None,
            price_in: None,
            price_out: None,
            price_cache_read: None,
            enabled: false,
        })
        .await
        .unwrap();
    let updated = store.get_model("openai", "gpt-4o").await.unwrap().unwrap();
    assert_eq!(updated.display_name, "GPT-4o (2024)");
    assert!(!updated.enabled);
    assert_eq!(store.list_models().await.unwrap().len(), 1);
}

#[tokio::test]
async fn route_resolve_tenant_then_global() {
    let store = mem().await;
    let t = Uuid::new_v4();

    store
        .upsert_route(NewRoute {
            alias: "fast".into(),
            tenant_id: None,
            provider: "openai".into(),
            native_model: "gpt-4o-mini".into(),
            fallback: vec![],
            priority: 0,
            enabled: true,
        })
        .await
        .unwrap();
    store
        .upsert_route(NewRoute {
            alias: "fast".into(),
            tenant_id: Some(t),
            provider: "anthropic".into(),
            native_model: "claude".into(),
            fallback: vec![FallbackTarget {
                provider: "openai".into(),
                native_model: "gpt-4o-mini".into(),
            }],
            priority: 0,
            enabled: true,
        })
        .await
        .unwrap();

    let tenant_route = store.resolve("fast", Some(t)).await.unwrap().unwrap();
    assert_eq!(tenant_route.provider, "anthropic");
    assert_eq!(tenant_route.fallback.len(), 1);

    let global_route = store.resolve("fast", None).await.unwrap().unwrap();
    assert_eq!(global_route.provider, "openai");
    assert!(global_route.fallback.is_empty());

    assert!(store.resolve("missing", None).await.unwrap().is_none());
    assert_eq!(store.list_routes(None).await.unwrap().len(), 2);
}

#[tokio::test]
async fn migrations_are_idempotent() {
    // Connecting applies migrations; a second run over the same pool is a no-op.
    let store = mem().await;
    unillm_storage::migrate::run_sqlite(store.pool())
        .await
        .unwrap();
}
