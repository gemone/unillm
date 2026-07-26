//! Cross-DB gate (`DESIGN.md` §11.2, M4.5): the same CRUD + usage suite against PostgreSQL, proving
//! the storage traits are backend-portable. Compiled only with the `postgres` feature, and skipped
//! at runtime unless `UNILLM_PG_TEST_URL` is set (so the default `cargo test` run is SQLite-only).
//!
//! ```text
//! docker compose up -d postgres
//! UNILLM_PG_TEST_URL=postgres://unillm:unillm@localhost:5432/unillm \
//!     cargo test -p unillm-storage --features postgres --test pg
//! ```

#![cfg(feature = "postgres")]

use uuid::Uuid;

use unillm_storage::{
    GroupBy, KeyStore, LogStore, ModelStore, NewModel, NewRequestLog, NewRoute, NewUsage,
    NewVirtualKey, PostgresStore, RouteStore, StoreError, UpdateKey,
};

/// Connect to the PG instance at `UNILLM_PG_TEST_URL`, or `None` to skip.
async fn pg_store() -> Option<PostgresStore> {
    let url = std::env::var("UNILLM_PG_TEST_URL").ok()?;
    Some(PostgresStore::connect(&url).await.expect("connect"))
}

/// Exercise every storage trait against Postgres in one sequential pass (shared dev DB).
#[tokio::test]
async fn pg_full_round_trip() {
    let Some(store) = pg_store().await else {
        eprintln!("skipped: set UNILLM_PG_TEST_URL to run the Postgres gate");
        return;
    };

    // --- keys ---
    let k = store
        .create_key(NewVirtualKey {
            key_hash: format!("pg-{}", Uuid::new_v4()),
            key_prefix: "sk-unill".into(),
            tenant_id: Uuid::new_v4(),
            scopes: vec!["data".into()],
            model_allowlist: None,
            budget_daily_tokens: Some(1000),
            rpm: Some(10),
            tpm: None,
            max_concurrency: None,
            expires_at: None,
        })
        .await
        .unwrap();
    assert!(store.find_by_hash(&k.key_hash).await.unwrap().is_some());
    assert!(
        store
            .list_keys(None)
            .await
            .unwrap()
            .iter()
            .any(|x| x.id == k.id)
    );
    let updated = store
        .update_key(
            k.id,
            UpdateKey {
                rpm: Some(20),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.rpm, Some(20));
    store.revoke_key(k.id).await.unwrap();
    assert!(
        store
            .get_key(k.id)
            .await
            .unwrap()
            .unwrap()
            .revoked_at
            .is_some()
    );

    // --- models ---
    store
        .upsert_model(NewModel {
            provider: "openai".into(),
            native_model: "pg-test-model".into(),
            display_name: "PG Test".into(),
            context_window: None,
            max_output: None,
            price_in: Some(1.0),
            price_out: Some(2.0),
            price_cache_read: None,
            enabled: true,
        })
        .await
        .unwrap();
    assert!(
        store
            .get_model("openai", "pg-test-model")
            .await
            .unwrap()
            .is_some()
    );
    assert!(!store.list_models().await.unwrap().is_empty());
    store.delete_model("openai", "pg-test-model").await.unwrap();

    // --- routes ---
    store
        .upsert_route(NewRoute {
            alias: "pg-alias".into(),
            tenant_id: None,
            provider: "openai".into(),
            native_model: "gpt-4o".into(),
            fallback: vec![],
            priority: 0,
            enabled: true,
        })
        .await
        .unwrap();
    assert!(store.resolve("pg-alias", None).await.unwrap().is_some());
    store.delete_route("pg-alias", None).await.unwrap();
    assert!(matches!(
        store.delete_route("pg-alias", None).await,
        Err(StoreError::NotFound(_))
    ));

    // --- logs + usage ---
    store
        .insert_request_log(
            NewRequestLog {
                request_id: "pg-r1".into(),
                virtual_key_id: k.id,
                tenant_id: k.tenant_id,
                provider: "openai".into(),
                model: "gpt-4o".into(),
                inbound_format: "openai_chat".into(),
                outbound_format: "openai_chat".into(),
                status: 200,
                latency_ms: Some(5),
            },
            Some(NewUsage {
                input_tokens: 10,
                output_tokens: 5,
                cache_read: 0,
                cache_creation: 0,
                cost_usd: Some(0.001),
            }),
        )
        .await
        .unwrap();
    assert!(!store.list_logs(None, None, 10).await.unwrap().is_empty());
    let by_model = store
        .usage_summary(None, None, None, None, Some(GroupBy::Model))
        .await
        .unwrap();
    assert!(by_model.iter().any(|b| b.key.as_deref() == Some("gpt-4o")));
}
