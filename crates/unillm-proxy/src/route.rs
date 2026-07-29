//! Routing & alias resolution (`DESIGN.md` §10.2).
//!
//! A model alias resolves — via `RouteStore` (the DB `routes` table, M4.3) — to an ordered chain of
//! `(provider, native_model)` targets: the primary followed by its fallbacks. The handler walks the
//! chain, retrying retriable upstream failures. Explicit `(provider, model)` references skip the
//! table and become a single-target chain.

use serde_json::Value;
use unillm_core::ir::ModelRef;
use unillm_core::{CoreError, ProviderId};
use unillm_storage::RouteRow;

/// A single backend target.
#[derive(Debug, Clone)]
pub struct RouteTarget {
    pub provider: ProviderId,
    pub native_model: String,
}

/// Parse a storage `provider` string (the snake_case `ProviderId` serialization) back into the enum.
fn parse_provider(s: &str) -> Result<ProviderId, CoreError> {
    serde_json::from_value::<ProviderId>(Value::String(s.to_string())).map_err(|_| {
        CoreError::InvalidRequest {
            message: format!("unknown provider '{s}'"),
        }
    })
}

/// Flatten a resolved [`RouteRow`] into an ordered primary-then-fallback chain of [`RouteTarget`]s.
pub fn row_to_chain(row: &RouteRow) -> Result<Vec<RouteTarget>, CoreError> {
    let mut chain = vec![RouteTarget {
        provider: parse_provider(&row.provider)?,
        native_model: row.native_model.clone(),
    }];
    for f in &row.fallback {
        chain.push(RouteTarget {
            provider: parse_provider(&f.provider)?,
            native_model: f.native_model.clone(),
        });
    }
    Ok(chain)
}

/// The client-facing model identifier of a [`ModelRef`] (the alias, or the native model for an
/// explicit reference) — what a key's `model_allowlist` is checked against.
pub fn model_id(model: &ModelRef) -> String {
    match model {
        ModelRef::Alias(alias) => alias.clone(),
        ModelRef::Explicit { model, .. } => model.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unillm_storage::model::FallbackTarget;

    #[test]
    fn row_flattens_to_primary_then_fallbacks() {
        let row = RouteRow {
            alias: "fast".into(),
            tenant_id: None,
            provider: "openai".into(),
            native_model: "gpt-4o-mini".into(),
            fallback: vec![FallbackTarget {
                provider: "anthropic".into(),
                native_model: "claude".into(),
            }],
            priority: 0,
            enabled: true,
        };
        let chain = row_to_chain(&row).unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].provider, ProviderId::Openai);
        assert_eq!(chain[1].provider, ProviderId::Anthropic);
    }

    #[test]
    fn unknown_provider_is_invalid_request() {
        let row = RouteRow {
            alias: "x".into(),
            tenant_id: None,
            provider: "not-a-provider".into(),
            native_model: "m".into(),
            fallback: vec![],
            priority: 0,
            enabled: true,
        };
        assert!(matches!(
            row_to_chain(&row),
            Err(CoreError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn model_id_alias_vs_explicit() {
        assert_eq!(model_id(&ModelRef::Alias("gpt-4o".into())), "gpt-4o");
        assert_eq!(
            model_id(&ModelRef::Explicit {
                provider: ProviderId::Openai,
                model: "gpt-4o".into()
            }),
            "gpt-4o"
        );
    }

    #[test]
    fn row_to_chain_preserves_fallback_order() {
        let row = RouteRow {
            alias: "x".into(),
            tenant_id: None,
            provider: "openai".into(),
            native_model: "a".into(),
            fallback: vec![
                FallbackTarget {
                    provider: "anthropic".into(),
                    native_model: "b".into(),
                },
                FallbackTarget {
                    provider: "deepseek".into(),
                    native_model: "c".into(),
                },
            ],
            priority: 0,
            enabled: true,
        };
        let chain = row_to_chain(&row).unwrap();
        let got: Vec<_> = chain
            .iter()
            .map(|t| (t.provider, t.native_model.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                (ProviderId::Openai, "a"),
                (ProviderId::Anthropic, "b"),
                (ProviderId::Deepseek, "c"),
            ]
        );
    }

    #[test]
    fn row_to_chain_bad_fallback_provider_errors() {
        // A bad provider in a *fallback* entry also fails fast.
        let row = RouteRow {
            alias: "x".into(),
            tenant_id: None,
            provider: "openai".into(),
            native_model: "a".into(),
            fallback: vec![FallbackTarget {
                provider: "bogus".into(),
                native_model: "b".into(),
            }],
            priority: 0,
            enabled: true,
        };
        assert!(matches!(
            row_to_chain(&row),
            Err(CoreError::InvalidRequest { .. })
        ));
    }
}
