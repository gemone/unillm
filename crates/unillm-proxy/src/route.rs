//! Routing & alias resolution (`DESIGN.md` §10.2).
//!
//! A model alias resolves to an ordered chain of `(provider, native_model)` targets: the primary
//! followed by its fallbacks. The handler walks the chain, retrying retriable upstream failures.

use std::collections::HashMap;

use unillm_core::ir::ModelRef;
use unillm_core::{CoreError, ProviderId};

/// A single backend target.
#[derive(Debug, Clone)]
pub struct RouteTarget {
    pub provider: ProviderId,
    pub native_model: String,
}

/// A route: a primary target plus ordered fallbacks.
#[derive(Debug, Clone)]
pub struct Route {
    pub primary: RouteTarget,
    pub fallback: Vec<RouteTarget>,
}

impl Route {
    pub fn single(provider: ProviderId, native_model: impl Into<String>) -> Self {
        Self {
            primary: RouteTarget {
                provider,
                native_model: native_model.into(),
            },
            fallback: Vec::new(),
        }
    }
}

/// The routing table: model alias → [`Route`].
#[derive(Debug, Clone, Default)]
pub struct Routes {
    map: HashMap<String, Route>,
}

impl Routes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, alias: impl Into<String>, route: Route) {
        self.map.insert(alias.into(), route);
    }

    pub fn get(&self, alias: &str) -> Option<&Route> {
        self.map.get(alias)
    }
}

/// Resolve a [`ModelRef`] into an ordered target chain (`DESIGN.md` §10.2). An explicit
/// `(provider, model)` pair is a single-target route with no fallback.
pub fn resolve_chain(model: &ModelRef, routes: &Routes) -> Result<Vec<RouteTarget>, CoreError> {
    match model {
        ModelRef::Explicit { provider, model } => Ok(vec![RouteTarget {
            provider: *provider,
            native_model: model.clone(),
        }]),
        ModelRef::Alias(alias) => {
            let route = routes.get(alias).ok_or_else(|| CoreError::NotFound {
                message: format!("no route for model alias '{alias}'"),
            })?;
            let mut chain = vec![route.primary.clone()];
            chain.extend(route.fallback.iter().cloned());
            Ok(chain)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_resolves_to_primary_then_fallbacks() {
        let mut routes = Routes::new();
        routes.insert(
            "fast",
            Route {
                primary: RouteTarget {
                    provider: ProviderId::Openai,
                    native_model: "gpt-4o-mini".into(),
                },
                fallback: vec![RouteTarget {
                    provider: ProviderId::Anthropic,
                    native_model: "claude".into(),
                }],
            },
        );
        let chain = resolve_chain(&ModelRef::Alias("fast".into()), &routes).unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].provider, ProviderId::Openai);
        assert_eq!(chain[1].provider, ProviderId::Anthropic);
    }

    #[test]
    fn explicit_model_is_single_target() {
        let routes = Routes::new();
        let chain = resolve_chain(
            &ModelRef::Explicit {
                provider: ProviderId::Deepseek,
                model: "deepseek-chat".into(),
            },
            &routes,
        )
        .unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].native_model, "deepseek-chat");
    }

    #[test]
    fn unknown_alias_is_not_found() {
        let routes = Routes::new();
        let err = resolve_chain(&ModelRef::Alias("nope".into()), &routes).unwrap_err();
        assert!(matches!(err, CoreError::NotFound { .. }));
    }
}
