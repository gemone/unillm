//! Golden JSON round-trip tests for every canonical IR type (`DESIGN.md` §4) and the error/stream
//! models (§15, §4.9).
//!
//! `round_trip` asserts the *stable* round-trip property: `parse(serialize(parse(json))) == parse(json)`.
//! (Defaults and `skip_serializing_if` mean `serialize(parse(json))` may legitimately differ from
//! the input text — e.g. an omitted `stream:false` — so we compare the typed values, not the text.)
//! Targeted assertions below additionally pin the load-bearing serde rules so a silent regression
//! (wrong tag, stringified `input`, etc.) cannot hide behind a passing round-trip.

use pretty_assertions::assert_eq;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use unillm_core::*;

fn round_trip<T>(json: &str)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let parsed: T =
        serde_json::from_str(json).unwrap_or_else(|e| panic!("parse failed: {e}\n{json}"));
    let reser = serde_json::to_string(&parsed).unwrap_or_else(|e| panic!("serialize failed: {e}"));
    let reparsed: T =
        serde_json::from_str(&reser).unwrap_or_else(|e| panic!("reparse failed: {e}\n{reser}"));
    assert_eq!(
        parsed, reparsed,
        "round-trip not stable\ninput: {json}\nout:   {reser}"
    );
}

/// Like `round_trip`, but also asserts the JSON value is unchanged across parse→serialize (strict).
fn round_trip_strict<T>(json: &str)
where
    T: Serialize + DeserializeOwned,
{
    let parsed: T = serde_json::from_str(json).unwrap_or_else(|e| panic!("parse failed: {e}"));
    let reser = serde_json::to_string(&parsed).unwrap();
    let before: Value = serde_json::from_str(json).unwrap();
    let after: Value = serde_json::from_str(&reser).unwrap();
    assert_eq!(
        before, after,
        "strict round-trip changed the JSON\nout: {reser}"
    );
}

// --- Request -----------------------------------------------------------------

#[test]
fn request_minimal_alias() {
    round_trip::<Request>(
        r#"{"model":"claude-sonnet-4-6","input":[{"type":"message","role":"user","content":"hi"}]}"#,
    );
}

#[test]
fn request_full_example_design_4_11() {
    // The canonical example from DESIGN.md §4.11 — round-trips exactly.
    round_trip_strict::<Request>(
        r#"{
            "model": "claude-sonnet-4-6",
            "instructions": "You are a helpful assistant.",
            "input": [
                { "type": "message", "role": "user", "content": "What's the weather in SF?" }
            ],
            "tools": [{
                "name": "get_weather",
                "description": "Get current weather",
                "input_schema": { "type": "object", "properties": { "q": { "type": "string" } }, "required": ["q"] }
            }],
            "tool_choice": { "type": "auto" },
            "stream": true,
            "cache": { "kind": "explicit", "breakpoints": [{ "at": "instructions" }, { "at": "last" }], "ttl": "5m" }
        }"#,
    );
}

#[test]
fn request_defaults_apply() {
    let r: Request = serde_json::from_str(r#"{"model":"gpt-4o","input":[]}"#).unwrap();
    assert!(!r.stream, "stream defaults to false");
    assert!(
        matches!(r.cache, CacheStrategy::Auto),
        "cache defaults to Auto"
    );
    assert!(r.metadata.is_empty(), "metadata defaults to empty");
    assert!(r.tools.is_none());
}

#[test]
fn model_ref_explicit_round_trips() {
    round_trip::<ModelRef>(r#"{"provider":"openai","model":"gpt-4o"}"#);
    let alias: ModelRef = serde_json::from_str(r#""claude-sonnet-4-6""#).unwrap();
    assert_eq!(alias, ModelRef::Alias("claude-sonnet-4-6".into()));
    let explicit: ModelRef =
        serde_json::from_str(r#"{"provider":"deepseek","model":"deepseek-chat"}"#).unwrap();
    assert_eq!(
        explicit,
        ModelRef::Explicit {
            provider: ProviderId::Deepseek,
            model: "deepseek-chat".into()
        }
    );
}

// --- Items & content ---------------------------------------------------------

#[test]
fn item_message_text_and_blocks() {
    round_trip::<Item>(r#"{"type":"message","role":"user","content":"hi"}"#);
    round_trip::<Item>(
        r#"{"type":"message","role":"assistant","content":[{"type":"text","text":"hello"}]}"#,
    );
}

#[test]
fn item_reasoning_with_encrypted() {
    round_trip::<Item>(r#"{"type":"reasoning","summary":"thinking...","encrypted":"opaque-blob"}"#);
    round_trip::<Item>(r#"{"type":"reasoning","summary":"thinking..."}"#);
}

#[test]
fn function_call_arguments_is_a_json_string() {
    // DESIGN.md §4.2: arguments is a JSON *string*, not a parsed object.
    let item: Item = serde_json::from_str(
        r#"{"type":"function_call","id":"c1","name":"get_weather","arguments":"{\"q\":\"sf\"}"}"#,
    )
    .unwrap();
    match item {
        Item::FunctionCall { arguments, .. } => {
            // Must remain a string holding JSON, exactly as on the wire.
            assert_eq!(arguments, r#"{"q":"sf"}"#);
            // And that string must itself be valid JSON.
            let _: Value = serde_json::from_str(&arguments).unwrap();
        }
        other => panic!("expected FunctionCall, got {other:?}"),
    }
}

#[test]
fn function_call_output_round_trips() {
    round_trip::<Item>(
        r#"{"type":"function_call_output","call_id":"c1","output":"{\"temp\":62}"}"#,
    );
}

#[test]
fn tool_use_input_is_a_parsed_object() {
    // DESIGN.md §4.3: tool_use.input is a parsed JSON object (canonical), not a string.
    let block: ContentBlock = serde_json::from_str(
        r#"{"type":"tool_use","id":"c1","name":"get_weather","input":{"q":"sf"}}"#,
    )
    .unwrap();
    match block {
        ContentBlock::ToolUse { input, .. } => {
            assert!(input.is_object(), "input must deserialize as a JSON object");
            assert_eq!(input["q"], "sf");
        }
        other => panic!("expected ToolUse, got {other:?}"),
    }
}

#[test]
fn content_block_variants() {
    round_trip::<ContentBlock>(r#"{"type":"text","text":"hi"}"#);
    round_trip::<ContentBlock>(
        r#"{"type":"image","source":{"type":"url","url":"https://x/a.png"}}"#,
    );
    round_trip::<ContentBlock>(
        r#"{"type":"image","source":{"type":"base64","media_type":"image/png","data":"BASE64"}}"#,
    );
    round_trip::<ContentBlock>(
        r#"{"type":"tool_result","tool_use_id":"c1","content":"{\"temp\":62}"}"#,
    );
}

#[test]
fn cache_control_shape() {
    // DESIGN.md §4.8: {type:"ephemeral", ttl?}
    round_trip_strict::<CacheControl>(r#"{"type":"ephemeral"}"#);
    round_trip_strict::<CacheControl>(r#"{"type":"ephemeral","ttl":"1h"}"#);
    let cc: CacheControl = serde_json::from_str(r#"{"type":"ephemeral"}"#).unwrap();
    assert!(matches!(cc, CacheControl::Ephemeral { ttl: None }));
}

// --- Tooling -----------------------------------------------------------------

#[test]
fn tool_choice_variants() {
    round_trip_strict::<ToolChoice>(r#"{"type":"auto"}"#);
    round_trip_strict::<ToolChoice>(r#"{"type":"none"}"#);
    round_trip_strict::<ToolChoice>(r#"{"type":"required"}"#);
    round_trip_strict::<ToolChoice>(r#"{"type":"named","name":"get_weather"}"#);
}

#[test]
fn tool_def_round_trips() {
    round_trip::<ToolDef>(
        r#"{"name":"get_weather","description":"d","input_schema":{"type":"object"}}"#,
    );
}

// --- Cache strategy ----------------------------------------------------------

#[test]
fn cache_strategy_variants() {
    round_trip_strict::<CacheStrategy>(r#"{"kind":"auto"}"#);
    round_trip_strict::<CacheStrategy>(r#"{"kind":"none"}"#);
    // explicit with ttl defaulting to 5m when omitted is not value-stable, so use round_trip.
    round_trip::<CacheStrategy>(r#"{"kind":"explicit","breakpoints":[{"at":"last"}]}"#);
    round_trip_strict::<CacheStrategy>(
        r#"{"kind":"explicit","breakpoints":[{"at":"instructions"},{"at":"message","index":2}],"ttl":"1h"}"#,
    );
}

#[test]
fn breakpoint_variants() {
    round_trip_strict::<Breakpoint>(r#"{"at":"instructions"}"#);
    round_trip_strict::<Breakpoint>(r#"{"at":"message","index":3}"#);
    round_trip_strict::<Breakpoint>(r#"{"at":"last"}"#);
}

#[test]
fn ttl_values() {
    assert_eq!(serde_json::to_string(&Ttl::FiveMinutes).unwrap(), r#""5m""#);
    assert_eq!(serde_json::to_string(&Ttl::OneHour).unwrap(), r#""1h""#);
}

// --- Response / Usage --------------------------------------------------------

#[test]
fn response_full() {
    round_trip::<Response>(
        r#"{
            "id":"msg_1","model":"claude-sonnet-4-6","provider":"anthropic",
            "output":[{"type":"message","role":"assistant","content":"hi"}],
            "stop_reason":"end_turn",
            "usage":{"input_tokens":10,"output_tokens":5,"cache_read":0,"cache_creation":0,"cost_usd":0.0002}
        }"#,
    );
}

#[test]
fn stop_reason_all_variants() {
    for s in [
        "end_turn",
        "max_tokens",
        "stop_sequence",
        "tool_use",
        "refusal",
        "paused",
        "other",
    ] {
        let v: Value = serde_json::from_str(&format!("\"{s}\"")).unwrap();
        let sr: StopReason = serde_json::from_value(v).unwrap();
        assert_eq!(serde_json::to_string(&sr).unwrap(), format!("\"{s}\""));
    }
}

#[test]
fn usage_total_input_invariant_and_cost_optional() {
    let u: Usage = serde_json::from_str(
        r#"{"input_tokens":10,"output_tokens":5,"cache_read":3,"cache_creation":2}"#,
    )
    .unwrap();
    // DESIGN.md §4.7 invariant: input + cache_read + cache_creation == provider total prompt tokens.
    assert_eq!(u.total_input(), 15);
    assert!(u.cost_usd.is_none(), "cost_usd omitted when absent");

    let with_cost: Usage = serde_json::from_str(
        r#"{"input_tokens":10,"output_tokens":5,"cache_read":0,"cache_creation":0,"cost_usd":0.001}"#,
    )
    .unwrap();
    assert_eq!(with_cost.cost_usd, Some(0.001));
}

#[test]
fn provider_id_variants() {
    for (name, expected) in [
        ("openai", ProviderId::Openai),
        ("anthropic", ProviderId::Anthropic),
        ("openrouter", ProviderId::Openrouter),
        ("deepseek", ProviderId::Deepseek),
    ] {
        let p: ProviderId = serde_json::from_str(&format!("\"{name}\"")).unwrap();
        assert_eq!(p, expected);
        assert_eq!(serde_json::to_string(&p).unwrap(), format!("\"{name}\""));
    }
}

// --- Stream events -----------------------------------------------------------

#[test]
fn stream_event_variants() {
    round_trip_strict::<StreamEvent>(
        r#"{"type":"created","response":{"id":"msg_1","model":"m","provider":"openai"}}"#,
    );
    round_trip_strict::<StreamEvent>(r#"{"type":"text_delta","text":"hel"}"#);
    round_trip_strict::<StreamEvent>(
        r#"{"type":"tool_call_delta","id":"c1","name":"get_weather","arguments_delta":"{\"q\":"}"#,
    );
    round_trip_strict::<StreamEvent>(
        r#"{"type":"output_item_added","index":0,"item":{"type":"message","role":"assistant","content":""}}"#,
    );
    round_trip::<StreamEvent>(
        r#"{"type":"completed","response":{"id":"r","model":"m","provider":"anthropic","output":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_read":0,"cache_creation":0}}}"#,
    );
}

#[test]
fn stream_event_error_carries_core_error() {
    let ev: StreamEvent = serde_json::from_str(
        r#"{"type":"error","error":{"kind":"rate_limited","message":"slow down"}}"#,
    )
    .unwrap();
    match ev {
        StreamEvent::Error { error } => {
            assert!(matches!(error, CoreError::RateLimited { .. }));
            assert_eq!(error.status_code(), 429);
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// --- CoreError ---------------------------------------------------------------

#[test]
fn core_error_all_kinds_round_trip() {
    for json in [
        r#"{"kind":"invalid_request","message":"bad"}"#,
        r#"{"kind":"unauthorized","message":"no key"}"#,
        r#"{"kind":"not_found","message":"no model"}"#,
        r#"{"kind":"rate_limited","message":"slow"}"#,
        r#"{"kind":"io","message":"network"}"#,
        r#"{"kind":"stream","message":"broke"}"#,
        r#"{"kind":"serde","message":"bad json"}"#,
        r#"{"kind":"other","message":"?"}"#,
        r#"{"kind":"provider_error","status":503,"message":"down","raw":{"up":"detail"}}"#,
        r#"{"kind":"provider_error","status":400,"message":"bad"}"#,
    ] {
        round_trip_strict::<CoreError>(json);
    }
}
