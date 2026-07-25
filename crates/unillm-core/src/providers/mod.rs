//! Provider adapter implementations.

pub mod anthropic;
pub mod chat_completions;

pub use anthropic::Anthropic;
pub use chat_completions::ChatCompletions;
