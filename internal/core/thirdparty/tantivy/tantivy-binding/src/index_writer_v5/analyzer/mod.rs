//! This is totally copied from src/analyzer

mod analyzer;
mod build_in_analyzer;
mod filter;
mod stop_words;
mod tokenizers;
mod util;

pub(crate) use self::analyzer::create_analyzer;
pub(crate) use self::build_in_analyzer::standard_analyzer;
