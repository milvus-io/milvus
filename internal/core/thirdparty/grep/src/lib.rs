//! Typed, memory-only field matching for Milvus.
//!
//! Argument-string parsing belongs to execution-side Go code. This crate compiles a
//! normalized specification and returns matching lines with content-relative
//! UTF-8 byte spans. No CLI, file, logging initialization, or worker pool is
//! used. Inputs and outputs contain no database identifiers.
//!
//! Execution uses the C ABI in [`ffi`], which includes result descriptor storage
//! in the byte budget. Compile a normalized pattern specification with [`Plan`]:
//!
//! ```
//! use milvus_grep_core::{CompileLimits, MatchOptions, Plan};
//!
//! let plan = Plan::compile(&[b"error"], MatchOptions::default(), CompileLimits {
//!     max_regex_bytes: 1024 * 1024, max_dfa_cache_bytes: 1024 * 1024,
//! })?;
//! # Ok::<(), milvus_grep_core::Error>(())
//! ```

mod error;
pub mod ffi;
mod plan;
mod result;
mod session;

pub use error::{Error, InputKind, LimitKind};
pub use plan::{Boundary, CaseMode, CompileLimits, MatchOptions, PatternKind, Plan};
pub use result::{BatchResult, Line, Span, TextResult};
pub use session::{CancellationToken, Session};

#[cfg(test)]
mod core_tests;
