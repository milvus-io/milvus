//! Typed, memory-only field matching for Milvus.
//!
//! Argument-string parsing belongs to execution-side Go code. This crate compiles a
//! normalized specification and returns matching lines with content-relative
//! UTF-8 byte spans. No CLI, file, logging initialization, or worker pool is
//! used. Inputs and outputs contain no database identifiers.
//!
//! ```
//! use milvus_grep_core::{CompileLimits, ExecutionLimits, MatchOptions, Plan, Session, Span};
//!
//! let plan = Plan::compile(&[b"error"], MatchOptions::default(), CompileLimits {
//!     max_regex_bytes: 1024 * 1024, max_dfa_cache_bytes: 1024 * 1024,
//! })?;
//! let mut session = Session::new(&plan);
//! let result = session.execute_batch(&[b"ok\nerror: error"], ExecutionLimits {
//!     max_result_bytes: 4096,
//! })?;
//! let line = &result.texts[0].lines[0];
//! assert_eq!(&*line.content, "error: error");
//! assert_eq!(&*line.spans, &[Span { start: 0, end: 5 }, Span { start: 7, end: 12 }]);
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
pub use session::{CancellationToken, ExecutionLimits, Session};
