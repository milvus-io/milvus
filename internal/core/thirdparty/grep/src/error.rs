use std::{collections::TryReserveError, fmt, str::Utf8Error};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputKind {
    Pattern,
    Text,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitKind {
    PatternBytes,
    ResultBytes,
}

/// Private Rust error categories, not Milvus wire codes. Consumers must keep
/// request-pattern errors distinct from corrupt stored text or adapter errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    InvalidConfiguration(&'static str),
    PatternPolicy(&'static str),
    Syntax(Box<regex_syntax::Error>),
    InvalidUtf8 {
        kind: InputKind,
        index: usize,
        source: Utf8Error,
    },
    ResourceLimit(LimitKind),
    /// Upstream merges some compiler limits and regex errors into text. Keep
    /// the original error; do not classify by parsing its Display message.
    EngineBuild(grep::regex::Error),
    Allocation(TryReserveError),
    Cancelled,
    Internal(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(reason) => write!(f, "invalid configuration: {reason}"),
            Self::PatternPolicy(reason) => write!(f, "unsupported pattern: {reason}"),
            Self::Syntax(err) => write!(f, "invalid regex: {err}"),
            Self::InvalidUtf8 {
                kind,
                index,
                source,
            } => write!(f, "invalid UTF-8 in {kind:?} {index}: {source}"),
            Self::ResourceLimit(kind) => write!(f, "resource limit exceeded: {kind:?}"),
            Self::EngineBuild(err) => write!(f, "regex engine build failed: {err}"),
            Self::Allocation(err) => write!(f, "allocation failed: {err}"),
            Self::Cancelled => f.write_str("grep session cancelled"),
            Self::Internal(reason) => write!(f, "internal grep failure: {reason}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Syntax(err) => Some(err.as_ref()),
            Self::InvalidUtf8 { source, .. } => Some(source),
            Self::EngineBuild(err) => Some(err),
            Self::Allocation(err) => Some(err),
            _ => None,
        }
    }
}

impl From<TryReserveError> for Error {
    fn from(err: TryReserveError) -> Self {
        Self::Allocation(err)
    }
}
