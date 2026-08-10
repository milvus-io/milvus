use core::{fmt, str};
use std::io;

#[derive(Debug)]
pub enum TantivyBindingError {
    JsonError(serde_json::Error),
    TantivyError(tantivy::TantivyError),
    TantivyErrorV5(tantivy_5::TantivyError),
    IOError(io::Error),
    InvalidArgument(String),
    InternalError(String),
}

impl From<&str> for TantivyBindingError {
    fn from(value: &str) -> Self {
        TantivyBindingError::InternalError(value.to_string())
    }
}

impl From<io::Error> for TantivyBindingError {
    fn from(value: io::Error) -> Self {
        TantivyBindingError::IOError(value)
    }
}

impl From<serde_json::Error> for TantivyBindingError {
    fn from(value: serde_json::Error) -> Self {
        TantivyBindingError::JsonError(value)
    }
}

impl From<tantivy::TantivyError> for TantivyBindingError {
    fn from(value: tantivy::TantivyError) -> Self {
        TantivyBindingError::TantivyError(value)
    }
}

impl From<tantivy_5::TantivyError> for TantivyBindingError {
    fn from(value: tantivy_5::TantivyError) -> Self {
        TantivyBindingError::TantivyErrorV5(value)
    }
}


/// Stable discriminant carried over the FFI boundary so the C++ side can
/// classify an error without parsing the Display text (whose wording is a
/// human-facing detail, not a contract). Exported through cbindgen; values
/// are part of the C ABI -- append, never renumber.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TantivyBindingErrorCode {
    Ok = 0,
    /// Malformed caller input (analyzer params, query arguments).
    InvalidArgument = 1,
    /// I/O failure underneath the index (read/open); typically transient.
    Io = 2,
    /// Persisted index data failed validation / deserialization.
    DataCorruption = 3,
    /// JSON (de)serialization failure.
    Json = 4,
    /// Any other tantivy engine error.
    Tantivy = 5,
    /// Binding-internal error (ad-hoc failures, poisoned locks, ...).
    Internal = 6,
}

fn tantivy_error_code(e: &tantivy::TantivyError) -> TantivyBindingErrorCode {
    use tantivy::TantivyError::*;
    match e {
        IoError(_) | OpenDirectoryError(_) | OpenReadError(_) => {
            TantivyBindingErrorCode::Io
        }
        DataCorruption(_) | IncompatibleIndex(_) => {
            TantivyBindingErrorCode::DataCorruption
        }
        InvalidArgument(_) => TantivyBindingErrorCode::InvalidArgument,
        _ => TantivyBindingErrorCode::Tantivy,
    }
}

fn tantivy_5_error_code(e: &tantivy_5::TantivyError) -> TantivyBindingErrorCode {
    use tantivy_5::TantivyError::*;
    match e {
        IoError(_) | OpenDirectoryError(_) | OpenReadError(_) => {
            TantivyBindingErrorCode::Io
        }
        DataCorruption(_) | IncompatibleIndex(_) => {
            TantivyBindingErrorCode::DataCorruption
        }
        InvalidArgument(_) => TantivyBindingErrorCode::InvalidArgument,
        _ => TantivyBindingErrorCode::Tantivy,
    }
}

impl TantivyBindingError {
    pub fn code(&self) -> TantivyBindingErrorCode {
        match self {
            TantivyBindingError::JsonError(_) => TantivyBindingErrorCode::Json,
            TantivyBindingError::IOError(_) => TantivyBindingErrorCode::Io,
            TantivyBindingError::InvalidArgument(_) => {
                TantivyBindingErrorCode::InvalidArgument
            }
            TantivyBindingError::InternalError(_) => {
                TantivyBindingErrorCode::Internal
            }
            TantivyBindingError::TantivyError(e) => tantivy_error_code(e),
            TantivyBindingError::TantivyErrorV5(e) => tantivy_5_error_code(e),
        }
    }
}

impl fmt::Display for TantivyBindingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TantivyBindingError::JsonError(e) => write!(f, "JsonError: {}", e),
            TantivyBindingError::TantivyError(e) => write!(f, "TantivyError: {}", e),
            TantivyBindingError::IOError(e) => write!(f, "IOError: {}", e),
            TantivyBindingError::TantivyErrorV5(e) => write!(f, "TantivyErrorV5: {}", e),
            TantivyBindingError::InvalidArgument(e) => write!(f, "InvalidArgument: {}", e),
            TantivyBindingError::InternalError(e) => write!(f, "InternalError: {}", e),
        }
    }
}

impl std::error::Error for TantivyBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TantivyBindingError::JsonError(e) => Some(e),
            TantivyBindingError::TantivyError(e) => Some(e),
            TantivyBindingError::IOError(e) => Some(e),
            TantivyBindingError::TantivyErrorV5(e) => Some(e),
            TantivyBindingError::InvalidArgument(_) => None,
            TantivyBindingError::InternalError(_) => None,
        }
    }
}

impl From<str::Utf8Error> for TantivyBindingError {
    fn from(value: str::Utf8Error) -> Self {
        TantivyBindingError::InternalError(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TantivyBindingError>;
