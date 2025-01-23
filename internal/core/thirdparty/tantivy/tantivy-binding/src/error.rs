use core::{fmt, str};

#[derive(Debug)]
pub enum TantivyBindingError {
    JsonError(serde_json::Error),
    TantivyError(tantivy::TantivyError),
    InvalidArgument(String),
    InternalError(String),
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

impl fmt::Display for TantivyBindingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TantivyBindingError::JsonError(e) => write!(f, "JsonError: {}", e),
            TantivyBindingError::TantivyError(e) => write!(f, "TantivyError: {}", e),
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
