use core::fmt;

use serde_json as json;

#[derive(Debug)]
pub enum TantivyBindingError {
    JsonError(serde_json::Error),
    InternalError(String),
}

impl From<serde_json::Error> for TantivyBindingError {
    fn from(value: serde_json::Error) -> Self {
        TantivyBindingError::JsonError(value)
    }
}

impl fmt::Display for TantivyBindingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TantivyBindingError::JsonError(e) => write!(f, "JsonError: {}", e),
            TantivyBindingError::InternalError(e) => write!(f, "InternalError: {}", e),
        }
    }
}

impl std::error::Error for TantivyBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TantivyBindingError::JsonError(e) => Some(e),
            TantivyBindingError::InternalError(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, TantivyBindingError>;
