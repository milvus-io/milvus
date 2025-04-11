use error::TantivyBindingError;

mod array;
mod data_type;
mod demo_c;
mod docid_collector;
mod error;
mod hashmap_c;
mod index_reader;
mod index_reader_c;
mod index_reader_text;
mod index_reader_text_c;
mod index_writer;
mod index_writer_c;
mod index_writer_text;
mod index_writer_text_c;
mod index_writer_v5;
mod index_writer_v7;
mod log;
mod string_c;
mod token_stream_c;
mod tokenizer_c;
mod util;
mod util_c;
mod vec_collector;

pub mod analyzer;

use error::Result;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TantivyIndexVersion {
    V5, // Version for compatibility (for 2.4.x)
    V7, // Latest version
}

impl TantivyIndexVersion {
    pub fn from_u32(version: u32) -> Result<Self> {
        match version {
            5 => Ok(Self::V5),
            7 => Ok(Self::V7),
            _ => Err(TantivyBindingError::InvalidArgument(format!(
                "unsupported version {}",
                version
            ))),
        }
    }

    pub fn as_u32(&self) -> u32 {
        match self {
            Self::V5 => 5,
            Self::V7 => 7,
        }
    }

    pub fn default_version() -> Self {
        Self::V7
    }
}
