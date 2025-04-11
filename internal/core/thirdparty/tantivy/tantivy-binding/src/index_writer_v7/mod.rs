//! Tantivy index version 7
//! This is the latest version of Tantivy index and is what we plan to use
//! in most cases.

pub(crate) mod index_writer;
pub(crate) mod index_writer_json_key_stats;
pub(crate) mod index_writer_text;

pub(crate) use index_writer::IndexWriterWrapperImpl;
pub(crate) use tantivy::TantivyDocument as TantivyDocumentV7;
