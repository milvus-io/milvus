//! Tantivy index version 5
//! This is the old version of Tantivy index (ex: Milvus 2.4.x uses).
//! We may still build tantivy index with version 5 for compatibility reasons where
//! there are some read nodes that can only read tantivy index with version 5.

mod analyzer;
pub(crate) mod index_writer;
pub(crate) mod index_writer_json_key_stats;
pub(crate) mod index_writer_text;

pub(crate) use index_writer::IndexWriterWrapperImpl;
pub(crate) use tantivy_5::Document as TantivyDocumentV5;
