use std::sync::Arc;

use log::info;
use tantivy::schema::{Schema, FAST};
use tantivy::Index;

use crate::error::Result;
use crate::index_writer_v7::index_writer::schema_builder_add_field;
use crate::index_writer_v7::IndexWriterWrapperImpl;
use crate::{data_type::TantivyDataType, index_writer::IndexWriterWrapper};

// Json key stats does not involve tantivy V5
impl IndexWriterWrapper {
    pub fn new_json_key_stats_writer(
        field_name: &str,
        path: &str,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> Result<IndexWriterWrapper> {
        info!("create json key stats writer, field_name: {}", field_name);
        let mut schema_builder = Schema::builder();
        let field =
            schema_builder_add_field(&mut schema_builder, field_name, TantivyDataType::Keyword);
        let _ = schema_builder.add_i64_field("doc_id", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path, schema)?;
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapper::V7(IndexWriterWrapperImpl {
            field,
            index_writer,
            index: Arc::new(index),
        }))
    }
}
