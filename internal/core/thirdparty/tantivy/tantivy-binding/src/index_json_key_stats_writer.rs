use std::sync::Arc;

use crate::data_type::TantivyDataType;
use crate::error::Result;
use crate::index_writer::{schema_builder_add_field, IndexWriterWrapper};
use either::Either;
use log::info;
use tantivy::schema::{Schema, FAST};
use tantivy::Index;

impl IndexWriterWrapper {
    pub(crate) fn create_json_key_stats_writer(
        field_name: &str,
        path: &str,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        in_ram: bool,
    ) -> Result<IndexWriterWrapper> {
        info!("create json key stats writer, field_name: {}", field_name);
        let mut schema_builder = Schema::builder();
        let field =
            schema_builder_add_field(&mut schema_builder, field_name, TantivyDataType::Keyword);
        let _ = schema_builder.add_i64_field("doc_id", FAST);
        let schema = schema_builder.build();
        let index = if in_ram {
            Index::create_in_ram(schema)
        } else {
            Index::create_in_dir(path, schema)?
        };
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapper {
            field,
            index_writer: Either::Left(index_writer),
            index: Arc::new(index),
            single_segment_writer: false,
        })
    }
}
