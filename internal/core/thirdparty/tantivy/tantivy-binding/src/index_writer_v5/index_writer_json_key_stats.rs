use either::Either;
use log::info;
use std::sync::Arc;
use tantivy_5::{
    schema::{Schema, FAST},
    Index,
};

use crate::data_type::TantivyDataType;
use crate::error::Result;
use crate::index_writer_v5::index_writer::schema_builder_add_field;

use super::IndexWriterWrapperImpl;

impl IndexWriterWrapperImpl {
    pub(crate) fn create_json_key_stats_writer(
        field_name: &str,
        path: &str,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        in_ram: bool,
    ) -> Result<IndexWriterWrapperImpl> {
        info!("create json key stats writer, field_name: {}", field_name);
        let mut schema_builder = Schema::builder();
        let field =
            schema_builder_add_field(&mut schema_builder, field_name, TantivyDataType::Keyword);
        let id_field = schema_builder.add_i64_field("doc_id", FAST);
        let schema = schema_builder.build();
        let index = if in_ram {
            Index::create_in_ram(schema)
        } else {
            Index::create_in_dir(path, schema)?
        };
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapperImpl {
            field,
            index_writer: Either::Left(index_writer),
            id_field: Some(id_field),
            _index: Arc::new(index),
        })
    }
}
