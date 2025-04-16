use log::info;
use std::sync::Arc;

use either::Either;
use tantivy_5::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST};
use tantivy_5::Index;

use crate::error::Result;

use super::analyzer::create_analyzer;
use super::IndexWriterWrapperImpl;

fn build_text_schema(field_name: &str, tokenizer_name: &str) -> (Schema, Field, Field) {
    let mut schema_builder = Schema::builder();
    // positions is required for matching phase.
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(tokenizer_name)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let option = TextOptions::default().set_indexing_options(indexing);
    let field = schema_builder.add_text_field(field_name, option);
    let id_field = schema_builder.add_i64_field("doc_id", FAST);
    (schema_builder.build(), field, id_field)
}

impl IndexWriterWrapperImpl {
    pub(crate) fn create_text_writer(
        field_name: &str,
        path: &str,
        tokenizer_name: &str,
        tokenizer_params: &str,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        in_ram: bool,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create text index writer, field_name: {}, tantivy_index_version 5",
            field_name
        );

        let tokenizer = create_analyzer(tokenizer_params)?;

        let (schema, field, id_field) = build_text_schema(field_name, tokenizer_name);
        let index = if in_ram {
            Index::create_in_ram(schema)
        } else {
            Index::create_in_dir(path, schema).unwrap()
        };
        index.tokenizers().register(tokenizer_name, tokenizer);
        let index_writer = index
            .writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)
            .unwrap();

        Ok(IndexWriterWrapperImpl {
            field,
            index_writer: Either::Left(index_writer),
            id_field: Some(id_field),
            _index: Arc::new(index),
        })
    }
}
