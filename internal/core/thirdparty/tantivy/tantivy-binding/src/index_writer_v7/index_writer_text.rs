use log::info;
use std::sync::Arc;

use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::Index;

use crate::analyzer::create_analyzer;
use crate::error::Result;

use super::IndexWriterWrapperImpl;

fn build_text_schema(field_name: &str, tokenizer_name: &str) -> (Schema, Field) {
    let mut schema_builder = Schema::builder();
    // positions is required for matching phase.
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(tokenizer_name)
        .set_fieldnorms(false)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let option = TextOptions::default().set_indexing_options(indexing);
    let field = schema_builder.add_text_field(field_name, option);
    schema_builder.enable_user_specified_doc_id();
    (schema_builder.build(), field)
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
            "create text index writer, field_name: {}, tantivy_index_version 7",
            field_name
        );

        let tokenizer = create_analyzer(tokenizer_params)?;

        let (schema, field) = build_text_schema(field_name, tokenizer_name);
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
            index_writer,
            index: Arc::new(index),
            id_field: None,
            enable_user_specified_doc_id: true,
        })
    }
}
