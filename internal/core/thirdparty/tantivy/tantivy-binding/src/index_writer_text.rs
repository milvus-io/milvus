use log::info;
use std::sync::Arc;

use either::Either;
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST};
use tantivy::tokenizer::TextAnalyzer;
use tantivy::Index;

use crate::{index_writer::IndexWriterWrapper, log::init_log};

fn build_text_schema(field_name: &String, tokenizer_name: &String) -> (Schema, Field) {
    let mut schema_builder = Schema::builder();
    // positions is required for matching phase.
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(&tokenizer_name)
        .set_fieldnorms(false)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let option = TextOptions::default().set_indexing_options(indexing);
    let field = schema_builder.add_text_field(&field_name, option);
    schema_builder.enable_user_specified_doc_id();
    (schema_builder.build(), field)
}

impl IndexWriterWrapper {
    pub(crate) fn create_text_writer(
        field_name: String,
        path: String,
        tokenizer_name: String,
        tokenizer: TextAnalyzer,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        in_ram: bool,
    ) -> IndexWriterWrapper {
        info!(
            "create text index writer, field_name: {}, tokenizer_name: {}",
            field_name, tokenizer_name
        );
        init_log();

        let (schema, field) = build_text_schema(&field_name, &tokenizer_name);
        let index: Index;
        if in_ram {
            index = Index::create_in_ram(schema);
        } else {
            index = Index::create_in_dir(path.clone(), schema).unwrap();
        }
        index.tokenizers().register(&tokenizer_name, tokenizer);
        let index_writer = index
            .writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)
            .unwrap();

        IndexWriterWrapper {
            field,
            index_writer: Either::Left(index_writer),
            single_segment_writer: false,
            index: Arc::new(index),
        }
    }
}
