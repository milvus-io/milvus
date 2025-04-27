use std::sync::Arc;

use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
use tantivy::Index;

use crate::error::Result;
use crate::index_writer::IndexWriterWrapper;
use crate::index_writer_v7::IndexWriterWrapperImpl;

fn build_ngram_schema(field_name: &str) -> (Schema, Field) {
    let mut schema_builder = Schema::builder();

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("raw")
        .set_fieldnorms(false)
        .set_index_option(IndexRecordOption::Basic);
    let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
    let field = schema_builder.add_text_field(field_name, text_options);
    schema_builder.enable_user_specified_doc_id();
    (schema_builder.build(), field)
}

impl IndexWriterWrapper {
    // create a text writer according to `tanviy_index_version`.
    // version 7 is the latest version and is what we should use in most cases.
    // We may also build with version 5 for compatibility for reader nodes with older versions.
    pub(crate) fn create_ngram_writer(
        field_name: &str,
        path: &str,
        min_gram: usize,
        max_gram: usize,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> Result<IndexWriterWrapper> {
        let tokenizer = TextAnalyzer::builder(NgramTokenizer::new(
            min_gram as usize,
            max_gram as usize,
            false,
        )?)
        .dynamic()
        .build();

        let (schema, field) = build_ngram_schema(field_name);

        let index = Index::create_in_dir(path, schema).unwrap();
        index.tokenizers().register("ngram", tokenizer);
        let index_writer = index
            .writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)
            .unwrap();

        Ok(IndexWriterWrapper::V7(IndexWriterWrapperImpl {
            field,
            index_writer,
            index: Arc::new(index),
        }))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::index_writer::IndexWriterWrapper;

    #[test]
    fn test_create_ngram_writer() {
        let dir = TempDir::new().unwrap();
        let _ = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            1,
            2,
            1,
            15000000,
        )
        .unwrap();
    }
}
