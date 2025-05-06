use std::sync::Arc;

use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
use tantivy::Index;

use crate::error::Result;
use crate::index_writer::IndexWriterWrapper;
use crate::index_writer_v7::IndexWriterWrapperImpl;

const NGRAM_TOKENIZER: &str = "ngram";

fn build_ngram_schema(field_name: &str) -> (Schema, Field) {
    let mut schema_builder = Schema::builder();

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer(NGRAM_TOKENIZER)
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
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
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
    use std::ffi::c_void;

    use tempfile::TempDir;

    use crate::{index_writer::IndexWriterWrapper, util::set_bitset};

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

    #[test]
    fn test_ngram_writer() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15000000,
        )
        .unwrap();

        writer.add("university", Some(0)).unwrap();
        writer.add("anthropology", Some(1)).unwrap();
        writer.add("economics", Some(2)).unwrap();
        writer.add("history", Some(3)).unwrap();
        writer.add("victoria", Some(4)).unwrap();
        writer.add("basics", Some(5)).unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();
        let mut res: Vec<u32> = vec![];
        reader
            .inner_match_ngram("ic", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![2, 4, 5]);
    }
}
