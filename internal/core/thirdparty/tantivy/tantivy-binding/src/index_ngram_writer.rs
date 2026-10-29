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

        let index = Index::create_in_dir(path, schema)?;
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        // Ngram writers are only used for sealed index builds. Keep
        // memory-budget-flushed segments and avoid background merge write
        // amplification.
        index_writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

        Ok(IndexWriterWrapper::V7(IndexWriterWrapperImpl {
            field,
            index_writer,
            index: Arc::new(index),
            enable_user_specified_doc_id: true,
            id_field: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, ffi::c_void};

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
        writer.add("economiCs", Some(6)).unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("ic", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![2, 4, 5].into_iter().collect::<HashSet<u32>>());
    }

    #[test]
    fn test_ngram_writer_chinese() {
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

        writer.add("ngram测试", Some(0)).unwrap();
        writer.add("测试ngram", Some(1)).unwrap();
        writer.add("测试ngram测试", Some(2)).unwrap();
        writer.add("你好世界", Some(3)).unwrap();
        writer.add("ngram需要被测试", Some(4)).unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("测试", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 1, 2, 4].into_iter().collect::<HashSet<u32>>());

        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("m测试", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 2].into_iter().collect::<HashSet<u32>>());

        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("需要被测试", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![4].into_iter().collect::<HashSet<u32>>());
    }

    // A literal shorter than min_gram must surface as an Err, never a panic:
    // these functions are reached through `extern "C"` frames, where a panic
    // aborts the whole process.
    #[test]
    fn test_ngram_short_literal_returns_error() {
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
        writer.add("订单（已取消）", Some(0)).unwrap();
        writer.commit().unwrap();
        let reader = writer.create_reader(set_bitset).unwrap();

        // one 3-byte character: byte length passes min_gram, char count does not
        for literal in ["订", "）", "a", ""] {
            let mut res: HashSet<u32> = HashSet::new();
            assert!(
                reader
                    .ngram_match_query(literal, 2, 3, &mut res as *mut _ as *mut c_void)
                    .is_err(),
                "literal {:?} should be rejected",
                literal
            );
            assert!(res.is_empty());
            assert!(
                reader.ngram_tokenize(&[literal], 2, 3).is_err(),
                "literal {:?} should be rejected",
                literal
            );
        }
        assert!(reader.ngram_tokenize(&["订单", "）"], 2, 3).is_err());
        assert!(reader.ngram_tokenize(&[], 2, 3).is_err());

        // sanity: a valid literal still works on the same reader
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("订单", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0].into_iter().collect::<HashSet<u32>>());
        assert!(!reader.ngram_tokenize(&["订单"], 2, 3).unwrap().is_empty());
    }

    // Interior NUL bytes are ordinary characters for the ngram index; the
    // FFI shims pass (pointer, length) so they reach here intact.
    #[test]
    fn test_ngram_literal_with_interior_nul() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            4,
            1,
            15000000,
        )
        .unwrap();
        writer.add("xab\0cy", Some(0)).unwrap();
        writer.add("xabzz", Some(1)).unwrap();
        writer.commit().unwrap();
        let reader = writer.create_reader(set_bitset).unwrap();

        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("ab\0c", 2, 4, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0].into_iter().collect::<HashSet<u32>>());

        let terms = reader.ngram_tokenize(&["ab\0c"], 2, 4).unwrap();
        assert_eq!(terms, vec!["ab\0c".to_string()]);
        let terms = reader.ngram_tokenize(&["xab\0cy"], 2, 4).unwrap();
        assert_eq!(terms.len(), 3);
        assert!(terms
            .iter()
            .all(|t| t.chars().count() == 4 && t.contains('\0')));
    }
}
