use std::sync::Arc;

use log::info;
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
use tantivy::{Index, IndexSettings, SingleSegmentIndexWriter};

use crate::error::{Result, TantivyBindingError};
use crate::index_ngram_document::{NgramBatchData, NgramDocument};
use crate::index_writer::IndexWriterWrapper;

const NGRAM_TOKENIZER: &str = "ngram";
// Reference-only batch size used to compare the direct writer with the old
// regular-writer pipeline in tests.
#[cfg(test)]
const REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NgramRow<'a> {
    pub(crate) doc_id: u32,
    pub(crate) value: Option<&'a str>,
}

pub(crate) struct NgramIndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) index_writer: SingleSegmentIndexWriter<NgramDocument>,
}

impl NgramIndexWriterWrapperImpl {
    pub(crate) fn finish_index(self) -> Result<Index> {
        let index = self.index_writer.finalize()?;
        let metas = index.searchable_segment_metas()?;
        let segment_ids: Vec<_> = metas.iter().map(|meta| meta.id().uuid_string()).collect();
        info!("tantivy index_writer finish, segments: {:?}", segment_ids);
        Ok(index)
    }
}

#[cfg(test)]
fn for_each_contiguous_ngram_batch<'rows, 'value, E>(
    rows: &'rows [NgramRow<'value>],
    batch_size: usize,
    mut callback: impl FnMut(u32, &'rows [NgramRow<'value>]) -> std::result::Result<(), E>,
) -> std::result::Result<(), E> {
    let batch_size = batch_size.max(1);
    let mut batch_begin = 0;

    while batch_begin < rows.len() {
        let mut batch_end = batch_begin + 1;
        while batch_end < rows.len()
            && batch_end - batch_begin < batch_size
            && rows[batch_end - 1].doc_id.checked_add(1) == Some(rows[batch_end].doc_id)
        {
            batch_end += 1;
        }

        callback(rows[batch_begin].doc_id, &rows[batch_begin..batch_end])?;
        batch_begin = batch_end;
    }

    Ok(())
}

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
        _num_threads: usize,
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

        let settings = IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..Default::default()
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_dir(path)?;
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let index_writer = SingleSegmentIndexWriter::new(index, overall_memory_budget_in_bytes)?;

        Ok(IndexWriterWrapper::NgramV7(NgramIndexWriterWrapperImpl {
            field,
            index_writer,
        }))
    }

    pub(crate) fn add_ngram_rows(&mut self, rows: &[NgramRow<'_>]) -> Result<()> {
        let IndexWriterWrapper::NgramV7(writer) = self else {
            return Err(TantivyBindingError::InternalError(
                "ngram batch submission requires an NGRAM V7 writer".to_string(),
            ));
        };
        let field = writer.field;

        let total_bytes = rows.iter().try_fold(0usize, |total, row| {
            let value_len = row.value.map_or(0, str::len);
            total.checked_add(value_len).ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "ngram batch text byte count overflows usize".to_string(),
                )
            })
        })?;
        u32::try_from(total_bytes).map_err(|_| {
            TantivyBindingError::InvalidArgument(
                "ngram batch text exceeds the u32 document range".to_string(),
            )
        })?;
        let mut validated_start = 0u32;
        for row in rows {
            if let Some(value) = row.value {
                if validated_start == u32::MAX {
                    return Err(TantivyBindingError::InvalidArgument(
                        "ngram present value starts at the reserved absent offset".to_string(),
                    ));
                }
                validated_start =
                    validated_start
                        .checked_add(value.len() as u32)
                        .ok_or_else(|| {
                            TantivyBindingError::InvalidArgument(
                                "ngram document range overflows u32".to_string(),
                            )
                        })?;
            }
        }

        let mut text = String::with_capacity(total_bytes);
        for row in rows {
            if let Some(value) = row.value {
                text.push_str(value);
            }
        }
        let batch_data = Arc::new(NgramBatchData::new(field, text)?);

        let mut start = 0u32;
        let documents = rows.iter().map(move |row| {
            let document = match row.value {
                Some(value) => {
                    let len = value.len() as u32;
                    let document =
                        NgramDocument::from_validated_range(batch_data.clone(), start, len);
                    start += len;
                    document
                }
                None => NgramDocument::absent(batch_data.clone()),
            };
            (row.doc_id, document)
        });
        writer.index_writer.add_documents_with_doc_ids(documents)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        ffi::c_void,
        path::Path,
        sync::Arc,
    };

    use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
    use tantivy::{
        schema::IndexRecordOption, DocSet, Index, IndexWriter, TantivyDocument, TERMINATED,
    };
    use tempfile::TempDir;

    use super::{
        build_ngram_schema, for_each_contiguous_ngram_batch, NgramRow, NGRAM_TOKENIZER,
        REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
    };
    use crate::{
        error::{Result, TantivyBindingError},
        index_ngram_document::{NgramBatchData, NgramDocument},
        index_reader::IndexReaderWrapper,
        index_writer::IndexWriterWrapper,
        util::set_bitset,
    };

    fn assert_ngram_operation_is_typed_error(result: Result<()>, operation: &str) {
        let Err(TantivyBindingError::InternalError(message)) = result else {
            panic!("{operation} must return a typed internal binding error");
        };
        assert!(message.contains("NGRAM-specific"), "{message}");
    }

    fn open_finished_ngram_reader(path: &Path) -> IndexReaderWrapper {
        let index = Index::open_in_dir(path).unwrap();
        IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap()
    }

    #[test]
    fn ngram_writer_uses_lightweight_document_variant() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        assert!(matches!(writer, IndexWriterWrapper::NgramV7(_)));
    }

    #[test]
    fn direct_ngram_writer_rejects_commit_while_active() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        assert_ngram_operation_is_typed_error(writer.commit(), "commit before finish");
    }

    #[test]
    fn direct_ngram_writer_rejects_reader_while_active() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        let Err(TantivyBindingError::InternalError(message)) = writer.create_reader(set_bitset)
        else {
            panic!("create reader before finish must return a typed internal binding error");
        };
        assert!(message.contains("finish"), "{message}");
    }

    #[test]
    fn direct_ngram_finish_disables_dedicated_docstore_compression() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        writer.finish_index().unwrap();

        let index = Index::open_in_dir(dir.path()).unwrap();
        assert!(!index.settings().docstore_compress_dedicated_thread);
    }

    #[test]
    fn ngram_writer_rejects_non_ngram_operations_with_typed_errors() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        let c_strings: [*const libc::c_char; 0] = [];
        let byte_ptrs: [*const u8; 0] = [];
        let lengths: [usize; 0] = [];
        let offsets: [*const i64; 0] = [];

        assert_ngram_operation_is_typed_error(writer.add("value", Some(0)), "generic add");
        assert_ngram_operation_is_typed_error(
            writer.add_array(["value"], Some(0)),
            "generic array add",
        );
        assert_ngram_operation_is_typed_error(writer.add_json("{}", Some(0)), "JSON add");
        assert_ngram_operation_is_typed_error(
            writer.add_json_batch(&c_strings, 0),
            "JSON batch add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_array_json(&c_strings, Some(0)),
            "JSON array add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_array_keywords(&c_strings, Some(0)),
            "keyword array add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_array_keywords_with_len(&byte_ptrs, &lengths, Some(0)),
            "length-delimited keyword array add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_json_key_stats(&c_strings, &offsets, &lengths),
            "JSON key stats add",
        );
        assert_ngram_operation_is_typed_error(writer.manual_merge(), "manual merge");
    }

    fn rows(count: usize, first_doc_id: u32) -> Vec<NgramRow<'static>> {
        (0..count)
            .map(|index| NgramRow {
                doc_id: first_doc_id + index as u32,
                value: Some("value"),
            })
            .collect()
    }

    fn grouped_batches(rows: &[NgramRow<'_>]) -> Vec<(u32, usize)> {
        let mut batches = Vec::new();
        for_each_contiguous_ngram_batch(
            rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                batches.push((first_doc_id, batch.len()));
                Ok::<_, ()>(())
            },
        )
        .unwrap();
        batches
    }

    #[test]
    fn ngram_batch_grouping_handles_boundaries() {
        let batch_size = REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE;
        let next_batch_doc_id = 100 + batch_size as u32;

        assert_eq!(grouped_batches(&rows(0, 100)), Vec::new());
        assert_eq!(grouped_batches(&rows(1, 100)), vec![(100, 1)]);
        assert_eq!(
            grouped_batches(&rows(batch_size - 1, 100)),
            vec![(100, batch_size - 1)]
        );
        assert_eq!(
            grouped_batches(&rows(batch_size, 100)),
            vec![(100, batch_size)]
        );
        assert_eq!(
            grouped_batches(&rows(batch_size + 1, 100)),
            vec![(100, batch_size), (next_batch_doc_id, 1)]
        );
    }

    #[test]
    fn ngram_batch_grouping_splits_at_gaps_without_dropping_absent_rows() {
        let rows = [
            NgramRow {
                doc_id: 7,
                value: Some("first"),
            },
            NgramRow {
                doc_id: 8,
                value: None,
            },
            NgramRow {
                doc_id: 9,
                value: Some(""),
            },
            NgramRow {
                doc_id: 15,
                value: Some("after-gap"),
            },
            NgramRow {
                doc_id: 16,
                value: None,
            },
        ];
        let mut seen_values = Vec::new();
        let mut batches = Vec::new();

        for_each_contiguous_ngram_batch(
            &rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                batches.push((first_doc_id, batch.len()));
                seen_values.extend(batch.iter().map(|row| row.value));
                Ok::<_, ()>(())
            },
        )
        .unwrap();

        assert_eq!(batches, vec![(7, 3), (15, 2)]);
        assert_eq!(
            seen_values,
            vec![Some("first"), None, Some(""), Some("after-gap"), None]
        );
    }

    #[test]
    fn direct_ngram_add_rows_validates_the_whole_slice_before_writing() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        let mut invalid_rows = rows(REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE + 1, 0);
        invalid_rows[REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE].doc_id = 0;

        assert!(writer.add_ngram_rows(&invalid_rows).is_err());

        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 0,
                value: Some("recovered"),
            }])
            .unwrap();
        writer.finish_index().unwrap();

        let snapshot = index_snapshot(dir.path());
        assert_eq!(snapshot.segment_count, 1);
        assert_eq!(snapshot.max_docs, vec![1]);
        assert_eq!(snapshot.postings.get(b"re".as_slice()), Some(&vec![0]));
    }

    #[test]
    fn v7_batch_submission_accepts_an_exact_size_document_iterator() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        let IndexWriterWrapper::NgramV7(mut writer) = writer else {
            panic!("ngram writer must use the NGRAM Tantivy V7 variant");
        };
        let field = writer.field;
        let text = "alphangram测试".to_string();
        let batch = Arc::new(NgramBatchData::new(field, text).unwrap());
        let ranges = [(0, 5), (5, 0), (5, "ngram测试".len() as u32)];
        let documents = ranges
            .into_iter()
            .map(|(start, len)| NgramDocument::present(batch.clone(), start, len).unwrap());

        writer
            .index_writer
            .add_documents_with_doc_ids([4, 5, 6].into_iter().zip(documents))
            .unwrap();
        writer.finish_index().unwrap();

        let reader = open_finished_ngram_reader(dir.path());
        assert_eq!(reader.count().unwrap(), 7);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct IndexSnapshot {
        segment_count: usize,
        max_docs: Vec<u32>,
        postings: BTreeMap<Vec<u8>, Vec<u32>>,
    }

    fn index_snapshot(path: &Path) -> IndexSnapshot {
        let index = Index::open_in_dir(path).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let field = index.schema().get_field("test").unwrap();
        let mut max_docs = Vec::new();
        let mut postings = BTreeMap::<Vec<u8>, Vec<u32>>::new();

        for segment_reader in searcher.segment_readers() {
            max_docs.push(segment_reader.max_doc());
            let inverted_index = segment_reader.inverted_index(field).unwrap();
            let mut terms = inverted_index.terms().stream().unwrap();
            while terms.advance() {
                let term = terms.key().to_vec();
                let mut term_postings = inverted_index
                    .read_postings_from_terminfo(terms.value(), IndexRecordOption::Basic)
                    .unwrap();
                let docs = postings.entry(term).or_default();
                while term_postings.doc() != TERMINATED {
                    docs.push(term_postings.doc());
                    term_postings.advance();
                }
            }
        }
        max_docs.sort_unstable();
        for docs in postings.values_mut() {
            docs.sort_unstable();
        }

        IndexSnapshot {
            segment_count: searcher.segment_readers().len(),
            max_docs,
            postings,
        }
    }

    fn build_default_document_index(path: &Path, rows: &[NgramRow<'_>]) {
        let tokenizer = TextAnalyzer::builder(NgramTokenizer::new(2, 3, false).unwrap())
            .dynamic()
            .build();
        let (schema, field) = build_ngram_schema("test");
        let index = Index::create_in_dir(path, schema).unwrap();
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let mut writer: IndexWriter<TantivyDocument> =
            index.writer_with_num_threads(1, 15_000_000).unwrap();
        writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

        for_each_contiguous_ngram_batch(
            rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                let documents = batch.iter().map(|row| {
                    let mut document = TantivyDocument::default();
                    if let Some(value) = row.value {
                        document.add_text(field, value);
                    }
                    document
                });
                writer
                    .add_documents_with_doc_id(first_doc_id, documents)
                    .map(|_| ())
            },
        )
        .unwrap();
        writer.commit().unwrap();
    }

    fn build_regular_lightweight_document_index(path: &Path, rows: &[NgramRow<'_>]) {
        let tokenizer = TextAnalyzer::builder(NgramTokenizer::new(2, 3, false).unwrap())
            .dynamic()
            .build();
        let (schema, field) = build_ngram_schema("test");
        let index = Index::create_in_dir(path, schema).unwrap();
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let mut writer: IndexWriter<NgramDocument> =
            index.writer_with_num_threads(1, 15_000_000).unwrap();
        writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

        for_each_contiguous_ngram_batch(
            rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                let total_bytes = batch.iter().map(|row| row.value.map_or(0, str::len)).sum();
                let mut text = String::with_capacity(total_bytes);
                for row in batch {
                    if let Some(value) = row.value {
                        text.push_str(value);
                    }
                }
                let batch_data = Arc::new(NgramBatchData::new(field, text).unwrap());
                let mut start = 0u32;
                let documents = batch.iter().map(move |row| match row.value {
                    Some(value) => {
                        let len = value.len() as u32;
                        let document =
                            NgramDocument::from_validated_range(batch_data.clone(), start, len);
                        start += len;
                        document
                    }
                    None => NgramDocument::absent(batch_data.clone()),
                });
                writer
                    .add_documents_with_doc_id(first_doc_id, documents)
                    .map(|_| ())
            },
        )
        .unwrap();
        writer.commit().unwrap();
    }

    fn build_finished_ngram_index(path: &Path, rows: &[NgramRow<'_>]) {
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            path.to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        writer.add_ngram_rows(rows).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn direct_and_regular_lightweight_writers_match_postings_and_max_doc() {
        let values = [
            Some("alpha"),
            None,
            Some(""),
            Some("nul\0byte"),
            Some("ngram测试"),
            None,
        ];
        let doc_ids = [40, 41, 42, 100, 101, 109];
        let rows: Vec<_> = doc_ids
            .into_iter()
            .zip(values)
            .map(|(doc_id, value)| NgramRow { doc_id, value })
            .collect();
        let regular_dir = TempDir::new().unwrap();
        let direct_dir = TempDir::new().unwrap();

        build_regular_lightweight_document_index(regular_dir.path(), &rows);
        build_finished_ngram_index(direct_dir.path(), &rows);

        let regular = index_snapshot(regular_dir.path());
        let direct = index_snapshot(direct_dir.path());
        assert_eq!(direct, regular);
        assert_eq!(direct.segment_count, 1);
        assert_eq!(direct.max_docs, vec![110]);
    }

    fn build_lightweight_document_index(path: &Path, rows: &[NgramRow<'_>]) {
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            path.to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        writer.add_ngram_rows(rows).unwrap();
        writer.finish().unwrap();
    }

    fn assert_default_and_lightweight_equivalent(rows: &[NgramRow<'_>]) -> IndexSnapshot {
        let default_dir = TempDir::new().unwrap();
        let lightweight_dir = TempDir::new().unwrap();
        build_default_document_index(default_dir.path(), rows);
        build_lightweight_document_index(lightweight_dir.path(), rows);

        let default_snapshot = index_snapshot(default_dir.path());
        let lightweight_snapshot = index_snapshot(lightweight_dir.path());
        assert_eq!(default_snapshot, lightweight_snapshot);
        lightweight_snapshot
    }

    #[test]
    fn lightweight_documents_match_default_documents_around_batch_boundaries() {
        for count in [
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE - 1,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE + 1,
        ] {
            let values: Vec<_> = (0..count)
                .map(|index| format!("row-{index:02}-测试"))
                .collect();
            let rows: Vec<_> = values
                .iter()
                .enumerate()
                .map(|(index, value)| NgramRow {
                    doc_id: 20 + index as u32,
                    value: Some(value.as_str()),
                })
                .collect();

            let snapshot = assert_default_and_lightweight_equivalent(&rows);
            assert_eq!(snapshot.segment_count, 1);
            assert_eq!(snapshot.max_docs, vec![20 + count as u32]);
        }
    }

    #[test]
    fn lightweight_documents_preserve_gaps_empty_values_and_trailing_absent_rows() {
        let values = [
            Some("alpha"),
            None,
            Some(""),
            Some("nul\0byte"),
            Some("ngram测试"),
            None,
        ];
        let doc_ids = [40, 41, 42, 100, 101, 109];
        let rows: Vec<_> = doc_ids
            .into_iter()
            .zip(values)
            .map(|(doc_id, value)| NgramRow { doc_id, value })
            .collect();

        let snapshot = assert_default_and_lightweight_equivalent(&rows);
        assert_eq!(snapshot.segment_count, 1);
        assert_eq!(snapshot.max_docs, vec![110]);
    }

    #[test]
    fn lightweight_documents_own_text_after_input_rows_are_dropped() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        {
            let values = [String::from("alpha测试"), String::from("nul\0byte")];
            let rows = [
                NgramRow {
                    doc_id: 3,
                    value: Some(values[0].as_str()),
                },
                NgramRow {
                    doc_id: 4,
                    value: Some(values[1].as_str()),
                },
            ];
            writer.add_ngram_rows(&rows).unwrap();
        }
        writer.finish().unwrap();

        let snapshot = index_snapshot(dir.path());
        assert_eq!(snapshot.max_docs, vec![5]);
        assert!(snapshot.postings.contains_key(b"al".as_slice()));
    }

    #[test]
    fn batched_ngram_documents_match_per_row_postings_and_max_doc() {
        let values: Vec<_> = (0..65)
            .map(|index| (100 + index, Some(format!("row-{index:02}-alpha"))))
            .chain([
                (200, Some(String::new())),
                (201, Some("nul\0byte".to_string())),
                (205, Some("ngram测试".to_string())),
                (209, None),
            ])
            .collect();
        let mut rows: Vec<_> = values
            .iter()
            .map(|(doc_id, value)| NgramRow {
                doc_id: *doc_id,
                value: value.as_deref(),
            })
            .collect();
        rows[1].value = None;
        rows[31].value = Some("");

        let reference_dir = TempDir::new().unwrap();
        let batched_dir = TempDir::new().unwrap();
        build_default_document_index(reference_dir.path(), &rows);
        let mut batched = IndexWriterWrapper::create_ngram_writer(
            "test",
            batched_dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        batched.add_ngram_rows(&rows[..33]).unwrap();
        batched.add_ngram_rows(&rows[33..65]).unwrap();
        batched.add_ngram_rows(&rows[65..]).unwrap();
        batched.finish().unwrap();

        assert_eq!(
            index_snapshot(reference_dir.path()),
            index_snapshot(batched_dir.path())
        );
    }

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

        let values = [
            "university",
            "anthropology",
            "economics",
            "history",
            "victoria",
            "basics",
            "economiCs",
        ];
        let rows: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(doc_id, value)| NgramRow {
                doc_id: doc_id as u32,
                value: Some(value),
            })
            .collect();
        writer.add_ngram_rows(&rows).unwrap();

        writer.finish().unwrap();

        let reader = open_finished_ngram_reader(dir.path());
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

        let values = [
            "ngram测试",
            "测试ngram",
            "测试ngram测试",
            "你好世界",
            "ngram需要被测试",
        ];
        let rows: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(doc_id, value)| NgramRow {
                doc_id: doc_id as u32,
                value: Some(value),
            })
            .collect();
        writer.add_ngram_rows(&rows).unwrap();

        writer.finish().unwrap();

        let reader = open_finished_ngram_reader(dir.path());
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
}
