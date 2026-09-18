use std::sync::Arc;

use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
use tantivy::{Index, IndexSettings};

use crate::error::{Result, TantivyBindingError};
use crate::index_ngram_document::{NgramBatchData, NgramDocument};
use crate::index_reader::IndexReaderWrapper;
use crate::index_reader_c::SetBitsetFn;
use crate::index_writer::IndexWriterWrapper;
use crate::index_writer_batch::submit_with_doc_ids;

#[derive(Clone, Copy)]
pub(crate) struct NgramRow<'a> {
    pub(crate) doc_id: u32,
    pub(crate) value: Option<&'a str>,
}

pub(crate) struct NgramIndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) index_writer: tantivy::IndexWriter<NgramDocument>,
    index: Arc<Index>,
}

impl NgramIndexWriterWrapperImpl {
    pub(crate) fn create_reader(&self, set_bitset: SetBitsetFn) -> Result<IndexReaderWrapper> {
        IndexReaderWrapper::from_index(self.index.clone(), set_bitset)
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.index_writer.commit()?;
        Ok(())
    }

    pub(crate) fn manual_merge(&mut self) -> Result<()> {
        let metas = self.index_writer.index().searchable_segment_metas()?;
        let policy = self.index_writer.get_merge_policy();
        for candidate in policy.compute_merge_candidates(&metas) {
            self.index_writer.merge(&candidate.0).wait()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        self.index_writer.commit()?;
        futures::executor::block_on(self.index_writer.garbage_collect_files())?;
        self.index_writer.wait_merging_threads()?;
        let metas = self.index.searchable_segment_metas()?;
        let segment_ids: Vec<_> = metas.iter().map(|meta| meta.id().uuid_string()).collect();
        log::info!("tantivy index_writer finish, segments: {:?}", segment_ids);
        Ok(())
    }
}

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
    // Use the regular V7 writer with lightweight NGRAM documents.
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

        // NGRAM has no stored fields; compress its docstore blocks inline.
        let settings = IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..Default::default()
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_dir(path)?;
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        // Ngram writers are only used for sealed index builds. Keep
        // memory-budget-flushed segments and avoid background merge write
        // amplification.
        index_writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

        Ok(IndexWriterWrapper::NgramV7(NgramIndexWriterWrapperImpl {
            field,
            index_writer,
            index: Arc::new(index),
        }))
    }
}

impl IndexWriterWrapper {
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
                TantivyBindingError::InternalError(
                    "ngram batch text byte count overflows usize".to_string(),
                )
            })
        })?;
        u32::try_from(total_bytes).map_err(|_| {
            TantivyBindingError::InternalError(
                "ngram batch text exceeds the u32 document range".to_string(),
            )
        })?;
        let mut validated_start = 0u32;
        for row in rows {
            if let Some(value) = row.value {
                if validated_start == u32::MAX {
                    return Err(TantivyBindingError::InternalError(
                        "ngram present value starts at the reserved absent offset".to_string(),
                    ));
                }
                validated_start =
                    validated_start
                        .checked_add(value.len() as u32)
                        .ok_or_else(|| {
                            TantivyBindingError::InternalError(
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
        let documents = rows
            .iter()
            .map(move |row| {
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
                document
            })
            .collect();
        let doc_ids: Vec<_> = rows.iter().map(|row| row.doc_id).collect();
        submit_with_doc_ids(&writer.index_writer, &doc_ids, documents)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        ffi::c_void,
        path::Path,
    };

    use tantivy::{schema::IndexRecordOption, DocSet, Index, TantivyDocument, TERMINATED};
    use tempfile::TempDir;

    use super::{build_ngram_schema, NgramRow, NGRAM_TOKENIZER};
    use crate::index_writer_batch::DOCUMENT_BATCH_SIZE;
    use crate::{index_writer::IndexWriterWrapper, util::set_bitset};

    #[derive(Debug, Eq, PartialEq)]
    struct IndexSnapshot {
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
        for segment in searcher.segment_readers() {
            max_docs.push(segment.max_doc());
            let inverted = segment.inverted_index(field).unwrap();
            let mut terms = inverted.terms().stream().unwrap();
            while terms.advance() {
                let docs = postings.entry(terms.key().to_vec()).or_default();
                let mut term_postings = inverted
                    .read_postings_from_terminfo(terms.value(), IndexRecordOption::Basic)
                    .unwrap();
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
        IndexSnapshot { max_docs, postings }
    }

    // Reference the old per-document path, independently of the new batching
    // helper, and compare every posting rather than only a few query results.
    fn build_default_document_index(path: &Path, rows: &[NgramRow<'_>]) {
        let (schema, field) = build_ngram_schema("test");
        let index = Index::create_in_dir(path, schema).unwrap();
        index.tokenizers().register(
            NGRAM_TOKENIZER,
            tantivy::tokenizer::NgramTokenizer::new(2, 3, false).unwrap(),
        );
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 15_000_000)
            .unwrap();
        writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
        for row in rows {
            let mut document = TantivyDocument::default();
            if let Some(value) = row.value {
                document.add_text(field, value);
            }
            writer
                .add_document_with_doc_id(row.doc_id, document)
                .unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
    }

    #[test]
    fn lightweight_batches_match_default_postings_and_max_doc() {
        let values = [
            Some("alpha"),
            None,
            Some(""),
            Some("nul\0byte"),
            Some("中文测试"),
        ];
        for count in [
            DOCUMENT_BATCH_SIZE - 1,
            DOCUMENT_BATCH_SIZE,
            DOCUMENT_BATCH_SIZE + 1,
        ] {
            let mut rows: Vec<_> = (0..count)
                .map(|i| NgramRow {
                    // Exercise both contiguous runs and a gap in a batch.
                    doc_id: 40 + i as u32 + if i >= DOCUMENT_BATCH_SIZE { 7 } else { 0 },
                    value: values[i % values.len()],
                })
                .collect();
            rows.last_mut().unwrap().value = None;
            let reference_dir = TempDir::new().unwrap();
            build_default_document_index(reference_dir.path(), &rows);

            let batch_dir = TempDir::new().unwrap();
            let mut writer = IndexWriterWrapper::create_ngram_writer(
                "test",
                batch_dir.path().to_str().unwrap(),
                2,
                3,
                1,
                15_000_000,
            )
            .unwrap();
            writer.add_ngram_rows(&rows).unwrap();
            writer.finish().unwrap();

            let snapshot = index_snapshot(batch_dir.path());
            assert_eq!(snapshot, index_snapshot(reference_dir.path()));
            assert_eq!(snapshot.max_docs, [rows.last().unwrap().doc_id + 1]);
        }
    }

    #[test]
    fn ngram_batch_validates_all_ids_before_submission() {
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
        let mut rows: Vec<_> = (0..=DOCUMENT_BATCH_SIZE)
            .map(|i| NgramRow {
                doc_id: i as u32,
                value: Some("discarded"),
            })
            .collect();
        for invalid_id in [0, tantivy::indexer::merger::MAX_DOC_LIMIT - 1] {
            rows.last_mut().unwrap().doc_id = invalid_id;
            assert!(writer.add_ngram_rows(&rows).is_err());
        }
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 0,
                value: Some("recovered"),
            }])
            .unwrap();
        writer.finish().unwrap();
        let snapshot = index_snapshot(dir.path());
        assert_eq!(snapshot.max_docs, [1]);
        assert_eq!(snapshot.postings.get(b"re".as_slice()), Some(&vec![0]));
        assert!(!snapshot.postings.contains_key(b"di".as_slice()));
    }

    #[test]
    fn ngram_finish_disables_dedicated_docstore_compression() {
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
        writer.finish().unwrap();
        let index = Index::open_in_dir(dir.path()).unwrap();
        assert!(!index.settings().docstore_compress_dedicated_thread);
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

        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 0,
                value: Some("university"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 1,
                value: Some("anthropology"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 2,
                value: Some("economics"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 3,
                value: Some("history"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 4,
                value: Some("victoria"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 5,
                value: Some("basics"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 6,
                value: Some("economiCs"),
            }])
            .unwrap();

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

        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 0,
                value: Some("ngram测试"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 1,
                value: Some("测试ngram"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 2,
                value: Some("测试ngram测试"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 3,
                value: Some("你好世界"),
            }])
            .unwrap();
        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 4,
                value: Some("ngram需要被测试"),
            }])
            .unwrap();

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
}
