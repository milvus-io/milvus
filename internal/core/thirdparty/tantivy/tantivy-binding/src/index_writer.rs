use index_writer_v5::TantivyDocumentV5;
use index_writer_v7::TantivyDocumentV7;
use libc::c_char;

use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_ngram_writer::NgramIndexWriterWrapperImpl;
use crate::index_reader::IndexReaderWrapper;
use crate::index_reader_c::SetBitsetFn;
use crate::log::init_log;
use crate::util::ptr_len_to_str;
use crate::{index_writer_v5, index_writer_v7, TantivyIndexVersion};

pub trait TantivyValue<D> {
    fn add_to_document(&self, field: u32, document: &mut D);
}

pub(crate) fn row_value_to_str<'a>(ptr: *const u8, len: usize) -> Result<&'a str> {
    if ptr.is_null() {
        if len == 0 {
            return Ok("");
        }
        return Err(TantivyBindingError::InvalidArgument(
            "string value pointer is null for a nonempty value".to_string(),
        ));
    }
    ptr_len_to_str(ptr, len)
}

fn validate_row_layout(value_count: usize, row_offsets: &[usize], doc_ids: &[i64]) -> Result<()> {
    if row_offsets.len() != doc_ids.len() + 1 {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "row offset count {} must equal row count {} plus one",
            row_offsets.len(),
            doc_ids.len()
        )));
    }
    if row_offsets.first().copied() != Some(0) {
        return Err(TantivyBindingError::InvalidArgument(
            "row offsets must start at zero".to_string(),
        ));
    }
    if row_offsets.last().copied() != Some(value_count) {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "row offsets must end at value count {value_count}"
        )));
    }
    if row_offsets.windows(2).any(|window| window[0] > window[1]) {
        return Err(TantivyBindingError::InvalidArgument(
            "row offsets must be nondecreasing".to_string(),
        ));
    }
    if doc_ids.iter().any(|doc_id| *doc_id < 0) {
        return Err(TantivyBindingError::InvalidArgument(
            "document IDs must be nonnegative".to_string(),
        ));
    }
    if doc_ids.windows(2).any(|window| window[0] >= window[1]) {
        return Err(TantivyBindingError::InvalidArgument(
            "document IDs must be strictly increasing within a batch".to_string(),
        ));
    }
    Ok(())
}

pub enum IndexWriterWrapper {
    V5(IndexWriterState<index_writer_v5::IndexWriterWrapperImpl>),
    V7(IndexWriterState<index_writer_v7::IndexWriterWrapperImpl>),
    NgramV7(NgramIndexWriterWrapperImpl),
}

pub(crate) struct IndexWriterState<W> {
    writer: W,
    last_row_batch_doc_id: Option<i64>,
}

impl<W> IndexWriterState<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            last_row_batch_doc_id: None,
        }
    }

    fn validate_row_batch_doc_ids(&self, doc_ids: &[i64]) -> Result<()> {
        if let (Some(previous), Some(first)) = (self.last_row_batch_doc_id, doc_ids.first()) {
            if *first <= previous {
                return Err(TantivyBindingError::InvalidArgument(
                    "document IDs must increase across row batches".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn record_row_batch_doc_ids(&mut self, doc_ids: &[i64]) {
        self.last_row_batch_doc_id = doc_ids.last().copied();
    }
}

pub enum FinishedIndex {
    V5(tantivy_5::Index),
    V7(tantivy::Index),
}

fn unsupported_ngram_operation(operation: &str) -> TantivyBindingError {
    TantivyBindingError::InternalError(format!(
        "{} is not supported by the NGRAM-specific Tantivy writer",
        operation
    ))
}

impl IndexWriterWrapper {
    pub fn new_direct(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        memory_budget: usize,
        tantivy_index_version: TantivyIndexVersion,
        user_specified_doc_id: bool,
    ) -> Result<IndexWriterWrapper> {
        init_log();
        match tantivy_index_version {
            TantivyIndexVersion::V5 => Ok(IndexWriterWrapper::V5(IndexWriterState::new(
                index_writer_v5::IndexWriterWrapperImpl::new_with_single_segment(
                    field_name,
                    data_type,
                    path,
                    memory_budget,
                    user_specified_doc_id,
                )?,
            ))),
            TantivyIndexVersion::V7 => Ok(IndexWriterWrapper::V7(IndexWriterState::new(
                index_writer_v7::IndexWriterWrapperImpl::new_with_single_segment(
                    field_name,
                    data_type,
                    path,
                    memory_budget,
                    user_specified_doc_id,
                )?,
            ))),
        }
    }
    // create a IndexWriterWrapper according to `tanviy_index_version`.
    // version 7 is the latest version and is what we should use in most cases.
    // We may also build with version 5 for compatibility for reader nodes with older versions.
    pub fn new(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        tanviy_index_version: TantivyIndexVersion,
        enable_user_specified_doc_id: bool,
        enable_background_merge: bool,
    ) -> Result<IndexWriterWrapper> {
        init_log();
        match tanviy_index_version {
            TantivyIndexVersion::V5 => {
                let writer = index_writer_v5::IndexWriterWrapperImpl::new(
                    field_name,
                    data_type,
                    path,
                    num_threads,
                    overall_memory_budget_in_bytes,
                    enable_background_merge,
                )?;
                Ok(IndexWriterWrapper::V5(IndexWriterState::new(writer)))
            }
            TantivyIndexVersion::V7 => {
                let writer = index_writer_v7::IndexWriterWrapperImpl::new(
                    field_name,
                    data_type,
                    path,
                    num_threads,
                    overall_memory_budget_in_bytes,
                    enable_user_specified_doc_id,
                    enable_background_merge,
                )?;
                Ok(IndexWriterWrapper::V7(IndexWriterState::new(writer)))
            }
        }
    }
    pub fn new_with_single_segment(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
    ) -> Result<IndexWriterWrapper> {
        Self::new_direct(
            field_name,
            data_type,
            path,
            15 * 1024 * 1024,
            TantivyIndexVersion::V5,
            false,
        )
    }

    pub fn create_reader(&self, set_bitset: SetBitsetFn) -> Result<IndexReaderWrapper> {
        match self {
            IndexWriterWrapper::V5(_) => {
                return Err(TantivyBindingError::InternalError(
                    "create reader with tantivy index version 5 
                is not supported from tantivy with version 7"
                        .into(),
                ));
            }
            IndexWriterWrapper::V7(state) => {
                if matches!(
                    &state.writer.index_writer,
                    index_writer_v7::index_writer::Writer::Direct(_)
                ) {
                    return Err(TantivyBindingError::InternalError(
                        "create reader from a direct V7 writer requires finish".to_string(),
                    ));
                }
                state.writer.create_reader(set_bitset)
            }
            IndexWriterWrapper::NgramV7(_) => {
                Err(unsupported_ngram_operation("create reader before finish"))
            }
        }
    }

    pub fn finish_index(self) -> Result<FinishedIndex> {
        match self {
            IndexWriterWrapper::V5(state) => state.writer.finish_index().map(FinishedIndex::V5),
            IndexWriterWrapper::V7(state) => state.writer.finish_index().map(FinishedIndex::V7),
            IndexWriterWrapper::NgramV7(writer) => writer.finish_index().map(FinishedIndex::V7),
        }
    }

    pub fn finish_and_create_reader(self, set_bitset: SetBitsetFn) -> Result<IndexReaderWrapper> {
        match self {
            IndexWriterWrapper::V5(_) => Err(TantivyBindingError::InternalError(
                "creating a reader from a Tantivy 5 writer is not supported".to_string(),
            )),
            writer => match writer.finish_index()? {
                FinishedIndex::V7(index) => {
                    IndexReaderWrapper::from_index(std::sync::Arc::new(index), set_bitset)
                }
                FinishedIndex::V5(_) => unreachable!("V5 writers are rejected before finish"),
            },
        }
    }

    pub fn add<T>(&mut self, data: T, offset: Option<i64>) -> Result<()>
    where
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocumentV7>,
    {
        match self {
            IndexWriterWrapper::V5(state) => state.writer.add(data, offset),
            IndexWriterWrapper::V7(state) => state.writer.add(data, offset.unwrap() as u32),
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("generic add")),
        }
    }

    pub fn add_batch<T, I>(&mut self, data: I, offset_begin: i64) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocumentV7>,
    {
        match self {
            IndexWriterWrapper::V5(state) => state.writer.add_batch(data, offset_begin),
            IndexWriterWrapper::V7(state) => state.writer.add_batch(data, offset_begin as u32),
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("generic batch add")),
        }
    }

    pub fn add_strings_with_len(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offset_begin: i64,
    ) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(state) => {
                state.writer.add_strings_with_len(ptrs, lens, offset_begin)
            }
            IndexWriterWrapper::V7(state) => {
                state
                    .writer
                    .add_strings_with_len(ptrs, lens, offset_begin as u32)
            }
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("string batch add")),
        }
    }

    pub fn add_rows<T, I>(
        &mut self,
        values: I,
        row_offsets: &[usize],
        doc_ids: &[i64],
    ) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocumentV7>,
    {
        let values = values.into_iter().collect::<Vec<_>>();
        validate_row_layout(values.len(), row_offsets, doc_ids)?;
        match self {
            IndexWriterWrapper::V5(state) => {
                state.validate_row_batch_doc_ids(doc_ids)?;
                state.writer.add_rows(&values, row_offsets, doc_ids)?;
                state.record_row_batch_doc_ids(doc_ids);
                Ok(())
            }
            IndexWriterWrapper::V7(state) => {
                state.validate_row_batch_doc_ids(doc_ids)?;
                state.writer.add_rows(&values, row_offsets, doc_ids)?;
                state.record_row_batch_doc_ids(doc_ids);
                Ok(())
            }
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("generic row add")),
        }
    }

    pub fn add_string_rows(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        row_offsets: &[usize],
        doc_ids: &[i64],
    ) -> Result<()> {
        if ptrs.len() != lens.len() {
            return Err(TantivyBindingError::InvalidArgument(
                "string value pointer and length counts differ".to_string(),
            ));
        }
        validate_row_layout(ptrs.len(), row_offsets, doc_ids)?;
        match self {
            IndexWriterWrapper::V5(state) => {
                state.validate_row_batch_doc_ids(doc_ids)?;
                state
                    .writer
                    .add_string_rows(ptrs, lens, row_offsets, doc_ids)?;
                state.record_row_batch_doc_ids(doc_ids);
                Ok(())
            }
            IndexWriterWrapper::V7(state) => {
                state.validate_row_batch_doc_ids(doc_ids)?;
                state
                    .writer
                    .add_string_rows(ptrs, lens, row_offsets, doc_ids)?;
                state.record_row_batch_doc_ids(doc_ids);
                Ok(())
            }
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("string row add")),
        }
    }

    pub fn add_json_rows(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        row_offsets: &[usize],
        doc_ids: &[i64],
    ) -> Result<()> {
        if ptrs.len() != lens.len() {
            return Err(TantivyBindingError::InvalidArgument(
                "JSON value pointer and length counts differ".to_string(),
            ));
        }
        validate_row_layout(ptrs.len(), row_offsets, doc_ids)?;
        if row_offsets
            .windows(2)
            .any(|window| window[1] - window[0] > 1)
        {
            return Err(TantivyBindingError::InvalidArgument(
                "a JSON row must contain zero or one serialized value".to_string(),
            ));
        }
        match self {
            IndexWriterWrapper::V5(state) => {
                state.validate_row_batch_doc_ids(doc_ids)?;
                state
                    .writer
                    .add_json_rows(ptrs, lens, row_offsets, doc_ids)?;
                state.record_row_batch_doc_ids(doc_ids);
                Ok(())
            }
            IndexWriterWrapper::V7(state) => {
                state.validate_row_batch_doc_ids(doc_ids)?;
                state
                    .writer
                    .add_json_rows(ptrs, lens, row_offsets, doc_ids)?;
                state.record_row_batch_doc_ids(doc_ids);
                Ok(())
            }
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("JSON row add")),
        }
    }

    pub fn add_array<T, I>(&mut self, data: I, offset: Option<i64>) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocumentV7>,
    {
        match self {
            IndexWriterWrapper::V5(state) => state.writer.add_array(data, offset),
            IndexWriterWrapper::V7(state) => state.writer.add_array(data, offset.unwrap() as u32),
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("generic array add")),
        }
    }

    pub fn add_json(&mut self, data: &str, offset: Option<i64>) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(_) => {
                return Err(TantivyBindingError::InternalError(
                    "add json with tantivy index version 5 is not supported from tantivy with version 7"
                        .into(),
                ));
            }
            IndexWriterWrapper::V7(state) => state.writer.add_json(data, offset.unwrap() as u32),
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("JSON add")),
        }
    }

    pub fn add_json_batch(&mut self, datas: &[*const c_char], offset_begin: i64) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(_) => {
                return Err(TantivyBindingError::InternalError(
                    "add json batch with tantivy index version 5 is not supported".into(),
                ));
            }
            IndexWriterWrapper::V7(state) => {
                state.writer.add_json_batch(datas, offset_begin as u32)
            }
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("JSON batch add")),
        }
    }

    pub fn add_array_json(&mut self, datas: &[*const c_char], offset: Option<i64>) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(_) => {
                return Err(TantivyBindingError::InternalError(
                    "add array json with tantivy index version 5 is not supported from tantivy with version 7"
                        .into(),
                ));
            }
            IndexWriterWrapper::V7(state) => {
                state.writer.add_array_json(datas, offset.unwrap() as u32)
            }
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("JSON array add")),
        }
    }

    pub fn add_array_keywords(
        &mut self,
        datas: &[*const c_char],
        offset: Option<i64>,
    ) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(state) => state.writer.add_array_keywords(datas, offset),
            IndexWriterWrapper::V7(state) => state
                .writer
                .add_array_keywords(datas, offset.unwrap() as u32),
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("keyword array add")),
        }
    }

    pub fn add_array_keywords_with_len(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offset: Option<i64>,
    ) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(state) => {
                state.writer.add_array_keywords_with_len(ptrs, lens, offset)
            }
            IndexWriterWrapper::V7(state) => {
                state
                    .writer
                    .add_array_keywords_with_len(ptrs, lens, offset.unwrap() as u32)
            }
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation(
                "length-delimited keyword array add",
            )),
        }
    }

    pub fn add_json_key_stats(
        &mut self,
        keys: &[*const c_char],
        json_offsets: &[*const i64],
        json_offsets_len: &[usize],
    ) -> Result<()> {
        assert!(keys.len() == json_offsets.len());
        assert!(keys.len() == json_offsets_len.len());
        match self {
            IndexWriterWrapper::V5(state) => {
                state
                    .writer
                    .add_json_key_stats(keys, json_offsets, json_offsets_len)
            }
            IndexWriterWrapper::V7(state) => {
                state
                    .writer
                    .add_json_key_stats(keys, json_offsets, json_offsets_len)
            }
            IndexWriterWrapper::NgramV7(_) => {
                Err(unsupported_ngram_operation("JSON key stats add"))
            }
        }
    }

    #[allow(dead_code)]
    pub fn manual_merge(&mut self) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(state) => state.writer.manual_merge(),
            IndexWriterWrapper::V7(state) => state.writer.manual_merge(),
            IndexWriterWrapper::NgramV7(_) => Err(unsupported_ngram_operation("manual merge")),
        }
    }

    #[allow(dead_code)]
    pub fn commit(&mut self) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(state) => state.writer.commit(),
            IndexWriterWrapper::V7(state) => state.writer.commit(),
            IndexWriterWrapper::NgramV7(_) => {
                Err(unsupported_ngram_operation("commit before finish"))
            }
        }
    }

    #[allow(dead_code)]
    pub fn finish(self) -> Result<()> {
        self.finish_index().map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::CString, ops::Bound};

    use rand::Rng;
    use tempfile::{tempdir, TempDir};

    use crate::{
        data_type::TantivyDataType, error::TantivyBindingError, util::set_bitset,
        TantivyIndexVersion,
    };

    use super::IndexWriterWrapper;

    #[test]
    fn test_v7_direct_writer_finishes_one_segment() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            true,
        )
        .unwrap();

        writer.add::<i64>(7, Some(3)).unwrap();
        writer.add::<i64>(9, Some(8)).unwrap();
        writer.finish().unwrap();

        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        let segments = index.searchable_segment_metas().unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].max_doc(), 9);
    }

    #[test]
    fn test_v7_direct_writer_rejects_reader_before_finish() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            true,
        )
        .unwrap();
        writer.add::<i64>(7, Some(3)).unwrap();

        let Err(TantivyBindingError::InternalError(message)) = writer.create_reader(set_bitset)
        else {
            panic!("direct V7 writer must reject create_reader before finish");
        };
        assert!(message.contains("direct"));
        assert!(message.contains("finish"));
    }

    #[test]
    fn test_v7_direct_batch_add_preserves_sparse_document_ids() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            true,
        )
        .unwrap();

        writer.add_batch([11_i64, 22, 33], 5).unwrap();
        writer.finish().unwrap();

        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        let segments = index.searchable_segment_metas().unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].max_doc(), 8);

        let reader = index.reader().unwrap();
        let field = index.schema().get_field("number").unwrap();
        let query = tantivy::query::TermQuery::new(
            tantivy::Term::from_field_i64(field, 22),
            tantivy::schema::IndexRecordOption::Basic,
        );
        let hits = reader
            .searcher()
            .search(&query, &tantivy::collector::TopDocs::with_limit(1))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1.doc_id, 6);
    }

    #[test]
    fn test_v7_direct_primitive_rows_preserve_sparse_ids_and_empty_documents() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            true,
        )
        .unwrap();

        writer
            .add_rows([11_i64, 22, 33], &[0, 1, 1, 3], &[2, 5, 9])
            .unwrap();
        writer.finish().unwrap();

        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        let segment = index.searchable_segment_metas().unwrap();
        assert_eq!(segment.len(), 1);
        assert_eq!(segment[0].max_doc(), 10);

        let reader = index.reader().unwrap();
        let field = index.schema().get_field("number").unwrap();
        let query = tantivy::query::TermQuery::new(
            tantivy::Term::from_field_i64(field, 33),
            tantivy::schema::IndexRecordOption::Basic,
        );
        let hits = reader
            .searcher()
            .search(&query, &tantivy::collector::TopDocs::with_limit(1))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1.doc_id, 9);
    }

    #[test]
    fn test_v7_direct_string_rows_distinguish_null_empty_and_embedded_nul() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "keyword",
            TantivyDataType::Keyword,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            true,
        )
        .unwrap();
        let values = [b"nul\0inside".as_slice()];
        let ptrs = vec![std::ptr::null(), values[0].as_ptr()];
        let lens = vec![0, values[0].len()];

        writer
            .add_string_rows(&ptrs, &lens, &[0, 0, 1, 2], &[1, 4, 7])
            .unwrap();
        writer.finish().unwrap();

        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        let reader = index.reader().unwrap();
        let field = index.schema().get_field("keyword").unwrap();
        for (value, expected_doc_id) in [("", 4), ("nul\0inside", 7)] {
            let query = tantivy::query::TermQuery::new(
                tantivy::Term::from_field_text(field, value),
                tantivy::schema::IndexRecordOption::Basic,
            );
            let hits = reader
                .searcher()
                .search(&query, &tantivy::collector::TopDocs::with_limit(1))
                .unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].1.doc_id, expected_doc_id);
        }
    }

    #[test]
    fn test_v7_direct_json_rows_accept_empty_and_single_value_rows() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "json",
            TantivyDataType::JSON,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            false,
        )
        .unwrap();
        let values = [br#"{"key":"value"}"#.as_slice()];
        let ptrs = values
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        let lens = values.iter().map(|value| value.len()).collect::<Vec<_>>();

        writer
            .add_json_rows(&ptrs, &lens, &[0, 0, 1], &[3, 8])
            .unwrap();
        writer.finish().unwrap();

        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        assert_eq!(index.searchable_segment_metas().unwrap()[0].max_doc(), 2);
        let reader = index.reader().unwrap();
        let doc_ids = reader
            .searcher()
            .segment_reader(0)
            .fast_fields()
            .i64("doc_id")
            .unwrap();
        assert_eq!(doc_ids.first(0), Some(3));
        assert_eq!(doc_ids.first(1), Some(8));

        let mut matches = std::collections::HashSet::new();
        let wrapper = crate::index_reader::IndexReaderWrapper::from_index(
            std::sync::Arc::new(index),
            set_bitset,
        )
        .unwrap();
        wrapper
            .json_term_query_keyword(
                "key",
                "value",
                &mut matches as *mut _ as *mut std::ffi::c_void,
            )
            .unwrap();
        assert_eq!(matches, std::collections::HashSet::from([8]));
    }

    #[test]
    fn test_v5_direct_json_rows_preserve_empty_documents_and_explicit_ids_across_batches() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "json",
            TantivyDataType::JSON,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V5,
            false,
        )
        .unwrap();
        let first_batch_values = [br#"{"key":"first"}"#.as_slice()];
        let first_batch_ptrs = first_batch_values
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        let first_batch_lens = first_batch_values
            .iter()
            .map(|value| value.len())
            .collect::<Vec<_>>();

        writer
            .add_json_rows(&first_batch_ptrs, &first_batch_lens, &[0, 0, 1], &[3, 8])
            .unwrap();

        let second_batch_values = [br#"{"key":"second"}"#.as_slice()];
        let second_batch_ptrs = second_batch_values
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        let second_batch_lens = second_batch_values
            .iter()
            .map(|value| value.len())
            .collect::<Vec<_>>();
        writer
            .add_json_rows(&second_batch_ptrs, &second_batch_lens, &[0, 1], &[13])
            .unwrap();
        assert!(writer
            .add_json_rows(&second_batch_ptrs, &second_batch_lens, &[0, 1], &[13],)
            .is_err());

        writer.finish().unwrap();

        let index = tantivy_5::Index::open_in_dir(dir.path()).unwrap();
        let segments = index.searchable_segment_metas().unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].max_doc(), 3);
        let reader = index.reader().unwrap();
        let doc_ids = reader
            .searcher()
            .segment_reader(0)
            .fast_fields()
            .i64("doc_id")
            .unwrap();
        assert_eq!(doc_ids.first(0), Some(3));
        assert_eq!(doc_ids.first(1), Some(8));
        assert_eq!(doc_ids.first(2), Some(13));

        let query = tantivy_5::query::QueryParser::for_index(&index, Vec::new())
            .parse_query("json.key:first")
            .unwrap();
        let hits = reader
            .searcher()
            .search(&query, &tantivy_5::collector::TopDocs::with_limit(1))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(doc_ids.first(hits[0].1.doc_id), Some(8));
    }

    #[test]
    fn test_row_batches_reject_invalid_layouts_and_document_ids() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            true,
        )
        .unwrap();

        assert!(writer.add_rows([1_i64], &[1, 1], &[0]).is_err());
        assert!(writer.add_rows([1_i64], &[0, 2], &[0]).is_err());
        assert!(writer.add_rows([1_i64], &[0, 1], &[-1]).is_err());
        assert!(writer.add_rows([1_i64, 2], &[0, 1, 2], &[4, 4]).is_err());
        assert!(writer
            .add_rows([1_i64], &[0, 1], &[i64::from(u32::MAX) + 1])
            .is_err());
    }

    #[test]
    fn test_v5_direct_writer_rejects_commit_and_manual_merge() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V5,
            false,
        )
        .unwrap();

        assert!(writer.commit().is_err());
        assert!(writer.manual_merge().is_err());
    }

    #[test]
    fn test_v5_finish_and_create_reader_rejects_without_finalizing() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V5,
            false,
        )
        .unwrap();
        writer.add::<i64>(7, Some(0)).unwrap();

        assert!(writer.finish_and_create_reader(set_bitset).is_err());

        let index = tantivy_5::Index::open_in_dir(dir.path()).unwrap();
        assert!(index.searchable_segment_metas().unwrap().is_empty());
    }

    #[test]
    fn test_row_batches_reject_nonincreasing_document_ids_across_batches() {
        for (version, direct) in [
            (TantivyIndexVersion::V5, true),
            (TantivyIndexVersion::V5, false),
            (TantivyIndexVersion::V7, true),
            (TantivyIndexVersion::V7, false),
        ] {
            let dir = TempDir::new().unwrap();
            let mut writer = if direct {
                IndexWriterWrapper::new_direct(
                    "number",
                    TantivyDataType::I64,
                    dir.path().to_str().unwrap().to_string(),
                    15_000_000,
                    version,
                    false,
                )
                .unwrap()
            } else {
                IndexWriterWrapper::new(
                    "number",
                    TantivyDataType::I64,
                    dir.path().to_str().unwrap().to_string(),
                    1,
                    15_000_000,
                    version,
                    false,
                    false,
                )
                .unwrap()
            };

            writer.add_rows([1_i64], &[0, 1], &[5]).unwrap();
            assert!(writer.add_rows([2_i64], &[0, 1], &[5]).is_err());
            assert!(writer.add_rows([3_i64], &[0, 1], &[4]).is_err());
        }
    }

    #[test]
    fn test_v7_direct_string_batch_accepts_embedded_nul() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "keyword",
            TantivyDataType::Keyword,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V7,
            true,
        )
        .unwrap();
        let values = [b"plain".as_slice(), b"nul\0inside".as_slice()];
        let ptrs = values
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        let lens = values.iter().map(|value| value.len()).collect::<Vec<_>>();

        writer.add_strings_with_len(&ptrs, &lens, 2).unwrap();
        writer.finish().unwrap();

        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        let reader = index.reader().unwrap();
        let field = index.schema().get_field("keyword").unwrap();
        let query = tantivy::query::TermQuery::new(
            tantivy::Term::from_field_text(field, "nul\0inside"),
            tantivy::schema::IndexRecordOption::Basic,
        );
        let hits = reader
            .searcher()
            .search(&query, &tantivy::collector::TopDocs::with_limit(1))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1.doc_id, 3);
    }

    #[test]
    fn test_v7_direct_json_key_stats_batches_non_explicit_documents() {
        let temp_dir = tempdir().unwrap();
        let mut writer = IndexWriterWrapper::create_json_key_stats_writer(
            "test",
            temp_dir.path().to_str().unwrap(),
            1,
            15 * 1024 * 1024,
            TantivyIndexVersion::V7,
            false,
            true,
        )
        .unwrap();
        let key = CString::new("/shared").unwrap();
        let offsets = [11_i64, 42_i64, 77_i64];
        writer
            .add_json_key_stats(&[key.as_ptr()], &[offsets.as_ptr()], &[offsets.len()])
            .unwrap();
        writer.finish().unwrap();

        let index = tantivy::Index::open_in_dir(temp_dir.path()).unwrap();
        assert_eq!(index.searchable_segment_metas().unwrap().len(), 1);
        let reader = index.reader().unwrap();
        let field = index.schema().get_field("test").unwrap();
        let query = tantivy::query::TermQuery::new(
            tantivy::Term::from_field_text(field, "/shared"),
            tantivy::schema::IndexRecordOption::Basic,
        );
        let hits = reader
            .searcher()
            .search(&query, &tantivy::collector::Count)
            .unwrap();
        assert_eq!(hits, offsets.len());
    }

    #[test]
    fn test_v5_direct_writer_finishes_one_segment() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new_direct(
            "number",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_string(),
            15_000_000,
            TantivyIndexVersion::V5,
            false,
        )
        .unwrap();

        writer.add::<i64>(7, Some(0)).unwrap();
        writer.add::<i64>(9, Some(1)).unwrap();
        writer.finish().unwrap();

        let index = tantivy_5::Index::open_in_dir(dir.path()).unwrap();
        let segments = index.searchable_segment_metas().unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].max_doc(), 2);
    }

    #[test]
    fn test_v7_in_memory_direct_writer_finishes_with_reader() {
        let mut writer = IndexWriterWrapper::create_text_writer(
            "text",
            "",
            "milvus_tokenizer",
            "{}",
            "",
            1,
            15_000_000,
            true,
            false,
            true,
            TantivyIndexVersion::V7,
        )
        .unwrap();
        writer.add::<&str>("hello world", Some(0)).unwrap();

        let reader = writer.finish_and_create_reader(set_bitset).unwrap();

        assert_eq!(reader.count().unwrap(), 1);
        assert_eq!(reader.index.searchable_segment_metas().unwrap().len(), 1);
    }

    #[test]
    fn test_v7_growing_text_writer_remains_regular() {
        let mut writer = IndexWriterWrapper::create_text_writer(
            "text",
            "",
            "milvus_tokenizer",
            "{}",
            "",
            1,
            15_000_000,
            true,
            true,
            false,
            TantivyIndexVersion::V7,
        )
        .unwrap();
        writer.add::<&str>("hello world", Some(0)).unwrap();

        writer.commit().unwrap();
        let reader = writer.create_reader(set_bitset).unwrap();

        assert_eq!(reader.count().unwrap(), 1);
    }

    #[test]
    fn test_build_index_version5() {
        let field_name = "number";
        let data_type = TantivyDataType::I64;
        let dir = TempDir::new().unwrap();

        {
            let mut index_wrapper = IndexWriterWrapper::new(
                field_name,
                data_type,
                dir.path().to_str().unwrap().to_string(),
                1,
                50_000_000,
                TantivyIndexVersion::V5,
                false,
                false,
            )
            .unwrap();

            for i in 0..10 {
                index_wrapper.add::<i64>(i, Some(i as i64)).unwrap();
            }
            index_wrapper.commit().unwrap();
        }

        use tantivy_5::{query, Index, ReloadPolicy};
        let index = Index::open_in_dir(dir.path()).unwrap();
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let query = query::RangeQuery::new_i64_bounds(
            field_name.to_string(),
            Bound::Included(0),
            Bound::Included(9),
        );
        let res = reader
            .searcher()
            .search(&query, &tantivy_5::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(res.len(), 10);
    }

    #[test]
    fn test_build_index_version5_single_segment() {
        let field_name = "number";
        let data_type = TantivyDataType::I64;
        let dir = TempDir::new().unwrap();

        {
            let mut index_wrapper = IndexWriterWrapper::new_with_single_segment(
                field_name,
                data_type,
                dir.path().to_str().unwrap().to_string(),
            )
            .unwrap();

            for i in 0..10 {
                index_wrapper.add::<i64>(i, None).unwrap();
            }
            index_wrapper.finish().unwrap();
        }

        use tantivy_5::{collector, query, Index, ReloadPolicy};
        let index = Index::open_in_dir(dir.path()).unwrap();
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let query = query::RangeQuery::new_i64_bounds(
            field_name.to_string(),
            Bound::Included(0),
            Bound::Included(9),
        );
        let res = reader
            .searcher()
            .search(&query, &collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(res.len(), 10);
    }

    #[test]
    fn test_build_text_index_version5() {
        let field_name = "text";
        let dir = TempDir::new().unwrap();

        {
            let mut index_wrapper = IndexWriterWrapper::create_text_writer(
                field_name,
                dir.path().to_str().unwrap(),
                "default",
                "",
                "",
                1,
                50_000_000,
                false,
                false,
                false,
                TantivyIndexVersion::V5,
            )
            .unwrap();

            for i in 0..10 {
                index_wrapper.add("hello", Some(i as i64)).unwrap();
            }
            index_wrapper.commit().unwrap();
        }

        use tantivy_5::{collector, query, schema, Index, ReloadPolicy, Term};
        let index = Index::open_in_dir(dir.path()).unwrap();
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let text = index.schema().get_field("text").unwrap();
        let query = query::TermQuery::new(
            Term::from_field_text(text, "hello"),
            schema::IndexRecordOption::Basic,
        );
        let res = reader
            .searcher()
            .search(&query, &collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(res.len(), 10);
    }

    #[test]
    pub fn test_add_json_key_stats() {
        use crate::index_writer::IndexWriterWrapper;

        let temp_dir = tempdir().unwrap();
        let mut index_writer = IndexWriterWrapper::create_json_key_stats_writer(
            "test",
            temp_dir.path().to_str().unwrap(),
            1,
            15 * 1024 * 1024,
            TantivyIndexVersion::V7,
            false,
            false,
        )
        .unwrap();

        let keys = (0..100).map(|i| format!("key{:05}", i)).collect::<Vec<_>>();
        let mut total_count = 0;
        let mut rng = rand::thread_rng();
        let json_offsets: Vec<Vec<i64>> = (0..100)
            .map(|_| {
                let count = rng.random_range(0..1000);
                total_count += count;
                (0..count)
                    .map(|_| rng.random_range(0..i64::MAX))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let json_offsets_len = json_offsets
            .iter()
            .map(|offsets| offsets.len())
            .collect::<Vec<_>>();
        let json_offsets: Vec<*const i64> = json_offsets
            .iter()
            .map(|offsets| offsets.as_ptr())
            .collect();
        let c_keys: Vec<CString> = keys.into_iter().map(|k| CString::new(k).unwrap()).collect();
        let key_ptrs: Vec<*const libc::c_char> = c_keys.iter().map(|cs| cs.as_ptr()).collect();

        index_writer
            .add_json_key_stats(&key_ptrs, &json_offsets, &json_offsets_len)
            .unwrap();

        index_writer.commit().unwrap();
        let count = index_writer
            .create_reader(set_bitset)
            .unwrap()
            .count()
            .unwrap();
        assert_eq!(count, total_count);
    }

    #[test]
    fn test_v5_direct_json_key_stats_add_finish_and_query() {
        let temp_dir = tempdir().unwrap();
        let mut writer = IndexWriterWrapper::create_json_key_stats_writer(
            "test",
            temp_dir.path().to_str().unwrap(),
            1,
            15 * 1024 * 1024,
            TantivyIndexVersion::V5,
            false,
            true,
        )
        .unwrap();
        let key = CString::new("/shared").unwrap();
        let offsets = [11_i64, 42_i64];
        writer
            .add_json_key_stats(&[key.as_ptr()], &[offsets.as_ptr()], &[offsets.len()])
            .unwrap();
        writer.finish().unwrap();

        let index = tantivy_5::Index::open_in_dir(temp_dir.path()).unwrap();
        assert_eq!(index.searchable_segment_metas().unwrap().len(), 1);
        let field = index.schema().get_field("test").unwrap();
        let reader = index.reader().unwrap();
        let query = tantivy_5::query::TermQuery::new(
            tantivy_5::Term::from_field_text(field, "/shared"),
            tantivy_5::schema::IndexRecordOption::Basic,
        );
        let hits = reader
            .searcher()
            .search(&query, &tantivy_5::collector::Count)
            .unwrap();
        assert_eq!(hits, 2);
    }

    #[test]
    fn test_control_user_specified_doc_id() {
        let enabled = [true, false];
        for enable in enabled {
            let dir = TempDir::new().unwrap();
            let mut index_wrapper = IndexWriterWrapper::new(
                "test",
                TantivyDataType::I64,
                dir.path().to_str().unwrap().to_string(),
                1,
                100_000_000,
                TantivyIndexVersion::V7,
                enable,
                false,
            )
            .unwrap();

            index_wrapper.add(1 as i64, Some(0)).unwrap();
            index_wrapper.commit().unwrap();

            let reader = index_wrapper.create_reader(set_bitset).unwrap();
            let count = reader.count().unwrap();
            assert_eq!(count, 1);
        }
    }
}
