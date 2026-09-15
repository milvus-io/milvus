//! Bounded document batches for the regular Tantivy writers.

use either::Either;
use tantivy::schema::Document;
use tantivy::{IndexWriter, TantivyDocument};

use crate::error::{Result, TantivyBindingError};
use crate::index_writer::{IndexWriterWrapper, TantivyValue};
use crate::index_writer_v5::TantivyDocumentV5;
use crate::util::ptr_len_to_str;

pub(crate) const DOCUMENT_BATCH_SIZE: usize = 4096;

fn invalid_batch(message: &str) -> TantivyBindingError {
    // These layouts and row IDs are produced by Milvus, not by a user request.
    TantivyBindingError::InternalError(message.to_owned())
}

fn validate_layout(value_count: usize, offsets: &[usize], ids: &[i64]) -> Result<()> {
    if offsets.len() != ids.len() + 1
        || offsets.first() != Some(&0)
        || offsets.last() != Some(&value_count)
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
    {
        return Err(invalid_batch("invalid row batch offsets"));
    }
    if ids.iter().any(|id| *id < 0) || ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid_batch(
            "row batch document IDs must be nonnegative and increasing",
        ));
    }
    Ok(())
}

pub(crate) fn v7_doc_ids(ids: &[i64]) -> Result<Vec<u32>> {
    ids.iter()
        .map(|&id| {
            // max_doc is one past the final ID and must be below MAX_DOC_LIMIT.
            match u32::try_from(id) {
                Ok(id) if id < tantivy::indexer::merger::MAX_DOC_LIMIT - 1 => Ok(id),
                _ => Err(invalid_batch("document ID exceeds Tantivy's segment limit")),
            }
        })
        .collect()
}

/// The existing writer accepts a start ID plus contiguous documents. Split at
/// gaps without renumbering rows or inserting documents for missing IDs.
pub(crate) fn submit_with_doc_ids<D: Document>(
    writer: &IndexWriter<D>,
    ids: &[u32],
    documents: Vec<D>,
) -> Result<()> {
    if ids.len() != documents.len() {
        return Err(invalid_batch("document and ID counts differ"));
    }
    // Unlike the PR's direct writer, the current regular writer validates only
    // the start ID of each submitted run. Check the whole batch before sending
    // the first run so a malformed later ID cannot cause a partial write.
    if ids
        .iter()
        .any(|&id| id >= tantivy::indexer::merger::MAX_DOC_LIMIT - 1)
        || ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid_batch(
            "document IDs must increase and fit Tantivy's segment limit",
        ));
    }
    let mut documents = documents.into_iter();
    let mut begin = 0;
    while begin < ids.len() {
        let mut end = begin + 1;
        while end < ids.len()
            && end - begin < DOCUMENT_BATCH_SIZE
            && ids[end - 1].checked_add(1) == Some(ids[end])
        {
            end += 1;
        }

        writer.add_documents_with_doc_id(ids[begin], documents.by_ref().take(end - begin))?;
        begin = end;
    }
    Ok(())
}

fn string_values<'a>(ptrs: &[*const u8], lens: &[usize]) -> Result<Vec<&'a str>> {
    if ptrs.len() != lens.len() {
        return Err(invalid_batch("string pointer and length counts differ"));
    }
    ptrs.iter()
        .zip(lens)
        .map(|(&ptr, &len)| {
            if len == 0 {
                Ok("")
            } else if ptr.is_null() {
                Err(invalid_batch("nonempty string has a null pointer"))
            } else {
                ptr_len_to_str(ptr, len)
            }
        })
        .collect()
}

impl IndexWriterWrapper {
    pub fn add_batch<T, I>(&mut self, data: I, offset_begin: i64) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocument>,
    {
        let mut data = data.into_iter().peekable();
        let mut next_id = offset_begin;
        while data.peek().is_some() {
            let values: Vec<_> = data.by_ref().take(DOCUMENT_BATCH_SIZE).collect();
            let end = next_id
                .checked_add(values.len() as i64)
                .ok_or_else(|| invalid_batch("document ID overflow"))?;
            let ids: Vec<_> = (next_id..end).collect();
            let offsets: Vec<_> = (0..=values.len()).collect();
            self.add_rows(values, &offsets, &ids)?;
            next_id = end;
        }
        Ok(())
    }

    pub fn add_strings_with_len(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offset_begin: i64,
    ) -> Result<()> {
        let values = string_values(ptrs, lens)?;
        if matches!(self, Self::NgramV7(_)) {
            // Keep the existing bulk string entry point usable for NGRAM
            // callers such as BuildWithRawDataForUT.
            let end = offset_begin
                .checked_add(values.len() as i64)
                .ok_or_else(|| invalid_batch("document ID overflow"))?;
            let ids = v7_doc_ids(&(offset_begin..end).collect::<Vec<_>>())?;
            let rows: Vec<_> = values
                .into_iter()
                .zip(ids)
                .map(|(value, doc_id)| crate::index_ngram_writer::NgramRow {
                    doc_id,
                    value: Some(value),
                })
                .collect();
            for batch in rows.chunks(DOCUMENT_BATCH_SIZE) {
                self.add_ngram_rows(batch)?;
            }
            Ok(())
        } else {
            self.add_batch(values, offset_begin)
        }
    }

    pub fn add_string_rows(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offsets: &[usize],
        ids: &[i64],
    ) -> Result<()> {
        self.add_rows(string_values(ptrs, lens)?, offsets, ids)
    }

    pub fn add_json_rows(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offsets: &[usize],
        ids: &[i64],
    ) -> Result<()> {
        validate_layout(ptrs.len(), offsets, ids)?;
        if offsets.windows(2).any(|pair| pair[1] - pair[0] > 1) {
            return Err(invalid_batch(
                "JSON row contains more than one serialized value",
            ));
        }
        // Parse the complete batch before submitting any of it to the writer.
        let values = string_values(ptrs, lens)?
            .into_iter()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        self.add_rows(values, offsets, ids)
    }

    pub fn add_rows<T, I>(&mut self, values: I, offsets: &[usize], ids: &[i64]) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocument>,
    {
        let values: Vec<_> = values.into_iter().collect();
        validate_layout(values.len(), offsets, ids)?;
        match self {
            Self::V5(state) => {
                state.validate_row_batch_doc_ids(ids)?;
                let writer = &mut state.writer;
                let mut documents = Vec::with_capacity(ids.len());
                for (row, &id) in ids.iter().enumerate() {
                    let mut document = TantivyDocumentV5::default();
                    for value in &values[offsets[row]..offsets[row + 1]] {
                        value.add_to_document(writer.field.field_id(), &mut document);
                    }
                    if let Some(id_field) = writer.id_field {
                        document.add_i64(id_field, id);
                    }
                    documents.push(document);
                }
                match &mut writer.index_writer {
                    Either::Left(writer) => {
                        writer.run(documents.into_iter().map(tantivy_5::UserOperation::Add))?;
                    }
                    Either::Right(writer) => {
                        // Preserve the legacy V5 writer's implicit sequential IDs.
                        for document in documents {
                            writer.add_document(document)?;
                        }
                    }
                }
                state.record_row_batch_doc_ids(ids);
            }
            Self::V7(state) => {
                state.validate_row_batch_doc_ids(ids)?;
                let writer = &mut state.writer;
                if ids.iter().any(|&id| id > i64::from(u32::MAX)) {
                    return Err(invalid_batch("document ID does not fit Tantivy version 7"));
                }
                let mut documents = Vec::with_capacity(ids.len());
                for (row, &id) in ids.iter().enumerate() {
                    let mut document = TantivyDocument::default();
                    for value in &values[offsets[row]..offsets[row + 1]] {
                        value.add_to_document(writer.field.field_id(), &mut document);
                    }
                    if !writer.enable_user_specified_doc_id {
                        document.add_i64(writer.id_field.unwrap(), id);
                    }
                    documents.push(document);
                }
                if writer.enable_user_specified_doc_id {
                    let v7_ids = v7_doc_ids(ids)?;
                    submit_with_doc_ids(&writer.index_writer, &v7_ids, documents)?;
                } else {
                    writer.index_writer.run(
                        documents
                            .into_iter()
                            .map(tantivy::indexer::UserOperation::Add),
                    )?;
                }
                state.record_row_batch_doc_ids(ids);
            }
            Self::NgramV7(_) => return Err(invalid_batch("NGRAM requires text row batches")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tantivy::{collector::TopDocs, query::TermQuery, schema::IndexRecordOption, Index, Term};
    use tempfile::TempDir;

    use super::*;
    use crate::{data_type::TantivyDataType, util::set_bitset, TantivyIndexVersion};

    fn writer(data_type: TantivyDataType, explicit_ids: bool) -> (TempDir, IndexWriterWrapper) {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::new(
            "field",
            data_type,
            dir.path().to_str().unwrap().to_owned(),
            1,
            15_000_000,
            TantivyIndexVersion::V7,
            explicit_ids,
            false,
        )
        .unwrap();
        (dir, writer)
    }

    fn hits(index: &Index, term: Term) -> Vec<u32> {
        let reader = index.reader().unwrap();
        let mut hits: Vec<_> = reader
            .searcher()
            .search(
                &TermQuery::new(term, IndexRecordOption::Basic),
                &TopDocs::with_limit(100),
            )
            .unwrap()
            .into_iter()
            .map(|(_, address)| address.doc_id)
            .collect();
        hits.sort_unstable();
        hits
    }

    #[test]
    fn row_batches_preserve_sparse_ids_multi_values_and_empty_rows() {
        let (dir, mut writer) = writer(TantivyDataType::I64, true);
        writer
            .add_rows([11_i64, 22, 33], &[0, 1, 1, 3], &[2, 3, 9])
            .unwrap();
        writer.add_rows([44_i64], &[0, 1, 1], &[10, 14]).unwrap();
        writer.finish().unwrap();
        let index = Index::open_in_dir(dir.path()).unwrap();
        let field = index.schema().get_field("field").unwrap();
        assert_eq!(hits(&index, Term::from_field_i64(field, 33)), [9]);
        assert_eq!(hits(&index, Term::from_field_i64(field, 44)), [10]);
        let reader =
            crate::index_reader::IndexReaderWrapper::from_index(Arc::new(index), set_bitset)
                .unwrap();
        assert_eq!(reader.count().unwrap(), 15);
    }

    #[test]
    fn string_batches_preserve_empty_null_utf8_and_embedded_nul() {
        let (dir, mut writer) = writer(TantivyDataType::Keyword, true);
        let value = "nul\0测试";
        writer
            .add_string_rows(
                &[std::ptr::null(), value.as_ptr()],
                &[0, value.len()],
                &[0, 0, 1, 2],
                &[1, 4, 7],
            )
            .unwrap();
        writer.finish().unwrap();
        let index = Index::open_in_dir(dir.path()).unwrap();
        let field = index.schema().get_field("field").unwrap();
        assert_eq!(hits(&index, Term::from_field_text(field, "")), [4]);
        assert_eq!(hits(&index, Term::from_field_text(field, value)), [7]);
    }

    #[test]
    fn numeric_batches_cross_boundary_with_nonzero_start() {
        let (dir, mut writer) = writer(TantivyDataType::I64, true);
        writer
            .add_batch(0..(DOCUMENT_BATCH_SIZE + 3) as i64, 100)
            .unwrap();
        writer.finish().unwrap();
        let index = Index::open_in_dir(dir.path()).unwrap();
        let field = index.schema().get_field("field").unwrap();
        for value in [
            0,
            DOCUMENT_BATCH_SIZE as i64 - 1,
            DOCUMENT_BATCH_SIZE as i64 + 2,
        ] {
            assert_eq!(
                hits(&index, Term::from_field_i64(field, value)),
                [value as u32 + 100]
            );
        }
    }

    #[test]
    fn fast_field_mode_preserves_original_row_ids() {
        let (dir, mut writer) = writer(TantivyDataType::I64, false);
        writer
            .add_rows([11_i64, 22], &[0, 1, 1, 2], &[3, 7, 12])
            .unwrap();
        writer.finish().unwrap();
        let index = Index::open_in_dir(dir.path()).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = searcher.segment_reader(0);
        let ids = segment.fast_fields().i64("doc_id").unwrap();
        assert_eq!(
            (0..3).map(|id| ids.first(id).unwrap()).collect::<Vec<_>>(),
            [3, 7, 12]
        );
    }

    #[test]
    fn v5_batches_keep_fast_ids_and_legacy_implicit_ids() {
        for legacy in [false, true] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().to_str().unwrap().to_owned();
            let mut writer = if legacy {
                IndexWriterWrapper::new_with_single_segment("field", TantivyDataType::I64, path)
                    .unwrap()
            } else {
                IndexWriterWrapper::new(
                    "field",
                    TantivyDataType::I64,
                    path,
                    1,
                    15_000_000,
                    TantivyIndexVersion::V5,
                    false,
                    false,
                )
                .unwrap()
            };
            writer
                .add_rows([11_i64, 22], &[0, 1, 1, 2], &[3, 7, 12])
                .unwrap();
            writer.add_batch([33_i64], 13).unwrap();
            writer.finish().unwrap();
            let index = tantivy_5::Index::open_in_dir(dir.path()).unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 4);
            if !legacy {
                let ids = searcher
                    .segment_reader(0)
                    .fast_fields()
                    .i64("doc_id")
                    .unwrap();
                assert_eq!(
                    (0..4).map(|id| ids.first(id).unwrap()).collect::<Vec<_>>(),
                    [3, 7, 12, 13]
                );
            }
        }
    }

    #[test]
    fn invalid_rows_and_json_are_rejected_before_submission() {
        // JSON schemas use the doc_id fast field, not explicit Tantivy IDs.
        let (dir, mut writer) = writer(TantivyDataType::JSON, false);
        let text = r#"{"key": 1}"#;
        for (offsets, ids) in [
            (vec![1, 1], vec![0]),
            (vec![0, 2], vec![0]),
            (vec![0, 1], vec![-1]),
            (vec![0, 1], vec![i64::from(u32::MAX) + 1]),
            (vec![0, 1, 1], vec![4, 4]),
        ] {
            assert!(writer
                .add_json_rows(&[text.as_ptr()], &[text.len()], &offsets, &ids)
                .is_err());
        }
        let bad = "{";
        assert!(writer
            .add_json_rows(
                &[text.as_ptr(), bad.as_ptr()],
                &[text.len(), bad.len()],
                &[0, 1, 2],
                &[2, 9]
            )
            .is_err());
        writer
            .add_json_rows(&[text.as_ptr()], &[text.len()], &[0, 1, 1], &[0, 1])
            .unwrap();
        writer.finish().unwrap();
        let index = Index::open_in_dir(dir.path()).unwrap();
        assert_eq!(index.reader().unwrap().searcher().num_docs(), 2);
    }

    #[test]
    fn explicit_ids_reject_segment_limit_before_submission() {
        let (_dir, mut writer) = writer(TantivyDataType::I64, true);
        assert!(writer
            .add_rows([1_i64, 2], &[0, 1, 2], &[0, u32::MAX as i64])
            .is_err());
        // No document from the invalid batch may have reserved ID 0.
        writer.add_rows([3_i64], &[0, 1], &[0]).unwrap();
    }

    #[test]
    fn row_batches_enforce_order_in_v5_and_v7_fast_field_modes() {
        for version in [TantivyIndexVersion::V5, TantivyIndexVersion::V7] {
            for kind in 0..3 {
                let data_type = match kind {
                    0 => TantivyDataType::I64,
                    1 => TantivyDataType::Keyword,
                    _ => TantivyDataType::JSON,
                };
                let dir = TempDir::new().unwrap();
                let mut writer = IndexWriterWrapper::new(
                    "field",
                    data_type,
                    dir.path().to_str().unwrap().to_owned(),
                    1,
                    15_000_000,
                    version,
                    false,
                    false,
                )
                .unwrap();
                let add_row = |writer: &mut IndexWriterWrapper, id| match kind {
                    0 => writer.add_rows([11_i64], &[0, 1], &[id]),
                    1 => {
                        let value = "nul\0测试";
                        writer.add_string_rows(&[value.as_ptr()], &[value.len()], &[0, 1], &[id])
                    }
                    2 => {
                        let value = r#"{"key": 11}"#;
                        writer.add_json_rows(&[value.as_ptr()], &[value.len()], &[0, 1], &[id])
                    }
                    _ => unreachable!(),
                };
                add_row(&mut writer, 5).unwrap();
                writer.commit().unwrap();
                writer
                    .add_rows(std::iter::empty::<i64>(), &[0], &[])
                    .unwrap();
                for id in [5, 4] {
                    let error = add_row(&mut writer, id).unwrap_err();
                    assert!(matches!(error, TantivyBindingError::InternalError(_)));
                    assert!(error.to_string().contains("increase across row batches"));
                }
                // Invalid layouts must not advance the ordering state either.
                assert!(writer.add_rows([11_i64], &[0, 0], &[100]).is_err());
                add_row(&mut writer, 6).unwrap();
                writer.finish().unwrap();

                // Read persisted fast fields: rejected batches must neither add
                // documents nor renumber the accepted Milvus row IDs.
                let mut ids = Vec::new();
                match version {
                    TantivyIndexVersion::V5 => {
                        let index = tantivy_5::Index::open_in_dir(dir.path()).unwrap();
                        let reader = index.reader().unwrap();
                        for segment in reader.searcher().segment_readers() {
                            let field = segment.fast_fields().i64("doc_id").unwrap();
                            ids.extend((0..segment.max_doc()).map(|doc| field.first(doc).unwrap()));
                        }
                    }
                    TantivyIndexVersion::V7 => {
                        let index = Index::open_in_dir(dir.path()).unwrap();
                        let reader = index.reader().unwrap();
                        for segment in reader.searcher().segment_readers() {
                            let field = segment.fast_fields().i64("doc_id").unwrap();
                            ids.extend((0..segment.max_doc()).map(|doc| field.first(doc).unwrap()));
                        }
                    }
                }
                ids.sort_unstable();
                assert_eq!(ids, [5, 6]);
            }
        }
    }

    #[test]
    fn writer_rejects_out_of_order_ids_across_batches() {
        let (_dir, mut writer) = writer(TantivyDataType::I64, true);
        // A preceding scalar write is tracked by Tantivy itself. Its error
        // must still propagate when the next row batch is out of order.
        writer.add(1_i64, Some(5)).unwrap();
        let error = writer.add_rows([2_i64], &[0, 1], &[4]).unwrap_err();
        assert!(matches!(error, TantivyBindingError::TantivyError(_)));
        assert!(error.to_string().contains("strictly ordered"));
    }
}
