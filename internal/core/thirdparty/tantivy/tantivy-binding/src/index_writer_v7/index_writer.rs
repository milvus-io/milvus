use std::sync::Arc;

use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy::indexer::UserOperation;
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, SchemaBuilder, TextFieldIndexing,
    TextOptions, FAST, STRING,
};
use tantivy::{doc, Index, IndexWriter, SingleSegmentIndexWriter, TantivyDocument};

use crate::convert_to_rust_slice;
use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_reader::IndexReaderWrapper;
use crate::index_reader_c::SetBitsetFn;
use crate::index_writer::{row_value_to_str, TantivyValue};
use crate::util::{c_ptr_to_str, ptr_len_to_str};

const BATCH_SIZE: usize = 4096;

#[inline]
pub(crate) fn schema_builder_add_field(
    schema_builder: &mut SchemaBuilder,
    field_name: &str,
    data_type: TantivyDataType,
) -> Field {
    match data_type {
        TantivyDataType::I64 => {
            schema_builder.add_i64_field(field_name, NumericOptions::default().set_indexed())
        }
        TantivyDataType::F64 => {
            schema_builder.add_f64_field(field_name, NumericOptions::default().set_indexed())
        }
        TantivyDataType::Bool => {
            schema_builder.add_bool_field(field_name, NumericOptions::default().set_indexed())
        }
        TantivyDataType::Keyword => {
            let text_field_indexing = TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_fieldnorms(false)
                .set_index_option(IndexRecordOption::Basic);
            let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
            schema_builder.add_text_field(field_name, text_options)
        }
        TantivyDataType::Text => {
            panic!("text should be indexed with analyzer");
        }
        TantivyDataType::JSON => schema_builder.add_json_field(&field_name, STRING | FAST),
    }
}

impl TantivyValue<TantivyDocument> for i64 {
    #[inline]
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_i64(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for u64 {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_u64(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for f64 {
    #[inline]
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_f64(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for &str {
    #[inline]
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_text(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for bool {
    #[inline]
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_bool(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for serde_json::Value {
    #[inline]
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_field_value(Field::from_field_id(field), self);
    }
}

pub struct IndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) index_writer: Writer,
    pub(crate) index: Arc<Index>,
    pub(crate) id_field: Option<Field>,
    pub(crate) enable_user_specified_doc_id: bool,
    pub(crate) enable_background_merge: bool,
}

pub(crate) enum Writer {
    Regular(IndexWriter),
    Direct(SingleSegmentIndexWriter),
}

impl IndexWriterWrapperImpl {
    pub(crate) fn from_direct_parts(
        field: Field,
        index: Index,
        index_writer: SingleSegmentIndexWriter,
        id_field: Option<Field>,
        enable_user_specified_doc_id: bool,
    ) -> Self {
        Self {
            field,
            index_writer: Writer::Direct(index_writer),
            index: Arc::new(index),
            id_field,
            enable_user_specified_doc_id,
            enable_background_merge: false,
        }
    }

    pub fn new(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        enable_user_specified_doc_id: bool,
        enable_background_merge: bool,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create index writer, field_name: {}, data_type: {:?}, tantivy_index_version 7, enable_background_merge: {}",
            field_name, data_type, enable_background_merge
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, field_name, data_type);
        let id_field = if enable_user_specified_doc_id {
            schema_builder.enable_user_specified_doc_id();
            None
        } else {
            Some(schema_builder.add_i64_field("doc_id", FAST))
        };
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        if !enable_background_merge {
            // Sealed index builds end with an explicit merge-all in finish();
            // background policy-driven merges would only waste IO and race
            // with it, so disable them entirely for build-mode writers.
            index_writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
        }
        Ok(IndexWriterWrapperImpl {
            field,
            index_writer: Writer::Regular(index_writer),
            index: Arc::new(index),
            id_field,
            enable_user_specified_doc_id,
            enable_background_merge,
        })
    }

    pub fn new_with_single_segment(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        memory_budget: usize,
        enable_user_specified_doc_id: bool,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create direct index writer, field_name: {}, data_type: {:?}, tantivy_index_version 7",
            field_name, data_type
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, field_name, data_type);
        let id_field = if enable_user_specified_doc_id {
            schema_builder.enable_user_specified_doc_id();
            None
        } else {
            Some(schema_builder.add_i64_field("doc_id", FAST))
        };
        let schema = schema_builder.build();
        let index = Index::builder().schema(schema).create_in_dir(&path)?;
        let index_writer = SingleSegmentIndexWriter::new(index.clone(), memory_budget)?;
        Ok(Self::from_direct_parts(
            field,
            index,
            index_writer,
            id_field,
            enable_user_specified_doc_id,
        ))
    }

    pub fn create_reader(&self, set_bitset: SetBitsetFn) -> Result<IndexReaderWrapper> {
        IndexReaderWrapper::from_index(self.index.clone(), set_bitset)
    }

    #[inline]
    fn add_document(&mut self, mut document: TantivyDocument, offset: u32) -> Result<()> {
        if self.enable_user_specified_doc_id {
            match &mut self.index_writer {
                Writer::Regular(writer) => {
                    writer.add_document_with_doc_id(offset, document)?;
                }
                Writer::Direct(writer) => {
                    writer.add_documents_with_doc_ids(std::iter::once((offset, document)))?;
                }
            }
        } else {
            document.add_i64(self.id_field.unwrap(), offset as i64);
            match &mut self.index_writer {
                Writer::Regular(writer) => {
                    writer.add_document(document)?;
                }
                Writer::Direct(writer) => writer.add_document(document)?,
            }
        }
        Ok(())
    }

    fn add_documents(
        &mut self,
        mut documents: Vec<TantivyDocument>,
        offset_begin: u32,
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        if self.enable_user_specified_doc_id {
            match &mut self.index_writer {
                Writer::Regular(writer) => {
                    writer.add_documents_with_doc_id(offset_begin, documents)?;
                }
                Writer::Direct(writer) => {
                    writer.add_documents_with_doc_ids(
                        documents
                            .into_iter()
                            .enumerate()
                            .map(|(index, document)| (offset_begin + index as u32, document)),
                    )?;
                }
            }
        } else {
            let id_field = self.id_field.unwrap();
            for (index, document) in documents.iter_mut().enumerate() {
                document.add_i64(id_field, i64::from(offset_begin + index as u32));
            }
            match &mut self.index_writer {
                Writer::Regular(writer) => {
                    writer.run(documents.into_iter().map(UserOperation::Add))?;
                }
                Writer::Direct(writer) => writer.add_documents(documents)?,
            }
        }
        Ok(())
    }

    fn add_documents_with_doc_ids(
        &mut self,
        mut documents: Vec<(u32, TantivyDocument)>,
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        if self.enable_user_specified_doc_id {
            match &mut self.index_writer {
                Writer::Regular(writer) => {
                    for (doc_id, document) in documents {
                        writer.add_document_with_doc_id(doc_id, document)?;
                    }
                }
                Writer::Direct(writer) => writer.add_documents_with_doc_ids(documents)?,
            }
        } else {
            let id_field = self.id_field.unwrap();
            for (doc_id, document) in &mut documents {
                document.add_i64(id_field, i64::from(*doc_id));
            }
            let documents = documents.into_iter().map(|(_, document)| document);
            match &mut self.index_writer {
                Writer::Regular(writer) => {
                    writer.run(documents.map(UserOperation::Add))?;
                }
                Writer::Direct(writer) => {
                    writer.add_documents(documents)?;
                }
            }
        }
        Ok(())
    }

    pub fn add_batch<T, I>(&mut self, data: I, offset_begin: u32) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocument>,
    {
        let iterator = data.into_iter();
        let mut documents = Vec::with_capacity(iterator.size_hint().0);
        for value in iterator {
            let mut document = TantivyDocument::default();
            value.add_to_document(self.field.field_id(), &mut document);
            documents.push(document);
        }
        self.add_documents(documents, offset_begin)
    }

    pub fn add_rows<T: TantivyValue<TantivyDocument>>(
        &mut self,
        values: &[T],
        row_offsets: &[usize],
        doc_ids: &[i64],
    ) -> Result<()> {
        let doc_ids = doc_ids
            .iter()
            .map(|&doc_id| {
                u32::try_from(doc_id).map_err(|_| {
                    TantivyBindingError::InvalidArgument(
                        "document ID does not fit Tantivy version 7".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut documents = Vec::with_capacity(doc_ids.len());
        for (row, &doc_id) in doc_ids.iter().enumerate() {
            let mut document = TantivyDocument::default();
            for value in &values[row_offsets[row]..row_offsets[row + 1]] {
                value.add_to_document(self.field.field_id(), &mut document);
            }
            documents.push((doc_id, document));
        }
        self.add_documents_with_doc_ids(documents)?;
        Ok(())
    }

    pub fn add_string_rows(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        row_offsets: &[usize],
        doc_ids: &[i64],
    ) -> Result<()> {
        let mut values = Vec::with_capacity(ptrs.len());
        for (&ptr, &len) in ptrs.iter().zip(lens) {
            values.push(row_value_to_str(ptr, len)?);
        }
        self.add_rows(&values, row_offsets, doc_ids)
    }

    pub fn add_json_rows(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        row_offsets: &[usize],
        doc_ids: &[i64],
    ) -> Result<()> {
        let doc_ids = doc_ids
            .iter()
            .map(|&doc_id| {
                u32::try_from(doc_id).map_err(|_| {
                    TantivyBindingError::InvalidArgument(
                        "document ID does not fit Tantivy version 7".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut documents = Vec::with_capacity(doc_ids.len());
        for (row, &doc_id) in doc_ids.iter().enumerate() {
            let mut document = TantivyDocument::default();
            if row_offsets[row] != row_offsets[row + 1] {
                let index = row_offsets[row];
                let value = row_value_to_str(ptrs[index], lens[index])?;
                serde_json::from_str::<serde_json::Value>(value)?
                    .add_to_document(self.field.field_id(), &mut document);
            }
            documents.push((doc_id, document));
        }
        self.add_documents_with_doc_ids(documents)?;
        Ok(())
    }

    pub fn add_strings_with_len(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offset_begin: u32,
    ) -> Result<()> {
        if ptrs.len() != lens.len() {
            return Err(TantivyBindingError::InvalidArgument(
                "string batch pointer and length counts differ".to_string(),
            ));
        }
        let mut documents = Vec::with_capacity(ptrs.len());
        for (&ptr, &len) in ptrs.iter().zip(lens) {
            let data = ptr_len_to_str(ptr, len)?;
            let mut document = TantivyDocument::default();
            document.add_text(self.field, data);
            documents.push(document);
        }
        self.add_documents(documents, offset_begin)
    }

    pub fn add<T: TantivyValue<TantivyDocument>>(&mut self, data: T, offset: u32) -> Result<()> {
        let mut document = TantivyDocument::default();
        data.add_to_document(self.field.field_id(), &mut document);

        self.add_document(document, offset)
    }

    pub fn add_array<T: TantivyValue<TantivyDocument>, I>(
        &mut self,
        data: I,
        offset: u32,
    ) -> Result<()>
    where
        I: IntoIterator<Item = T>,
    {
        let mut document = TantivyDocument::default();
        data.into_iter()
            .for_each(|d| d.add_to_document(self.field.field_id(), &mut document));

        self.add_document(document, offset)
    }

    pub fn add_array_keywords(&mut self, datas: &[*const c_char], offset: u32) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = c_ptr_to_str(*element)?;
            document.add_field_value(self.field, data);
        }

        self.add_document(document, offset)
    }

    pub fn add_array_keywords_with_len(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offset: u32,
    ) -> Result<()> {
        debug_assert_eq!(ptrs.len(), lens.len());
        let mut document = TantivyDocument::default();
        for i in 0..ptrs.len() {
            let data = ptr_len_to_str(ptrs[i], lens[i])?;
            document.add_field_value(self.field, data);
        }

        self.add_document(document, offset)
    }

    pub fn add_json(&mut self, data: &str, offset: u32) -> Result<()> {
        let j = serde_json::from_str::<serde_json::Value>(data)?;
        let mut document = TantivyDocument::default();
        j.add_to_document(self.field.field_id(), &mut document);

        self.add_document(document, offset)
    }

    /// Batch add multiple JSON documents, each as a separate document with sequential offsets.
    pub fn add_json_batch(&mut self, datas: &[*const c_char], offset_begin: u32) -> Result<()> {
        let mut documents = Vec::with_capacity(datas.len());
        for &data_ptr in datas {
            let data = c_ptr_to_str(data_ptr)?;
            let j = serde_json::from_str::<serde_json::Value>(data)?;
            let mut document = TantivyDocument::default();
            j.add_to_document(self.field.field_id(), &mut document);
            documents.push(document);
        }
        self.add_documents(documents, offset_begin)
    }

    pub fn add_array_json(&mut self, datas: &[*const c_char], offset: u32) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = c_ptr_to_str(*element)?;
            let j = serde_json::from_str::<serde_json::Value>(data)?;
            j.add_to_document(self.field.field_id(), &mut document);
        }

        self.add_document(document, offset)
    }

    pub fn add_json_key_stats(
        &mut self,
        keys: &[*const c_char],
        json_offsets: &[*const i64],
        json_offsets_len: &[usize],
    ) -> Result<()> {
        let id_field = self.id_field.unwrap();

        let mut batch = Vec::with_capacity(BATCH_SIZE);
        for i in 0..keys.len() {
            let key = c_ptr_to_str(keys[i])
                .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;

            let offsets = unsafe { convert_to_rust_slice!(json_offsets[i], json_offsets_len[i]) };

            for offset in offsets {
                batch.push(doc!(
                    id_field => *offset,
                    self.field => key,
                ));
                if batch.len() == BATCH_SIZE {
                    let documents = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                    match &mut self.index_writer {
                        Writer::Regular(writer) => {
                            writer.run(documents.into_iter().map(UserOperation::Add))?;
                        }
                        Writer::Direct(writer) => writer.add_documents(documents)?,
                    }
                }
            }
        }

        if !batch.is_empty() {
            match &mut self.index_writer {
                Writer::Regular(writer) => {
                    writer.run(batch.into_iter().map(UserOperation::Add))?;
                }
                Writer::Direct(writer) => writer.add_documents(batch)?,
            }
        }

        Ok(())
    }

    pub fn manual_merge(&mut self) -> Result<()> {
        let Writer::Regular(index_writer) = &mut self.index_writer else {
            return Err(TantivyBindingError::InternalError(
                "manual merge is not supported by a direct writer".to_string(),
            ));
        };
        let metas = index_writer.index().searchable_segment_metas()?;
        let policy = index_writer.get_merge_policy();
        let candidates = policy.compute_merge_candidates(metas.as_slice());
        for candidate in candidates {
            index_writer.merge(candidate.0.as_slice()).wait()?;
        }
        Ok(())
    }

    pub fn finish_index(self) -> Result<Index> {
        let mut index_writer = match self.index_writer {
            Writer::Direct(index_writer) => {
                return Ok(index_writer.finalize()?);
            }
            Writer::Regular(index_writer) => index_writer,
        };
        index_writer.commit()?;

        if !self.enable_background_merge {
            // Build-mode writers use NoMergePolicy (set in new()), so no
            // background merge can race this explicit merge-all. Collapse the
            // auto-flushed segments into a single one. Background-merge writers
            // (e.g. growing segments) are left to their own policy and are not
            // forced to a single segment here.
            let segment_ids = self.index.searchable_segment_ids()?;
            if segment_ids.len() > 1 {
                index_writer.merge(&segment_ids).wait()?;
            }
        }
        block_on(index_writer.garbage_collect_files())?;
        index_writer.wait_merging_threads()?;

        // TODO: remove this log when #45590 is solved
        let metas = self.index.searchable_segment_metas()?;
        let segment_ids: Vec<_> = metas.iter().map(|m| m.id().uuid_string()).collect();
        info!("tantivy index_writer finish, segments: {:?}", segment_ids);

        Ok(self.index.as_ref().clone())
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        let Writer::Regular(index_writer) = &mut self.index_writer else {
            return Err(TantivyBindingError::InternalError(
                "commit is not supported by a direct writer".to_string(),
            ));
        };
        index_writer.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tantivy::Index;
    use tempfile::TempDir;

    use super::IndexWriterWrapperImpl;
    use crate::data_type::TantivyDataType;

    // tantivy's smallest per-thread arena (MEMORY_BUDGET_NUM_BYTES_MIN = 15 MB).
    // With a single indexing thread this is tight enough that the doc count below
    // spills into several auto-flushed segments before the finish-time commit,
    // which is exactly the multi-segment build this test needs to exercise.
    const MIN_MEMORY_BUDGET: usize = 15_000_000;
    const NUM_DOCS: i64 = 1_000_000;

    fn build_i64_writer(path: &str, enable_background_merge: bool) -> IndexWriterWrapperImpl {
        IndexWriterWrapperImpl::new(
            "number",
            TantivyDataType::I64,
            path.to_string(),
            1, // single thread -> smallest arena -> forces multiple flushed segments
            MIN_MEMORY_BUDGET,
            false, // enable_user_specified_doc_id
            enable_background_merge,
        )
        .unwrap()
    }

    /// A build-mode (enable_background_merge == false) V7 writer must collapse the
    /// auto-flushed segments into exactly one searchable segment in finish().
    ///
    /// Regression guard for the finish-time merge-all (issue #51054): the V7 writer
    /// previously shipped sealed indexes as many ~15 MB segments, and if the merge
    /// is ever dropped again the index silently regresses to multi-segment with only
    /// perf/logs to reveal it. The precondition assert keeps the test honest — it
    /// proves the workload really produced >1 segment before finish() merged them.
    #[test]
    fn test_sealed_build_finishes_single_segment() {
        let dir = TempDir::new().unwrap();
        let mut writer = build_i64_writer(dir.path().to_str().unwrap(), false);
        for i in 0..NUM_DOCS {
            writer.add::<i64>(i, i as u32).unwrap();
        }
        writer.commit().unwrap();

        // Precondition: the build workload genuinely auto-flushes multiple segments,
        // so the single-segment assertion after finish() is meaningful.
        let before = writer.index.searchable_segment_metas().unwrap();
        assert!(
            before.len() > 1,
            "expected the build workload to auto-flush multiple segments, got {}",
            before.len()
        );

        // finish() on a build-mode writer must merge them down to exactly one.
        writer.finish_index().unwrap();

        let index = Index::open_in_dir(dir.path()).unwrap();
        let after = index.searchable_segment_metas().unwrap();
        assert_eq!(
            after.len(),
            1,
            "sealed build must produce exactly one tantivy segment, got {}",
            after.len()
        );
    }
}
