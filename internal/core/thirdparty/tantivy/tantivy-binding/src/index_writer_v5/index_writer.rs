use core::slice;
use std::sync::Arc;

use either::Either;
use libc::c_char;
use log::info;
use tantivy_5::schema::{
    Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED,
    STRING,
};
use tantivy_5::{
    doc, Document as TantivyDocument, Index, IndexWriter, SingleSegmentIndexWriter, UserOperation,
};

use crate::convert_to_rust_slice;
use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_writer::{row_value_to_str, TantivyValue};
use crate::util::{c_ptr_to_str, ptr_len_to_str};

const BATCH_SIZE: usize = 4096;

pub(crate) struct IndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) index_writer: Either<IndexWriter, SingleSegmentIndexWriter>,
    pub(crate) id_field: Option<Field>,
    pub(crate) _index: Arc<Index>,
    pub(crate) enable_background_merge: bool,
    pub(crate) next_doc_id: i64,
}

#[inline]
pub(crate) fn schema_builder_add_field(
    schema_builder: &mut SchemaBuilder,
    field_name: &str,
    data_type: TantivyDataType,
) -> Field {
    match data_type {
        TantivyDataType::I64 => schema_builder.add_i64_field(field_name, INDEXED),
        TantivyDataType::F64 => schema_builder.add_f64_field(field_name, INDEXED),
        TantivyDataType::Bool => schema_builder.add_bool_field(field_name, INDEXED),
        TantivyDataType::Keyword => {
            let text_field_indexing = TextFieldIndexing::default()
                .set_tokenizer("raw")
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
        document.add_field_value(Field::from_field_id(field), self.clone());
    }
}

impl IndexWriterWrapperImpl {
    pub(crate) fn from_direct_parts(
        field: Field,
        index: Index,
        index_writer: SingleSegmentIndexWriter,
        id_field: Option<Field>,
    ) -> Self {
        Self {
            field,
            index_writer: Either::Right(index_writer),
            id_field,
            _index: Arc::new(index),
            enable_background_merge: false,
            next_doc_id: 0,
        }
    }

    pub fn new(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        enable_background_merge: bool,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create index writer, field_name: {}, data_type: {:?}, tantivy_index_version 5, enable_background_merge: {}",
            field_name, data_type, enable_background_merge
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, field_name, data_type);
        // We cannot build direct connection from rows in multi-segments to milvus row data. So we have this doc_id field.
        let id_field = schema_builder.add_i64_field("doc_id", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        if !enable_background_merge {
            // Sealed index builds end with an explicit merge-all in finish();
            // background policy-driven merges would only waste IO and race
            // with it, so disable them entirely for build-mode writers.
            index_writer.set_merge_policy(Box::new(tantivy_5::merge_policy::NoMergePolicy));
        }
        Ok(IndexWriterWrapperImpl {
            field,
            index_writer: Either::Left(index_writer),
            id_field: Some(id_field),
            _index: Arc::new(index),
            enable_background_merge,
            next_doc_id: 0,
        })
    }

    pub fn new_with_single_segment(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        memory_budget: usize,
        _enable_user_specified_doc_id: bool,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create single segment index writer, field_name: {}, data_type: {:?}, tantivy_index_version 5",
            field_name, data_type
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, field_name, data_type);
        // Tantivy 5's direct writer only supports sequential document IDs. Keep
        // the Milvus row offset in a fast field so sparse/null rows retain their
        // original identity just like the legacy regular writer.
        let id_field = Some(schema_builder.add_i64_field("doc_id", FAST));
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer = SingleSegmentIndexWriter::new(index.clone(), memory_budget)?;
        Ok(Self::from_direct_parts(
            field,
            index,
            index_writer,
            id_field,
        ))
    }

    #[inline]
    fn add_document(&mut self, mut document: TantivyDocument, offset: Option<i64>) -> Result<()> {
        let doc_id = offset.unwrap_or(self.next_doc_id);
        self.next_doc_id = doc_id.checked_add(1).ok_or_else(|| {
            TantivyBindingError::InvalidArgument("document ID overflow".to_string())
        })?;
        if let Some(id_field) = self.id_field {
            document.add_i64(id_field, doc_id);
        }

        match &mut self.index_writer {
            Either::Left(writer) => {
                writer.add_document(document)?;
            }
            Either::Right(single_segment_writer) => {
                single_segment_writer.add_document(document)?;
            }
        }
        Ok(())
    }

    pub fn add<T: TantivyValue<TantivyDocument>>(
        &mut self,
        data: T,
        offset: Option<i64>,
    ) -> Result<()> {
        let mut document = TantivyDocument::default();
        data.add_to_document(self.field.field_id(), &mut document);

        self.add_document(document, offset)
    }

    pub fn add_batch<T, I>(&mut self, data: I, offset_begin: i64) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocument>,
    {
        for (index, value) in data.into_iter().enumerate() {
            self.add(value, Some(offset_begin + index as i64))?;
        }
        Ok(())
    }

    pub fn add_rows<T: TantivyValue<TantivyDocument>>(
        &mut self,
        values: &[T],
        row_offsets: &[usize],
        doc_ids: &[i64],
    ) -> Result<()> {
        if let Some(&first) = doc_ids.first() {
            if first < self.next_doc_id {
                return Err(TantivyBindingError::InvalidArgument(
                    "document IDs must increase across row batches".to_string(),
                ));
            }
        }
        match &mut self.index_writer {
            Either::Left(writer) => {
                let id_field = self.id_field.unwrap();
                let mut operations = Vec::with_capacity(doc_ids.len());
                for (row, &doc_id) in doc_ids.iter().enumerate() {
                    let mut document = TantivyDocument::default();
                    for value in &values[row_offsets[row]..row_offsets[row + 1]] {
                        value.add_to_document(self.field.field_id(), &mut document);
                    }
                    document.add_i64(id_field, doc_id);
                    operations.push(UserOperation::Add(document));
                }
                writer.run(operations)?;
                if let Some(last) = doc_ids.last() {
                    self.next_doc_id = last.checked_add(1).ok_or_else(|| {
                        TantivyBindingError::InvalidArgument("document ID overflow".to_string())
                    })?;
                }
            }
            Either::Right(writer) => {
                let id_field = self.id_field.unwrap();
                for (row, &doc_id) in doc_ids.iter().enumerate() {
                    let mut document = TantivyDocument::default();
                    for value in &values[row_offsets[row]..row_offsets[row + 1]] {
                        value.add_to_document(self.field.field_id(), &mut document);
                    }
                    document.add_i64(id_field, doc_id);
                    writer.add_document(document)?;
                    self.next_doc_id = doc_id.checked_add(1).ok_or_else(|| {
                        TantivyBindingError::InvalidArgument("document ID overflow".to_string())
                    })?;
                }
            }
        }
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
        let mut values = Vec::with_capacity(ptrs.len());
        for (&ptr, &len) in ptrs.iter().zip(lens) {
            values.push(serde_json::from_str::<serde_json::Value>(
                row_value_to_str(ptr, len)?,
            )?);
        }
        self.add_rows(&values, row_offsets, doc_ids)
    }

    pub fn add_strings_with_len(
        &mut self,
        ptrs: &[*const u8],
        lens: &[usize],
        offset_begin: i64,
    ) -> Result<()> {
        if ptrs.len() != lens.len() {
            return Err(TantivyBindingError::InvalidArgument(
                "string batch pointer and length counts differ".to_string(),
            ));
        }
        for (index, (&ptr, &len)) in ptrs.iter().zip(lens).enumerate() {
            let data = ptr_len_to_str(ptr, len)?;
            self.add(data, Some(offset_begin + index as i64))?;
        }
        Ok(())
    }

    pub fn add_array<T: TantivyValue<TantivyDocument>, I>(
        &mut self,
        data: I,
        offset: Option<i64>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = T>,
    {
        let mut document = TantivyDocument::default();
        data.into_iter()
            .for_each(|d| d.add_to_document(self.field.field_id(), &mut document));

        self.add_document(document, offset)
    }

    pub fn add_json(&mut self, data: &str, offset: Option<i64>) -> Result<()> {
        let j = serde_json::from_str::<serde_json::Value>(data)?;
        let mut document = TantivyDocument::default();
        j.add_to_document(self.field.field_id(), &mut document);

        self.add_document(document, offset)
    }

    pub fn add_array_json(&mut self, datas: &[*const c_char], offset: Option<i64>) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = c_ptr_to_str(*element)?;
            let j = serde_json::from_str::<serde_json::Value>(data)?;
            j.add_to_document(self.field.field_id(), &mut document);
        }

        self.add_document(document, offset)
    }

    pub fn add_array_keywords(
        &mut self,
        datas: &[*const c_char],
        offset: Option<i64>,
    ) -> Result<()> {
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
        offset: Option<i64>,
    ) -> Result<()> {
        debug_assert_eq!(ptrs.len(), lens.len());
        let mut document = TantivyDocument::default();
        for i in 0..ptrs.len() {
            let data = ptr_len_to_str(ptrs[i], lens[i])?;
            document.add_field_value(self.field, data);
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
        match &mut self.index_writer {
            Either::Left(writer) => {
                let mut batch = Vec::with_capacity(BATCH_SIZE);
                for i in 0..keys.len() {
                    let key = c_ptr_to_str(keys[i])
                        .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
                    let offsets =
                        unsafe { convert_to_rust_slice!(json_offsets[i], json_offsets_len[i]) };
                    for offset in offsets {
                        batch.push(UserOperation::Add(doc!(
                            id_field => *offset,
                            self.field => key,
                        )));
                        if batch.len() >= BATCH_SIZE {
                            writer.run(std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(BATCH_SIZE),
                            ))?;
                        }
                    }
                }
                if !batch.is_empty() {
                    writer.run(batch)?;
                }
            }
            Either::Right(writer) => {
                for i in 0..keys.len() {
                    let key = c_ptr_to_str(keys[i])
                        .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
                    let offsets =
                        unsafe { convert_to_rust_slice!(json_offsets[i], json_offsets_len[i]) };
                    for offset in offsets {
                        writer.add_document(doc!(
                            id_field => *offset,
                            self.field => key,
                        ))?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn manual_merge(&mut self) -> Result<()> {
        let Some(index_writer) = self.index_writer.as_mut().left() else {
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
        let enable_background_merge = self.enable_background_merge;
        match self.index_writer {
            Either::Left(mut index_writer) => {
                index_writer.commit()?;

                if !enable_background_merge {
                    // Build-mode writers use NoMergePolicy (set in new()), so no
                    // background merge can race this explicit merge-all. Collapse
                    // the auto-flushed segments into a single one. Background-merge
                    // writers (e.g. growing segments) keep their own policy and are
                    // not forced to a single segment here.
                    let segment_ids = index_writer.index().searchable_segment_ids()?;
                    if segment_ids.len() > 1 {
                        index_writer.merge(&segment_ids).wait()?;
                    }
                }

                index_writer.garbage_collect_files().wait()?;

                index_writer.wait_merging_threads()?;

                let metas = self._index.searchable_segment_metas()?;
                let segment_ids: Vec<_> = metas.iter().map(|m| m.id().uuid_string()).collect();
                info!("tantivy index_writer finish, segments: {:?}", segment_ids);
            }
            Either::Right(single_segment_index_writer) => {
                return Ok(single_segment_index_writer.finalize()?);
            }
        }
        Ok(self._index.as_ref().clone())
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        let Some(index_writer) = self.index_writer.as_mut().left() else {
            return Err(TantivyBindingError::InternalError(
                "commit is not supported by a direct writer".to_string(),
            ));
        };
        index_writer.commit()?;
        Ok(())
    }
}
