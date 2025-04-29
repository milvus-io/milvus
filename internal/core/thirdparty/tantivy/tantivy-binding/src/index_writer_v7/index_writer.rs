use std::ffi::CStr;
use std::sync::Arc;

use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy::indexer::UserOperation;
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
};
use tantivy::{doc, Index, IndexWriter, TantivyDocument};

use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_reader::IndexReaderWrapper;
use crate::index_reader_c::SetBitsetFn;
use crate::index_writer::TantivyValue;

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

pub struct IndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) index_writer: IndexWriter,
    pub(crate) index: Arc<Index>,
}

impl IndexWriterWrapperImpl {
    pub fn new(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create index writer, field_name: {}, data_type: {:?}, tantivy_index_version 7",
            field_name, data_type
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, field_name, data_type);
        schema_builder.enable_user_specified_doc_id();
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapperImpl {
            field,
            index_writer,
            index: Arc::new(index),
        })
    }

    pub fn create_reader(&self, set_bitset: SetBitsetFn) -> Result<IndexReaderWrapper> {
        IndexReaderWrapper::from_index(self.index.clone(), set_bitset)
    }

    #[inline]
    fn add_document(&mut self, document: TantivyDocument, offset: u32) -> Result<()> {
        self.index_writer
            .add_document_with_doc_id(offset, document)?;
        Ok(())
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
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }

        self.add_document(document, offset)
    }

    pub fn add_json_key_stats(
        &mut self,
        keys: &[*const c_char],
        json_offsets: &[*const i64],
        json_offsets_len: &[usize],
    ) -> Result<()> {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let id_field = self
            .index_writer
            .index()
            .schema()
            .get_field("doc_id")
            .unwrap();
        for i in 0..keys.len() {
            let key = unsafe { CStr::from_ptr(keys[i]) }
                .to_str()
                .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;

            let offsets =
                unsafe { std::slice::from_raw_parts(json_offsets[i], json_offsets_len[i]) };

            for offset in offsets {
                batch.push(UserOperation::Add(doc!(
                    id_field => *offset,
                    self.field => key,
                )));

                if batch.len() >= BATCH_SIZE {
                    self.index_writer.run(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(BATCH_SIZE),
                    ))?;
                }
            }
        }

        if !batch.is_empty() {
            self.index_writer.run(batch)?;
        }

        Ok(())
    }

    pub fn manual_merge(&mut self) -> Result<()> {
        let metas = self.index_writer.index().searchable_segment_metas()?;
        let policy = self.index_writer.get_merge_policy();
        let candidates = policy.compute_merge_candidates(metas.as_slice());
        for candidate in candidates {
            self.index_writer.merge(candidate.0.as_slice()).wait()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.index_writer.commit()?;
        // self.manual_merge();
        block_on(self.index_writer.garbage_collect_files())?;
        self.index_writer.wait_merging_threads()?;
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.index_writer.commit()?;
        Ok(())
    }
}
