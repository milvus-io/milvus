use std::ffi::CStr;
use std::sync::Arc;

use either::Either;
use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy_5::schema::{
    Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED,
};
use tantivy_5::{doc, Document as TantivyDocument, Index, IndexWriter, SingleSegmentIndexWriter, UserOperation};

use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_writer::TantivyValue;

const BATCH_SIZE: usize = 4096;

pub(crate) struct IndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) index_writer: Either<IndexWriter, SingleSegmentIndexWriter>,
    pub(crate) id_field: Option<Field>,
    pub(crate) _index: Arc<Index>,
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

impl IndexWriterWrapperImpl {
    pub fn new(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create index writer, field_name: {}, data_type: {:?}, tantivy_index_version 5",
            field_name, data_type
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, field_name, data_type);
        // We cannot build direct connection from rows in multi-segments to milvus row data. So we have this doc_id field.
        let id_field = schema_builder.add_i64_field("doc_id", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapperImpl {
            field,
            index_writer: Either::Left(index_writer),
            id_field: Some(id_field),
            _index: Arc::new(index),
        })
    }

    pub fn new_with_single_segment(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
    ) -> Result<IndexWriterWrapperImpl> {
        info!(
            "create single segment index writer, field_name: {}, data_type: {:?}, tantivy_index_version 5",
            field_name, data_type
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, field_name, data_type);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer = SingleSegmentIndexWriter::new(index.clone(), 15 * 1024 * 1024)?;
        Ok(IndexWriterWrapperImpl {
            field,
            index_writer: Either::Right(index_writer),
            id_field: None,
            _index: Arc::new(index),
        })
    }

    #[inline]
    fn add_document(&mut self, mut document: TantivyDocument, offset: Option<i64>) -> Result<()> {
        if let Some(id_field) = self.id_field {
            document.add_i64(id_field, offset.unwrap());
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

    pub fn add_array_keywords(
        &mut self,
        datas: &[*const c_char],
        offset: Option<i64>,
    ) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }

        self.add_document(document, offset)
    }

    pub fn add_json_key_stats(
        &mut self,
        keys: &[*const i8],
        json_offsets: &[*const i64],
        json_offsets_len: &[usize],
    ) -> Result<()> {
        let writer = self.index_writer.as_ref().left().unwrap();
        let id_field = self.id_field.unwrap();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
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

        Ok(())
    }

    pub fn manual_merge(&mut self) -> Result<()> {
        let index_writer = self.index_writer.as_mut().left().unwrap();
        let metas = index_writer.index().searchable_segment_metas()?;
        let policy = index_writer.get_merge_policy();
        let candidates = policy.compute_merge_candidates(metas.as_slice());
        for candidate in candidates {
            index_writer.merge(candidate.0.as_slice()).wait()?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        match self.index_writer {
            Either::Left(mut index_writer) => {
                index_writer.commit()?;
                // self.manual_merge();
                block_on(index_writer.garbage_collect_files())?;
                index_writer.wait_merging_threads()?;
            }
            Either::Right(single_segment_index_writer) => {
                single_segment_index_writer
                    .finalize()
                    .expect("failed to build inverted index");
            }
        }
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.index_writer.as_mut().left().unwrap().commit()?;
        Ok(())
    }
}
