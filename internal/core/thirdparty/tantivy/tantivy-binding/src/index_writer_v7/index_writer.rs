use std::ffi::CStr;
use std::sync::Arc;

use either::Either;
use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy::indexer::UserOperation;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED,
};
use tantivy::{doc, Index, IndexWriter, SingleSegmentIndexWriter, TantivyDocument};

use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_reader::IndexReaderWrapper;
use crate::index_writer::TantivyValue;
use crate::log::init_log;

const BATCH_SIZE: usize = 4096;

#[inline]
fn schema_builder_add_field(
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
            schema_builder.add_text_field(&field_name, text_options)
        }
        TantivyDataType::Text => {
            panic!("text should be indexed with analyzer");
        }
    }
}

impl TantivyValue<TantivyDocument> for i64 {
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
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_f64(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for &str {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_text(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for bool {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_bool(Field::from_field_id(field), *self);
    }
}

pub struct IndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) index_writer: Either<IndexWriter, SingleSegmentIndexWriter>,
    pub(crate) id_field: Option<Field>,
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
            index: Arc::new(index),
        })
    }

    pub fn new_with_single_segment(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
    ) -> Result<IndexWriterWrapperImpl> {
        init_log();
        info!(
            "create single segment index writer, field_name: {}, data_type: {:?}, tantivy_index_version 7",
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
            index: Arc::new(index),
        })
    }

    pub fn create_reader(&self) -> Result<IndexReaderWrapper> {
        IndexReaderWrapper::from_index(self.index.clone())
    }

    #[inline]
    fn add_document(&mut self, mut document: TantivyDocument, offset: Option<i64>) -> Result<()> {
        if let Some(id_field) = self.id_field {
            document.add_i64(id_field, offset.unwrap());
        }

        match &mut self.index_writer {
            Either::Left(writer) => {
                let _ = writer.add_document(document)?;
            }
            Either::Right(single_segment_writer) => {
                let _ = single_segment_writer.add_document(document)?;
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

    pub fn add_string_by_batch(
        &mut self,
        data: &[*const c_char],
        offset: Option<i64>,
    ) -> Result<()> {
        match &self.index_writer {
            Either::Left(_) => self.add_strings(data, offset.unwrap()),
            Either::Right(_) => self.add_strings_by_single_segment(data),
        }
    }

    fn add_strings(&mut self, data: &[*const c_char], offset: i64) -> Result<()> {
        let writer = self.index_writer.as_ref().left().unwrap();
        let id_field = self.id_field.unwrap();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        data.iter()
            .enumerate()
            .try_for_each(|(idx, key)| -> Result<()> {
                let key = unsafe { CStr::from_ptr(*key) }
                    .to_str()
                    .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
                let key_offset = offset + idx as i64;
                batch.push(UserOperation::Add(doc!(
                    id_field => key_offset,
                    self.field => key,
                )));
                if batch.len() >= BATCH_SIZE {
                    writer.run(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(BATCH_SIZE),
                    ))?;
                }

                Ok(())
            })?;

        if !batch.is_empty() {
            writer.run(std::mem::replace(
                &mut batch,
                Vec::with_capacity(BATCH_SIZE),
            ))?;
        }

        Ok(())
    }

    fn add_strings_by_single_segment(&mut self, data: &[*const c_char]) -> Result<()> {
        let writer = self.index_writer.as_mut().right().unwrap();
        for key in data {
            let key = unsafe { CStr::from_ptr(*key) }
                .to_str()
                .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
            writer.add_document(doc!(self.field => key))?;
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
