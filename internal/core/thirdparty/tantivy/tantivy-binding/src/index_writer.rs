use std::ffi::CStr;
use std::sync::Arc;

use either::Either;
use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED,
};
use tantivy::{doc, Index, IndexWriter, SingleSegmentIndexWriter, TantivyDocument};

use crate::data_type::TantivyDataType;

use crate::error::Result;
use crate::index_reader::IndexReaderWrapper;
use crate::log::init_log;

pub(crate) struct IndexWriterWrapper {
    pub(crate) field: Field,
    pub(crate) index_writer: Either<IndexWriter, SingleSegmentIndexWriter>,
    pub(crate) id_field: Option<Field>,
    pub(crate) index: Arc<Index>,
}

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

impl IndexWriterWrapper {
    pub fn new(
        field_name: String,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> Result<IndexWriterWrapper> {
        init_log();
        info!(
            "create index writer, field_name: {}, data_type: {:?}",
            field_name, data_type
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, &field_name, data_type);
        // We cannot build direct connection from rows in multi-segments to milvus row data. So we have this doc_id field.
        let id_field = schema_builder.add_i64_field("doc_id", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapper {
            field,
            index_writer: Either::Left(index_writer),
            id_field: Some(id_field),
            index: Arc::new(index),
        })
    }

    pub fn new_with_single_segment(
        field_name: String,
        data_type: TantivyDataType,
        path: String,
    ) -> Result<IndexWriterWrapper> {
        init_log();
        info!(
            "create single segment index writer, field_name: {}, data_type: {:?}",
            field_name, data_type
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, &field_name, data_type);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        let index_writer = SingleSegmentIndexWriter::new(index.clone(), 15 * 1024 * 1024)?;
        Ok(IndexWriterWrapper {
            field,
            index_writer: Either::Right(index_writer),
            id_field: None,
            index: Arc::new(index),
        })
    }

    pub fn create_reader(&self) -> Result<IndexReaderWrapper> {
        IndexReaderWrapper::from_index(self.index.clone())
    }

    fn index_writer_add_document(&mut self, document: TantivyDocument) -> Result<()> {
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

    // Add document ID to the document (if needed)
    fn add_doc_id(&self, document: &mut TantivyDocument, offset: Option<i64>) {
        if let Some(offset) = offset {
            if let Some(id_field) = self.id_field {
                document.add_i64(id_field, offset);
            }
        }
    }

    pub fn add_integer<T>(&mut self, data: T, offset: Option<i64>) -> Result<()>
    where
        T: Into<i64>,
    {
        let mut doc = TantivyDocument::default();
        doc.add_i64(self.field, data.into());
        self.add_doc_id(&mut doc, offset);
        self.index_writer_add_document(doc)
    }

    pub fn add_float<T>(&mut self, data: T, offset: Option<i64>) -> Result<()>
    where
        T: Into<f64>,
    {
        let mut doc = TantivyDocument::default();
        doc.add_f64(self.field, data.into());
        self.add_doc_id(&mut doc, offset);
        self.index_writer_add_document(doc)
    }

    pub fn add_bool(&mut self, data: bool, offset: Option<i64>) -> Result<()> {
        let mut doc = TantivyDocument::default();
        doc.add_bool(self.field, data);
        self.add_doc_id(&mut doc, offset);
        self.index_writer_add_document(doc)
    }

    pub fn add_string(&mut self, data: &str, offset: Option<i64>) -> Result<()> {
        let mut doc = TantivyDocument::default();
        doc.add_text(self.field, data);
        self.add_doc_id(&mut doc, offset);
        self.index_writer_add_document(doc)
    }

    pub fn add_array_integer<T>(&mut self, datas: &[T], offset: Option<i64>) -> Result<()>
    where
        T: Into<i64> + Copy,
    {
        let mut document = TantivyDocument::default();
        for data in datas {
            let value: i64 = (*data).into();
            document.add_i64(self.field, value);
        }
        self.add_doc_id(&mut document, offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_float<T>(&mut self, datas: &[T], offset: Option<i64>) -> Result<()>
    where
        T: Into<f64> + Copy,
    {
        let mut document = TantivyDocument::default();
        for data in datas {
            let value: f64 = (*data).into();
            document.add_f64(self.field, value);
        }
        self.add_doc_id(&mut document, offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_bool(&mut self, datas: &[bool], offset: Option<i64>) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_bool(self.field, *data);
        }
        self.add_doc_id(&mut document, offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_keyword(
        &mut self,
        datas: &[*const c_char],
        offset: Option<i64>,
    ) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_text(self.field, data.to_str()?);
        }
        self.add_doc_id(&mut document, offset);
        self.index_writer_add_document(document)
    }

    fn manual_merge(&mut self) -> Result<()> {
        if let Either::Left(index_writer) = &mut self.index_writer {
            let metas = index_writer.index().searchable_segment_metas()?;
            let policy = index_writer.get_merge_policy();
            let candidates = policy.compute_merge_candidates(metas.as_slice());
            for candidate in candidates {
                index_writer.merge(candidate.0.as_slice()).wait()?;
            }
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
        if let Either::Left(index_writer) = &mut self.index_writer {
            index_writer.commit()?;
        }
        Ok(())
    }
}
