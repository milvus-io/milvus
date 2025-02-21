use std::ffi::CStr;
use std::sync::Arc;

use either::Either;
use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy::indexer::UserOperation;
use tantivy::schema::{
    Field, IndexRecordOption, OwnedValue, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
    Value, FAST, INDEXED,
};
use tantivy::{
    doc, tokenizer, Document, Index, IndexWriter, SingleSegmentIndexWriter, TantivyDocument,
};

use crate::data_type::TantivyDataType;

use crate::error::Result;
use crate::index_reader::IndexReaderWrapper;
use crate::log::init_log;

#[macro_export]
macro_rules! add_batch_documents {
    ($index_writer:expr, $data:expr, $field:expr, $id_field:expr, $offset:ident) => {{
        let mut ops = Vec::with_capacity(1000);
        for d in $data {
            ops.push(UserOperation::Add(doc!(
                $id_field.unwrap() => $offset,
                $field => d,
            )));
            $offset += 1;
            if ops.len() == 1000 {
                $index_writer.index_writer_add_batch_documents(ops)?;
                ops = Vec::with_capacity(1000);
            }
        }
        $index_writer.index_writer_add_batch_documents(ops)
    }};
}

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

    fn index_writer_add_batch_documents(
        &self,
        ops: Vec<UserOperation<TantivyDocument>>,
    ) -> Result<()> {
        match self.index_writer {
            Either::Left(ref writer) => {
                let _ = writer.run(ops)?;
            }
            Either::Right(_) => {
                panic!("unexpected writer");
            }
        }
        Ok(())
    }

    fn index_writer_add_document(&self, document: TantivyDocument) -> Result<()> {
        match self.index_writer {
            Either::Left(ref writer) => {
                let _ = writer.add_document(document)?;
            }
            Either::Right(_) => {
                panic!("unexpected writer");
            }
        }
        Ok(())
    }

    fn single_segment_index_writer_add_document(
        &mut self,
        document: TantivyDocument,
    ) -> Result<()> {
        match self.index_writer {
            Either::Left(_) => {
                panic!("unexpected writer");
            }
            Either::Right(ref mut single_segmnet_writer) => {
                let _ = single_segmnet_writer.add_document(document)?;
            }
        }
        Ok(())
    }

    pub fn add_i8s(&mut self, data: &[i8], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().map(|&i| i as i64),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_i16s(&mut self, data: &[i16], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().map(|&i| i as i64),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_i32s(&mut self, data: &[i32], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().map(|&i| i as i64),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_i64s(&mut self, data: &[i64], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().copied(),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_f32s(&mut self, data: &[f32], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().map(|&i| i as f64),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_f64s(&mut self, data: &[f64], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().copied(),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_bools(&mut self, data: &[bool], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().copied(),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_strings(&mut self, data: &[&str], mut offset: i64) -> Result<()> {
        add_batch_documents!(
            self,
            data.iter().copied(),
            self.field,
            self.id_field,
            offset
        )
    }

    pub fn add_array_i8s(&mut self, datas: &[i8], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as i64));
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_i16s(&mut self, datas: &[i16], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as i64));
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_i32s(&mut self, datas: &[i32], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as i64));
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_i64s(&mut self, datas: &[i64], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, data);
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_f32s(&mut self, datas: &[f32], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as f64));
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_f64s(&mut self, datas: &[f64], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, data);
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_bools(&mut self, datas: &[bool], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, data);
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_array_keywords(&mut self, datas: &[*const c_char], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }
        document.add_i64(self.id_field.unwrap(), offset);
        self.index_writer_add_document(document)
    }

    pub fn add_i8_by_single_segment_writer(&mut self, data: i8) -> Result<()> {
        self.add_i64_by_single_segment_writer(data.into())
    }

    pub fn add_i16_by_single_segment_writer(&mut self, data: i16) -> Result<()> {
        self.add_i64_by_single_segment_writer(data.into())
    }

    pub fn add_i32_by_single_segment_writer(&mut self, data: i32) -> Result<()> {
        self.add_i64_by_single_segment_writer(data.into())
    }

    pub fn add_i64_by_single_segment_writer(&mut self, data: i64) -> Result<()> {
        self.single_segment_index_writer_add_document(doc!(
            self.field => data
        ))
    }

    pub fn add_f32_by_single_segment_writer(&mut self, data: f32) -> Result<()> {
        self.add_f64_by_single_segment_writer(data.into())
    }

    pub fn add_f64_by_single_segment_writer(&mut self, data: f64) -> Result<()> {
        self.single_segment_index_writer_add_document(doc!(
            self.field => data
        ))
    }

    pub fn add_bool_by_single_segment_writer(&mut self, data: bool) -> Result<()> {
        self.single_segment_index_writer_add_document(doc!(
            self.field => data
        ))
    }

    pub fn add_string_by_single_segment_writer(&mut self, data: &str) -> Result<()> {
        self.single_segment_index_writer_add_document(doc!(
            self.field => data
        ))
    }

    pub fn add_array_i8s_by_single_segment_writer(&mut self, datas: &[i8]) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as i64));
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_array_i16s_by_single_segment_writer(&mut self, datas: &[i16]) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as i64));
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_array_i32s_by_single_segment_writer(&mut self, datas: &[i32]) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as i64));
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_array_i64s_by_single_segment_writer(&mut self, datas: &[i64]) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, data);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_array_f32s_by_single_segment_writer(&mut self, datas: &[f32]) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, &(*data as f64));
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_array_f64s_by_single_segment_writer(&mut self, datas: &[f64]) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, data);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_array_bools_by_single_segment_writer(&mut self, datas: &[bool]) -> Result<()> {
        let mut document = TantivyDocument::default();
        for data in datas {
            document.add_field_value(self.field, data);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_array_keywords_by_single_segment_writer(
        &mut self,
        datas: &[*const c_char],
    ) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }
        self.single_segment_index_writer_add_document(document)
    }

    fn manual_merge(&mut self) -> Result<()> {
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
