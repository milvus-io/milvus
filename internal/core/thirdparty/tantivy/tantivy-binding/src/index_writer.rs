use std::ffi::CStr;
use std::sync::Arc;

use futures::executor::block_on;
use libc::c_char;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST, INDEXED,
};
use tantivy::{doc, tokenizer, Document, Index, IndexWriter};

use crate::data_type::TantivyDataType;

use crate::index_reader::IndexReaderWrapper;
use crate::log::init_log;

pub(crate) struct IndexWriterWrapper {
    pub(crate) field: Field,
    pub(crate) index_writer: IndexWriter,
    pub(crate) id_field: Field,
    pub(crate) index: Arc<Index>,
}

impl IndexWriterWrapper {
    pub fn new(
        field_name: String,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> IndexWriterWrapper {
        init_log();

        let field: Field;
        let mut schema_builder = Schema::builder();
        let mut use_raw_tokenizer = false;
        match data_type {
            TantivyDataType::I64 => {
                field = schema_builder.add_i64_field(&field_name, INDEXED);
            }
            TantivyDataType::F64 => {
                field = schema_builder.add_f64_field(&field_name, INDEXED);
            }
            TantivyDataType::Bool => {
                field = schema_builder.add_bool_field(&field_name, INDEXED);
            }
            TantivyDataType::Keyword => {
                let text_field_indexing = TextFieldIndexing::default()
                    .set_tokenizer("raw_tokenizer")
                    .set_index_option(IndexRecordOption::Basic);
                let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
                field = schema_builder.add_text_field(&field_name, text_options);
                use_raw_tokenizer = true;
            }
        }
        let id_field = schema_builder.add_i64_field("doc_id", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema).unwrap();
        if use_raw_tokenizer {
            index
                .tokenizers()
                .register("raw_tokenizer", tokenizer::RawTokenizer::default());
        }
        let index_writer = index
            .writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)
            .unwrap();
        IndexWriterWrapper {
            field,
            index_writer,
            id_field,
            index: Arc::new(index),
        }
    }

    pub fn add_i8(&mut self, data: i8, offset: i64) {
        self.add_i64(data.into(), offset)
    }

    pub fn add_i16(&mut self, data: i16, offset: i64) {
        self.add_i64(data.into(), offset)
    }

    pub fn add_i32(&mut self, data: i32, offset: i64) {
        self.add_i64(data.into(), offset)
    }

    pub fn add_i64(&mut self, data: i64, offset: i64) {
        self.index_writer
            .add_document(doc!(
                self.field => data,
                self.id_field => offset,
            ))
            .unwrap();
    }

    pub fn add_f32(&mut self, data: f32, offset: i64) {
        self.add_f64(data.into(), offset)
    }

    pub fn add_f64(&mut self, data: f64, offset: i64) {
        self.index_writer
            .add_document(doc!(
                self.field => data,
                self.id_field => offset,
            ))
            .unwrap();
    }

    pub fn add_bool(&mut self, data: bool, offset: i64) {
        self.index_writer
            .add_document(doc!(
                self.field => data,
                self.id_field => offset,
            ))
            .unwrap();
    }

    pub fn add_string(&mut self, data: &str, offset: i64) {
        self.index_writer
            .add_document(doc!(
                self.field => data,
                self.id_field => offset,
            ))
            .unwrap();
    }

    pub fn add_multi_i8s(&mut self, datas: &[i8], offset: i64) {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    pub fn add_multi_i16s(&mut self, datas: &[i16], offset: i64) {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    pub fn add_multi_i32s(&mut self, datas: &[i32], offset: i64) {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    pub fn add_multi_i64s(&mut self, datas: &[i64], offset: i64) {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    pub fn add_multi_f32s(&mut self, datas: &[f32], offset: i64) {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as f64);
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    pub fn add_multi_f64s(&mut self, datas: &[f64], offset: i64) {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    pub fn add_multi_bools(&mut self, datas: &[bool], offset: i64) {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    pub fn add_multi_keywords(&mut self, datas: &[*const c_char], offset: i64) {
        let mut document = Document::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str().unwrap());
        }
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document).unwrap();
    }

    fn manual_merge(&mut self) {
        let metas = self
            .index_writer
            .index()
            .searchable_segment_metas()
            .unwrap();
        let policy = self.index_writer.get_merge_policy();
        let candidates = policy.compute_merge_candidates(metas.as_slice());
        for candidate in candidates {
            self.index_writer
                .merge(candidate.0.as_slice())
                .wait()
                .unwrap();
        }
    }

    pub fn finish(mut self) {
        self.index_writer.commit().unwrap();
        // self.manual_merge();
        block_on(self.index_writer.garbage_collect_files()).unwrap();
        self.index_writer.wait_merging_threads().unwrap();
    }

    pub(crate) fn commit(&mut self) {
        self.index_writer.commit().unwrap();
    }
}
