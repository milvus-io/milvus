use std::ops::Bound;
use std::str::FromStr;

use futures::executor::block_on;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{Query, RangeQuery, TermQuery, RegexQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, INDEXED};
use tantivy::{doc, tokenizer, Index, IndexReader, IndexWriter, ReloadPolicy, Term};

use crate::data_type::TantivyDataType;
use crate::index_reader::IndexReaderWrapper;
use crate::util::make_bounds;

pub struct IndexWriterWrapper {
    pub field_name: String,
    pub field: Field,
    pub data_type: TantivyDataType,
    pub path: String,
    pub index: Index,
    pub index_writer: IndexWriter,
}

impl IndexWriterWrapper {
    pub fn create_reader(&self) -> IndexReaderWrapper {
        IndexReaderWrapper::new(&self.index, &self.field_name, self.field)
    }

    pub fn new(field_name: String, data_type: TantivyDataType, path: String) -> IndexWriterWrapper {
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
            _ => {
                panic!("not supported");
            }
        }
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema).unwrap();
        if use_raw_tokenizer {
            index
                .tokenizers()
                .register("raw_tokenizer", tokenizer::RawTokenizer::default());
        }
        let index_writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        IndexWriterWrapper {
            field_name: field_name,
            field: field,
            data_type: data_type,
            path: path,
            index: index,
            index_writer: index_writer,
        }
    }

    pub fn add_i8(&self, data: i8) {
        self.add_i64(data.into())
    }

    pub fn add_i16(&self, data: i16) {
        self.add_i64(data.into())
    }

    pub fn add_i32(&self, data: i32) {
        self.add_i64(data.into())
    }

    pub fn add_i64(&self, data: i64) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn add_f32(&self, data: f32) {
        self.add_f64(data.into())
    }

    pub fn add_f64(&self, data: f64) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn add_bool(&self, data: bool) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn add_keyword(&self, data: &str) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn finish(&mut self) {
        self.index_writer.commit().unwrap();
        block_on(self.index_writer.garbage_collect_files()).unwrap();
    }
}
