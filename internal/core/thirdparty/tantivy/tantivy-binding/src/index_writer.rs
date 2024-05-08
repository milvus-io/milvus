use futures::executor::block_on;

use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, INDEXED};
use tantivy::{doc, tokenizer, Index, IndexWriter, SingleSegmentIndexWriter};

use crate::data_type::TantivyDataType;
use crate::index_writer;
use crate::log::init_log;

pub struct IndexWriterWrapper {
    pub field_name: String,
    pub field: Field,
    pub data_type: TantivyDataType,
    pub path: String,
    pub index_writer: SingleSegmentIndexWriter,
}

impl IndexWriterWrapper {
    pub fn new(field_name: String, data_type: TantivyDataType, path: String) -> IndexWriterWrapper {
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
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema).unwrap();
        if use_raw_tokenizer {
            index
                .tokenizers()
                .register("raw_tokenizer", tokenizer::RawTokenizer::default());
        }
        let index_writer = SingleSegmentIndexWriter::new(index, 15 * 1024 * 1024).unwrap();
        IndexWriterWrapper {
            field_name,
            field,
            data_type,
            path,
            index_writer,
        }
    }

    pub fn add_i8(&mut self, data: i8) {
        self.add_i64(data.into())
    }

    pub fn add_i16(&mut self, data: i16) {
        self.add_i64(data.into())
    }

    pub fn add_i32(&mut self, data: i32) {
        self.add_i64(data.into())
    }

    pub fn add_i64(&mut self, data: i64) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn add_f32(&mut self, data: f32) {
        self.add_f64(data.into())
    }

    pub fn add_f64(&mut self, data: f64) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn add_bool(&mut self, data: bool) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn add_keyword(&mut self, data: &str) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn finish(mut self) {
        self.index_writer
            .finalize()
            .expect("failed to build inverted index");
    }
}
