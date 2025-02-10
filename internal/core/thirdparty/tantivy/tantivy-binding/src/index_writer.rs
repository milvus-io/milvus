use std::ffi::CStr;

use futures::executor::block_on;
use libc::c_char;
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, INDEXED};
use tantivy::{doc, tokenizer, Document, Index, SingleSegmentIndexWriter};

use crate::data_type::TantivyDataType;

use crate::error::Result;
use crate::index_reader::IndexReaderWrapper;
use crate::log::init_log;

pub struct IndexWriterWrapper {
    pub field_name: String,
    pub field: Field,
    pub data_type: TantivyDataType,
    pub path: String,
    pub index_writer: SingleSegmentIndexWriter,
}

impl IndexWriterWrapper {
    pub fn new(
        field_name: String,
        data_type: TantivyDataType,
        path: String,
    ) -> Result<IndexWriterWrapper> {
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
        let index = Index::create_in_dir(path.clone(), schema)?;
        if use_raw_tokenizer {
            index
                .tokenizers()
                .register("raw_tokenizer", tokenizer::RawTokenizer::default());
        }
        let index_writer = SingleSegmentIndexWriter::new(index, 15 * 1024 * 1024)?;
        Ok(IndexWriterWrapper {
            field_name,
            field,
            data_type,
            path,
            index_writer,
        })
    }

    pub fn add_i8(&mut self, data: i8) -> Result<()> {
        self.add_i64(data.into())
    }

    pub fn add_i16(&mut self, data: i16) -> Result<()> {
        self.add_i64(data.into())
    }

    pub fn add_i32(&mut self, data: i32) -> Result<()> {
        self.add_i64(data.into())
    }

    pub fn add_i64(&mut self, data: i64) -> Result<()> {
        let _ = self.index_writer.add_document(doc!(
            self.field => data,
        ))?;
        Ok(())
    }

    pub fn add_f32(&mut self, data: f32) -> Result<()> {
        self.add_f64(data.into())
    }

    pub fn add_f64(&mut self, data: f64) -> Result<()> {
        let _ = self.index_writer.add_document(doc!(
            self.field => data,
        ))?;
        Ok(())
    }

    pub fn add_bool(&mut self, data: bool) -> Result<()> {
        let _ = self.index_writer.add_document(doc!(
            self.field => data,
        ))?;
        Ok(())
    }

    pub fn add_keyword(&mut self, data: &str) -> Result<()> {
        let _ = self.index_writer.add_document(doc!(self.field => data))?;
        Ok(())
    }

    pub fn add_multi_i8s(&mut self, datas: &[i8]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_multi_i16s(&mut self, datas: &[i16]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_multi_i32s(&mut self, datas: &[i32]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_multi_i64s(&mut self, datas: &[i64]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_multi_f32s(&mut self, datas: &[f32]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as f64);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_multi_f64s(&mut self, datas: &[f64]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_multi_bools(&mut self, datas: &[bool]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_multi_keywords(&mut self, datas: &[*const c_char]) -> Result<()> {
        let mut document = Document::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }
        let _ = self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.index_writer.finalize()?;
        Ok(())
    }
}
