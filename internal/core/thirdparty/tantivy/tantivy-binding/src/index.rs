use tantivy::schema::{Field, Schema, INDEXED};
use tantivy::{doc, Index, IndexWriter};

use crate::data_type::TantivyDataType;

pub struct IndexWrapper {
    pub field_name: String,
    pub field: Field,
    pub data_type: TantivyDataType,
    pub path: String,
    pub index: Index,
    pub index_writer: IndexWriter,
}

impl IndexWrapper {
    pub fn new(field_name: String, data_type: TantivyDataType, path: String) -> IndexWrapper {
        let schema: Schema;
        let index: Index;
        let field: Field;
        let index_writer: IndexWriter;
        match data_type {
            TantivyDataType::I64 => {
                let mut schema_builder = Schema::builder();
                field = schema_builder.add_i64_field(&field_name, INDEXED);
                schema = schema_builder.build();
                index = Index::create_in_dir(path.clone(), schema).unwrap();
                index_writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
            }
            _ => {
                panic!("not supported");
            }
        }
        IndexWrapper {
            field_name: field_name,
            field: field,
            data_type: data_type,
            path: path,
            index: index,
            index_writer: index_writer,
        }
    }

    pub fn add_i64(&mut self, data: i64) {
        self.index_writer
            .add_document(doc!(self.field => data))
            .unwrap();
    }

    pub fn add_i64s(&mut self, datas: Vec<i64>) {
        for data in datas {
            self.add_i64(data);
        }
    }

    pub fn finish(&mut self) {
        self.index_writer.commit();
    }
}
