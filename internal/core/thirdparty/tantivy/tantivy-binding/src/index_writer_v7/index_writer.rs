use std::ffi::CStr;
use std::sync::Arc;

use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy::indexer::UserOperation;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED,
};
use tantivy::{doc, Index, IndexWriter, TantivyDocument};

use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_reader::IndexReaderWrapper;
use crate::index_writer::TantivyValue;

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
            schema_builder.add_text_field(field_name, text_options)
        }
        TantivyDataType::Text => {
            panic!("text should be indexed with analyzer");
        }
    }
}

impl TantivyValue<TantivyDocument> for i8 {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_i64(Field::from_field_id(field), *self as i64);
    }
}

impl TantivyValue<TantivyDocument> for i16 {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_i64(Field::from_field_id(field), *self as i64);
    }
}

impl TantivyValue<TantivyDocument> for i32 {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_i64(Field::from_field_id(field), *self as i64);
    }
}

impl TantivyValue<TantivyDocument> for i64 {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_i64(Field::from_field_id(field), *self);
    }
}

impl TantivyValue<TantivyDocument> for f32 {
    fn add_to_document(&self, field: u32, document: &mut TantivyDocument) {
        document.add_f64(Field::from_field_id(field), *self as f64);
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
    pub(crate) index_writer: IndexWriter,
    pub(crate) id_field: Field,
    pub(crate) index: Arc<Index>,
}

impl IndexWriterWrapperImpl {
    pub fn new(
        field_name: &str,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        in_ram: bool,
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
        let index = if in_ram {
            Index::create_in_ram(schema)
        } else {
            Index::create_in_dir(path.clone(), schema)?
        };
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapperImpl {
            field,
            index_writer,
            id_field,
            index: Arc::new(index),
        })
    }

    pub fn create_reader(&self) -> Result<IndexReaderWrapper> {
        IndexReaderWrapper::from_index(self.index.clone())
    }

    #[inline]
    fn add_document(&mut self, mut document: TantivyDocument, offset: i64) -> Result<()> {
        document.add_i64(self.id_field, offset);
        self.index_writer.add_document(document)?;
        Ok(())
    }

    pub fn add_data_by_batch<T: TantivyValue<TantivyDocument>>(
        &mut self,
        batch_data: &[T],
        offset_begin: i64,
    ) -> Result<()> {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        for (idx, data) in batch_data.into_iter().enumerate() {
            let offset = offset_begin + idx as i64;

            let mut doc = TantivyDocument::default();
            data.add_to_document(self.field.field_id(), &mut doc);
            doc.add_i64(self.id_field, offset);

            batch.push(UserOperation::Add(doc));
            if batch.len() == BATCH_SIZE {
                self.index_writer.run(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(BATCH_SIZE),
                ))?;
            }
        }

        if !batch.is_empty() {
            self.index_writer.run(batch)?;
        }

        Ok(())
    }

    pub fn add_array<T: TantivyValue<TantivyDocument>, I>(
        &mut self,
        data: I,
        offset: i64,
    ) -> Result<()>
    where
        I: IntoIterator<Item = T>,
    {
        let mut document = TantivyDocument::default();
        data.into_iter()
            .for_each(|d| d.add_to_document(self.field.field_id(), &mut document));

        self.add_document(document, offset)
    }

    pub fn add_array_keywords(&mut self, datas: &[*const c_char], offset: i64) -> Result<()> {
        let mut document = TantivyDocument::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }

        self.add_document(document, offset)
    }

    pub fn add_string_by_batch(&mut self, data: &[*const c_char], offset: i64) -> Result<()> {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        for (idx, key) in data.into_iter().enumerate() {
            let key = unsafe { CStr::from_ptr(*key) }
                .to_str()
                .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
            let key_offset = offset + idx as i64;
            batch.push(UserOperation::Add(doc!(
                self.id_field => key_offset,
                self.field => key,
            )));
            if batch.len() >= BATCH_SIZE {
                self.index_writer.run(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(BATCH_SIZE),
                ))?;
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

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use tempfile::tempdir;

    use crate::TantivyIndexVersion;

    #[test]
    pub fn test_add_json_key_stats() {
        use crate::data_type::TantivyDataType;
        use crate::index_writer::IndexWriterWrapper;

        let temp_dir = tempdir().unwrap();
        let mut index_writer = IndexWriterWrapper::new(
            "test",
            TantivyDataType::Keyword,
            temp_dir.path().to_str().unwrap().to_string(),
            1,
            15 * 1024 * 1024,
            TantivyIndexVersion::V7,
        )
        .unwrap();

        let keys = (0..10000)
            .map(|i| format!("key{:05}", i))
            .collect::<Vec<_>>();

        let c_keys: Vec<CString> = keys.into_iter().map(|k| CString::new(k).unwrap()).collect();
        let key_ptrs: Vec<*const libc::c_char> = c_keys.iter().map(|cs| cs.as_ptr()).collect();

        index_writer
            .add_string_by_batch(&key_ptrs, Some(0))
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index_writer.create_reader().unwrap();
        let count: u32 = reader.count().unwrap();
        assert_eq!(count, 10000);
    }
}
