use core::slice;
use std::ffi::CStr;
use std::sync::Arc;

use either::Either;
use futures::executor::block_on;
use libc::c_char;
use log::info;
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, SchemaBuilder, TextFieldIndexing,
    TextOptions, FAST, INDEXED,
};
use tantivy::{doc, Document, Index, IndexWriter, SingleSegmentIndexWriter, UserOperation};

use crate::convert_to_rust_slice;
use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_reader::IndexReaderWrapper;
use crate::index_reader_c::SetBitsetFn;
use crate::log::init_log;

const BATCH_SIZE: usize = 4096;

pub(crate) struct IndexWriterWrapper {
    pub(crate) field: Field,
    pub(crate) index_writer: Either<IndexWriter, SingleSegmentIndexWriter>,
    pub(crate) index: Arc<Index>,
    pub(crate) single_segment_writer: bool,
}

#[inline]
pub(crate) fn schema_builder_add_field(
    schema_builder: &mut SchemaBuilder,
    field_name: &str,
    data_type: TantivyDataType,
) -> Field {
    match data_type {
        TantivyDataType::I64 => {
            schema_builder.add_i64_field(field_name, NumericOptions::default().set_indexed())
        }
        TantivyDataType::F64 => {
            schema_builder.add_f64_field(field_name, NumericOptions::default().set_indexed())
        }
        TantivyDataType::Bool => {
            schema_builder.add_bool_field(field_name, NumericOptions::default().set_indexed())
        }
        TantivyDataType::Keyword => {
            let text_field_indexing = TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_fieldnorms(false)
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
        in_ram: bool,
    ) -> Result<IndexWriterWrapper> {
        init_log();
        info!(
            "create index writer, field_name: {}, data_type: {:?}, num threads {}",
            field_name, data_type, num_threads
        );
        let mut schema_builder = Schema::builder();
        let field = schema_builder_add_field(&mut schema_builder, &field_name, data_type);
        schema_builder.enable_user_specified_doc_id();
        let schema = schema_builder.build();
        let index = if in_ram {
            Index::create_in_ram(schema)
        } else {
            Index::create_in_dir(path.clone(), schema)?
        };
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        Ok(IndexWriterWrapper {
            field,
            index_writer: Either::Left(index_writer),
            index: Arc::new(index),
            single_segment_writer: false,
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
            index: Arc::new(index),
            single_segment_writer: true,
        })
    }

    pub fn create_reader(&self, set_bitset: SetBitsetFn) -> Result<IndexReaderWrapper> {
        IndexReaderWrapper::from_index(self.index.clone(), set_bitset)
    }

    fn index_writer_add_document(&self, offset: u32, document: Document) -> Result<()> {
        match self.index_writer {
            Either::Left(ref writer) => {
                let _ = writer.add_document_with_doc_id(offset, document)?;
            }
            Either::Right(_) => {
                panic!("unexpected writer");
            }
        }
        Ok(())
    }

    fn single_segment_index_writer_add_document(&mut self, document: Document) -> Result<()> {
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

    #[inline]
    pub fn add_i8(&mut self, data: i8, offset: i64) -> Result<()> {
        self.add_i64(data.into(), offset)
    }

    #[inline]
    pub fn add_i16(&mut self, data: i16, offset: i64) -> Result<()> {
        self.add_i64(data.into(), offset)
    }

    #[inline]
    pub fn add_i32(&mut self, data: i32, offset: i64) -> Result<()> {
        self.add_i64(data.into(), offset)
    }

    #[inline]
    pub fn add_i64(&mut self, data: i64, offset: i64) -> Result<()> {
        self.index_writer_add_document(offset as u32, doc!(self.field => data))
    }

    #[inline]
    pub fn add_f32(&mut self, data: f32, offset: i64) -> Result<()> {
        self.add_f64(data.into(), offset)
    }

    #[inline]
    pub fn add_f64(&mut self, data: f64, offset: i64) -> Result<()> {
        self.index_writer_add_document(offset as u32, doc!(self.field => data))
    }

    #[inline]
    pub fn add_bool(&mut self, data: bool, offset: i64) -> Result<()> {
        self.index_writer_add_document(offset as u32, doc!(self.field => data))
    }

    #[inline]
    pub fn add_string(&mut self, data: &str, offset: i64) -> Result<()> {
        self.index_writer_add_document(offset as u32, doc!(self.field => data))
    }

    // add in batch within BATCH_SIZE
    pub fn add_json_key_stats(
        &mut self,
        keys: &[*const c_char],
        json_offsets: &[*const i64],
        json_offsets_len: &[usize],
    ) -> Result<()> {
        let writer = self.index_writer.as_ref().left().unwrap();
        let id_field = writer.index().schema().get_field("doc_id").unwrap();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        keys.iter()
            .zip(json_offsets.iter())
            .zip(json_offsets_len.iter())
            .try_for_each(|((key, json_offsets), json_offsets_len)| -> Result<()> {
                let key = unsafe { CStr::from_ptr(*key) }
                    .to_str()
                    .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;
                let json_offsets =
                    unsafe { convert_to_rust_slice!(*json_offsets, *json_offsets_len) };

                for offset in json_offsets {
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

                Ok(())
            })?;

        if !batch.is_empty() {
            writer.run(batch)?;
        }

        Ok(())
    }

    pub fn add_multi_i8s(&mut self, datas: &[i8], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        self.index_writer_add_document(offset as u32, document)
    }

    pub fn add_multi_i16s(&mut self, datas: &[i16], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        self.index_writer_add_document(offset as u32, document)
    }

    pub fn add_multi_i32s(&mut self, datas: &[i32], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        self.index_writer_add_document(offset as u32, document)
    }

    pub fn add_multi_i64s(&mut self, datas: &[i64], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        self.index_writer_add_document(offset as u32, document)
    }

    pub fn add_multi_f32s(&mut self, datas: &[f32], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as f64);
        }
        self.index_writer_add_document(offset as u32, document)
    }

    pub fn add_multi_f64s(&mut self, datas: &[f64], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        self.index_writer_add_document(offset as u32, document)
    }

    pub fn add_multi_bools(&mut self, datas: &[bool], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        self.index_writer_add_document(offset as u32, document)
    }

    pub fn add_multi_keywords(&mut self, datas: &[*const c_char], offset: i64) -> Result<()> {
        let mut document = Document::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }
        self.index_writer_add_document(offset as u32, document)
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

    pub fn add_multi_i8s_by_single_segment_writer(&mut self, datas: &[i8]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_multi_i16s_by_single_segment_writer(&mut self, datas: &[i16]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_multi_i32s_by_single_segment_writer(&mut self, datas: &[i32]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as i64);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_multi_i64s_by_single_segment_writer(&mut self, datas: &[i64]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_multi_f32s_by_single_segment_writer(&mut self, datas: &[f32]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data as f64);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_multi_f64s_by_single_segment_writer(&mut self, datas: &[f64]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_multi_bools_by_single_segment_writer(&mut self, datas: &[bool]) -> Result<()> {
        let mut document = Document::default();
        for data in datas {
            document.add_field_value(self.field, *data);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn add_multi_keywords_by_single_segment_writer(
        &mut self,
        datas: &[*const c_char],
    ) -> Result<()> {
        let mut document = Document::default();
        for element in datas {
            let data = unsafe { CStr::from_ptr(*element) };
            document.add_field_value(self.field, data.to_str()?);
        }
        self.single_segment_index_writer_add_document(document)
    }

    pub fn finish(self) -> Result<()> {
        match self.index_writer {
            Either::Left(mut index_writer) => {
                index_writer.commit()?;

                // merge all segments
                let segment_ids = index_writer.index().searchable_segment_ids()?;
                if segment_ids.len() > 1 {
                    let _ = index_writer.merge(&segment_ids).wait();
                }

                index_writer.garbage_collect_files().wait()?;

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

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use rand::Rng;
    use tempfile::tempdir;

    #[test]
    pub fn test_add_json_key_stats() {
        use crate::data_type::TantivyDataType;
        use crate::index_writer::IndexWriterWrapper;

        let temp_dir = tempdir().unwrap();
        let mut index_writer = IndexWriterWrapper::new(
            "test".to_string(),
            TantivyDataType::Keyword,
            temp_dir.path().to_str().unwrap().to_string(),
            1,
            15 * 1024 * 1024,
            false,
        )
        .unwrap();

        let keys = (0..100).map(|i| format!("key{:05}", i)).collect::<Vec<_>>();
        let mut total_count = 0;
        let mut rng = rand::thread_rng();
        let json_offsets = (0..100)
            .map(|_| {
                let count = rng.gen_range(0, 1000);
                total_count += count;
                (0..count)
                    .map(|_| rng.gen_range(0, i64::MAX))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let json_offsets_len = json_offsets
            .iter()
            .map(|offsets| offsets.len())
            .collect::<Vec<_>>();
        let json_offsets = json_offsets.iter().map(|x| x.as_ptr()).collect::<Vec<_>>();
        let c_keys: Vec<CString> = keys.into_iter().map(|k| CString::new(k).unwrap()).collect();
        let key_ptrs: Vec<*const libc::c_char> = c_keys.iter().map(|cs| cs.as_ptr()).collect();

        index_writer
            .add_json_key_stats(&key_ptrs, &json_offsets, &json_offsets_len)
            .unwrap();

        index_writer.commit().unwrap();
        let count: u32 = index_writer
            .index
            .load_metas()
            .unwrap()
            .segments
            .iter()
            .map(|s| s.max_doc())
            .sum();
        assert_eq!(count, total_count);
    }
}
