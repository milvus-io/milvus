use index_writer_v5::TantivyDocumentV5;
use index_writer_v7::TantivyDocumentV7;
use libc::c_char;

use crate::data_type::TantivyDataType;

use crate::error::{Result, TantivyBindingError};
use crate::index_reader::IndexReaderWrapper;
use crate::log::init_log;
use crate::{index_writer_v5, index_writer_v7, TantivyIndexVersion};

pub trait TantivyValue<D> {
    fn add_to_document(&self, field: u32, document: &mut D);
}

pub enum IndexWriterWrapper {
    V5(index_writer_v5::IndexWriterWrapperImpl),
    V7(index_writer_v7::IndexWriterWrapperImpl),
}

impl IndexWriterWrapper {
    // create a IndexWriterWrapper according to `tanviy_index_version`.
    // version 7 is the latest version and is what we should use in most cases.
    // We may also build with version 5 for compatibility for reader nodes with older versions.
    pub fn new(
        field_name: String,
        data_type: TantivyDataType,
        path: String,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        tanviy_index_version: TantivyIndexVersion,
    ) -> Result<IndexWriterWrapper> {
        init_log();
        match tanviy_index_version {
            TantivyIndexVersion::V5 => {
                let writer = index_writer_v5::IndexWriterWrapperImpl::new(
                    field_name,
                    data_type,
                    path,
                    num_threads,
                    overall_memory_budget_in_bytes,
                )?;
                Ok(IndexWriterWrapper::V5(writer))
            }
            TantivyIndexVersion::V7 => {
                let writer = index_writer_v7::IndexWriterWrapperImpl::new(
                    field_name,
                    data_type,
                    path,
                    num_threads,
                    overall_memory_budget_in_bytes,
                )?;
                Ok(IndexWriterWrapper::V7(writer))
            }
        }
    }

    pub fn new_with_single_segment(
        field_name: String,
        data_type: TantivyDataType,
        path: String,
        tanviy_index_version: TantivyIndexVersion,
    ) -> Result<IndexWriterWrapper> {
        init_log();
        match tanviy_index_version {
            TantivyIndexVersion::V5 => {
                let writer = index_writer_v5::IndexWriterWrapperImpl::new_with_single_segment(
                    field_name, data_type, path,
                )?;
                Ok(IndexWriterWrapper::V5(writer))
            }
            TantivyIndexVersion::V7 => {
                let writer = index_writer_v7::IndexWriterWrapperImpl::new_with_single_segment(
                    field_name, data_type, path,
                )?;
                Ok(IndexWriterWrapper::V7(writer))
            }
        }
    }

    pub fn create_reader(&self) -> Result<IndexReaderWrapper> {
        match self {
            IndexWriterWrapper::V5(_) => {
                return Err(TantivyBindingError::InternalError(
                    "create reader with tantivy index version 5 
                is not supported from tantivy with version 7"
                        .into(),
                ));
            }
            IndexWriterWrapper::V7(writer) => writer.create_reader(),
        }
    }

    pub fn add<T>(&mut self, data: T, offset: Option<i64>) -> Result<()>
    where
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocumentV7>,
    {
        match self {
            IndexWriterWrapper::V5(writer) => writer.add(data, offset),
            IndexWriterWrapper::V7(writer) => writer.add(data, offset),
        }
    }

    pub fn add_array<T, I>(&mut self, data: I, offset: Option<i64>) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: TantivyValue<TantivyDocumentV5> + TantivyValue<TantivyDocumentV7>,
    {
        match self {
            IndexWriterWrapper::V5(writer) => writer.add_array(data, offset),
            IndexWriterWrapper::V7(writer) => writer.add_array(data, offset),
        }
    }

    pub fn add_array_keywords(
        &mut self,
        datas: &[*const c_char],
        offset: Option<i64>,
    ) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(writer) => writer.add_array_keywords(datas, offset),
            IndexWriterWrapper::V7(writer) => writer.add_array_keywords(datas, offset),
        }
    }

    #[allow(dead_code)]
    pub fn manual_merge(&mut self) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(writer) => writer.manual_merge(),
            IndexWriterWrapper::V7(writer) => writer.manual_merge(),
        }
    }

    #[allow(dead_code)]
    pub fn commit(&mut self) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(writer) => writer.commit(),
            IndexWriterWrapper::V7(writer) => writer.commit(),
        }
    }

    #[allow(dead_code)]
    pub fn finish(self) -> Result<()> {
        match self {
            IndexWriterWrapper::V5(writer) => writer.finish(),
            IndexWriterWrapper::V7(writer) => writer.finish(),
        }
    }
}
