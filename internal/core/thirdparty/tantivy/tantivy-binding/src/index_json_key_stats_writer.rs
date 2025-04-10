use crate::error::Result;
use crate::index_writer::IndexWriterWrapper;
use crate::{index_writer_v5, index_writer_v7, TantivyIndexVersion};

impl IndexWriterWrapper {
    pub fn new_json_key_stats_writer(
        field_name: &str,
        path: &str,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        tanviy_index_version: TantivyIndexVersion,
        in_ram: bool,
    ) -> Result<IndexWriterWrapper> {
        match tanviy_index_version {
            TantivyIndexVersion::V5 => Ok(IndexWriterWrapper::V5(
                index_writer_v5::IndexWriterWrapperImpl::create_json_key_stats_writer(
                    field_name,
                    path,
                    num_threads,
                    overall_memory_budget_in_bytes,
                    in_ram,
                )?,
            )),
            TantivyIndexVersion::V7 => Ok(IndexWriterWrapper::V7(
                index_writer_v7::IndexWriterWrapperImpl::create_json_key_stats_writer(
                    field_name,
                    path,
                    num_threads,
                    overall_memory_budget_in_bytes,
                    in_ram,
                )?,
            )),
        }
    }
}
