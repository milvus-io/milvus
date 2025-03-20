use crate::error::Result;
use crate::index_writer::IndexWriterWrapper;
use crate::{index_writer_v5, index_writer_v7, TantivyIndexVersion};

impl IndexWriterWrapper {
    // create a text writer according to `tanviy_index_version`.
    // version 7 is the latest version and is what we should use in most cases.
    // We may also build with version 5 for compatibility for reader nodes with older versions.
    pub(crate) fn create_text_writer(
        field_name: String,
        path: String,
        tokenizer_name: String,
        tokenizer_params: &str,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        in_ram: bool,
        tanviy_index_version: TantivyIndexVersion,
    ) -> Result<IndexWriterWrapper> {
        match tanviy_index_version {
            TantivyIndexVersion::V5 => Ok(IndexWriterWrapper::V5(
                index_writer_v5::IndexWriterWrapperImpl::create_text_writer(
                    field_name,
                    path,
                    tokenizer_name,
                    tokenizer_params,
                    num_threads,
                    overall_memory_budget_in_bytes,
                    in_ram,
                )?,
            )),
            TantivyIndexVersion::V7 => Ok(IndexWriterWrapper::V7(
                index_writer_v7::IndexWriterWrapperImpl::create_text_writer(
                    field_name,
                    path,
                    tokenizer_name,
                    tokenizer_params,
                    num_threads,
                    overall_memory_budget_in_bytes,
                    in_ram,
                )?,
            )),
        }
    }
}
