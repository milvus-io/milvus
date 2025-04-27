use std::ffi::c_char;
use std::ffi::CStr;

use crate::array::RustResult;
use crate::cstr_to_str;
use crate::index_writer::IndexWriterWrapper;
use crate::log::init_log;
use crate::util::create_binding;

#[no_mangle]
pub extern "C" fn tantivy_create_ngram_writer(
    field_name: *const c_char,
    path: *const c_char,
    min_gram: usize,
    max_gram: usize,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
) -> RustResult {
    init_log();
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);

    match IndexWriterWrapper::create_ngram_writer(
        field_name_str,
        path_str,
        min_gram,
        max_gram,
        num_threads,
        overall_memory_budget_in_bytes,
    ) {
        Ok(index_writer_wrapper) => RustResult::from_ptr(create_binding(index_writer_wrapper)),
        Err(err) => RustResult::from_error(format!(
            "create ngram writer failed with error: {}",
            err.to_string(),
        )),
    }
}
