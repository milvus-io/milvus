use std::ffi::c_char;
use std::ffi::CStr;

use crate::array::RustResult;
use crate::cstr_to_str;
use crate::index_writer::IndexWriterWrapper;
use crate::log::init_log;
use crate::util::create_binding;

#[no_mangle]
pub extern "C" fn tantivy_create_json_key_stats_writer(
    field_name: *const c_char,
    path: *const c_char,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
) -> RustResult {
    init_log();
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);

    match IndexWriterWrapper::new_json_key_stats_writer(
        field_name_str,
        path_str,
        num_threads,
        overall_memory_budget_in_bytes,
    ) {
        Ok(wrapper) => RustResult::from_ptr(create_binding(wrapper)),
        Err(err) => RustResult::from_error(format!(
            "create json key stats writer failed with error: {}",
            err.to_string(),
        )),
    }
}
