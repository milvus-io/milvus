use std::ffi::c_char;
use std::ffi::CStr;
use log::warn;

use crate::array::RustResult;
use crate::cstr_to_str;
use crate::index_writer::IndexWriterWrapper;
use crate::log::init_log;
use crate::util::create_binding;
use crate::TantivyIndexVersion;

#[no_mangle]
pub extern "C" fn tantivy_create_text_writer(
    field_name: *const c_char,
    path: *const c_char,
    tantivy_index_version: u32,
    analyzer_name: *const c_char,
    analyzer_params: *const c_char,
    analyzer_extra_info: *const c_char,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
    in_ram: bool,
) -> RustResult {
    init_log();
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);
    let analyzer_name_str = cstr_to_str!(analyzer_name);
    let analyzer_params_str = cstr_to_str!(analyzer_params);
    let analyzer_extra_info_str = cstr_to_str!(analyzer_extra_info);


    let tantivy_index_version = match TantivyIndexVersion::from_u32(tantivy_index_version) {
        Ok(v) => v,
        Err(e) => return RustResult::from_error(e.to_string()),
    };

    warn!("test-- create text writer with analyzer_extra_info: {}", analyzer_extra_info_str);
    match IndexWriterWrapper::create_text_writer(
        field_name_str,
        path_str,
        analyzer_name_str,
        analyzer_params_str,
        analyzer_extra_info_str,
        num_threads,
        overall_memory_budget_in_bytes,
        in_ram,
        tantivy_index_version,
    ) {
        Ok(wrapper) => RustResult::from_ptr(create_binding(wrapper)),
        Err(err) => RustResult::from_error(format!(
            "create tokenizer failed with error: {} param: {}",
            err.to_string(),
            analyzer_params_str,
        )),
    }
}
