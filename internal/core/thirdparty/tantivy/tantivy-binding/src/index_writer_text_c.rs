use std::ffi::c_char;
use std::ffi::CStr;

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
    tokenizer_name: *const c_char,
    analyzer_params: *const c_char,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
    in_ram: bool,
) -> RustResult {
    init_log();
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);
    let tokenizer_name_str = cstr_to_str!(tokenizer_name);
    let params = cstr_to_str!(analyzer_params);
    match IndexWriterWrapper::create_text_writer(
        String::from(field_name_str),
        String::from(path_str),
        String::from(tokenizer_name_str),
        params,
        num_threads,
        overall_memory_budget_in_bytes,
        in_ram,
        TantivyIndexVersion::default_version(),
    ) {
        Ok(wrapper) => RustResult::from_ptr(create_binding(wrapper)),
        Err(err) => RustResult::from_error(format!(
            "create tokenizer failed with error: {} param: {}",
            err.to_string(),
            params,
        )),
    }
}
