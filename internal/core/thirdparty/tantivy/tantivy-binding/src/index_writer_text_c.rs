use std::ffi::c_char;
use std::ffi::c_void;
use std::ffi::CStr;

use crate::array::RustResult;
use crate::cstr_to_str;
use crate::error::Result;
use crate::index_writer::IndexWriterWrapper;
use crate::log::init_log;
use crate::string_c::c_str_to_str;
use crate::tokenizer::create_tokenizer;
use crate::util::create_binding;

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
    let analyzer = create_tokenizer(params);
    match analyzer {
        Ok(text_analyzer) => {
            let wrapper = IndexWriterWrapper::create_text_writer(
                String::from(field_name_str),
                String::from(path_str),
                String::from(tokenizer_name_str),
                text_analyzer,
                num_threads,
                overall_memory_budget_in_bytes,
                in_ram,
            );
            RustResult::from_ptr(create_binding(wrapper))
        }
        Err(err) => RustResult::from_error(format!(
            "create tokenizer failed with error: {} param: {}",
            err.to_string(),
            params,
        )),
    }
}
