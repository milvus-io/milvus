use std::ffi::CStr;

use libc::{c_char, c_void};

use crate::{
    array::RustResult, cstr_to_str, index_reader::IndexReaderWrapper, log::init_log,
    tokenizer::create_tokenizer,
};

#[no_mangle]
pub extern "C" fn tantivy_match_query(ptr: *mut c_void, query: *const c_char) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let query = cstr_to_str!(query);
        (*real).match_query(query).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_register_tokenizer(
    ptr: *mut c_void,
    tokenizer_name: *const c_char,
    analyzer_params: *const c_char,
) -> RustResult {
    init_log();
    let real = ptr as *mut IndexReaderWrapper;
    let tokenizer_name = cstr_to_str!(tokenizer_name);
    let params = cstr_to_str!(analyzer_params);
    let analyzer = create_tokenizer(params);
    match analyzer {
        Ok(text_analyzer) => unsafe {
            (*real).register_tokenizer(String::from(tokenizer_name), text_analyzer);
            Ok(()).into()
        },
        Err(err) => RustResult::from_error(err.to_string()),
    }
}
