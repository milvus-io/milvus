use std::ffi::CStr;

use libc::{c_char, c_void};

use crate::{
    analyzer::create_analyzer, array::RustResult, cstr_to_str, index_reader::IndexReaderWrapper,
    log::init_log,
};

#[no_mangle]
pub extern "C" fn tantivy_match_query(
    ptr: *mut c_void,
    query: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let query = cstr_to_str!(query);
    unsafe { (*real).match_query(query, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_phrase_match_query(
    ptr: *mut c_void,
    query: *const c_char,
    slop: u32,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let query = cstr_to_str!(query);
    unsafe { (*real).phrase_match_query(query, slop, bitset).into() }
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
    let analyzer = create_analyzer(params);
    match analyzer {
        Ok(text_analyzer) => unsafe {
            (*real).register_tokenizer(String::from(tokenizer_name), text_analyzer);
            Ok(()).into()
        },
        Err(err) => RustResult::from_error(err.to_string()),
    }
}
