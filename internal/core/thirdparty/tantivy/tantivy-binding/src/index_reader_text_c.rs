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
    min_should_match: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let query = cstr_to_str!(query);
    if min_should_match > 1 {
        unsafe {
            (*real)
                .match_query_with_minimum(query, min_should_match, bitset)
                .into()
        }
    } else {
        unsafe { (*real).match_query(query, bitset).into() }
    }
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
pub extern "C" fn tantivy_fuzzy_match_query(
    ptr: *mut c_void,
    query: *const c_char,
    max_edit_distance: u32,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let query = cstr_to_str!(query);
    unsafe {
        (*real)
            .fuzzy_match_query(query, max_edit_distance, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_register_tokenizer(
    ptr: *mut c_void,
    tokenizer_name: *const c_char,
    analyzer_params: *const c_char,
    analyzer_extra_info: *const c_char,
) -> RustResult {
    init_log();
    let real = ptr as *mut IndexReaderWrapper;
    let tokenizer_name = cstr_to_str!(tokenizer_name);
    let params = cstr_to_str!(analyzer_params);

    let extra_info_str = cstr_to_str!(analyzer_extra_info);
    let analyzer = create_analyzer(params, extra_info_str);
    match analyzer {
        Ok(text_analyzer) => unsafe {
            (*real).register_tokenizer(String::from(tokenizer_name), text_analyzer);
            Ok(()).into()
        },
        Err(err) => RustResult::from_binding_error(&err),
    }
}

/// Clone the registered analyzer, including resolved external resources.
/// The writer and its snapshot own their copy independently.
///
/// # Safety
/// `ptr` must point to an IndexReaderWrapper kept alive for this call.
#[no_mangle]
pub unsafe extern "C" fn tantivy_create_text_batch_writer(
    ptr: *const c_void,
    memory_budget: usize,
) -> RustResult {
    use crate::{
        index_writer::IndexWriterWrapper, index_writer_v7::IndexWriterWrapperImpl,
        util::create_binding,
    };
    let reader = &*(ptr as *const IndexReaderWrapper);
    let result = reader
        .index
        .tokenizer_for_field(reader.field)
        .map_err(crate::error::TantivyBindingError::from)
        .and_then(|analyzer| {
            IndexWriterWrapperImpl::with_text_analyzer(
                &reader.field_name,
                "",
                "query_batch",
                analyzer,
                1,
                memory_budget,
                true,
                false,
            )
        });
    match result {
        Ok(writer) => RustResult::from_ptr(create_binding(IndexWriterWrapper::V7(writer))),
        Err(error) => RustResult::from_binding_error(&error),
    }
}
