use core::slice;
use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustResult,
    convert_to_rust_slice, cstr_to_str,
    index_reader::IndexReaderWrapper,
    util::{create_binding, free_binding},
    util_c::tantivy_index_exist,
};

pub(crate) type SetBitsetFn = extern "C" fn(*mut c_void, *const u32, usize);

#[no_mangle]
pub extern "C" fn tantivy_load_index(
    path: *const c_char,
    load_in_mmap: bool,
    set_bitset: SetBitsetFn,
) -> RustResult {
    assert!(tantivy_index_exist(path));
    let path_str = cstr_to_str!(path);
    match IndexReaderWrapper::load(path_str, load_in_mmap, set_bitset) {
        Ok(w) => RustResult::from_ptr(create_binding(w)),
        Err(e) => RustResult::from_error(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_free_index_reader(ptr: *mut c_void) {
    free_binding::<IndexReaderWrapper>(ptr);
}

// -------------------------query--------------------
#[no_mangle]
pub extern "C" fn tantivy_reload_index(ptr: *mut c_void) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe { (*real).reload().into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_count(ptr: *mut c_void) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe { (*real).count().into() }
}

#[no_mangle]
pub extern "C" fn tantivy_terms_query_bool(
    ptr: *mut c_void,
    terms: *const bool,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe { (*real).terms_query_bool(terms, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_terms_query_i64(
    ptr: *mut c_void,
    terms: *const i64,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe { (*real).terms_query_i64(terms, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_terms_query_f64(
    ptr: *mut c_void,
    terms: *const f64,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe { (*real).terms_query_f64(terms, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_terms_query_keyword(
    ptr: *mut c_void,
    terms: *const *const c_char,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe { (*real).terms_query_keyword(terms, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_keyword_i64(
    ptr: *mut c_void,
    term: *const c_char,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let term = cstr_to_str!(term);
    unsafe { (*real).term_query_keyword_i64(term).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_i64(
    ptr: *mut c_void,
    lower_bound: i64,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .lower_bound_range_query_i64(lower_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_bool(
    ptr: *mut c_void,
    lower_bound: bool,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .lower_bound_range_query_bool(lower_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_i64(
    ptr: *mut c_void,
    upper_bound: i64,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .upper_bound_range_query_i64(upper_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_bool(
    ptr: *mut c_void,
    upper_bound: bool,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .upper_bound_range_query_bool(upper_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_range_query_i64(
    ptr: *mut c_void,
    lower_bound: i64,
    upper_bound: i64,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .range_query_i64(lower_bound, upper_bound, lb_inclusive, ub_inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_range_query_bool(
    ptr: *mut c_void,
    lower_bound: bool,
    upper_bound: bool,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .range_query_bool(lower_bound, upper_bound, lb_inclusive, ub_inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_f64(
    ptr: *mut c_void,
    lower_bound: f64,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .lower_bound_range_query_f64(lower_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_f64(
    ptr: *mut c_void,
    upper_bound: f64,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .upper_bound_range_query_f64(upper_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_range_query_f64(
    ptr: *mut c_void,
    lower_bound: f64,
    upper_bound: f64,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .range_query_f64(lower_bound, upper_bound, lb_inclusive, ub_inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_keyword(
    ptr: *mut c_void,
    lower_bound: *const c_char,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let lower_bound = cstr_to_str!(lower_bound);
    unsafe {
        (*real)
            .lower_bound_range_query_keyword(lower_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_keyword(
    ptr: *mut c_void,
    upper_bound: *const c_char,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let upper_bound = cstr_to_str!(upper_bound);
    unsafe {
        (*real)
            .upper_bound_range_query_keyword(upper_bound, inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_range_query_keyword(
    ptr: *mut c_void,
    lower_bound: *const c_char,
    upper_bound: *const c_char,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let lower_bound = cstr_to_str!(lower_bound);
    let upper_bound = cstr_to_str!(upper_bound);
    unsafe {
        (*real)
            .range_query_keyword(lower_bound, upper_bound, lb_inclusive, ub_inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_prefix_query_keyword(
    ptr: *mut c_void,
    prefix: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let prefix = cstr_to_str!(prefix);
    unsafe { (*real).prefix_query_keyword(prefix, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_regex_query(
    ptr: *mut c_void,
    pattern: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let pattern = cstr_to_str!(pattern);
    unsafe { (*real).regex_query(pattern, bitset).into() }
}

// -------------------------json query--------------------
#[no_mangle]
pub extern "C" fn tantivy_json_term_query_i64(
    ptr: *mut c_void,
    json_path: *const c_char,
    term: i64,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe { (*real).json_term_query_i64(json_path, term, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_json_term_query_f64(
    ptr: *mut c_void,
    json_path: *const c_char,
    term: f64,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe { (*real).json_term_query_f64(json_path, term, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_json_term_query_bool(
    ptr: *mut c_void,
    json_path: *const c_char,
    term: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe { (*real).json_term_query_bool(json_path, term, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_json_term_query_keyword(
    ptr: *mut c_void,
    json_path: *const c_char,
    term: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let term = cstr_to_str!(term);
    unsafe {
        (*real)
            .json_term_query_keyword(json_path, term, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_exist_query(
    ptr: *mut c_void,
    json_path: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe { (*real).json_exist_query(json_path, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_json_range_query_i64(
    ptr: *mut c_void,
    json_path: *const c_char,
    lower_bound: i64,
    higher_bound: i64,
    lb_unbounded: bool,
    up_unbounded: bool,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe {
        (*real)
            .json_range_query(
                json_path,
                lower_bound,
                higher_bound,
                lb_unbounded,
                up_unbounded,
                lb_inclusive,
                ub_inclusive,
                bitset,
            )
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_range_query_f64(
    ptr: *mut c_void,
    json_path: *const c_char,
    lower_bound: f64,
    higher_bound: f64,
    lb_unbounded: bool,
    up_unbounded: bool,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe {
        (*real)
            .json_range_query(
                json_path,
                lower_bound,
                higher_bound,
                lb_unbounded,
                up_unbounded,
                lb_inclusive,
                ub_inclusive,
                bitset,
            )
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_range_query_bool(
    ptr: *mut c_void,
    json_path: *const c_char,
    lower_bound: bool,
    higher_bound: bool,
    lb_unbounded: bool,
    up_unbounded: bool,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe {
        (*real)
            .json_range_query(
                json_path,
                lower_bound,
                higher_bound,
                lb_unbounded,
                up_unbounded,
                lb_inclusive,
                ub_inclusive,
                bitset,
            )
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_range_query_keyword(
    ptr: *mut c_void,
    json_path: *const c_char,
    lower_bound: *const c_char,
    higher_bound: *const c_char,
    lb_unbounded: bool,
    up_unbounded: bool,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let lower_bound = cstr_to_str!(lower_bound);
    let higher_bound = cstr_to_str!(higher_bound);
    unsafe {
        (*real)
            .json_range_query_keyword(
                json_path,
                lower_bound,
                higher_bound,
                lb_unbounded,
                up_unbounded,
                lb_inclusive,
                ub_inclusive,
                bitset,
            )
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_regex_query(
    ptr: *mut c_void,
    json_path: *const c_char,
    pattern: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let pattern = cstr_to_str!(pattern);
    unsafe { (*real).json_regex_query(json_path, pattern, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_json_prefix_query(
    ptr: *mut c_void,
    json_path: *const c_char,
    prefix: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let prefix = cstr_to_str!(prefix);
    unsafe { (*real).json_prefix_query(json_path, prefix, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_ngram_match_query(
    ptr: *mut c_void,
    literal: *const c_char,
    min_gram: usize,
    max_gram: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let literal = cstr_to_str!(literal);

    unsafe {
        (*real)
            .ngram_match_query(literal, min_gram, max_gram, bitset)
            .into()
    }
}
