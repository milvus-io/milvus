use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustResult,
    cstr_to_str,
    index_reader::IndexReaderWrapper,
    util::{create_binding, free_binding},
    util_c::tantivy_index_exist,
};

#[no_mangle]
pub extern "C" fn tantivy_load_index(path: *const c_char) -> RustResult {
    assert!(tantivy_index_exist(path));
    let path_str = cstr_to_str!(path);
    match IndexReaderWrapper::load(path_str) {
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
pub extern "C" fn tantivy_term_query_i64(ptr: *mut c_void, term: i64) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe { (*real).term_query_i64(term).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_i64(
    ptr: *mut c_void,
    lower_bound: i64,
    inclusive: bool,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .lower_bound_range_query_i64(lower_bound, inclusive)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_i64(
    ptr: *mut c_void,
    upper_bound: i64,
    inclusive: bool,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .upper_bound_range_query_i64(upper_bound, inclusive)
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
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .range_query_i64(lower_bound, upper_bound, lb_inclusive, ub_inclusive)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_f64(ptr: *mut c_void, term: f64) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe { (*real).term_query_f64(term).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_f64(
    ptr: *mut c_void,
    lower_bound: f64,
    inclusive: bool,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .lower_bound_range_query_f64(lower_bound, inclusive)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_f64(
    ptr: *mut c_void,
    upper_bound: f64,
    inclusive: bool,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .upper_bound_range_query_f64(upper_bound, inclusive)
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
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .range_query_f64(lower_bound, upper_bound, lb_inclusive, ub_inclusive)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_bool(ptr: *mut c_void, term: bool) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe { (*real).term_query_bool(term).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_keyword(ptr: *mut c_void, term: *const c_char) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let term = cstr_to_str!(term);
    unsafe { (*real).term_query_keyword(term).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_keyword_i64(ptr: *mut c_void, term: *const c_char) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let term = cstr_to_str!(term);
    unsafe { (*real).term_query_keyword_i64(term).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_keyword(
    ptr: *mut c_void,
    lower_bound: *const c_char,
    inclusive: bool,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let lower_bound = cstr_to_str!(lower_bound);
    unsafe {
        (*real)
            .lower_bound_range_query_keyword(lower_bound, inclusive)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_keyword(
    ptr: *mut c_void,
    upper_bound: *const c_char,
    inclusive: bool,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let upper_bound = cstr_to_str!(upper_bound);
    unsafe {
        (*real)
            .upper_bound_range_query_keyword(upper_bound, inclusive)
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
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let lower_bound = cstr_to_str!(lower_bound);
    let upper_bound = cstr_to_str!(upper_bound);
    unsafe {
        (*real)
            .range_query_keyword(lower_bound, upper_bound, lb_inclusive, ub_inclusive)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_prefix_query_keyword(
    ptr: *mut c_void,
    prefix: *const c_char,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let prefix = cstr_to_str!(prefix);
    unsafe { (*real).prefix_query_keyword(prefix).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_regex_query(ptr: *mut c_void, pattern: *const c_char) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let pattern = cstr_to_str!(pattern);
    unsafe { (*real).regex_query(pattern).into() }
}
