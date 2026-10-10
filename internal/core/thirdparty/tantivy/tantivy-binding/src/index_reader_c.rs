use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustResult,
    convert_to_rust_slice, cstr_to_str,
    data_type::JsonExistValueType,
    index_reader::IndexReaderWrapper,
    ptr_to_str,
    util::{create_binding, free_binding},
    util_c::tantivy_index_exist,
};

pub(crate) type SetBitsetFn = extern "C" fn(*mut c_void, *const u32, usize);
pub(crate) type RegexMatchFn = extern "C" fn(*mut c_void, *const u8, usize) -> bool;

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
        Err(e) => RustResult::from_binding_error(&e),
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
pub extern "C" fn tantivy_index_size_bytes(ptr: *mut c_void) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe { (*real).index_size_bytes().into() }
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

// Lengths describe UTF-8 byte spans, so embedded NUL is part of the term.
#[no_mangle]
pub extern "C" fn tantivy_terms_query_keyword_with_len(
    ptr: *mut c_void,
    terms: *const *const c_char,
    lengths: *const usize,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let terms = match unsafe { keyword_spans(terms, lengths, len) } {
        Ok(terms) => terms,
        Err(error) => return RustResult::from_binding_error(&error),
    };
    unsafe {
        (*(ptr as *mut IndexReaderWrapper))
            .terms_query_keyword_strs(&terms, bitset)
            .into()
    }
}

// The returned views borrow caller memory only for the synchronous FFI call.
unsafe fn keyword_spans<'a>(
    terms: *const *const c_char,
    lengths: *const usize,
    len: usize,
) -> crate::error::Result<Vec<&'a str>> {
    use crate::error::TantivyBindingError;
    if len == 0 {
        return Ok(Vec::new());
    }
    if terms.is_null() || lengths.is_null() {
        return Err(TantivyBindingError::InvalidArgument(
            "null keyword span array".into(),
        ));
    }
    let terms = std::slice::from_raw_parts(terms, len);
    let lengths = std::slice::from_raw_parts(lengths, len);
    terms
        .iter()
        .zip(lengths)
        .map(|(&term, &length)| keyword_span(term, length))
        .collect()
}

unsafe fn keyword_span<'a>(term: *const c_char, length: usize) -> crate::error::Result<&'a str> {
    use crate::error::TantivyBindingError;
    if length == 0 {
        return Ok("");
    }
    if term.is_null() {
        return Err(TantivyBindingError::InvalidArgument(
            "null keyword span".into(),
        ));
    }
    std::str::from_utf8(std::slice::from_raw_parts(term as *const u8, length))
        .map_err(|error| TantivyBindingError::InvalidArgument(error.to_string()))
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
pub extern "C" fn tantivy_term_query_keyword_i64_with_len(
    ptr: *mut c_void,
    term: *const c_char,
    term_len: usize,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let term = match unsafe { keyword_span(term, term_len) } {
        Ok(value) => value,
        Err(error) => return RustResult::from_binding_error(&error),
    };
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
pub extern "C" fn tantivy_lower_bound_range_query_keyword_with_len(
    ptr: *mut c_void,
    lower_bound: *const c_char,
    lower_bound_len: usize,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let lower_bound = match unsafe { keyword_span(lower_bound, lower_bound_len) } {
        Ok(value) => value,
        Err(error) => return RustResult::from_binding_error(&error),
    };
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
pub extern "C" fn tantivy_upper_bound_range_query_keyword_with_len(
    ptr: *mut c_void,
    upper_bound: *const c_char,
    upper_bound_len: usize,
    inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let upper_bound = match unsafe { keyword_span(upper_bound, upper_bound_len) } {
        Ok(value) => value,
        Err(error) => return RustResult::from_binding_error(&error),
    };
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
pub extern "C" fn tantivy_range_query_keyword_with_len(
    ptr: *mut c_void,
    lower_bound: *const c_char,
    lower_bound_len: usize,
    upper_bound: *const c_char,
    upper_bound_len: usize,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let lower_bound = match unsafe { keyword_span(lower_bound, lower_bound_len) } {
        Ok(value) => value,
        Err(error) => return RustResult::from_binding_error(&error),
    };
    let upper_bound = match unsafe { keyword_span(upper_bound, upper_bound_len) } {
        Ok(value) => value,
        Err(error) => return RustResult::from_binding_error(&error),
    };
    unsafe {
        (*real)
            .range_query_keyword(lower_bound, upper_bound, lb_inclusive, ub_inclusive, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_prefix_query_keyword(
    ptr: *mut c_void,
    prefix: *const u8,
    prefix_len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let prefix = ptr_to_str!(prefix, prefix_len);
    unsafe { (*real).prefix_query_keyword(prefix, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_regex_query(
    ptr: *mut c_void,
    pattern: *const u8,
    pattern_len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let pattern = ptr_to_str!(pattern, pattern_len);
    unsafe { (*real).regex_query(pattern, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_regex_match_query(
    ptr: *mut c_void,
    matcher_ctx: *mut c_void,
    matcher: RegexMatchFn,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        (*real)
            .regex_match_query(matcher_ctx, matcher, bitset)
            .into()
    }
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
pub extern "C" fn tantivy_json_term_query_u64(
    ptr: *mut c_void,
    json_path: *const c_char,
    term: u64,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe { (*real).json_term_query_u64(json_path, term, bitset).into() }
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
pub extern "C" fn tantivy_json_term_query_keyword_with_len(
    ptr: *mut c_void,
    json_path: *const c_char,
    term: *const c_char,
    length: usize,
    bitset: *mut c_void,
) -> RustResult {
    let json_path = cstr_to_str!(json_path);
    let term = match unsafe { keyword_span(term, length) } {
        Ok(term) => term,
        Err(error) => return RustResult::from_binding_error(&error),
    };
    unsafe {
        (*(ptr as *mut IndexReaderWrapper))
            .json_term_query_keyword(json_path, term, bitset)
            .into()
    }
}

// Batch JSON terms queries
#[no_mangle]
pub extern "C" fn tantivy_json_terms_query_i64(
    ptr: *mut c_void,
    json_path: *const c_char,
    terms: *const i64,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe {
        (*real)
            .json_terms_query_i64(json_path, terms, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_terms_query_u64(
    ptr: *mut c_void,
    json_path: *const c_char,
    terms: *const u64,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe {
        (*real)
            .json_terms_query_u64(json_path, terms, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_terms_query_f64(
    ptr: *mut c_void,
    json_path: *const c_char,
    terms: *const f64,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe {
        (*real)
            .json_terms_query_f64(json_path, terms, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_terms_query_bool(
    ptr: *mut c_void,
    json_path: *const c_char,
    terms: *const bool,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe {
        (*real)
            .json_terms_query_bool(json_path, terms, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_terms_query_keyword(
    ptr: *mut c_void,
    json_path: *const c_char,
    terms: *const *const c_char,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let terms = unsafe { convert_to_rust_slice!(terms, len) };
    unsafe {
        (*real)
            .json_terms_query_keyword(json_path, terms, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_terms_query_keyword_with_len(
    ptr: *mut c_void,
    json_path: *const c_char,
    terms: *const *const c_char,
    lengths: *const usize,
    len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let json_path = cstr_to_str!(json_path);
    let terms = match unsafe { keyword_spans(terms, lengths, len) } {
        Ok(terms) => terms,
        Err(error) => return RustResult::from_binding_error(&error),
    };
    unsafe {
        (*(ptr as *mut IndexReaderWrapper))
            .json_terms_query_keyword_strs(json_path, &terms, bitset)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_json_exist_query(
    ptr: *mut c_void,
    json_path: *const c_char,
    json_subpaths: bool,
    value_type: JsonExistValueType,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    unsafe {
        (*real)
            .json_exist_query(json_path, json_subpaths, value_type, bitset)
            .into()
    }
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
pub extern "C" fn tantivy_json_range_query_u64(
    ptr: *mut c_void,
    json_path: *const c_char,
    lower_bound: u64,
    higher_bound: u64,
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
pub extern "C" fn tantivy_json_range_query_keyword_with_len(
    ptr: *mut c_void,
    json_path: *const c_char,
    lower_bound: *const c_char,
    lower_bound_len: usize,
    higher_bound: *const c_char,
    higher_bound_len: usize,
    lb_unbounded: bool,
    up_unbounded: bool,
    lb_inclusive: bool,
    ub_inclusive: bool,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let lower_bound = match unsafe { keyword_span(lower_bound, lower_bound_len) } {
        Ok(value) => value,
        Err(error) => return RustResult::from_binding_error(&error),
    };
    let higher_bound = match unsafe { keyword_span(higher_bound, higher_bound_len) } {
        Ok(value) => value,
        Err(error) => return RustResult::from_binding_error(&error),
    };
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
    pattern: *const u8,
    pattern_len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let pattern = ptr_to_str!(pattern, pattern_len);
    unsafe { (*real).json_regex_query(json_path, pattern, bitset).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_json_prefix_query(
    ptr: *mut c_void,
    json_path: *const c_char,
    prefix: *const u8,
    prefix_len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let json_path = cstr_to_str!(json_path);
    let prefix = ptr_to_str!(prefix, prefix_len);
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

#[no_mangle]
pub extern "C" fn tantivy_ngram_tokenize(
    ptr: *mut c_void,
    literals: *const *const c_char,
    literals_len: usize,
    min_gram: usize,
    max_gram: usize,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let literals_slice = unsafe { convert_to_rust_slice!(literals, literals_len) };

    let mut literal_strs: Vec<&str> = Vec::with_capacity(literals_len);
    for &lit in literals_slice {
        literal_strs.push(cstr_to_str!(lit));
    }

    unsafe {
        (*real)
            .ngram_tokenize(&literal_strs, min_gram, max_gram)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_ngram_term_posting_list(
    ptr: *mut c_void,
    term: *const c_char,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let term = cstr_to_str!(term);
    unsafe { (*real).ngram_term_posting_list(term, bitset).into() }
}

#[cfg(test)]
mod keyword_span_tests {
    use super::{keyword_span, keyword_spans};
    use crate::error::TantivyBindingError;

    #[test]
    fn keyword_spans_preserve_nul_and_empty_values() {
        let bytes = b"a\0b";
        let pointers = [bytes.as_ptr().cast(), std::ptr::null()];
        let lengths = [bytes.len(), 0];
        let values = unsafe { keyword_spans(pointers.as_ptr(), lengths.as_ptr(), 2) }.unwrap();
        assert_eq!(values, ["a\0b", ""]);
        assert!(
            unsafe { keyword_spans(std::ptr::null(), std::ptr::null(), 0) }
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn keyword_spans_reject_invalid_arguments() {
        assert!(matches!(
            unsafe { keyword_span(std::ptr::null(), 1) },
            Err(TantivyBindingError::InvalidArgument(_))
        ));
        let invalid_utf8 = [0xffu8];
        assert!(matches!(
            unsafe { keyword_span(invalid_utf8.as_ptr().cast(), 1) },
            Err(TantivyBindingError::InvalidArgument(_))
        ));
        assert!(matches!(
            unsafe { keyword_spans(std::ptr::null(), std::ptr::null(), 1) },
            Err(TantivyBindingError::InvalidArgument(_))
        ));
    }
}
