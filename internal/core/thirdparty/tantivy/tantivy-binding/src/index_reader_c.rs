use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustResult,
    convert_to_rust_slice, cstr_to_str,
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

// The ngram entry points take (pointer, length) rather than C strings: a
// LIKE literal may carry an interior NUL, and truncating it there would make
// the rust side query a different literal than the one the C++ gate checked.
#[no_mangle]
pub extern "C" fn tantivy_ngram_match_query(
    ptr: *mut c_void,
    literal: *const c_char,
    literal_len: usize,
    min_gram: usize,
    max_gram: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let literal = ptr_to_str!(literal, literal_len);

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
    literal_lens: *const usize,
    literals_len: usize,
    min_gram: usize,
    max_gram: usize,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let literals_slice = unsafe { convert_to_rust_slice!(literals, literals_len) };
    let lens_slice = unsafe { convert_to_rust_slice!(literal_lens, literals_len) };

    let mut literal_strs: Vec<&str> = Vec::with_capacity(literals_len);
    for (&lit, &len) in literals_slice.iter().zip(lens_slice.iter()) {
        literal_strs.push(ptr_to_str!(lit, len));
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
    term_len: usize,
    bitset: *mut c_void,
) -> RustResult {
    let real = ptr as *mut IndexReaderWrapper;
    let term = ptr_to_str!(term, term_len);
    unsafe { (*real).ngram_term_posting_list(term, bitset).into() }
}
