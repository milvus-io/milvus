use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustArray,
    index_reader::IndexReaderWrapper,
    util::{create_binding, free_binding},
    util_c::tantivy_index_exist,
};

#[no_mangle]
pub extern "C" fn tantivy_load_index(path: *const c_char) -> *mut c_void {
    assert!(tantivy_index_exist(path));
    let path_str = unsafe { CStr::from_ptr(path) };
    let wrapper = IndexReaderWrapper::load(path_str.to_str().unwrap());
    create_binding(wrapper)
}

#[no_mangle]
pub extern "C" fn tantivy_free_index_reader(ptr: *mut c_void) {
    free_binding::<IndexReaderWrapper>(ptr);
}

// -------------------------query--------------------
#[no_mangle]
pub extern "C" fn tantivy_index_count(ptr: *mut c_void) -> u32 {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe { (*real).count() }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_i64(ptr: *mut c_void, term: i64) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).term_query_i64(term);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_i64(
    ptr: *mut c_void,
    lower_bound: i64,
    inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).lower_bound_range_query_i64(lower_bound, inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_i64(
    ptr: *mut c_void,
    upper_bound: i64,
    inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).upper_bound_range_query_i64(upper_bound, inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_range_query_i64(
    ptr: *mut c_void,
    lower_bound: i64,
    upper_bound: i64,
    lb_inclusive: bool,
    ub_inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).range_query_i64(lower_bound, upper_bound, lb_inclusive, ub_inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_f64(ptr: *mut c_void, term: f64) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).term_query_f64(term);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_f64(
    ptr: *mut c_void,
    lower_bound: f64,
    inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).lower_bound_range_query_f64(lower_bound, inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_f64(
    ptr: *mut c_void,
    upper_bound: f64,
    inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).upper_bound_range_query_f64(upper_bound, inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_range_query_f64(
    ptr: *mut c_void,
    lower_bound: f64,
    upper_bound: f64,
    lb_inclusive: bool,
    ub_inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).range_query_f64(lower_bound, upper_bound, lb_inclusive, ub_inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_bool(ptr: *mut c_void, term: bool) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let hits = (*real).term_query_bool(term);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_keyword(ptr: *mut c_void, term: *const c_char) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let c_str = CStr::from_ptr(term);
        let hits = (*real).term_query_keyword(c_str.to_str().unwrap());
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_lower_bound_range_query_keyword(
    ptr: *mut c_void,
    lower_bound: *const c_char,
    inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let c_lower_bound = CStr::from_ptr(lower_bound);
        let hits =
            (*real).lower_bound_range_query_keyword(c_lower_bound.to_str().unwrap(), inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_upper_bound_range_query_keyword(
    ptr: *mut c_void,
    upper_bound: *const c_char,
    inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let c_upper_bound = CStr::from_ptr(upper_bound);
        let hits =
            (*real).upper_bound_range_query_keyword(c_upper_bound.to_str().unwrap(), inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_range_query_keyword(
    ptr: *mut c_void,
    lower_bound: *const c_char,
    upper_bound: *const c_char,
    lb_inclusive: bool,
    ub_inclusive: bool,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let c_lower_bound = CStr::from_ptr(lower_bound);
        let c_upper_bound = CStr::from_ptr(upper_bound);
        let hits = (*real).range_query_keyword(
            c_lower_bound.to_str().unwrap(),
            c_upper_bound.to_str().unwrap(),
            lb_inclusive,
            ub_inclusive,
        );
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_prefix_query_keyword(
    ptr: *mut c_void,
    prefix: *const c_char,
) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let c_str = CStr::from_ptr(prefix);
        let hits = (*real).prefix_query_keyword(c_str.to_str().unwrap());
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_regex_query(ptr: *mut c_void, pattern: *const c_char) -> RustArray {
    let real = ptr as *mut IndexReaderWrapper;
    unsafe {
        let c_str = CStr::from_ptr(pattern);
        let hits = (*real).regex_query(c_str.to_str().unwrap());
        RustArray::from_vec(hits)
    }
}
