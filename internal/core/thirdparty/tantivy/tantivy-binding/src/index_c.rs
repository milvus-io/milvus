use core::slice;
use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustArray, data_type::TantivyDataType, index::IndexWrapper,
    util_c::create_tantivy_binding,
};

#[no_mangle]
pub extern "C" fn tantivy_create_index(
    field_name: *const c_char,
    data_type: TantivyDataType,
    path: *const c_char,
) -> *mut c_void {
    let field_name_str = unsafe { CStr::from_ptr(field_name) };
    let path_str = unsafe { CStr::from_ptr(path) };
    let wrapper = IndexWrapper::new(
        String::from(field_name_str.to_str().unwrap()),
        data_type,
        String::from(path_str.to_str().unwrap()),
    );
    create_tantivy_binding(wrapper)
}

#[no_mangle]
pub extern "C" fn tantivy_finish_index(ptr: *mut c_void) {
    let real = ptr as *mut IndexWrapper;
    unsafe {
        (*real).finish();
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_count(ptr: *mut c_void) -> u32 {
    let real = ptr as *mut IndexWrapper;
    unsafe { (*real).count() }
}

#[no_mangle]
pub extern "C" fn tantivy_free_index(ptr: *mut c_void) {
    let real = ptr as *mut IndexWrapper;
    unsafe {
        drop(Box::from_raw(real));
    }
}

// -------------------------build--------------------
#[no_mangle]
pub extern "C" fn tantivy_index_add_int8s(ptr: *mut c_void, array: *const i8, len: usize) {
    let real = ptr as *mut IndexWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for data in arr {
            (*real).add_i8(*data);
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int16s(ptr: *mut c_void, array: *const i16, len: usize) {
    let real = ptr as *mut IndexWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for data in arr {
            (*real).add_i16(*data);
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int32s(ptr: *mut c_void, array: *const i32, len: usize) {
    let real = ptr as *mut IndexWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for data in arr {
            (*real).add_i32(*data);
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64s(ptr: *mut c_void, array: *const i64, len: usize) {
    let real = ptr as *mut IndexWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for data in arr {
            (*real).add_i64(*data);
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f32s(ptr: *mut c_void, array: *const f32, len: usize) {
    let real = ptr as *mut IndexWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for data in arr {
            (*real).add_f32(*data);
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f64s(ptr: *mut c_void, array: *const f64, len: usize) {
    let real = ptr as *mut IndexWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for data in arr {
            (*real).add_f64(*data);
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_bools(ptr: *mut c_void, array: *const bool, len: usize) {
    let real = ptr as *mut IndexWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for data in arr {
            (*real).add_bool(*data);
        }
    }
}

// TODO: this is not a very efficient way, since we must call this function many times, which
// will bring a lot of overhead caused by the rust binding.
#[no_mangle]
pub extern "C" fn tantivy_index_add_keyword(ptr: *mut c_void, s: *const c_char) {
    let real = ptr as *mut IndexWrapper;
    let c_str = unsafe { CStr::from_ptr(s) };
    unsafe { (*real).add_keyword(c_str.to_str().unwrap()) }
}

// -------------------------query--------------------
#[no_mangle]
pub extern "C" fn tantivy_term_query_i64(ptr: *mut c_void, term: i64) -> RustArray {
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
    unsafe {
        let hits = (*real).range_query_i64(lower_bound, upper_bound, lb_inclusive, ub_inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_f64(ptr: *mut c_void, term: f64) -> RustArray {
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
    unsafe {
        let hits = (*real).range_query_f64(lower_bound, upper_bound, lb_inclusive, ub_inclusive);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_bool(ptr: *mut c_void, term: bool) -> RustArray {
    let real = ptr as *mut IndexWrapper;
    unsafe {
        let hits = (*real).term_query_bool(term);
        RustArray::from_vec(hits)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_term_query_keyword(ptr: *mut c_void, term: *const c_char) -> RustArray {
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
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
    let real = ptr as *mut IndexWrapper;
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
pub extern "C" fn tantivy_prefix_query_keyword(ptr: *mut c_void, prefix: *const c_char) -> RustArray {
    let real = ptr as *mut IndexWrapper;
    unsafe {
        let c_str = CStr::from_ptr(prefix);
        let hits = (*real).prefix_query_keyword(c_str.to_str().unwrap());
        RustArray::from_vec(hits)
    }
}
