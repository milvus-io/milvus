use core::slice;
use std::ffi::{c_char, c_void, CStr};

use crate::{
    data_type::TantivyDataType,
    index_writer::IndexWriterWrapper,
    util::{create_binding, free_binding},
};

#[no_mangle]
pub extern "C" fn tantivy_create_index(
    field_name: *const c_char,
    data_type: TantivyDataType,
    path: *const c_char,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
) -> *mut c_void {
    let field_name_str = unsafe { CStr::from_ptr(field_name) };
    let path_str = unsafe { CStr::from_ptr(path) };
    let wrapper = IndexWriterWrapper::new(
        String::from(field_name_str.to_str().unwrap()),
        data_type,
        String::from(path_str.to_str().unwrap()),
        num_threads,
        overall_memory_budget_in_bytes,
    );
    create_binding(wrapper)
}

#[no_mangle]
pub extern "C" fn tantivy_free_index_writer(ptr: *mut c_void) {
    free_binding::<IndexWriterWrapper>(ptr);
}

// tantivy_finish_index will finish the index writer, and the index writer can't be used any more.
// After this was called, you should reset the pointer to null.
#[no_mangle]
pub extern "C" fn tantivy_finish_index(ptr: *mut c_void) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe { Box::from_raw(real).finish() }
}

// -------------------------build--------------------
#[no_mangle]
pub extern "C" fn tantivy_index_add_int8s(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
    offset_begin: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            (*real).add_i8(*data, offset_begin + (index as i64));
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int16s(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
    offset_begin: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            (*real).add_i16(*data, offset_begin + (index as i64));
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int32s(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
    offset_begin: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            (*real).add_i32(*data, offset_begin + (index as i64));
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64s(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
    offset_begin: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            (*real).add_i64(*data, offset_begin + (index as i64));
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f32s(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
    offset_begin: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            (*real).add_f32(*data, offset_begin + (index as i64));
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f64s(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
    offset_begin: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            (*real).add_f64(*data, offset_begin + (index as i64));
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_bools(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
    offset_begin: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            (*real).add_bool(*data, offset_begin + (index as i64));
        }
    }
}

// TODO: this is not a very efficient way, since we must call this function many times, which
// will bring a lot of overhead caused by the rust binding.
#[no_mangle]
pub extern "C" fn tantivy_index_add_string(ptr: *mut c_void, s: *const c_char, offset: i64) {
    let real = ptr as *mut IndexWriterWrapper;
    let c_str = unsafe { CStr::from_ptr(s) };
    unsafe { (*real).add_string(c_str.to_str().unwrap(), offset) }
}

// --------------------------------------------- array ------------------------------------------

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int8s(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i8s(arr, offset)
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int16s(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i16s(arr, offset);
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int32s(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i32s(arr, offset);
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int64s(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i64s(arr, offset);
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_f32s(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_f32s(arr, offset);
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_f64s(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_f64s(arr, offset);
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_bools(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_bools(arr, offset);
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_keywords(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
    offset: i64,
) {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_keywords(arr, offset)
    }
}
