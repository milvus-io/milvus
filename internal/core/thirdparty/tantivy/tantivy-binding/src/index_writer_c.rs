use core::slice;
use std::ffi::{c_char, c_void, CStr};

use tantivy::Index;

use crate::{
    array::RustResult,
    cstr_to_str,
    data_type::TantivyDataType,
    error::Result,
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
) -> RustResult {
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);
    match IndexWriterWrapper::new(
        String::from(field_name_str),
        data_type,
        String::from(path_str),
        num_threads,
        overall_memory_budget_in_bytes,
    ) {
        Ok(wrapper) => RustResult::from_ptr(create_binding(wrapper)),
        Err(e) => RustResult::from_error(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_free_index_writer(ptr: *mut c_void) {
    free_binding::<IndexWriterWrapper>(ptr);
}

// tantivy_finish_index will finish the index writer, and the index writer can't be used any more.
// After this was called, you should reset the pointer to null.
#[no_mangle]
pub extern "C" fn tantivy_finish_index(ptr: *mut c_void) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe { Box::from_raw(real).finish().into() }
}

#[no_mangle]
pub extern "C" fn tantivy_commit_index(ptr: *mut c_void) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe { (*real).commit().into() }
}

#[no_mangle]
pub extern "C" fn tantivy_create_reader_from_writer(ptr: *mut c_void) -> RustResult {
    let writer = ptr as *mut IndexWriterWrapper;
    let reader = unsafe { (*writer).create_reader() };
    match reader {
        Ok(r) => RustResult::from_ptr(create_binding(r)),
        Err(e) => RustResult::from_error(e.to_string()),
    }
}

// -------------------------build--------------------
#[no_mangle]
pub extern "C" fn tantivy_index_add_int8s(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, offset_begin, IndexWriterWrapper::add_i8, &mut (*real)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int16s(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, offset_begin, IndexWriterWrapper::add_i16, &mut (*real)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int32s(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, offset_begin, IndexWriterWrapper::add_i32, &mut (*real)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64s(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };

    unsafe { execute(arr, offset_begin, IndexWriterWrapper::add_i64, &mut (*real)).into() }
}

fn execute<T: Copy>(
    arr: &[T],
    offset: i64,
    mut e: fn(&mut IndexWriterWrapper, T, i64) -> Result<()>,
    w: &mut IndexWriterWrapper,
) -> Result<()> {
    unsafe {
        for (index, data) in arr.iter().enumerate() {
            e(w, *data, offset + (index as i64))?;
        }
    }
    Ok(())
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f32s(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, offset_begin, IndexWriterWrapper::add_f32, &mut (*real)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f64s(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, offset_begin, IndexWriterWrapper::add_f64, &mut (*real)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_bools(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute(
            arr,
            offset_begin,
            IndexWriterWrapper::add_bool,
            &mut (*real),
        )
        .into()
    }
}

// TODO: this is not a very efficient way, since we must call this function many times, which
// will bring a lot of overhead caused by the rust binding.
#[no_mangle]
pub extern "C" fn tantivy_index_add_string(
    ptr: *mut c_void,
    s: *const c_char,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let s = cstr_to_str!(s);
    unsafe { (*real).add_string(s, offset).into() }
}

// --------------------------------------------- array ------------------------------------------

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int8s(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i8s(arr, offset).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int16s(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i16s(arr, offset).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int32s(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i32s(arr, offset).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_int64s(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_i64s(arr, offset).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_f32s(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_f32s(arr, offset).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_f64s(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_f64s(arr, offset).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_bools(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = slice::from_raw_parts(array, len);
        (*real).add_multi_bools(arr, offset).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_multi_keywords(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = if len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(array, len) }
        };
        (*real).add_multi_keywords(arr, offset).into()
    }
}
