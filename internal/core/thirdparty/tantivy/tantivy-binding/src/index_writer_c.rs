use core::slice;
use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustResult,
    cstr_to_str,
    data_type::TantivyDataType,
    error::Result,
    index_writer::IndexWriterWrapper,
    util::{create_binding, free_binding},
};

macro_rules! convert_to_rust_slice {
    ($arr: expr, $len: expr) => {
        match $arr {
            // there is a UB in slice::from_raw_parts if the pointer is null
            x if x.is_null() => &[],
            _ => slice::from_raw_parts($arr, $len),
        }
    };
}

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
pub extern "C" fn tantivy_create_index_with_single_segment(
    field_name: *const c_char,
    data_type: TantivyDataType,
    path: *const c_char,
) -> RustResult {
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);
    match IndexWriterWrapper::new_with_single_segment(
        String::from(field_name_str),
        data_type,
        String::from(path_str),
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
fn execute<T: Copy>(
    arr: &[T],
    offset: Option<i64>,
    e: fn(&mut IndexWriterWrapper, T, Option<i64>) -> Result<()>,
    w: &mut IndexWriterWrapper,
) -> Result<()> {
    for (index, data) in arr.iter().enumerate() {
        let offset = if let Some(offset) = offset {
            Some(offset + (index as i64))
        } else {
            None
        };
        e(w, *data, offset)?;
    }
    Ok(())
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int8s(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute(
            arr,
            Some(offset_begin),
            IndexWriterWrapper::add_integer,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int8s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, None, IndexWriterWrapper::add_integer, &mut (*real)).into() }
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
    unsafe {
        execute(
            arr,
            Some(offset_begin),
            IndexWriterWrapper::add_integer,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int16s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, None, IndexWriterWrapper::add_integer, &mut (*real)).into() }
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
    unsafe {
        execute(
            arr,
            Some(offset_begin),
            IndexWriterWrapper::add_integer,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int32s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, None, IndexWriterWrapper::add_integer, &mut (*real)).into() }
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

    unsafe {
        execute(
            arr,
            Some(offset_begin),
            IndexWriterWrapper::add_integer,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };

    unsafe { execute(arr, None, IndexWriterWrapper::add_integer, &mut (*real)).into() }
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
    unsafe {
        execute(
            arr,
            Some(offset_begin),
            IndexWriterWrapper::add_float,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f32s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, None, IndexWriterWrapper::add_float, &mut (*real)).into() }
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
    unsafe {
        execute(
            arr,
            Some(offset_begin),
            IndexWriterWrapper::add_float,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, None, IndexWriterWrapper::add_float, &mut (*real)).into() }
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
            Some(offset_begin),
            IndexWriterWrapper::add_bool,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_bools_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe { execute(arr, None, IndexWriterWrapper::add_bool, &mut (*real)).into() }
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
    unsafe { (*real).add_string(s, Some(offset)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_string_by_single_segment_writer(
    ptr: *mut c_void,
    s: *const c_char,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let s = cstr_to_str!(s);
    unsafe { (*real).add_string(s, None).into() }
}

// --------------------------------------------- array ------------------------------------------

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int8s(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int8s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int16s(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int16s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int32s(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int32s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int64s(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_int64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_integer(arr, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_f32s(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_float(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_f32s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_float(arr, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_f64s(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_float(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_f64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_float(arr, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_bools(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_bool(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_bools_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_bool(arr, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_keywords(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_keyword(arr, Some(offset)).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_keywords_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_keyword(arr, None).into()
    }
}
