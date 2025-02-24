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
    offset: i64,
    e: fn(&mut IndexWriterWrapper, T, i64) -> Result<()>,
    w: &mut IndexWriterWrapper,
) -> Result<()> {
    for (index, data) in arr.iter().enumerate() {
        e(w, *data, offset + (index as i64))?;
    }
    Ok(())
}

fn execute_by_single_segment_writer<T: Copy>(
    arr: &[T],
    e: fn(&mut IndexWriterWrapper, T) -> Result<()>,
    w: &mut IndexWriterWrapper,
) -> Result<()> {
    for (_, data) in arr.iter().enumerate() {
        e(w, *data)?;
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
    unsafe { execute(arr, offset_begin, IndexWriterWrapper::add_i8, &mut (*real)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int8s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i8,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr,
            IndexWriterWrapper::add_i8_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
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
pub extern "C" fn tantivy_index_add_int16s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i16,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr,
            IndexWriterWrapper::add_i16_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
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
pub extern "C" fn tantivy_index_add_int32s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i32,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr,
            IndexWriterWrapper::add_i32_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
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

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };

    unsafe {
        execute_by_single_segment_writer(
            arr,
            IndexWriterWrapper::add_i64_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
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
pub extern "C" fn tantivy_index_add_f32s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const f32,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr,
            IndexWriterWrapper::add_f32_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
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
pub extern "C" fn tantivy_index_add_f64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr,
            IndexWriterWrapper::add_f64_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
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

#[no_mangle]
pub extern "C" fn tantivy_index_add_bools_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr,
            IndexWriterWrapper::add_bool_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_strings(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    // todo(SpadeA): avoid this vector allocation in the following PR
    let mut arr_str = Vec::with_capacity(len);
    for i in 0..len {
        let s = cstr_to_str!(arr[i]);
        arr_str.push(s);
    }

    unsafe {
        execute(
            &arr_str,
            offset,
            IndexWriterWrapper::add_string,
            &mut (*real),
        )
        .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_strings_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { slice::from_raw_parts(array, len) };
    let mut arr_str = Vec::with_capacity(len);
    for i in 0..len {
        let s = cstr_to_str!(arr[i]);
        arr_str.push(s);
    }

    unsafe {
        execute_by_single_segment_writer(
            &arr_str,
            IndexWriterWrapper::add_string_by_single_segment_writer,
            &mut (*real),
        )
        .into()
    }
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
        (*real).add_array_i8s(arr, offset).into()
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
        (*real).add_array_i8s_by_single_segment_writer(arr).into()
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
        (*real).add_array_i16s(arr, offset).into()
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
        (*real).add_array_i16s_by_single_segment_writer(arr).into()
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
        (*real).add_array_i32s(arr, offset).into()
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
        (*real).add_array_i32s_by_single_segment_writer(arr).into()
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
        (*real).add_array_i64s(arr, offset).into()
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
        (*real).add_array_i64s_by_single_segment_writer(arr).into()
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
        (*real).add_array_f32s(arr, offset).into()
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
        (*real).add_array_f32s_by_single_segment_writer(arr).into()
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
        (*real).add_array_f64s(arr, offset).into()
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
        (*real).add_array_f64s_by_single_segment_writer(arr).into()
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
        (*real).add_array_bools(arr, offset).into()
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
        (*real).add_array_bools_by_single_segment_writer(arr).into()
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
        (*real).add_array_keywords(arr, offset).into()
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
        (*real)
            .add_array_keywords_by_single_segment_writer(arr)
            .into()
    }
}
