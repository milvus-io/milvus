use std::ffi::{c_char, c_void, CStr};

use crate::{
    array::RustResult,
    cstr_to_str,
    data_type::TantivyDataType,
    error::{Result, TantivyBindingError},
    index_reader_c::SetBitsetFn,
    index_writer::IndexWriterWrapper,
    ptr_to_str,
    util::{create_binding, free_binding},
    TantivyIndexVersion,
};

fn invalid_batch(message: impl Into<String>) -> RustResult {
    RustResult::from_error(TantivyBindingError::InternalError(message.into()).to_string())
}

unsafe fn required_slice<'a, T>(
    ptr: *const T,
    len: usize,
    name: &str,
) -> std::result::Result<&'a [T], RustResult> {
    if ptr.is_null() {
        return Err(invalid_batch(format!("{name} pointer is null")));
    }
    if len > isize::MAX as usize / std::mem::size_of::<T>().max(1) {
        return Err(invalid_batch(format!("{name} slice is too large")));
    }
    Ok(std::slice::from_raw_parts(ptr, len))
}

unsafe fn optional_empty_slice<'a, T>(
    ptr: *const T,
    len: usize,
    name: &str,
) -> std::result::Result<&'a [T], RustResult> {
    if len == 0 {
        return Ok(&[]);
    }
    required_slice(ptr, len, name)
}

unsafe fn row_batch_slices<'a, T>(
    values: *const T,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> std::result::Result<(&'a [T], &'a [usize], &'a [i64]), RustResult> {
    let offset_count = row_count
        .checked_add(1)
        .ok_or_else(|| invalid_batch("row offset count overflows usize"))?;
    let values = optional_empty_slice(values, value_count, "values")?;
    let row_offsets = required_slice(row_offsets, offset_count, "row_offsets")?;
    let doc_ids = required_slice(doc_ids, row_count, "doc_ids")?;
    Ok((values, row_offsets, doc_ids))
}

#[macro_export]
macro_rules! convert_to_rust_slice {
    ($arr: expr, $len: expr) => {
        match $arr {
            // there is a UB in slice::from_raw_parts if the pointer is null
            x if x.is_null() => &[],
            _ => ::core::slice::from_raw_parts($arr, $len),
        }
    };
}

#[no_mangle]
pub extern "C" fn tantivy_create_index(
    field_name: *const c_char,
    data_type: TantivyDataType,
    path: *const c_char,
    tantivy_index_version: u32,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
    enable_user_specified_doc_id: bool,
    enable_background_merge: bool,
) -> RustResult {
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);

    let tantivy_index_version = match TantivyIndexVersion::from_u32(tantivy_index_version) {
        Ok(v) => v,
        Err(e) => return RustResult::from_binding_error(&e),
    };

    match IndexWriterWrapper::new(
        field_name_str,
        data_type,
        String::from(path_str),
        num_threads,
        overall_memory_budget_in_bytes,
        tantivy_index_version,
        enable_user_specified_doc_id,
        enable_background_merge,
    ) {
        Ok(wrapper) => RustResult::from_ptr(create_binding(wrapper)),
        Err(e) => RustResult::from_binding_error(&e),
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
        field_name_str,
        data_type,
        String::from(path_str),
    ) {
        Ok(wrapper) => RustResult::from_ptr(create_binding(wrapper)),
        Err(e) => RustResult::from_binding_error(&e),
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
    if ptr.is_null() {
        return invalid_batch("index writer handle is null");
    }
    let real = ptr as *mut IndexWriterWrapper;
    unsafe { Box::from_raw(real).finish().into() }
}

#[no_mangle]
pub extern "C" fn tantivy_commit_index(ptr: *mut c_void) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe { (*real).commit().into() }
}

#[no_mangle]
pub extern "C" fn tantivy_create_reader_from_writer(
    ptr: *mut c_void,
    set_bitset: SetBitsetFn,
) -> RustResult {
    let writer = ptr as *mut IndexWriterWrapper;
    let reader = unsafe { (*writer).create_reader(set_bitset) };
    match reader {
        Ok(r) => RustResult::from_ptr(create_binding(r)),
        Err(e) => RustResult::from_binding_error(&e),
    }
}

fn add_numeric_rows<T, U, F>(
    ptr: *mut c_void,
    values: *const T,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
    convert: F,
) -> RustResult
where
    U: crate::index_writer::TantivyValue<crate::index_writer_v5::TantivyDocumentV5>
        + crate::index_writer::TantivyValue<crate::index_writer_v7::TantivyDocumentV7>,
    F: Fn(&T) -> U,
{
    if ptr.is_null() {
        return invalid_batch("index writer handle is null");
    }
    if row_count == 0 {
        return RustResult::from_success();
    }
    let (values, row_offsets, doc_ids) =
        match unsafe { row_batch_slices(values, value_count, row_offsets, doc_ids, row_count) } {
            Ok(slices) => slices,
            Err(result) => return result,
        };
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        (*real)
            .add_rows(values.iter().map(convert), row_offsets, doc_ids)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int8_rows(
    ptr: *mut c_void,
    values: *const i8,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_numeric_rows(
        ptr,
        values,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        |value| i64::from(*value),
    )
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int16_rows(
    ptr: *mut c_void,
    values: *const i16,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_numeric_rows(
        ptr,
        values,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        |value| i64::from(*value),
    )
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int32_rows(
    ptr: *mut c_void,
    values: *const i32,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_numeric_rows(
        ptr,
        values,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        |value| i64::from(*value),
    )
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64_rows(
    ptr: *mut c_void,
    values: *const i64,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_numeric_rows(
        ptr,
        values,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        |value| *value,
    )
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f32_rows(
    ptr: *mut c_void,
    values: *const f32,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_numeric_rows(
        ptr,
        values,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        |value| f64::from(*value),
    )
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f64_rows(
    ptr: *mut c_void,
    values: *const f64,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_numeric_rows(
        ptr,
        values,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        |value| *value,
    )
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_bool_rows(
    ptr: *mut c_void,
    values: *const bool,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_numeric_rows(
        ptr,
        values,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        |value| *value,
    )
}

fn add_delimited_rows(
    ptr: *mut c_void,
    value_ptrs: *const *const u8,
    value_lens: *const usize,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
    json: bool,
) -> RustResult {
    if ptr.is_null() {
        return invalid_batch("index writer handle is null");
    }
    if row_count == 0 {
        return RustResult::from_success();
    }
    let (value_ptrs, row_offsets, doc_ids) =
        match unsafe { row_batch_slices(value_ptrs, value_count, row_offsets, doc_ids, row_count) }
        {
            Ok(slices) => slices,
            Err(result) => return result,
        };
    let value_lens = match unsafe { optional_empty_slice(value_lens, value_count, "value_lens") } {
        Ok(lens) => lens,
        Err(result) => return result,
    };
    if value_ptrs
        .iter()
        .zip(value_lens)
        .any(|(value_ptr, value_len)| value_ptr.is_null() && *value_len != 0)
    {
        return invalid_batch("string value pointer is null for a nonempty value");
    }
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        if json {
            (*real)
                .add_json_rows(value_ptrs, value_lens, row_offsets, doc_ids)
                .into()
        } else {
            (*real)
                .add_string_rows(value_ptrs, value_lens, row_offsets, doc_ids)
                .into()
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_string_rows(
    ptr: *mut c_void,
    value_ptrs: *const *const u8,
    value_lens: *const usize,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_delimited_rows(
        ptr,
        value_ptrs,
        value_lens,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        false,
    )
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_json_rows(
    ptr: *mut c_void,
    value_ptrs: *const *const u8,
    value_lens: *const usize,
    value_count: usize,
    row_offsets: *const usize,
    doc_ids: *const i64,
    row_count: usize,
) -> RustResult {
    add_delimited_rows(
        ptr,
        value_ptrs,
        value_lens,
        value_count,
        row_offsets,
        doc_ids,
        row_count,
        true,
    )
}

// -------------------------build--------------------
fn execute_by_single_segment_writer<T: Copy, I>(
    arr: I,
    e: fn(&mut IndexWriterWrapper, T, Option<i64>) -> Result<()>,
    w: &mut IndexWriterWrapper,
) -> Result<()>
where
    I: IntoIterator<Item = T>,
{
    for data in arr.into_iter() {
        e(w, data, None)?;
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        (*real)
            .add_batch(arr.iter().map(|num| *num as i64), offset_begin)
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr.into_iter().map(|num| *num as i64),
            IndexWriterWrapper::add::<i64>,
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        (*real)
            .add_batch(arr.iter().map(|num| *num as i64), offset_begin)
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr.into_iter().map(|num| *num as i64),
            IndexWriterWrapper::add::<i64>,
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        (*real)
            .add_batch(arr.iter().map(|num| *num as i64), offset_begin)
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr.into_iter().map(|num| *num as i64),
            IndexWriterWrapper::add::<i64>,
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };

    unsafe { (*real).add_batch(arr.iter().copied(), offset_begin).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_int64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const i64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { convert_to_rust_slice!(array, len) };

    unsafe {
        execute_by_single_segment_writer(
            arr.iter().copied(),
            IndexWriterWrapper::add::<i64>,
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        (*real)
            .add_batch(arr.iter().map(|num| *num as f64), offset_begin)
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr.into_iter().map(|num| *num as f64),
            IndexWriterWrapper::add::<f64>,
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe { (*real).add_batch(arr.iter().copied(), offset_begin).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_f64s_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const f64,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr.into_iter().map(|num| *num as f64),
            IndexWriterWrapper::add::<f64>,
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
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe { (*real).add_batch(arr.iter().copied(), offset_begin).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_bools_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const bool,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let arr = unsafe { convert_to_rust_slice!(array, len) };
    unsafe {
        execute_by_single_segment_writer(
            arr.iter().copied(),
            IndexWriterWrapper::add::<bool>,
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
    s: *const u8,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let s = ptr_to_str!(s, len);
    unsafe { (*real).add::<&str>(s, Some(offset)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_string_by_single_segment_writer(
    ptr: *mut c_void,
    s: *const u8,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let s = ptr_to_str!(s, len);
    unsafe { (*real).add::<&str>(s, None).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_json_key_stats_data_by_batch(
    ptr: *mut c_void,
    keys: *const *const c_char,
    json_offsets: *const *const i64,
    json_offsets_len: *const usize,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let json_offsets_len = unsafe { convert_to_rust_slice!(json_offsets_len, len) };
    let json_offsets = unsafe { convert_to_rust_slice!(json_offsets, len) };
    let keys = unsafe { convert_to_rust_slice!(keys, len) };
    unsafe {
        (*real)
            .add_json_key_stats(keys, json_offsets, json_offsets_len)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_json(
    ptr: *mut c_void,
    s: *const c_char,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    let s = cstr_to_str!(s);
    unsafe { (*real).add_json(s, Some(offset)).into() }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_json_batch(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_json_batch(arr, offset_begin).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_json(
    ptr: *mut c_void,
    array: *const *const c_char,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let arr = convert_to_rust_slice!(array, len);
        (*real).add_array_json(arr, Some(offset)).into()
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
        (*real)
            .add_array::<i64, _>(arr.into_iter().map(|num| *num as i64), Some(offset))
            .into()
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
        (*real)
            .add_array::<i64, _>(arr.into_iter().map(|num| *num as i64), None)
            .into()
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
        (*real)
            .add_array::<i64, _>(arr.into_iter().map(|num| *num as i64), Some(offset))
            .into()
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
        (*real)
            .add_array::<i64, _>(arr.into_iter().map(|num| *num as i64), None)
            .into()
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
        (*real)
            .add_array::<i64, _>(arr.into_iter().map(|num| *num as i64), Some(offset))
            .into()
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
        (*real)
            .add_array::<i64, _>(arr.into_iter().map(|num| *num as i64), None)
            .into()
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
        (*real)
            .add_array::<i64, _>(arr.iter().copied(), Some(offset))
            .into()
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
        (*real)
            .add_array::<i64, _>(arr.iter().copied(), None)
            .into()
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
        (*real)
            .add_array::<f64, _>(arr.into_iter().map(|num| *num as f64), Some(offset))
            .into()
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
        (*real)
            .add_array::<f64, _>(arr.into_iter().map(|num| *num as f64), None)
            .into()
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
        (*real)
            .add_array::<f64, _>(arr.iter().copied(), Some(offset))
            .into()
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
        (*real)
            .add_array::<f64, _>(arr.iter().copied(), None)
            .into()
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
        (*real)
            .add_array::<bool, _>(arr.iter().copied(), Some(offset))
            .into()
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
        (*real)
            .add_array::<bool, _>(arr.iter().copied(), None)
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_keywords(
    ptr: *mut c_void,
    array: *const *const u8,
    str_lens: *const usize,
    len: usize,
    offset: i64,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let ptrs = convert_to_rust_slice!(array, len);
        let lens = convert_to_rust_slice!(str_lens, len);
        (*real)
            .add_array_keywords_with_len(ptrs, lens, Some(offset))
            .into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_array_keywords_by_single_segment_writer(
    ptr: *mut c_void,
    array: *const *const u8,
    str_lens: *const usize,
    len: usize,
) -> RustResult {
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        let ptrs = convert_to_rust_slice!(array, len);
        let lens = convert_to_rust_slice!(str_lens, len);
        (*real).add_array_keywords_with_len(ptrs, lens, None).into()
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_strings_with_len(
    ptr: *mut c_void,
    array: *const *const u8,
    str_lens: *const usize,
    len: usize,
    offset_begin: i64,
) -> RustResult {
    if ptr.is_null() {
        return invalid_batch("index writer handle is null");
    }
    if len == 0 {
        return RustResult::from_success();
    }
    let ptrs = match unsafe { required_slice(array, len, "string pointers") } {
        Ok(ptrs) => ptrs,
        Err(result) => return result,
    };
    let lens = match unsafe { required_slice(str_lens, len, "string lengths") } {
        Ok(lens) => lens,
        Err(result) => return result,
    };
    let real = ptr as *mut IndexWriterWrapper;
    unsafe {
        (*real)
            .add_strings_with_len(ptrs, lens, offset_begin)
            .into()
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::array::free_rust_result;

    fn assert_internal_error(result: RustResult) {
        let success = result.success;
        let error = if result.error.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(result.error) }
                .to_string_lossy()
                .into_owned()
        };
        free_rust_result(result);
        assert!(!success, "invalid FFI input unexpectedly succeeded");
        assert!(error.contains("InternalError"), "unexpected error: {error}");
    }

    #[test]
    fn finish_rejects_null_writer_without_dereferencing_it() {
        assert_internal_error(tantivy_finish_index(ptr::null_mut()));
    }

    #[test]
    fn row_batch_ffi_rejects_bad_pointers_before_writing() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::new(
            "field",
            TantivyDataType::I64,
            dir.path().to_str().unwrap().to_owned(),
            1,
            15_000_000,
            TantivyIndexVersion::V7,
            true,
            false,
        )
        .unwrap();
        let handle = &mut writer as *mut IndexWriterWrapper as *mut c_void;
        let values = [11_i64];
        let offsets = [0_usize, 1];
        let ids = [0_i64];
        for (values, offsets, ids) in [
            (ptr::null(), offsets.as_ptr(), ids.as_ptr()),
            (values.as_ptr(), ptr::null(), ids.as_ptr()),
            (values.as_ptr(), offsets.as_ptr(), ptr::null()),
        ] {
            assert_internal_error(tantivy_index_add_int64_rows(
                handle, values, 1, offsets, ids, 1,
            ));
        }
        assert_internal_error(tantivy_index_add_int64_rows(
            handle,
            values.as_ptr(),
            1,
            offsets.as_ptr(),
            ids.as_ptr(),
            usize::MAX,
        ));
        let empty =
            tantivy_index_add_int64_rows(handle, ptr::null(), 0, ptr::null(), ptr::null(), 0);
        assert!(empty.success);
        free_rust_result(empty);
        // Invalid batches did not reserve ID 0 or submit any document.
        let valid = tantivy_index_add_int64_rows(
            handle,
            values.as_ptr(),
            1,
            offsets.as_ptr(),
            ids.as_ptr(),
            1,
        );
        assert!(valid.success);
        free_rust_result(valid);
        writer.finish().unwrap();
        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        assert_eq!(index.reader().unwrap().searcher().num_docs(), 1);
    }
}
