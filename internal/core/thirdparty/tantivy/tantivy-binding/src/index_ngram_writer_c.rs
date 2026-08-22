use std::ffi::CStr;
use std::ffi::{c_char, c_void};
use std::slice;

use crate::array::RustResult;
use crate::cstr_to_str;
use crate::error::{Result, TantivyBindingError};
use crate::index_ngram_writer::NgramRow;
use crate::index_writer::IndexWriterWrapper;
use crate::log::init_log;
use crate::util::create_binding;

fn invalid_argument(message: impl Into<String>) -> TantivyBindingError {
    TantivyBindingError::InvalidArgument(message.into())
}

unsafe fn validate_ngram_rows<'a>(
    ptrs: &'a [*const u8],
    lens: &[usize],
    doc_ids: &[i64],
    has_values: &[u8],
) -> Result<Vec<NgramRow<'a>>> {
    let mut rows = Vec::with_capacity(ptrs.len());
    let mut previous_doc_id = None;
    let max_exclusive_doc_id = tantivy::indexer::merger::MAX_DOC_LIMIT - 1;

    for row_index in 0..ptrs.len() {
        let raw_doc_id = doc_ids[row_index];
        let doc_id = u32::try_from(raw_doc_id).map_err(|_| {
            invalid_argument(format!(
                "ngram batch document ID {raw_doc_id} at row {row_index} is outside [0, {})",
                max_exclusive_doc_id
            ))
        })?;
        if doc_id >= max_exclusive_doc_id {
            return Err(invalid_argument(format!(
                "ngram batch document ID {raw_doc_id} at row {row_index} is outside [0, {})",
                max_exclusive_doc_id
            )));
        }
        if let Some(previous) = previous_doc_id {
            if doc_id <= previous {
                return Err(invalid_argument(format!(
                    "ngram batch document IDs must be strictly increasing: row {row_index} has {doc_id} after {previous}"
                )));
            }
        }
        previous_doc_id = Some(doc_id);

        let value = match has_values[row_index] {
            0 => {
                if lens[row_index] != 0 {
                    return Err(invalid_argument(format!(
                        "ngram batch absent value at row {raw_doc_id} must have length 0, got {}",
                        lens[row_index]
                    )));
                }
                None
            }
            1 => {
                if lens[row_index] == 0 {
                    Some("")
                } else {
                    if ptrs[row_index].is_null() {
                        return Err(invalid_argument(format!(
                            "ngram batch value pointer is null for non-empty row {raw_doc_id}"
                        )));
                    }
                    let bytes = slice::from_raw_parts(ptrs[row_index], lens[row_index]);
                    let value = std::str::from_utf8(bytes).map_err(|error| {
                        invalid_argument(format!(
                            "ngram batch invalid UTF-8 at row {raw_doc_id}: {error}"
                        ))
                    })?;
                    Some(value)
                }
            }
            has_value => {
                return Err(invalid_argument(format!(
                    "ngram batch has_value at row {raw_doc_id} must be 0 or 1, got {has_value}"
                )));
            }
        };
        rows.push(NgramRow { doc_id, value });
    }

    Ok(rows)
}

#[no_mangle]
pub extern "C" fn tantivy_create_ngram_writer(
    field_name: *const c_char,
    path: *const c_char,
    min_gram: usize,
    max_gram: usize,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
) -> RustResult {
    init_log();
    let field_name_str = cstr_to_str!(field_name);
    let path_str = cstr_to_str!(path);

    match IndexWriterWrapper::create_ngram_writer(
        field_name_str,
        path_str,
        min_gram,
        max_gram,
        num_threads,
        overall_memory_budget_in_bytes,
    ) {
        Ok(index_writer_wrapper) => RustResult::from_ptr(create_binding(index_writer_wrapper)),
        Err(err) => RustResult::from_error(format!(
            "create ngram writer failed with error: {}",
            err.to_string(),
        )),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_index_add_ngram_batch(
    writer: *mut c_void,
    ptrs: *const *const u8,
    lens: *const usize,
    doc_ids: *const i64,
    has_values: *const u8,
    len: usize,
) -> RustResult {
    if writer.is_null() {
        return RustResult::from_error(invalid_argument("ngram batch writer is null").to_string());
    }
    if len == 0 {
        return RustResult::from_success();
    }
    if ptrs.is_null() {
        return RustResult::from_error(invalid_argument("ngram batch ptrs is null").to_string());
    }
    if lens.is_null() {
        return RustResult::from_error(invalid_argument("ngram batch lens is null").to_string());
    }
    if doc_ids.is_null() {
        return RustResult::from_error(invalid_argument("ngram batch doc_ids is null").to_string());
    }
    if has_values.is_null() {
        return RustResult::from_error(
            invalid_argument("ngram batch has_values is null").to_string(),
        );
    }

    let ptrs = unsafe { slice::from_raw_parts(ptrs, len) };
    let lens = unsafe { slice::from_raw_parts(lens, len) };
    let doc_ids = unsafe { slice::from_raw_parts(doc_ids, len) };
    let has_values = unsafe { slice::from_raw_parts(has_values, len) };
    let rows = match unsafe { validate_ngram_rows(ptrs, lens, doc_ids, has_values) } {
        Ok(rows) => rows,
        Err(error) => return RustResult::from_error(error.to_string()),
    };

    let writer = unsafe { &mut *(writer as *mut IndexWriterWrapper) };
    writer.add_ngram_rows(&rows).into()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ffi::{c_void, CStr};
    use std::ptr;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::array::free_rust_result;
    use crate::index_reader::IndexReaderWrapper;
    use crate::util::set_bitset;

    struct BatchInput {
        values: Vec<Option<Vec<u8>>>,
        ptrs: Vec<*const u8>,
        lens: Vec<usize>,
        doc_ids: Vec<i64>,
        has_values: Vec<u8>,
    }

    impl BatchInput {
        fn new(rows: Vec<(i64, Option<Vec<u8>>)>) -> Self {
            let doc_ids = rows.iter().map(|(doc_id, _)| *doc_id).collect();
            let values: Vec<_> = rows.into_iter().map(|(_, value)| value).collect();
            let ptrs = values
                .iter()
                .map(|value| value.as_ref().map_or(ptr::null(), |bytes| bytes.as_ptr()))
                .collect();
            let lens = values
                .iter()
                .map(|value| value.as_ref().map_or(0, Vec::len))
                .collect();
            let has_values = values
                .iter()
                .map(|value| u8::from(value.is_some()))
                .collect();
            Self {
                values,
                ptrs,
                lens,
                doc_ids,
                has_values,
            }
        }

        fn call(&self, writer: &mut IndexWriterWrapper) -> RustResult {
            let result = tantivy_index_add_ngram_batch(
                writer as *mut IndexWriterWrapper as *mut c_void,
                self.ptrs.as_ptr(),
                self.lens.as_ptr(),
                self.doc_ids.as_ptr(),
                self.has_values.as_ptr(),
                self.values.len(),
            );
            std::hint::black_box(&self.values);
            result
        }
    }

    fn create_writer() -> (TempDir, IndexWriterWrapper) {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        (dir, writer)
    }

    fn result_error(result: RustResult) -> (bool, String) {
        let success = result.success;
        let error = if result.error.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(result.error) }
                .to_string_lossy()
                .into_owned()
        };
        free_rust_result(result);
        (success, error)
    }

    fn assert_success(result: RustResult) {
        let (success, error) = result_error(result);
        assert!(success, "expected success, got: {error}");
    }

    fn assert_error_contains(result: RustResult, expected: &str) {
        let (success, error) = result_error(result);
        assert!(!success, "expected error containing {expected:?}");
        assert!(
            error.contains(expected),
            "expected error containing {expected:?}, got {error:?}"
        );
    }

    fn finish_reader(dir: &TempDir, writer: IndexWriterWrapper) -> IndexReaderWrapper {
        writer.finish().unwrap();
        let index = tantivy::Index::open_in_dir(dir.path()).unwrap();
        IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap()
    }

    fn query(reader: &IndexReaderWrapper, literal: &str) -> HashSet<u32> {
        let mut result = HashSet::new();
        reader
            .ngram_match_query(
                literal,
                2,
                3,
                &mut result as *mut HashSet<u32> as *mut c_void,
            )
            .unwrap();
        result
    }

    #[test]
    fn ngram_batch_empty_accepts_null_top_level_arrays() {
        let (_dir, mut writer) = create_writer();
        assert_success(tantivy_index_add_ngram_batch(
            &mut writer as *mut IndexWriterWrapper as *mut c_void,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            0,
        ));
    }

    #[test]
    fn ngram_batch_preserves_nonzero_sparse_ids_nulls_and_text_bytes() {
        let (dir, mut writer) = create_writer();
        let batch = BatchInput::new(vec![
            (3, Some(b"alpha".to_vec())),
            (7, None),
            (11, Some(b"nul\0byte".to_vec())),
            (20, Some(Vec::new())),
            (23, Some("ngram测试".as_bytes().to_vec())),
        ]);
        assert_success(batch.call(&mut writer));

        let reader = finish_reader(&dir, writer);
        assert_eq!(query(&reader, "al"), HashSet::from([3]));
        assert_eq!(query(&reader, "l\0"), HashSet::from([11]));
        assert_eq!(query(&reader, "测试"), HashSet::from([23]));
        assert_eq!(reader.count().unwrap(), 24);
    }

    #[test]
    fn ngram_batch_accepts_valid_empty_with_null_data_pointer() {
        let (dir, mut writer) = create_writer();
        let ptrs = [ptr::null()];
        let lens = [0];
        let doc_ids = [2];
        let has_values = [1];
        assert_success(tantivy_index_add_ngram_batch(
            &mut writer as *mut IndexWriterWrapper as *mut c_void,
            ptrs.as_ptr(),
            lens.as_ptr(),
            doc_ids.as_ptr(),
            has_values.as_ptr(),
            1,
        ));
        let reader = finish_reader(&dir, writer);
        assert_eq!(reader.count().unwrap(), 3);
    }

    #[test]
    fn ngram_batch_rejects_duplicate_and_descending_ids() {
        let (_dir, mut writer) = create_writer();
        let duplicate = BatchInput::new(vec![
            (4, Some(b"first".to_vec())),
            (4, Some(b"second".to_vec())),
        ]);
        assert_error_contains(duplicate.call(&mut writer), "strictly increasing");

        let descending = BatchInput::new(vec![
            (9, Some(b"first".to_vec())),
            (8, Some(b"second".to_vec())),
        ]);
        assert_error_contains(descending.call(&mut writer), "strictly increasing");
    }

    #[test]
    fn ngram_batch_validates_document_id_boundaries() {
        let (_dir, mut negative_writer) = create_writer();
        let negative = BatchInput::new(vec![(-1, Some(b"bad".to_vec()))]);
        assert_error_contains(negative.call(&mut negative_writer), "document ID -1");

        let max_doc_limit = tantivy::indexer::merger::MAX_DOC_LIMIT;
        let (_dir, mut max_writer) = create_writer();
        let max_valid =
            BatchInput::new(vec![(i64::from(max_doc_limit - 2), Some(b"last".to_vec()))]);
        assert_success(max_valid.call(&mut max_writer));

        let (_dir, mut max_doc_writer) = create_writer();
        let max_doc = BatchInput::new(vec![(i64::from(max_doc_limit - 1), Some(b"bad".to_vec()))]);
        assert_error_contains(
            max_doc.call(&mut max_doc_writer),
            &format!("document ID {}", max_doc_limit - 1),
        );

        let (_dir, mut terminated_writer) = create_writer();
        let terminated = BatchInput::new(vec![(
            i64::from(tantivy::TERMINATED),
            Some(b"bad".to_vec()),
        )]);
        assert_error_contains(
            terminated.call(&mut terminated_writer),
            &format!("document ID {}", tantivy::TERMINATED),
        );

        let (_dir, mut u32_max_writer) = create_writer();
        let u32_max = BatchInput::new(vec![(i64::from(u32::MAX), Some(b"bad".to_vec()))]);
        assert_error_contains(
            u32_max.call(&mut u32_max_writer),
            &format!("document ID {}", u32::MAX),
        );
    }

    #[test]
    fn ngram_batch_rejects_null_top_level_arrays_for_nonempty_batch() {
        let (_dir, mut writer) = create_writer();
        let ptrs = [b"ok".as_ptr()];
        let lens = [2];
        let doc_ids = [0];
        let has_values = [1];
        let writer_ptr = &mut writer as *mut IndexWriterWrapper as *mut c_void;

        assert_error_contains(
            tantivy_index_add_ngram_batch(
                writer_ptr,
                ptr::null(),
                lens.as_ptr(),
                doc_ids.as_ptr(),
                has_values.as_ptr(),
                1,
            ),
            "ptrs",
        );
        assert_error_contains(
            tantivy_index_add_ngram_batch(
                writer_ptr,
                ptrs.as_ptr(),
                ptr::null(),
                doc_ids.as_ptr(),
                has_values.as_ptr(),
                1,
            ),
            "lens",
        );
        assert_error_contains(
            tantivy_index_add_ngram_batch(
                writer_ptr,
                ptrs.as_ptr(),
                lens.as_ptr(),
                ptr::null(),
                has_values.as_ptr(),
                1,
            ),
            "doc_ids",
        );
        assert_error_contains(
            tantivy_index_add_ngram_batch(
                writer_ptr,
                ptrs.as_ptr(),
                lens.as_ptr(),
                doc_ids.as_ptr(),
                ptr::null(),
                1,
            ),
            "has_values",
        );
    }

    #[test]
    fn ngram_batch_rejects_invalid_presence_and_data_pointer_shapes() {
        let (_dir, mut writer) = create_writer();
        let ptrs = [ptr::null()];
        let lens = [1];
        let doc_ids = [5];
        let has_values = [1];
        assert_error_contains(
            tantivy_index_add_ngram_batch(
                &mut writer as *mut IndexWriterWrapper as *mut c_void,
                ptrs.as_ptr(),
                lens.as_ptr(),
                doc_ids.as_ptr(),
                has_values.as_ptr(),
                1,
            ),
            "row 5",
        );

        let present = [2];
        assert_error_contains(
            tantivy_index_add_ngram_batch(
                &mut writer as *mut IndexWriterWrapper as *mut c_void,
                ptrs.as_ptr(),
                [0].as_ptr(),
                doc_ids.as_ptr(),
                present.as_ptr(),
                1,
            ),
            "has_value",
        );

        let absent = [0];
        assert_error_contains(
            tantivy_index_add_ngram_batch(
                &mut writer as *mut IndexWriterWrapper as *mut c_void,
                ptrs.as_ptr(),
                lens.as_ptr(),
                doc_ids.as_ptr(),
                absent.as_ptr(),
                1,
            ),
            "absent value",
        );
    }

    #[test]
    fn ngram_batch_invalid_utf8_reports_row_id() {
        let (_dir, mut writer) = create_writer();
        let batch = BatchInput::new(vec![(42, Some(vec![0xff, 0xfe]))]);
        let result = batch.call(&mut writer);
        assert_error_contains(result, "row 42");
    }

    #[test]
    fn ngram_batch_late_validation_error_does_not_partially_write() {
        let (dir, mut writer) = create_writer();
        let invalid = BatchInput::new(vec![
            (4, Some(b"first".to_vec())),
            (8, Some(vec![0xff, 0xfe])),
        ]);
        assert_error_contains(invalid.call(&mut writer), "row 8");

        let valid = BatchInput::new(vec![(0, Some(b"only".to_vec()))]);
        assert_success(valid.call(&mut writer));
        let reader = finish_reader(&dir, writer);
        assert_eq!(query(&reader, "on"), HashSet::from([0]));
        assert_eq!(reader.count().unwrap(), 1);
    }

    #[test]
    fn ngram_batch_copies_input_before_ffi_returns() {
        let (dir, mut writer) = create_writer();
        {
            let input = BatchInput::new(vec![
                (1, Some(b"borrowed-alpha".to_vec())),
                (5, Some("borrowed-测试".as_bytes().to_vec())),
            ]);
            assert_success(input.call(&mut writer));
        }
        let overwrite = vec![b'x'; 1024 * 1024];
        std::hint::black_box(overwrite);

        let reader = finish_reader(&dir, writer);
        assert_eq!(query(&reader, "alp"), HashSet::from([1]));
    }

    #[test]
    fn ngram_batch_preserves_writer_errors() {
        let (_dir, mut writer) = create_writer();
        let first = BatchInput::new(vec![(5, Some(b"first".to_vec()))]);
        assert_success(first.call(&mut writer));

        let out_of_order = BatchInput::new(vec![(4, Some(b"second".to_vec()))]);
        assert_error_contains(out_of_order.call(&mut writer), "strictly ordered");
    }
}
