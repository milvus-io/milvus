use std::collections::HashMap;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tantivy::tokenizer::TextAnalyzer;

use crate::array::RustResult;
use crate::error::{Result, TantivyBindingError};

// Match pkg/util/typeutil.HashString2LessUint32, including byte (not character)
// truncation and the reserved UINT32_MAX dimension. This is an index contract.
fn token_hash(text: &str) -> u32 {
    let bytes = text.as_bytes();
    crc32fast::hash(&bytes[..bytes.len().min(100)]) % u32::MAX
}

struct BM25Batch {
    data: Vec<u8>,
    offsets: Vec<u64>,
}

/// Borrowed views owned by `handle`. Free exactly once with tantivy_free_bm25_batch.
/// Each row is sorted (little-endian u32 hash, little-endian f32 term frequency).
#[repr(C)]
#[derive(Default)]
pub struct TantivyBM25Batch {
    pub data: *const u8,
    pub data_size: u64,
    pub offsets: *const u64,
    pub handle: *mut c_void,
}

fn tokenize_batch(analyzer: &mut TextAnalyzer, data: &[u8], offsets: &[u64]) -> Result<BM25Batch> {
    if offsets.first() != Some(&0)
        || offsets.last() != Some(&(data.len() as u64))
        || offsets.windows(2).any(|w| w[0] > w[1])
    {
        return Err(TantivyBindingError::InternalError(
            "invalid BM25 batch offsets".into(),
        ));
    }
    let mut batch = BM25Batch {
        data: Vec::new(),
        offsets: vec![0],
    };
    let mut frequencies = HashMap::<u32, f32>::new();
    let mut terms = Vec::new();
    for range in offsets.windows(2) {
        let bytes = &data[range[0] as usize..range[1] as usize];
        let text = std::str::from_utf8(bytes).map_err(|_| {
            TantivyBindingError::InvalidArgument("string data must be utf8 format".into())
        })?;
        // The existing CString-based stream truncates at the first NUL. Preserve
        // that behavior, and skip truly empty inputs just as BM25FunctionRunner does.
        if !text.is_empty() {
            let text = text.split('\0').next().unwrap();
            let mut stream = analyzer.token_stream(text);
            while stream.advance() {
                *frequencies
                    .entry(token_hash(&stream.token().text))
                    .or_default() += 1.0;
            }
        }
        terms.extend(frequencies.drain());
        terms.sort_unstable_by_key(|(hash, _)| *hash);
        for (hash, frequency) in terms.drain(..) {
            batch.data.extend_from_slice(&hash.to_le_bytes());
            batch.data.extend_from_slice(&frequency.to_le_bytes());
        }
        batch.offsets.push(batch.data.len() as u64);
    }
    Ok(batch)
}

/// Inputs are borrowed only for this call. The caller exclusively owns the analyzer.
/// On failure no partial result is returned. Panics must not cross the C ABI.
#[no_mangle]
pub unsafe extern "C" fn tantivy_tokenize_bm25(
    tokenizer: *mut c_void,
    data: *const u8,
    data_size: u64,
    offsets: *const u64,
    num_rows: u64,
    output: *mut TantivyBM25Batch,
) -> RustResult {
    if output.is_null() {
        return RustResult::from_binding_error(&TantivyBindingError::InternalError(
            "null BM25 batch output".into(),
        ));
    }
    *output = TantivyBM25Batch::default();
    let result = catch_unwind(AssertUnwindSafe(|| {
        if tokenizer.is_null()
            || offsets.is_null()
            || (data_size != 0 && data.is_null())
            || data_size > isize::MAX as u64
            || num_rows >= (isize::MAX as u64 / std::mem::size_of::<u64>() as u64)
        {
            return Err(TantivyBindingError::InternalError(
                "invalid BM25 batch buffers".into(),
            ));
        }
        let data = if data_size == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(data, data_size as usize)
        };
        let offsets = std::slice::from_raw_parts(offsets, num_rows as usize + 1);
        tokenize_batch(&mut *(tokenizer as *mut TextAnalyzer), data, offsets)
    }));
    match result {
        Ok(Ok(batch)) => {
            let batch = Box::new(batch);
            *output = TantivyBM25Batch {
                data: batch.data.as_ptr(),
                data_size: batch.data.len() as u64,
                offsets: batch.offsets.as_ptr(),
                handle: Box::into_raw(batch) as *mut c_void,
            };
            RustResult::from_success()
        }
        Ok(Err(error)) => RustResult::from_binding_error(&error),
        Err(_) => RustResult::from_binding_error(&TantivyBindingError::InternalError(
            "BM25 analyzer panicked".into(),
        )),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_free_bm25_batch(handle: *mut c_void) {
    if !handle.is_null() {
        drop(Box::from_raw(handle as *mut BM25Batch));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::create_analyzer;
    use crate::array::free_rust_result;
    use crate::error::TantivyBindingErrorCode;
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    #[test]
    fn hash_contract() {
        assert_eq!(token_hash("123456789"), 0xcbf43926);
        assert_eq!(crc32fast::hash(b"bm25-crc-1295crV"), u32::MAX);
        assert_eq!(token_hash("bm25-crc-1295crV"), 0);
        let prefix = "界".repeat(33); // The cutoff splits the following UTF-8 character.
        assert_eq!(
            token_hash(&format!("{prefix}éx")),
            token_hash(&format!("{prefix}éy"))
        );
    }

    #[test]
    fn batch_rows_and_ffi_ownership() {
        let mut analyzer = create_analyzer(r#"{"tokenizer":"whitespace"}"#, "").unwrap();
        let input = b"a a ba\0ignored";
        let offsets = [0, 5, 5, input.len() as u64];
        let mut output = TantivyBM25Batch::default();
        unsafe {
            let result = tantivy_tokenize_bm25(
                &mut analyzer as *mut _ as *mut c_void,
                input.as_ptr(),
                input.len() as u64,
                offsets.as_ptr(),
                3,
                &mut output,
            );
            assert!(result.success);
            assert!(!output.handle.is_null());
            let rows = std::slice::from_raw_parts(output.offsets, 4);
            assert_eq!(rows, &[0, 16, 16, 24]);
            let data = std::slice::from_raw_parts(output.data, output.data_size as usize);
            let mut terms: Vec<_> = data[..16]
                .chunks_exact(8)
                .map(|pair| {
                    (
                        u32::from_le_bytes(pair[..4].try_into().unwrap()),
                        f32::from_le_bytes(pair[4..].try_into().unwrap()),
                    )
                })
                .collect();
            let mut expected = vec![(token_hash("a"), 2.0), (token_hash("b"), 1.0)];
            expected.sort_unstable_by_key(|t| t.0);
            assert_eq!(terms, expected);
            terms.clear();
            assert_eq!(
                u32::from_le_bytes(data[16..20].try_into().unwrap()),
                token_hash("a")
            );
            free_rust_result(result);
            tantivy_free_bm25_batch(output.handle);
            tantivy_free_bm25_batch(std::ptr::null_mut());
        }
    }

    #[test]
    fn errors_return_no_partial_batch() {
        let mut analyzer = create_analyzer("{}", "").unwrap();
        for (data, offsets, code) in [
            (
                b"ok\xff".as_slice(),
                vec![0, 2, 3],
                TantivyBindingErrorCode::InvalidArgument,
            ),
            (
                b"ok".as_slice(),
                vec![0, 3, 2],
                TantivyBindingErrorCode::Internal,
            ),
            (
                b"ok".as_slice(),
                vec![1, 2],
                TantivyBindingErrorCode::Internal,
            ),
        ] {
            let mut output = TantivyBM25Batch::default();
            unsafe {
                let result = tantivy_tokenize_bm25(
                    &mut analyzer as *mut _ as *mut c_void,
                    data.as_ptr(),
                    data.len() as u64,
                    offsets.as_ptr(),
                    offsets.len() as u64 - 1,
                    &mut output,
                );
                assert!(!result.success);
                assert_eq!(result.error_code, code);
                assert!(output.handle.is_null());
                assert!(output.data.is_null());
                free_rust_result(result);
            }
        }
    }

    #[derive(Clone)]
    struct PanicTokenizer(bool);
    struct PanicStream;
    impl Tokenizer for PanicTokenizer {
        type TokenStream<'a> = PanicStream;
        fn token_stream<'a>(&'a mut self, _: &'a str) -> PanicStream {
            if self.0 {
                panic!("injected stream creation failure");
            }
            PanicStream
        }
    }
    impl TokenStream for PanicStream {
        fn advance(&mut self) -> bool {
            panic!("injected iteration failure");
        }
        fn token(&self) -> &Token {
            unreachable!()
        }
        fn token_mut(&mut self) -> &mut Token {
            unreachable!()
        }
    }

    #[test]
    fn catches_creation_and_iteration_panics() {
        for creation in [true, false] {
            let mut analyzer = TextAnalyzer::from(PanicTokenizer(creation));
            let mut output = TantivyBM25Batch::default();
            unsafe {
                let result = tantivy_tokenize_bm25(
                    &mut analyzer as *mut _ as *mut c_void,
                    b"x".as_ptr(),
                    1,
                    [0, 1].as_ptr(),
                    1,
                    &mut output,
                );
                assert!(!result.success);
                assert_eq!(result.error_code, TantivyBindingErrorCode::Internal);
                assert!(output.handle.is_null());
                free_rust_result(result);
            }
        }
    }
}
