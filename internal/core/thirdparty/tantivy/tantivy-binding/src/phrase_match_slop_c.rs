use std::ffi::CStr;

use libc::c_char;

use crate::{array::RustResult, phrase_match_slop::compute_phrase_match_slop};

#[no_mangle]
pub extern "C" fn tantivy_compute_phrase_match_slop(
    tokenizer_params: *const c_char,
    query: *const c_char,
    data: *const c_char,
    slop: *mut u32,
) -> RustResult {
    let params_str = match unsafe { CStr::from_ptr(tokenizer_params).to_str() } {
        Ok(s) => s,
        Err(e) => return RustResult::from_error(format!("Invalid tokenizer_params: {}", e)),
    };

    let query_str = match unsafe { CStr::from_ptr(query).to_str() } {
        Ok(s) => s,
        Err(e) => return RustResult::from_error(format!("Invalid query: {}", e)),
    };

    let data_str = match unsafe { CStr::from_ptr(data).to_str() } {
        Ok(s) => s,
        Err(e) => return RustResult::from_error(format!("Invalid data: {}", e)),
    };

    match compute_phrase_match_slop(params_str, query_str, data_str) {
        Ok(result) => {
            unsafe {
                *slop = result;
            }
            RustResult::from_success()
        }
        Err(e) => RustResult::from_error(e),
    }
}
