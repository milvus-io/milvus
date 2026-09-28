use std::ffi::{CStr, CString};

use libc::c_char;

use std::str;

// Be careful to use this function, since the returned str depends on the input to be not freed.
pub(crate) unsafe fn c_str_to_str<'a>(s: *const c_char) -> &'a str {
    let rs = CStr::from_ptr(s);
    str::from_utf8_unchecked(rs.to_bytes())
}

pub(crate) fn create_string(s: &str) -> *const c_char {
    match CString::new(s) {
        Ok(cs) => cs.into_raw(),
        // A C string cannot carry an interior NUL, and every error message
        // crossing the FFI is built here. Escape rather than panic: this runs
        // inside `extern "C"` frames, where a panic cannot unwind and aborts
        // the process -- turning a reportable error into a dead node.
        Err(_) => CString::new(s.replace('\0', "\\0"))
            .unwrap_or_default()
            .into_raw(),
    }
}

#[no_mangle]
pub extern "C" fn free_rust_string(ptr: *const c_char) {
    unsafe {
        let _ = CString::from_raw(ptr as *mut c_char);
    }
}
