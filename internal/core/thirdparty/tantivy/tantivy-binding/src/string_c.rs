use std::ffi::{CStr, CString};

use libc::c_char;

use std::str;

// Be careful to use this function, since the returned str depends on the input to be not freed.
pub(crate) unsafe fn c_str_to_str<'a>(s: *const c_char) -> &'a str {
    let rs = CStr::from_ptr(s);
    str::from_utf8_unchecked(rs.to_bytes())
}

pub(crate) fn create_string(s: &str) -> *const c_char {
    CString::new(s).unwrap().into_raw()
}

#[no_mangle]
pub extern "C" fn free_rust_string(ptr: *const c_char) {
    unsafe {
        let _ = CString::from_raw(ptr as *mut c_char);
    }
}
