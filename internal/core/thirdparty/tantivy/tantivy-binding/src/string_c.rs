use std::ffi::CString;

use libc::c_char;

pub(crate) fn create_string(s: String) -> *const c_char {
    CString::new(s).unwrap().into_raw()
}

#[no_mangle]
pub extern "C" fn free_rust_string(ptr: *const c_char) {
    unsafe {
        let _ = CString::from_raw(ptr as *mut c_char);
    }
}
