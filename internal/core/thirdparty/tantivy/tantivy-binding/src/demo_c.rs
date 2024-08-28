use std::{
    ffi::{c_char, CStr},
    slice,
};

#[no_mangle]
pub extern "C" fn print_vector_of_strings(ptr: *const *const c_char, len: usize) {
    let arr: &[*const c_char] = unsafe { slice::from_raw_parts(ptr, len) };
    for element in arr {
        let c_str = unsafe { CStr::from_ptr(*element) };
        println!("{}", c_str.to_str().unwrap());
    }
}
