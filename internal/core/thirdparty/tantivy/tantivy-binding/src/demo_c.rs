use std::{ffi::c_char, slice};

use crate::util::c_ptr_to_str;

#[no_mangle]
pub extern "C" fn print_vector_of_strings(ptr: *const *const c_char, len: usize) {
    let arr: &[*const c_char] = unsafe { slice::from_raw_parts(ptr, len) };
    for element in arr {
        let str = c_ptr_to_str(*element).unwrap();
        println!("{}", str);
    }
}
