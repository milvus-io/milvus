use core::slice;

use libc::{size_t, freeaddrinfo};
use scopeguard::defer_on_unwind;

#[repr(C)]
pub struct RustArray {
    array: *mut u32,
    len: size_t,
}

impl RustArray {
    pub fn from_vec(vec: Vec<u32>) -> RustArray {
        defer_on_unwind!({
            ::std::process::abort();
        });
        let v: Box<[u32]> = vec.into_boxed_slice();
        RustArray {
            len: v.len().try_into().expect("Integer Overflow"),
            array: Box::into_raw(v) as _,
        }
    }
}

#[no_mangle]
pub extern "C" fn free_rust_array(array: RustArray) {
    defer_on_unwind!({
        ::std::process::abort();
    });
    let RustArray { array, len } = array;
    unsafe {
        drop::<Box<[u32]>>(Box::from_raw(slice::from_raw_parts_mut(
            array,
            len.try_into().expect("Integer Overflow"),
        )));
    }
}
