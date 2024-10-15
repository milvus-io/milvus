use libc::size_t;

#[repr(C)]
pub struct RustArray {
    array: *mut i64,
    len: size_t,
    cap: size_t,
}

impl RustArray {
    pub fn from_vec(vec: Vec<i64>) -> RustArray {
        let len = vec.len();
        let cap = vec.capacity();
        let v = vec.leak();
        RustArray {
            array: v.as_mut_ptr(),
            len,
            cap,
        }
    }
}

#[no_mangle]
pub extern "C" fn free_rust_array(array: RustArray) {
    let RustArray { array, len, cap } = array;
    unsafe {
        Vec::from_raw_parts(array, len, cap);
    }
}
