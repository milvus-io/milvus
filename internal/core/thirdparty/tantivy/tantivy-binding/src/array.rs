use std::ffi::c_void;
use std::ptr::null;

use libc::c_char;
use libc::size_t;

use crate::error;
use crate::error::Result;
use crate::string_c::create_string;
use crate::string_c::free_rust_string;
use crate::util::free_binding;

#[repr(C)]
pub struct RustArray {
    array: *mut u32,
    len: size_t,
    cap: size_t,
}

impl RustArray {
    pub fn from_vec(vec: Vec<u32>) -> RustArray {
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

impl std::default::Default for RustArray {
    fn default() -> Self {
        RustArray {
            array: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

impl From<Vec<u32>> for RustArray {
    fn from(vec: Vec<u32>) -> Self {
        RustArray::from_vec(vec)
    }
}

#[no_mangle]
pub extern "C" fn free_rust_array(array: RustArray) {
    let RustArray { array, len, cap } = array;
    unsafe {
        Vec::from_raw_parts(array, len, cap);
    }
}

#[repr(C)]
pub struct RustArrayI64 {
    array: *mut i64,
    len: size_t,
    cap: size_t,
}

impl RustArrayI64 {
    pub fn from_vec(vec: Vec<i64>) -> RustArrayI64 {
        let len = vec.len();
        let cap = vec.capacity();
        let v = vec.leak();
        RustArrayI64 {
            array: v.as_mut_ptr(),
            len,
            cap,
        }
    }
}

impl std::default::Default for RustArrayI64 {
    fn default() -> Self {
        RustArrayI64 {
            array: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

impl From<Vec<i64>> for RustArrayI64 {
    fn from(vec: Vec<i64>) -> Self {
        RustArrayI64::from_vec(vec)
    }
}

#[no_mangle]
pub extern "C" fn free_rust_array_i64(array: RustArrayI64) {
    let RustArrayI64 { array, len, cap } = array;
    unsafe {
        Vec::from_raw_parts(array, len, cap);
    }
}

/// Array of byte strings for returning Vec<String> to C++. Each element is
/// `array[i]` with `lens[i]` bytes, NOT NUL-terminated: the strings are index
/// terms that may legitimately contain interior NUL bytes, which a C string
/// cannot carry. `array` and `lens` are boxed slices of `len` elements.
#[repr(C)]
pub struct RustStringArray {
    pub array: *mut *mut c_char,
    pub lens: *mut size_t,
    pub len: size_t,
}

impl RustStringArray {
    pub fn from_vec(vec: Vec<String>) -> RustStringArray {
        let len = vec.len();
        let mut ptrs: Vec<*mut c_char> = Vec::with_capacity(len);
        let mut lens: Vec<size_t> = Vec::with_capacity(len);
        for s in vec {
            let bytes = s.into_bytes().into_boxed_slice();
            lens.push(bytes.len());
            ptrs.push(Box::into_raw(bytes) as *mut c_char);
        }

        RustStringArray {
            array: Box::into_raw(ptrs.into_boxed_slice()) as *mut *mut c_char,
            lens: Box::into_raw(lens.into_boxed_slice()) as *mut size_t,
            len,
        }
    }
}

impl std::default::Default for RustStringArray {
    fn default() -> Self {
        RustStringArray {
            array: std::ptr::null_mut(),
            lens: std::ptr::null_mut(),
            len: 0,
        }
    }
}

impl From<Vec<String>> for RustStringArray {
    fn from(vec: Vec<String>) -> Self {
        RustStringArray::from_vec(vec)
    }
}

#[no_mangle]
pub extern "C" fn free_rust_string_array(array: RustStringArray) {
    let RustStringArray { array, lens, len } = array;
    if array.is_null() {
        return;
    }
    unsafe {
        let ptrs = Box::from_raw(std::slice::from_raw_parts_mut(array, len));
        let lens = Box::from_raw(std::slice::from_raw_parts_mut(lens, len));
        for (&p, &l) in ptrs.iter().zip(lens.iter()) {
            if !p.is_null() {
                drop(Box::from_raw(std::slice::from_raw_parts_mut(
                    p as *mut u8,
                    l,
                )));
            }
        }
    }
}

#[allow(dead_code)]
#[repr(C)]
pub enum Value {
    None(()),
    RustArray(RustArray),
    RustArrayI64(RustArrayI64),
    RustStringArray(RustStringArray),
    U32(u32),
    U64(u64),
    Ptr(*mut c_void),
}

macro_rules! impl_from_for_enum {
    ($enum_name:ident, $($variant:ident => $type:ty),*) => {
        $(
            impl From<$type> for $enum_name {
                fn from(value: $type) -> Self {
                    $enum_name::$variant(value.into())
                }
            }
        )*
    };
}

impl_from_for_enum!(Value, None => (), RustArrayI64 => RustArrayI64, RustArrayI64 => Vec<i64>, RustArray => RustArray, RustArray => Vec<u32>, RustStringArray => RustStringArray, RustStringArray => Vec<String>, U32 => u32, U64 => u64, Ptr => *mut c_void);

#[repr(C)]
pub struct RustResult {
    pub success: bool,
    pub value: Value,
    pub error: *const c_char,
}

impl RustResult {
    pub fn from_success() -> Self {
        RustResult {
            success: true,
            value: Value::None(()),
            error: std::ptr::null(),
        }
    }

    pub fn from_ptr(value: *mut c_void) -> Self {
        RustResult {
            success: true,
            value: Value::Ptr(value),
            error: std::ptr::null(),
        }
    }

    pub fn from_vec_i64(value: Vec<i64>) -> Self {
        RustResult {
            success: true,
            value: Value::RustArrayI64(RustArrayI64::from_vec(value)),
            error: std::ptr::null(),
        }
    }

    pub fn from_error(error: String) -> Self {
        RustResult {
            success: false,
            value: Value::None(()),
            error: create_string(&error),
        }
    }
}

impl<T> From<Result<T>> for RustResult
where
    T: Into<Value>,
{
    fn from(value: error::Result<T>) -> Self {
        match value {
            Ok(v) => RustResult {
                success: true,
                value: v.into(),
                error: null(),
            },
            Err(e) => RustResult {
                success: false,
                value: Value::None(()),
                error: create_string(&e.to_string()),
            },
        }
    }
}

#[no_mangle]
pub extern "C" fn free_rust_result(result: RustResult) {
    match result.value {
        Value::RustArray(array) => {
            if !array.array.is_null() {
                free_rust_array(array);
            }
        }
        Value::RustArrayI64(array) => {
            if !array.array.is_null() {
                free_rust_array_i64(array);
            }
        }
        Value::RustStringArray(array) => {
            if !array.array.is_null() {
                free_rust_string_array(array);
            }
        }
        _ => {}
    }
    if !result.error.is_null() {
        free_rust_string(result.error as *mut c_char);
    }
}

#[no_mangle]
pub extern "C" fn free_rust_error(error: *const c_char) {
    if !error.is_null() {
        free_rust_string(error as *mut c_char);
    }
}

// TODO: move to common
#[macro_export]
macro_rules! cstr_to_str {
    ($cstr:expr) => {
        match unsafe { CStr::from_ptr($cstr).to_str() } {
            Ok(f) => f,
            Err(e) => return RustResult::from_error(e.to_string()),
        }
    };
}

/// Convert a (pointer, length) pair to `&str`, supporting embedded NUL bytes.
/// This is the macro counterpart of `ptr_len_to_str` for use in FFI functions
/// that return `RustResult`.
#[macro_export]
macro_rules! ptr_to_str {
    ($ptr:expr, $len:expr) => {
        match std::str::from_utf8(unsafe {
            std::slice::from_raw_parts($ptr as *const u8, $len as usize)
        }) {
            Ok(s) => s,
            Err(e) => return RustResult::from_error(e.to_string()),
        }
    };
}

#[no_mangle]
pub extern "C" fn test_enum_with_array() -> RustResult {
    let array: Vec<u32> = vec![1, 2, 3];
    RustResult::from(Result::Ok(array))
}

#[no_mangle]
pub extern "C" fn test_enum_with_ptr() -> RustResult {
    let ptr = Box::into_raw(Box::new(1 as u32));
    RustResult::from(Result::Ok(ptr as *mut c_void))
}

#[no_mangle]
pub extern "C" fn free_test_ptr(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    free_binding::<u32>(ptr);
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    // An error message may quote the caller's literal, which can carry an
    // interior NUL. Building the C string must not panic: this conversion
    // happens inside `extern "C"` frames, where a panic aborts the process.
    #[test]
    fn test_error_message_with_interior_nul_does_not_panic() {
        let err: error::Result<()> = Err(error::TantivyBindingError::InternalError(
            "bad \0 literal".to_string(),
        ));
        let result = RustResult::from(err);
        assert!(!result.success);
        let msg = unsafe { CStr::from_ptr(result.error) }.to_str().unwrap();
        assert!(msg.contains("\\0"), "unexpected message: {}", msg);
        free_rust_result(result);

        let result = RustResult::from_error("a\0b".to_string());
        let msg = unsafe { CStr::from_ptr(result.error) }.to_str().unwrap();
        assert_eq!(msg, "a\\0b");
        free_rust_result(result);
    }

    #[test]
    fn test_rust_string_array_keeps_interior_nul() {
        let array =
            RustStringArray::from_vec(vec!["ab\0c".to_string(), String::new(), "测试".to_string()]);
        assert_eq!(array.len, 3);
        let got: Vec<Vec<u8>> = (0..array.len)
            .map(|i| unsafe {
                std::slice::from_raw_parts(*array.array.add(i) as *const u8, *array.lens.add(i))
                    .to_vec()
            })
            .collect();
        assert_eq!(got[0], b"ab\0c");
        assert!(got[1].is_empty());
        assert_eq!(got[2], "测试".as_bytes());
        free_rust_string_array(array);
        free_rust_string_array(RustStringArray::default());
    }
}
