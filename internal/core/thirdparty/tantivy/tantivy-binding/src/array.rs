use std::default;
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
pub enum Value {
    None(()),
    RustArray(RustArray),
    U32(u32),
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

impl_from_for_enum!(Value, None => (), RustArray => RustArray, RustArray => Vec<u32>, U32 => u32, Ptr => *mut c_void);

#[repr(C)]
pub struct RustResult {
    pub success: bool,
    pub value: Value,
    pub error: *const c_char,
}

impl RustResult {
    pub fn from_ptr(value: *mut c_void) -> Self {
        RustResult {
            success: true,
            value: Value::Ptr(value),
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
        _ => {}
    }
    unsafe {
        if !result.error.is_null() {
            free_rust_string(result.error as *mut c_char);
        }
    }
}

#[no_mangle]
pub extern "C" fn free_rust_error(error: *const c_char) {
    unsafe {
        if !error.is_null() {
            free_rust_string(error as *mut c_char);
        }
    }
}

// TODO: move to common
#[macro_export]
macro_rules! cstr_to_str {
    ($cstr:expr) => {
        unsafe {
            match CStr::from_ptr($cstr).to_str() {
                Ok(f) => f,
                Err(e) => return RustResult::from_error(e.to_string()),
            }
        }
    };
}

#[no_mangle]
pub extern "C" fn test_enum_with_array() -> RustResult {
    let array = vec![1, 2, 3];
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
