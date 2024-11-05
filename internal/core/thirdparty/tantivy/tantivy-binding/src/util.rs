use std::ffi::c_void;
use std::ops::Bound;
use serde_json as json;
use crate::error::TantivyError;

use tantivy::{directory::MmapDirectory, Index};

pub fn index_exist(path: &str) -> bool {
    let dir = MmapDirectory::open(path).unwrap();
    Index::exists(&dir).unwrap()
}

pub fn make_bounds<T>(bound: T, inclusive: bool) -> Bound<T> {
    if inclusive {
        Bound::Included(bound)
    } else {
        Bound::Excluded(bound)
    }
}

pub fn create_binding<T>(wrapper: T) -> *mut c_void {
    let bp = Box::new(wrapper);
    let p_heap: *mut T = Box::into_raw(bp);
    p_heap as *mut c_void
}

pub fn free_binding<T>(ptr: *mut c_void) {
    let real = ptr as *mut T;
    unsafe {
        drop(Box::from_raw(real));
    }
}

pub(crate) fn get_string_list(value: &json::Value, label: &str) -> Result<Vec<String>, TantivyError>{
    if !value.is_array(){
        return Err(format!("{} should be array", label).into())
    }

    let stop_words = value.as_array().unwrap();
    let mut str_list = Vec::<String>::new();
    for element in stop_words{
        match element.as_str(){
            Some(word) => str_list.push(word.to_string()),
            None => return Err(format!("{} list item should be string", label).into())
        }
    };
    Ok(str_list)
}