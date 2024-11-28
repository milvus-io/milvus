use serde_json as json;
use std::ffi::c_void;
use std::ops::Bound;
use tantivy::{directory::MmapDirectory, Index};

use crate::error::Result;
use crate::error::TantivyBindingError;
use crate::stop_words;

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
<<<<<<< HEAD
=======

pub(crate) fn get_string_list(value: &json::Value, label: &str) -> Result<Vec<String>> {
    if !value.is_array() {
        return Err(TantivyBindingError::InternalError(
            format!("{} should be array", label).to_string(),
        ));
    }

    let stop_words = value.as_array().unwrap();
    let mut str_list = Vec::<String>::new();
    for element in stop_words {
        match element.as_str() {
            Some(word) => str_list.push(word.to_string()),
            None => {
                return Err(TantivyBindingError::InternalError(
                    format!("{} list item should be string", label).to_string(),
                ))
            }
        }
    }
    Ok(str_list)
}

pub(crate) fn get_stop_words_list(str_list: Vec<String>) -> Vec<String> {
    let mut stop_words = Vec::new();
    for str in str_list {
        if str.len() > 0 && str.chars().nth(0).unwrap() == '_' {
            match str.as_str() {
                "_english_" => {
                    for word in stop_words::ENGLISH {
                        stop_words.push(word.to_string());
                    }
                    continue;
                }
                _other => {}
            }
        }
        stop_words.push(str);
    }
    stop_words
}
>>>>>>> e6af806a0d (enhance: optimize self defined rust error (#37975))
