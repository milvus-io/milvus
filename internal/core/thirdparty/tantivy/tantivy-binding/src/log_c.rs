use std::ffi::{c_char, CStr};

use crate::log::{set_log_callback, set_log_level, TantivyLogCallback};

#[no_mangle]
pub extern "C" fn tantivy_set_log_callback(callback: TantivyLogCallback) {
    set_log_callback(callback);
}

#[no_mangle]
pub extern "C" fn tantivy_set_log_level(level: *const c_char) {
    let level_str = unsafe { CStr::from_ptr(level) }.to_str().unwrap_or("info");

    let filter = match level_str {
        "trace" => log::LevelFilter::Trace,
        "debug" => log::LevelFilter::Debug,
        "info" => log::LevelFilter::Info,
        "warn" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        "fatal" | "panic" => log::LevelFilter::Error,
        _ => log::LevelFilter::Info,
    };

    set_log_level(filter);
}
