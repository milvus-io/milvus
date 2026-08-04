use std::ffi::{c_char, CString};
use std::slice;
use std::sync::Mutex;

use tantivy_binding::TantivyIndexVersion;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TantivyLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

type TantivyLogCallback = unsafe extern "C" fn(
    level: TantivyLogLevel,
    target: *const c_char,
    target_len: usize,
    file: *const c_char,
    file_len: usize,
    line: u32,
    message: *const c_char,
    message_len: usize,
) -> bool;

extern "C" {
    fn tantivy_index_exist(path: *const c_char) -> bool;
    fn tantivy_set_log_callback(callback: TantivyLogCallback);
    fn tantivy_set_log_level(level: *const c_char);
    fn tantivy_test_log_from_background_thread() -> bool;
}

#[derive(Debug)]
struct CapturedRecord {
    level: TantivyLogLevel,
    target: String,
    file: String,
    line: u32,
    message: String,
}

static RECORDS: Mutex<Vec<CapturedRecord>> = Mutex::new(Vec::new());

unsafe extern "C" fn capture_log(
    level: TantivyLogLevel,
    target: *const c_char,
    target_len: usize,
    file: *const c_char,
    file_len: usize,
    line: u32,
    message: *const c_char,
    message_len: usize,
) -> bool {
    RECORDS.lock().unwrap().push(CapturedRecord {
        level,
        target: copy_string(target, target_len),
        file: copy_string(file, file_len),
        line,
        message: copy_string(message, message_len),
    });
    true
}

unsafe fn copy_string(ptr: *const c_char, len: usize) -> String {
    String::from_utf8_lossy(slice::from_raw_parts(ptr.cast(), len)).into_owned()
}

#[test]
fn forwards_all_severities_from_background_thread() {
    let _ = TantivyIndexVersion::default_version();
    unsafe {
        tantivy_set_log_callback(capture_log);
        let trace = CString::new("trace").unwrap();
        tantivy_set_log_level(trace.as_ptr());

        // Milvus configures the level before the first Tantivy operation.
        let path = CString::new("/nonexistent/path/to/tantivy-index").unwrap();
        assert!(!tantivy_index_exist(path.as_ptr()));
    }

    assert!(unsafe { tantivy_test_log_from_background_thread() });

    unsafe {
        tantivy_set_log_level(c"info".as_ptr());
    }
    log::debug!(target: "tantivy::background", "filtered debug");
    log::info!(target: "tantivy::background", "visible info");

    let all_records = RECORDS.lock().unwrap();
    let records: Vec<_> = all_records
        .iter()
        .filter(|record| record.message.starts_with("bridge "))
        .collect();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].level, TantivyLogLevel::Trace);
    assert_eq!(records[1].level, TantivyLogLevel::Debug);
    assert_eq!(records[2].level, TantivyLogLevel::Info);
    assert_eq!(records[3].level, TantivyLogLevel::Warn);
    assert_eq!(records[4].level, TantivyLogLevel::Error);
    assert!(records
        .iter()
        .all(|record| record.target == "tantivy::background"));
    assert!(records
        .iter()
        .all(|record| record.file.ends_with("src/log_c.rs") && record.line > 0));
    assert!(!all_records
        .iter()
        .any(|record| record.message == "filtered debug"));
    assert!(all_records
        .iter()
        .any(|record| record.message == "visible info"));
}
