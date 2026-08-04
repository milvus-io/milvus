use std::ffi::c_char;
use std::sync::{Once, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TantivyLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

pub type TantivyLogCallback = unsafe extern "C" fn(
    level: TantivyLogLevel,
    target: *const c_char,
    target_len: usize,
    file: *const c_char,
    file_len: usize,
    line: u32,
    message: *const c_char,
    message_len: usize,
) -> bool;

static LOG_CALLBACK: OnceLock<TantivyLogCallback> = OnceLock::new();
static LOGGER: OnceLock<TantivyLogger> = OnceLock::new();

struct TantivyLogger {
    fallback: env_logger::Logger,
}

impl Log for TantivyLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let Some(callback) = LOG_CALLBACK.get().copied() else {
            self.fallback.log(record);
            return;
        };

        let message = record.args().to_string();
        let target = record.target().as_bytes();
        let file = record.file().unwrap_or_default().as_bytes();
        let handled = unsafe {
            callback(
                map_level(record.level()),
                target.as_ptr().cast(),
                target.len(),
                file.as_ptr().cast(),
                file.len(),
                record.line().unwrap_or_default(),
                message.as_ptr().cast(),
                message.len(),
            )
        };
        if !handled {
            self.fallback.log(record);
        }
    }

    fn flush(&self) {
        self.fallback.flush();
    }
}

fn map_level(level: Level) -> TantivyLogLevel {
    match level {
        Level::Trace => TantivyLogLevel::Trace,
        Level::Debug => TantivyLogLevel::Debug,
        Level::Info => TantivyLogLevel::Info,
        Level::Warn => TantivyLogLevel::Warn,
        Level::Error => TantivyLogLevel::Error,
    }
}

pub(crate) fn set_log_callback(callback: TantivyLogCallback) {
    // The bridge function has process lifetime. Repeated Milvus initialization
    // is idempotent and must not replace a callback while workers are logging.
    let _ = LOG_CALLBACK.set(callback);
}

pub(crate) fn set_log_level(level: LevelFilter) {
    // Finish one-time logger initialization before applying a runtime level so
    // first use cannot reset a level configured earlier by Milvus startup.
    init_log();
    log::set_max_level(level);
}

pub(crate) fn init_log() {
    static INITIALIZED: Once = Once::new();
    INITIALIZED.call_once(|| {
        let logger = LOGGER.get_or_init(|| TantivyLogger {
            // The log facade owns runtime filtering. Keeping the fallback open
            // to all levels allows tantivy_set_log_level to update it later.
            fallback: env_logger::Builder::new()
                .filter_level(log::LevelFilter::Trace)
                .build(),
        });
        log::set_logger(logger).expect("failed to initialize Tantivy logger");
        log::set_max_level(log::LevelFilter::Info);
    });
}
