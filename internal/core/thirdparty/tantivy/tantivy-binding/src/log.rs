use env_logger::Env;
use std::sync::Once;

pub(crate) fn init_log() {
    static _INITIALIZED: Once = Once::new();
    _INITIALIZED.call_once(|| {
        let _env = Env::default().filter_or("MY_LOG_LEVEL", "info");
        env_logger::init_from_env(_env);
    });
}
