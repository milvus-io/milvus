use std::sync::Once;

pub(crate) fn init_log() {
    static _INITIALIZED: Once = Once::new();
    _INITIALIZED.call_once(|| {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .init();
    });
}
