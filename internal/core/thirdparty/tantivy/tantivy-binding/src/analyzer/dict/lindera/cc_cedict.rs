use super::fetch;
use crate::error::{Result, TantivyBindingError};
use lindera_dictionary::dictionary_builder::cc_cedict::CcCedictBuilder;

#[cfg(feature = "lindera-cc-cedict")]
use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};

#[cfg(not(feature = "lindera-cc-cedict"))]
fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::fetch(params, CcCedictBuilder::new()).map_err(|e| {
        TantivyBindingError::InternalError(format!(
            "fetch cc_cedict failed with error: {}",
            e.to_string()
        ))
    })
}

#[cfg(not(feature = "lindera-cc-cedict"))]
pub fn load_cc_cedict(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    let mut params = fetch::FetchParams {
            lindera_dir: build_dir.to_string(),
            file_name: "CC-CEDICT-MeCab-0.1.0-20200409.tar.gz".to_string(),
            input_dir: "CC-CEDICT-MeCab-0.1.0-20200409".to_string(),
            output_dir: "lindera-cc-cedict".to_string(),
            dummy_input: "测试,0,0,-1131,*,*,*,*,ce4 shi4,測試,测试,to test (machinery etc)/to test (students)/test/quiz/exam/beta (software)/\n".to_string(),
            download_urls: vec![
                "https://lindera.s3.ap-northeast-1.amazonaws.com/CC-CEDICT-MeCab-0.1.0-20200409.tar.gz".to_string(),
                "https://lindera.dev/CC-CEDICT-MeCab-0.1.0-20200409.tar.gz".to_string(),
            ],
            md5_hash: "aba9748b70f37feede97b70c5d37f8a0".to_string(),
    };
    if download_url.len() > 0 {
        params.download_urls = download_url
    }

    download(&params)?;
    fetch::load(&params)
}

#[cfg(feature = "lindera-cc-cedict")]
pub fn load_cc_cedict(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    load_dictionary_from_kind(DictionaryKind::CcCedict).map_err(|_| {
        TantivyBindingError::InvalidArgument(format!(
            "lindera build in cc-cedict dictionary not found"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::load_cc_cedict;
    use env_logger;

    #[test]
    fn test_load_cc_cedict() {
        let _env = env_logger::Env::default().filter_or("MY_LOG_LEVEL", "info");
        env_logger::init_from_env(_env);

        let result = load_cc_cedict("/var/lib/milvus/dict/lindera".to_string(), vec![]);
        assert!(result.is_ok(), "err: {}", result.err().unwrap())
    }

    #[test]
    fn test_fetch_cc_cedict() {
        use lindera_dictionary;
        use std::env;
        use tokio::runtime::Runtime;

        let _env = env_logger::Env::default().filter_or("MY_LOG_LEVEL", "info");
        env_logger::init_from_env(_env);

        env::set_var("LINDERA_CACHE", "/logs/lindera");
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(
    lindera_dictionary::assets::fetch(
                lindera_dictionary::assets::FetchParams{
                    file_name: "CC-CEDICT-MeCab-0.1.0-20200409.tar.gz",
                    input_dir: "CC-CEDICT-MeCab-0.1.0-20200409",
                    output_dir: "lindera-cc-cedict",
                    dummy_input: "测试,0,0,-1131,*,*,*,*,ce4 shi4,測試,测试,to test (machinery etc)/to test (students)/test/quiz/exam/beta (software)/\n",
                    download_urls: &[
                        "https://lindera.s3.ap-northeast-1.amazonaws.com/CC-CEDICT-MeCab-0.1.0-20200409.tar.gz",
                        "https://lindera.dev/CC-CEDICT-MeCab-0.1.0-20200409.tar.gz",
                    ],
                    md5_hash: "aba9748b70f37feede97b70c5d37f8a0",
                },
            lindera_dictionary::dictionary_builder::cc_cedict::CcCedictBuilder::new(),
            )
        );
        assert!(result.is_ok(), "err: {}", result.err().unwrap())
    }
}
