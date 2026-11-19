use super::fetch;
use crate::error::Result;
#[cfg(any(feature = "analyzer-remote-tls", feature = "lindera-ipadic"))]
use crate::error::TantivyBindingError;
#[cfg(feature = "analyzer-remote-tls")]
use lindera_dictionary::dictionary_builder::ipadic::IpadicBuilder;

#[cfg(feature = "lindera-ipadic")]
use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};

#[cfg(all(feature = "analyzer-remote-tls", not(feature = "lindera-ipadic")))]
fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::fetch(params, IpadicBuilder::new()).map_err(|e| {
        TantivyBindingError::InternalError(format!(
            "fetch ipadic failed with error: {}",
            e.to_string()
        ))
    })
}

// Online download is compiled out; only a dictionary already built under
// `build_dir` can be used.
#[cfg(all(not(feature = "analyzer-remote-tls"), not(feature = "lindera-ipadic")))]
fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::ensure_prebuilt(params, "ipadic")
}

#[cfg(not(feature = "lindera-ipadic"))]
pub fn load_ipadic(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    let mut params = fetch::FetchParams {
        lindera_dir: build_dir.to_string(),
        file_name: "mecab-ipadic-2.7.0-20070801.tar.gz".to_string(),
        input_dir: "mecab-ipadic-2.7.0-20070801".to_string(),
        output_dir: "lindera-ipadic".to_string(),
        dummy_input: "テスト,1288,1288,-1000,名詞,固有名詞,一般,*,*,*,*,*,*\n".to_string(),
        download_urls: vec![
            "https://lindera.s3.ap-northeast-1.amazonaws.com/mecab-ipadic-2.7.0-20070801.tar.gz"
                .to_string(),
            "https://Lindera.dev/mecab-ipadic-2.7.0-20070801.tar.gz".to_string(),
        ],
        md5_hash: "3311c7c71a869ca141e1b8bde0c8666c".to_string(),
    };
    if download_url.len() > 0 {
        params.download_urls = download_url
    }

    download(&params)?;
    fetch::load(&params)
}

#[cfg(feature = "lindera-ipadic")]
pub fn load_ipadic(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    load_dictionary_from_kind(DictionaryKind::IPADIC).map_err(|_| {
        TantivyBindingError::InvalidArgument(format!(
            "lindera build in ipadic dictionary not found"
        ))
    })
}

#[cfg(all(test, any(feature = "lindera-ipadic", feature = "analyzer-remote-tls")))]
mod tests {
    use super::load_ipadic;
    use env_logger;

    #[test]
    fn test_load_ipadic() {
        let _env = env_logger::Env::default().filter_or("MY_LOG_LEVEL", "info");
        env_logger::init_from_env(_env);

        let result = load_ipadic("/var/lib/milvus/dict/lindera".to_string(), vec![]);
        assert!(result.is_ok(), "err: {}", result.err().unwrap())
    }
}

#[cfg(all(
    test,
    not(feature = "analyzer-remote-tls"),
    not(feature = "lindera-ipadic")
))]
mod remote_tls_disabled_tests {
    use super::load_ipadic;

    #[test]
    fn test_load_ipadic_remote_tls_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_ipadic(dir.path().to_str().unwrap().to_string(), vec![]);
        let err = result
            .err()
            .expect("load must fail without a prebuilt dict");
        assert!(
            err.to_string().contains("analyzer-remote-tls"),
            "unexpected error: {err}"
        );
    }
}
