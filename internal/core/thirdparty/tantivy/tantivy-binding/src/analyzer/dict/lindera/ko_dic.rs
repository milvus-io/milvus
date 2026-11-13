use super::fetch;
use crate::error::Result;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl",
    feature = "lindera-ko-dic"
))]
use crate::error::TantivyBindingError;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use lindera_dictionary::dictionary_builder::ko_dic::KoDicBuilder;

#[cfg(feature = "lindera-ko-dic")]
use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};

#[cfg(all(
    any(
        feature = "analyzer-remote-tls",
        feature = "analyzer-remote-tls-openssl"
    ),
    not(feature = "lindera-ko-dic")
))]
fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::fetch(params, KoDicBuilder::new()).map_err(|e| {
        TantivyBindingError::InternalError(format!(
            "fetch ko-dic failed with error: {}",
            e.to_string()
        ))
    })
}

// Online download is compiled out; only a dictionary already built under
// `build_dir` can be used.
#[cfg(all(
    not(any(
        feature = "analyzer-remote-tls",
        feature = "analyzer-remote-tls-openssl"
    )),
    not(feature = "lindera-ko-dic")
))]
fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::ensure_prebuilt(params, "ko-dic")
}

#[cfg(not(feature = "lindera-ko-dic"))]
pub fn load_ko_dic(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    let mut params = fetch::FetchParams {
        lindera_dir: build_dir.to_string(),
        file_name: "mecab-ko-dic-2.1.1-20180720.tar.gz".to_string(),
        input_dir: "mecab-ko-dic-2.1.1-20180720".to_string(),
        output_dir: "lindera-ko-dic".to_string(),
        dummy_input: "테스트,1785,3543,4721,NNG,행위,F,테스트,*,*,*,*\n".to_string(),
        download_urls: vec![
            "https://lindera.s3.ap-northeast-1.amazonaws.com/mecab-ko-dic-2.1.1-20180720.tar.gz"
                .to_string(),
            "https://Lindera.dev/mecab-ko-dic-2.1.1-20180720.tar.gz".to_string(),
        ],
        md5_hash: "b996764e91c96bc89dc32ea208514a96".to_string(),
        sha256_hash: "702ced21c6167e9d9aebc674ab5ee54af58d4443975f2940d37d0567c020591a".to_string(),
    };
    if download_url.len() > 0 {
        params.download_urls = download_url
    }

    download(&params)?;
    fetch::load(&params)
}

#[cfg(feature = "lindera-ko-dic")]
pub fn load_ko_dic(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    load_dictionary_from_kind(DictionaryKind::KoDic).map_err(|_| {
        TantivyBindingError::InvalidArgument(format!(
            "lindera build in ko-dic dictionary not found"
        ))
    })
}

#[cfg(all(
    test,
    any(
        feature = "lindera-ko-dic",
        feature = "analyzer-remote-tls",
        feature = "analyzer-remote-tls-openssl"
    )
))]
mod tests {
    use super::load_ko_dic;
    use env_logger;

    #[test]
    fn test_load_ko_dic() {
        let _env = env_logger::Env::default().filter_or("MY_LOG_LEVEL", "info");
        env_logger::init_from_env(_env);

        let result = load_ko_dic("/var/lib/milvus/dict/lindera".to_string(), vec![]);
        assert!(result.is_ok(), "err: {}", result.err().unwrap())
    }
}
