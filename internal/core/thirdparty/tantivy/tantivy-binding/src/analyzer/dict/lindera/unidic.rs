use super::fetch;
use crate::error::{Result, TantivyBindingError};
use lindera_dictionary::dictionary_builder::unidic::UnidicBuilder;

#[cfg(feature = "lindera-unidic")]
use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};

#[cfg(not(feature = "lindera-unidic"))]
fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::fetch(params, UnidicBuilder::new()).map_err(|e| {
        TantivyBindingError::InternalError(format!(
            "fetch unidic failed with error: {}",
            e.to_string()
        ))
    })
}

#[cfg(not(feature = "lindera-unidic"))]
pub fn load_unidic(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    let mut params = fetch::FetchParams {
        lindera_dir: build_dir.to_string(),
        file_name: "unidic-mecab-2.1.2.tar.gz".to_string(),
        input_dir: "unidic-mecab-2.1.2".to_string(),
        output_dir: "lindera-unidic".to_string(),
        dummy_input: "テスト,5131,5131,767,名詞,普通名詞,サ変可能,*,*,*,テスト,テスト-test,テスト,テスト,テスト,テスト,外,*,*,*,*\n".to_string(),
        download_urls: vec![
            "https://lindera.s3.ap-northeast-1.amazonaws.com/unidic-mecab-2.1.2.tar.gz".to_string(),
            "https://Lindera.dev/unidic-mecab-2.1.2.tar.gz".to_string(),
        ],
        md5_hash: "f4502a563e1da44747f61dcd2b269e35".to_string(),
    };
    if download_url.len() > 0 {
        params.download_urls = download_url
    }

    download(&params)?;
    fetch::load(&params)
}

#[cfg(feature = "lindera-unidic")]
pub fn load_ipadic(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    load_dictionary_from_kind(DictionaryKind::UniDic).map_err(|_| {
        TantivyBindingError::InvalidArgument(format!(
            "lindera build in unidic dictionary not found"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::load_unidic;
    use env_logger;

    #[test]
    fn test_load_unidic() {
        let _env = env_logger::Env::default().filter_or("MY_LOG_LEVEL", "info");
        env_logger::init_from_env(_env);

        let result = load_unidic("/var/lib/milvus/dict/lindera".to_string(), vec![]);
        assert!(result.is_ok(), "err: {}", result.err().unwrap())
    }
}
