use super::fetch;
use crate::error::{Result, TantivyBindingError};
#[cfg(feature = "lindera-ipadic-neologd")]
use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};
use lindera_dictionary::dictionary_builder::ipadic_neologd::IpadicNeologdBuilder;

#[cfg(not(feature = "lindera-ipadic-neologd"))]
fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::fetch(params, IpadicNeologdBuilder::new()).map_err(|e| {
        TantivyBindingError::InternalError(format!(
            "fetch ipadic-neologd failed with error: {}",
            e.to_string()
        ))
    })
}

#[cfg(not(feature = "lindera-ipadic-neologd"))]
pub fn load_ipadic_neologd(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    let mut params = fetch::FetchParams {
        lindera_dir: build_dir.to_string(),
        file_name: "mecab-ipadic-neologd-0.0.7-20200820.tar.gz".to_string(),
        input_dir: "mecab-ipadic-neologd-0.0.7-20200820".to_string(),
        output_dir: "lindera-ipadic-neologd".to_string(),
        dummy_input: "テスト,1288,1288,-1000,名詞,固有名詞,一般,*,*,*,*,*,*\n".to_string(),
        download_urls: vec![
            "https://lindera.s3.ap-northeast-1.amazonaws.com/mecab-ipadic-neologd-0.0.7-20200820.tar.gz".to_string(),
            "https://lindera.dev/mecab-ipadic-neologd-0.0.7-20200820.tar.gz".to_string(),
        ],
        md5_hash: "3561f0e76980a842dc828b460a8cae96".to_string(),
    };
    if download_url.len() > 0 {
        params.download_urls = download_url
    }

    download(&params)?;
    fetch::load(&params)
}

#[cfg(feature = "lindera-ipadic-neologd")]
pub fn load_ipadic_neologd(
    build_dir: String,
    download_url: Vec<String>,
) -> Result<lindera::dictionary::Dictionary> {
    load_dictionary_from_kind(DictionaryKind::IPADICNEologd).map_err(|_| {
        TantivyBindingError::InvalidArgument(format!(
            "lindera build in ipadic-neologd dictionary not found"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::load_ipadic_neologd;
    use env_logger;

    #[test]
    fn test_load_ipadic() {
        let _env = env_logger::Env::default().filter_or("MY_LOG_LEVEL", "info");
        env_logger::init_from_env(_env);

        let result = load_ipadic_neologd("/var/lib/milvus/dict/lindera".to_string(), vec![]);
        assert!(result.is_ok(), "err: {}", result.err().unwrap())
    }
}
