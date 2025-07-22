use super::fetch;
use crate::error::{Result, TantivyBindingError};
use lindera_dictionary::dictionary_builder::ko_dic::KoDicBuilder;
use tokio::runtime::Runtime;

#[cfg(feature = "lindera-ko-dic")]
use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};

#[cfg(not(feature = "lindera-ko-dic"))]
async fn download(params: &fetch::FetchParams) -> Result<()> {
    fetch::fetch(params, KoDicBuilder::new())
        .await
        .map_err(|e| {
            TantivyBindingError::InternalError(format!(
                "fetch ko-dic failed with error: {}",
                e.to_string()
            ))
        })
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
    };
    if download_url.len() > 0 {
        params.download_urls = download_url
    }

    let rt = Runtime::new().unwrap();
    rt.block_on(download(&params))?;
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

#[cfg(test)]
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
