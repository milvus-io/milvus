use std::fs;
use std::path::PathBuf;

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use std::env;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use std::error::Error;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use std::io::{self, Cursor, Read, Write};
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use std::path::Path;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use std::time::Instant;

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use flate2::read::GzDecoder;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use fs2::FileExt;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use tar::Archive;

use lindera::dictionary::Dictionary;
use lindera_dictionary::dictionary::character_definition::CharacterDefinition;
use lindera_dictionary::dictionary::connection_cost_matrix::ConnectionCostMatrix;
use lindera_dictionary::dictionary::prefix_dictionary::PrefixDictionary;
use lindera_dictionary::dictionary::unknown_dictionary::UnknownDictionary;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use lindera_dictionary::dictionary_builder::DictionaryBuilder;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use log::{error, info, warn};
#[cfg(feature = "analyzer-remote-tls")]
use md5::Context;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
#[cfg(feature = "analyzer-remote-tls")]
use reqwest::Client;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use tokio::runtime::Runtime;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use tokio::time::sleep;
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
use tokio::time::Duration;

use super::common;
use crate::error::TantivyBindingError;

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
const MAX_ROUND: usize = 3;

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
pub struct FileMutexGuard {
    file: Option<fs::File>,
    path: PathBuf,
}

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
impl FileMutexGuard {
    fn build(path: PathBuf) -> io::Result<FileMutexGuard> {
        let flock = fs::File::create(&path)?;
        flock.lock_exclusive()?;
        Ok(FileMutexGuard {
            file: Some(flock),
            path: path,
        })
    }
}

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
impl Drop for FileMutexGuard {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = file.unlock();
            drop(file); // drop file before remove file
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

// Only lindera_dir/output_dir are read when online download is compiled out.
#[cfg_attr(
    not(any(
        feature = "analyzer-remote-tls",
        feature = "analyzer-remote-tls-openssl"
    )),
    allow(dead_code)
)]
pub struct FetchParams {
    pub lindera_dir: String,

    /// Dictionary file name
    pub file_name: String,

    /// MeCab directory
    pub input_dir: String,

    /// Lindera directory
    pub output_dir: String,

    /// Dummy input for docs.rs
    pub dummy_input: String,

    /// URLs from which to fetch the asset
    pub download_urls: Vec<String>,

    /// MD5 hash of the file, checked by the analyzer-remote-tls download
    #[cfg_attr(not(feature = "analyzer-remote-tls"), allow(dead_code))]
    pub md5_hash: String,

    /// SHA-256 hash of the file, checked by the analyzer-remote-tls-openssl
    /// download
    #[cfg_attr(not(feature = "analyzer-remote-tls-openssl"), allow(dead_code))]
    pub sha256_hash: String,
}

#[cfg(all(
    any(
        feature = "analyzer-remote-tls",
        feature = "analyzer-remote-tls-openssl"
    ),
    not(target_os = "windows")
))]
fn empty_directory(dir: &Path) -> Result<(), Box<dyn Error>> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path)?;
            } else {
                std::fs::remove_file(&path)?;
            }
        }
    }
    Ok(())
}

#[cfg(all(
    any(
        feature = "analyzer-remote-tls",
        feature = "analyzer-remote-tls-openssl"
    ),
    target_os = "windows"
))]
fn copy_dir_all(src: &Path, dst: &Path) -> Result<(), Box<dyn Error>> {
    if !dst.exists() {
        std::fs::create_dir(dst)?;
    }

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let entry_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if entry_path.is_dir() {
            copy_dir_all(&entry_path, &dst_path)?;
        } else {
            std::fs::copy(&entry_path, &dst_path)?;
        }
    }
    Ok(())
}

#[cfg(feature = "analyzer-remote-tls")]
type HttpClient = Client;

#[cfg(feature = "analyzer-remote-tls-openssl")]
type HttpClient = super::fetch_openssl::Client;

#[cfg(feature = "analyzer-remote-tls")]
fn http_client() -> Result<HttpClient, Box<dyn Error>> {
    Ok(Client::builder()
        .user_agent(format!("Lindera/{}", env!("CARGO_PKG_VERSION")))
        .build()?)
}

#[cfg(feature = "analyzer-remote-tls-openssl")]
fn http_client() -> Result<HttpClient, Box<dyn Error>> {
    HttpClient::new(format!("Lindera/{}", env!("CARGO_PKG_VERSION")))
}

/// GETs `url` and returns the body, or a log-ready message naming `url`.
#[cfg(feature = "analyzer-remote-tls")]
async fn http_get(client: &HttpClient, url: &str) -> Result<Vec<u8>, String> {
    match client.get(url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.bytes().await {
            Ok(c) => Ok(c.to_vec()),
            Err(e) => Err(format!("Failed to read body from {}: {}", url, e)),
        },
        Ok(resp) => Err(format!(
            "HTTP download failed from {}: HTTP {}",
            url,
            resp.status()
        )),
        Err(e) => Err(format!("Request error from {}: {}", url, e)),
    }
}

#[cfg(feature = "analyzer-remote-tls-openssl")]
async fn http_get(client: &HttpClient, url: &str) -> Result<Vec<u8>, String> {
    client.get(url).await
}

/// Checks the downloaded file against the pinned hash.
#[cfg(feature = "analyzer-remote-tls")]
fn check_digest(content: &[u8], params: &FetchParams, url: &str) -> Result<(), String> {
    let mut context = Context::new();
    context.consume(content);
    let actual_md5 = format!("{:x}", context.compute());
    if actual_md5 == params.md5_hash {
        Ok(())
    } else {
        Err(format!(
            "MD5 mismatch from {}! Expected {}, got {}",
            url, params.md5_hash, actual_md5
        ))
    }
}

#[cfg(feature = "analyzer-remote-tls-openssl")]
fn check_digest(content: &[u8], params: &FetchParams, url: &str) -> Result<(), String> {
    super::fetch_openssl::check_sha256(content, &params.sha256_hash, url)
}

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
async fn download_with_retry(
    client: &HttpClient,
    download_urls: Vec<&str>,
    max_rounds: usize,
    params: &FetchParams,
) -> Result<Vec<u8>, Box<dyn Error>> {
    if download_urls.is_empty() {
        return Err("No download URLs provided".into());
    }

    for _ in 0..max_rounds {
        let mut urls = download_urls.clone();

        let mut rng = SmallRng::seed_from_u64(0);
        urls.shuffle(&mut rng);

        for url in urls {
            match http_get(client, url).await {
                Ok(content) => match check_digest(&content, params, url) {
                    Ok(()) => return Ok(content),
                    // continue to next url
                    Err(msg) => warn!("{}", msg),
                },
                // continue to next url
                Err(msg) => warn!("{}", msg),
            }
        }

        sleep(Duration::from_secs(1)).await;
    }

    error!("All {} attempts failed", max_rounds);
    Err("Failed to download a valid file from all sources".into())
}

#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
pub fn build(
    params: &FetchParams,
    builder: impl DictionaryBuilder,
    build_dir: &PathBuf,
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    tmp_dir: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    info!(
        "start donwload and build lindera dictionary. name: {} to {:?}\n",
        params.file_name.as_str(),
        output_dir,
    );

    // Source file path for build package
    let source_path_for_build = &build_dir.join(params.file_name.as_str());

    // Download source file to build directory
    let tmp_path = Path::new(&build_dir).join(params.file_name.to_owned() + ".download");

    // Download a tarball
    let client = http_client()?;

    let mut dest = fs::File::create(tmp_path.as_path())?;

    let rt = Runtime::new().unwrap();
    let content = rt.block_on(download_with_retry(
        &client,
        params.download_urls.iter().map(|s| s.as_str()).collect(),
        MAX_ROUND,
        params,
    ))?;

    io::copy(&mut Cursor::new(content.as_slice()), &mut dest)?;
    dest.flush()?;

    fs::rename(tmp_path.clone(), source_path_for_build)?;

    let tmp_extracted_path = tmp_dir.join(params.input_dir.as_str());
    let _ = std::fs::remove_dir_all(&tmp_dir);
    std::fs::create_dir_all(&tmp_dir)?;

    let mut tar_gz = fs::File::open(source_path_for_build)?;
    let mut buffer = Vec::new();
    tar_gz.read_to_end(&mut buffer)?;
    let cursor = Cursor::new(buffer);
    let decoder = GzDecoder::new(cursor);
    let mut archive = Archive::new(decoder);
    archive.unpack(&tmp_dir)?;

    #[cfg(target_os = "windows")]
    {
        // Recreate input_dir to avoid conflicts when copying the directory on Windows systems (which do not support overwriting directories).
        // Check if output_dir exists
        if input_dir.exists() {
            // Remove input_dir
            std::fs::remove_dir_all(&input_dir)?;

            // Make input_dir
            std::fs::create_dir_all(&input_dir)?;
        }

        // Copy tmp_path to input_dir
        copy_dir_all(&tmp_extracted_path, &input_dir)?;

        // remove tmp_path
        std::fs::remove_dir_all(&tmp_extracted_path)?;
    }
    #[cfg(not(target_os = "windows"))]
    {
        // Empty the input directory first to avoid conflicts when renaming the directory later on Linux and macOS systems (which do not support overwriting directories).
        empty_directory(&input_dir)?;
        fs::rename(tmp_extracted_path, &input_dir)?;
    }

    drop(dest);
    let _ = std::fs::remove_file(source_path_for_build);

    let tmp_path = build_dir.join(format!("tmp-output-{}", params.output_dir));
    let _ = std::fs::remove_dir_all(&tmp_path);

    builder.build_dictionary(&input_dir, &tmp_path)?;

    #[cfg(target_os = "windows")]
    {
        // Check if output_dir exists
        if output_dir.exists() {
            // Remove output_dir
            std::fs::remove_dir_all(&output_dir)?;

            // Make output_dir
            std::fs::create_dir_all(&output_dir)?;
        }

        // Copy tmp_path to output_dir
        copy_dir_all(&tmp_path, &output_dir)?;

        // remove tmp_path
        std::fs::remove_dir_all(&tmp_path)?;
    }

    #[cfg(not(target_os = "windows"))]
    {
        // Empty the output directory
        empty_directory(&output_dir)?;

        // Rename tmp_path to output_dir
        fs::rename(tmp_path, &output_dir)?;
    }
    info!(
        "finish donwload and build lindera dictionary. name: {} duration: {} ms\n",
        params.file_name.as_str(),
        start.elapsed().as_millis()
    );
    Ok(())
}

/// Fetch the necessary assets and then build the dictionary using `builder`
#[cfg(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
))]
pub fn fetch(params: &FetchParams, builder: impl DictionaryBuilder) -> Result<(), Box<dyn Error>> {
    let build_dir = PathBuf::from(params.lindera_dir.as_str());
    std::fs::create_dir_all(&build_dir)?;

    let input_dir = build_dir.join(params.input_dir.as_str());
    let output_dir = build_dir.join(params.output_dir.as_str());
    let lock_path = build_dir.join(format!("lindera-{}.lock", params.file_name.as_str()));

    // Skip create fs lock if already in cache
    if output_dir.is_dir() {
        return Ok(());
    }

    let _flock_guard = FileMutexGuard::build(lock_path)?;

    // Fast path where the data is already in cache
    if output_dir.is_dir() {
        return Ok(());
    }

    // Decompress a tar.gz file
    let tmp_dir = Path::new(&build_dir).join(format!("tmp-archive-{}", params.input_dir));

    let build_result = build(
        params,
        builder,
        &build_dir,
        &input_dir,
        &output_dir,
        &tmp_dir,
    );
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let _ = std::fs::remove_dir_all(&input_dir);

    build_result
}

/// Used when online download is compiled out: succeed only if the dictionary
/// was already built under `lindera_dir` (e.g. baked into the image).
#[cfg(not(any(
    feature = "analyzer-remote-tls",
    feature = "analyzer-remote-tls-openssl"
)))]
pub fn ensure_prebuilt(params: &FetchParams, dict: &str) -> Result<(), TantivyBindingError> {
    let dict_dir = PathBuf::from(&params.lindera_dir).join(&params.output_dir);
    if dict_dir.is_dir() {
        Ok(())
    } else {
        Err(common::download_disabled_error(dict))
    }
}

pub fn load(params: &FetchParams) -> Result<lindera::dictionary::Dictionary, TantivyBindingError> {
    let dict_dir = PathBuf::from(params.lindera_dir.clone()).join(params.output_dir.clone());
    let da_data = fs::read(dict_dir.join(common::DA_DATA))?;
    let vals_data = fs::read(dict_dir.join(common::VALS_DATA))?;
    let words_idx_data = fs::read(dict_dir.join(common::WORDS_IDX_DATA))?;
    let words_data = fs::read(dict_dir.join(common::WORDS_DATA))?;
    let connection_data = fs::read(dict_dir.join(common::CONNECTION_DATA))?;
    let char_definition_data = fs::read(dict_dir.join(common::CHAR_DEFINITION_DATA))?;
    let unknown_data = fs::read(dict_dir.join(common::UNKNOWN_DATA))?;

    let dict = Dictionary {
        prefix_dictionary: PrefixDictionary::load(
            da_data,
            vals_data,
            words_idx_data,
            words_data,
            true,
        ),
        connection_cost_matrix: ConnectionCostMatrix::load(connection_data),
        character_definition: CharacterDefinition::load(char_definition_data.as_slice()).map_err(
            |e| {
                TantivyBindingError::InternalError(format!(
                    "lindera load character definition failed, err:{}",
                    e
                ))
            },
        )?,
        unknown_dictionary: UnknownDictionary::load(unknown_data.as_slice()).map_err(|e| {
            TantivyBindingError::InternalError(format!(
                "lindera load unknown dictionary failed, err:{}",
                e
            ))
        })?,
    };
    Ok(dict)
}

#[cfg(all(test, feature = "analyzer-remote-tls-openssl"))]
mod openssl_download_tests {
    use super::super::fetch_openssl::tests::{ok, serve, sha256_hex, Pki};
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::sync::atomic::Ordering;

    fn tar_gz(content: &[u8]) -> Vec<u8> {
        let mut tar = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default()));
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "dict/matrix.def", content)
            .unwrap();
        tar.into_inner().unwrap().finish().unwrap()
    }

    fn params(urls: Vec<String>, archive: &[u8]) -> FetchParams {
        FetchParams {
            lindera_dir: String::new(),
            file_name: "dict.tar.gz".into(),
            input_dir: "dict".into(),
            output_dir: "lindera-dict".into(),
            dummy_input: String::new(),
            download_urls: urls,
            md5_hash: "not checked by this feature".into(),
            sha256_hash: sha256_hex(archive),
        }
    }

    async fn run(p: &FetchParams, client: &HttpClient) -> Result<Vec<u8>, String> {
        download_with_retry(
            client,
            p.download_urls.iter().map(|s| s.as_str()).collect(),
            MAX_ROUND,
            p,
        )
        .await
        .map_err(|e| e.to_string())
    }

    #[tokio::test]
    async fn accepts_matching_sha256() {
        let pki = Pki::new();
        let archive = tar_gz(b"1 1\n0 0 0\n");
        let (r, b) = ok(&archive);
        let (port, _) = serve(Some((&pki.server, None)), vec![("/d.tar.gz", r, b)]).await;
        let p = params(vec![format!("https://localhost:{port}/d.tar.gz")], &archive);
        assert_eq!(run(&p, &pki.client()).await.unwrap(), archive);
    }

    #[tokio::test]
    async fn rejects_wrong_sha256_and_tries_next_mirror() {
        let pki = Pki::new();
        let archive = tar_gz(b"1 1\n0 0 0\n");
        let tampered = tar_gz(b"1 1\n0 0 9\n");
        let (good_r, good_b) = ok(&archive);
        let (bad_r, bad_b) = ok(&tampered);
        let (good, good_hits) = serve(
            Some((&pki.server, None)),
            vec![("/d.tar.gz", good_r, good_b)],
        )
        .await;
        let (bad, bad_hits) =
            serve(Some((&pki.server, None)), vec![("/d.tar.gz", bad_r, bad_b)]).await;
        let good_url = format!("https://localhost:{good}/d.tar.gz");
        let bad_url = format!("https://localhost:{bad}/d.tar.gz");
        let client = pki.client();

        // The mirror order is shuffled, so try both orders; in one of them the
        // tampered mirror comes first and must be skipped.
        for urls in [
            vec![bad_url.clone(), good_url.clone()],
            vec![good_url.clone(), bad_url.clone()],
        ] {
            assert_eq!(
                run(&params(urls, &archive), &client).await.unwrap(),
                archive
            );
        }
        assert!(bad_hits.load(Ordering::SeqCst) >= 1);
        assert_eq!(good_hits.load(Ordering::SeqCst), 2);

        // With only the tampered mirror, every round fails.
        let err = run(&params(vec![bad_url], &archive), &client)
            .await
            .unwrap_err();
        assert!(
            err.contains("Failed to download"),
            "unexpected error: {err}"
        );
    }
}
