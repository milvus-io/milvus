use std::error::Error;
use std::path::Path;

use lindera::dictionary::Dictionary;
use lindera_dictionary::dictionary::character_definition::CharacterDefinition;
use lindera_dictionary::dictionary::connection_cost_matrix::ConnectionCostMatrix;
use lindera_dictionary::dictionary::prefix_dictionary::PrefixDictionary;
use lindera_dictionary::dictionary::unknown_dictionary::UnknownDictionary;
use log::{error, info, warn};
use md5::Context;
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use reqwest::Client;
use tokio::time::sleep;
use tokio::time::Duration;

use serde_json as json;
use std::fs;
use std::path::PathBuf;

use super::common;
use crate::error::TantivyBindingError;
use lindera_dictionary::dictionary_builder::DictionaryBuilder;

const MAX_ROUND: usize = 3;

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

    /// MD5 hash of the file
    pub md5_hash: String,
}

#[cfg(not(target_os = "windows"))]
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

#[cfg(target_os = "windows")]
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

async fn download_with_retry(
    client: &Client,
    download_urls: Vec<&str>,
    max_rounds: usize,
    expected_md5: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    if download_urls.is_empty() {
        return Err("No download URLs provided".into());
    }

    for _ in 0..max_rounds {
        let mut urls = download_urls.clone();

        let mut rng = SmallRng::seed_from_u64(0);
        urls.shuffle(&mut rng);

        for url in urls {
            match client.get(url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let content = resp
                        .bytes()
                        .await
                        .map_err(|e| TantivyBindingError::InternalError(e.to_string()))?;

                    // Calculate MD5 hash
                    let mut context = Context::new();
                    context.consume(&content);
                    let actual_md5 = format!("{:x}", context.compute());

                    if actual_md5 == expected_md5 {
                        return Ok(content.to_vec());
                    } else {
                        warn!(
                            "MD5 mismatch from {}! Expected {}, got {}",
                            url, expected_md5, actual_md5
                        );
                        // continue to next url
                    }
                }
                Ok(resp) => {
                    warn!("HTTP download failed from {}: HTTP {}", url, resp.status());
                    // continue to next url
                }
                Err(e) => {
                    warn!("Request error from {}: {}", url, e);
                    // continue to next url
                }
            }
        }

        sleep(Duration::from_secs(1)).await;
    }

    error!("All {} attempts failed", max_rounds);
    Err("Failed to download a valid file from all sources".into())
}

/// Fetch the necessary assets and then build the dictionary using `builder`
pub async fn fetch(
    params: &FetchParams,
    builder: impl DictionaryBuilder,
) -> Result<(), Box<dyn Error>> {
    use std::env;
    use std::fs::{rename, File};
    use std::io::{self, Cursor, Read, Write};
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    use flate2::read::GzDecoder;
    use tar::Archive;

    let start = Instant::now();
    info!(
        "start fetch lindera dictionary name: {}\n",
        params.file_name.as_str()
    );
    let build_dir = PathBuf::from(params.lindera_dir.as_str());
    std::fs::create_dir_all(&build_dir)?;

    let input_dir = build_dir.join(params.input_dir.as_str());
    let output_dir = build_dir.join(params.output_dir.as_str());

    // Fast path where the data is already in cache
    if output_dir.is_dir() {
        return Ok(());
    }

    // Source file path for build package
    let source_path_for_build = &build_dir.join(params.file_name.as_str());

    // Download source file to build directory
    let tmp_path = Path::new(&build_dir).join(params.file_name.to_owned() + ".download");

    // Download a tarball
    let client = Client::builder()
        .user_agent(format!("Lindera/{}", env!("CARGO_PKG_VERSION")))
        .build()?;

    let mut dest = File::create(tmp_path.as_path())?;
    let content = download_with_retry(
        &client,
        params.download_urls.iter().map(|s| s.as_str()).collect(),
        MAX_ROUND,
        params.md5_hash.as_str(),
    )
    .await?;

    io::copy(&mut Cursor::new(content.as_slice()), &mut dest)?;
    dest.flush()?;

    rename(tmp_path.clone(), source_path_for_build)?;

    // Decompress a tar.gz file
    let tmp_extract_path = Path::new(&build_dir).join(format!("tmp-archive-{}", params.input_dir));
    let tmp_extracted_path = tmp_extract_path.join(params.input_dir.as_str());
    let _ = std::fs::remove_dir_all(&tmp_extract_path);
    std::fs::create_dir_all(&tmp_extract_path)?;

    let mut tar_gz = File::open(source_path_for_build)?;
    let mut buffer = Vec::new();
    tar_gz.read_to_end(&mut buffer)?;
    let cursor = Cursor::new(buffer);
    let decoder = GzDecoder::new(cursor);
    let mut archive = Archive::new(decoder);
    archive.unpack(&tmp_extract_path)?;

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
        rename(tmp_extracted_path, &input_dir)?;
    }

    let _ = std::fs::remove_dir_all(&tmp_extract_path);
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
        rename(tmp_path, &output_dir)?;
    }

    let _ = std::fs::remove_dir_all(&input_dir);

    info!(
        "finish fetch lindera dictionary name: {} duration: {} ms\n",
        params.file_name.as_str(),
        start.elapsed().as_millis()
    );
    Ok(())
}

pub fn load(params: &FetchParams) -> Result<lindera::dictionary::Dictionary, TantivyBindingError> {
    let dict_dir = PathBuf::from(params.lindera_dir.clone()).join(params.output_dir.clone());
    let da_data = fs::read(dict_dir.join(common::DA_DATA))?;
    let vals_data = fs::read(dict_dir.join(common::VALS_DATA))?;
    let words_idx_data = fs::read(dict_dir.join(common::WORDS_IDX_DATA))?;
    let words_data = fs::read(dict_dir.join(common::WORDS_DATA))?;
    let connection_data = fs::read(dict_dir.join(common::CONNECTION_DATA))?;
    let char_definition_data = fs::read(dict_dir.join(common::CHAR_DEFINITION_DATA))?;
    let unkonwn_data = fs::read(dict_dir.join(common::UNKNOWN_DATA))?;

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
        unknown_dictionary: UnknownDictionary::load(unkonwn_data.as_slice()).map_err(|e| {
            TantivyBindingError::InternalError(format!(
                "lindera load unknown dictionary failed, err:{}",
                e
            ))
        })?,
    };
    Ok(dict)
}
