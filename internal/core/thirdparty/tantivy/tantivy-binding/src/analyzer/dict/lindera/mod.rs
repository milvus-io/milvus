mod cc_cedict;
mod common;
mod fetch;
mod ipadic;
mod ipadic_neologd;
mod ko_dic;
mod unidic;

use crate::error::Result;
use lindera::dictionary::{Dictionary, DictionaryKind};
use once_cell::sync::{Lazy, OnceCell};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

type DictionaryKey = (String, Option<PathBuf>);
type DictionaryCell = Arc<OnceCell<Arc<Dictionary>>>;

// Successful dictionaries remain resident until process exit. Replacing files
// or retargeting a previously loaded path requires a restart. Download URLs are
// mirrors, not dictionary identity; existing on-disk dictionaries also ignore them.
static DICTIONARIES: Lazy<Mutex<HashMap<DictionaryKey, DictionaryCell>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static LOADED_PATHS: Lazy<Mutex<HashMap<DictionaryKey, Arc<Dictionary>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn dictionary_key(kind: &DictionaryKind, build_dir: &str) -> Result<DictionaryKey> {
    let embedded = match kind {
        DictionaryKind::IPADIC => cfg!(feature = "lindera-ipadic"),
        DictionaryKind::CcCedict => cfg!(feature = "lindera-cc-cedict"),
        DictionaryKind::KoDic => cfg!(feature = "lindera-ko-dic"),
        DictionaryKind::IPADICNEologd => cfg!(feature = "lindera-ipadic-neologd"),
        DictionaryKind::UniDic => cfg!(feature = "lindera-unidic"),
    };
    let path = if embedded {
        None
    } else {
        let path = PathBuf::from(build_dir);
        Some(if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        })
    };
    Ok((kind.as_str().to_string(), path))
}

#[cfg(all(test, not(feature = "lindera-ipadic")))]
mod tests {
    use super::*;
    use crate::error::TantivyBindingError;
    use lindera_dictionary::dictionary_builder::{ipadic::IpadicBuilder, DictionaryBuilder};
    use std::fs;
    use std::sync::Barrier;

    fn fixture(root: &std::path::Path) {
        let input = root.join("input");
        fs::create_dir_all(&input).unwrap();
        fs::write(input.join("char.def"), "DEFAULT 0 1 0\n").unwrap();
        fs::write(input.join("unk.def"), "DEFAULT,0,0,0,*,*,*,*,*,*,*\n").unwrap();
        fs::write(input.join("matrix.def"), "1 1\n0 0 0\n").unwrap();
        fs::write(
            input.join("test.csv"),
            "test,0,0,-1000,*,*,*,*,*,*,test,*,*\n",
        )
        .unwrap();
        IpadicBuilder::new()
            .build_dictionary(&input, &root.join("lindera-ipadic"))
            .unwrap();
    }

    #[test]
    fn test_dictionary_cache_concurrent_and_identity() {
        let root = tempfile::tempdir().unwrap();
        fixture(root.path());
        let build_dir = root.path().to_str().unwrap().to_string();
        let barrier = Arc::new(Barrier::new(16));
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let barrier = barrier.clone();
                let build_dir = build_dir.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    load_dictionary_from_kind(&DictionaryKind::IPADIC, build_dir, vec![]).unwrap()
                })
            })
            .collect();
        let dictionaries: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        for dictionary in &dictionaries {
            assert!(Arc::ptr_eq(&dictionaries[0], dictionary));
        }
        let alias =
            load_dictionary_from_kind(&DictionaryKind::IPADIC, format!("{}/.", build_dir), vec![])
                .unwrap();
        assert!(Arc::ptr_eq(&dictionaries[0], &alias));
        let other = tempfile::tempdir().unwrap();
        fixture(other.path());
        let distinct = load_dictionary_from_kind(
            &DictionaryKind::IPADIC,
            other.path().to_str().unwrap().into(),
            vec![],
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&alias, &distinct));
        assert_ne!(
            dictionary_key(&DictionaryKind::IPADIC, &build_dir).unwrap(),
            dictionary_key(&DictionaryKind::KoDic, &build_dir).unwrap()
        );
    }

    #[test]
    fn test_dictionary_cache_survives_directory_removal_and_url_change() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("dictionary");
        fixture(&directory);
        let build_dir = directory.to_str().unwrap().to_string();
        let dictionary = load_dictionary_from_kind(
            &DictionaryKind::IPADIC,
            build_dir.clone(),
            vec!["https://first.invalid/dictionary".into()],
        )
        .unwrap();
        fs::rename(&directory, root.path().join("moved")).unwrap();
        let again = load_dictionary_from_kind(
            &DictionaryKind::IPADIC,
            build_dir,
            vec!["https://second.invalid/dictionary".into()],
        )
        .unwrap();
        assert!(Arc::ptr_eq(&dictionary, &again));
        assert!(!directory.exists());
    }

    #[test]
    fn test_dictionary_cache_relative_path() {
        let current_dir = std::env::current_dir().unwrap();
        let root = tempfile::tempdir_in(&current_dir).unwrap();
        fixture(root.path());
        let relative = root.path().strip_prefix(&current_dir).unwrap();
        assert_eq!(
            dictionary_key(&DictionaryKind::IPADIC, relative.to_str().unwrap())
                .unwrap()
                .1,
            Some(root.path().to_path_buf())
        );
        let dictionary = load_dictionary_from_kind(
            &DictionaryKind::IPADIC,
            relative.to_str().unwrap().into(),
            vec![],
        )
        .unwrap();
        let absolute = root.path().to_str().unwrap().to_string();
        root.close().unwrap();
        let again = load_dictionary_from_kind(&DictionaryKind::IPADIC, absolute, vec![]).unwrap();
        assert!(Arc::ptr_eq(&dictionary, &again));
    }

    #[test]
    #[cfg(unix)]
    fn test_dictionary_cache_pins_loaded_symlink() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        fixture(&first);
        fixture(&second);
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&first, &alias).unwrap();
        let load = |path: &std::path::Path| {
            load_dictionary_from_kind(
                &DictionaryKind::IPADIC,
                path.to_str().unwrap().into(),
                vec![],
            )
            .unwrap()
        };
        let dictionary = load(&alias);
        assert!(Arc::ptr_eq(&dictionary, &load(&first)));
        fs::remove_file(&alias).unwrap();
        std::os::unix::fs::symlink(&second, &alias).unwrap();
        assert!(Arc::ptr_eq(&dictionary, &load(&alias)));
        assert!(!Arc::ptr_eq(&dictionary, &load(&second)));
        let nested = first.join("nested");
        fs::create_dir(&nested).unwrap();
        let nested_alias = root.path().join("nested_alias");
        std::os::unix::fs::symlink(&nested, &nested_alias).unwrap();
        assert!(Arc::ptr_eq(&dictionary, &load(&nested_alias.join(".."))));
    }

    #[test]
    fn test_dictionary_cache_failed_load_retries() {
        let root = tempfile::tempdir().unwrap();
        fixture(root.path());
        let build_dir = root.path().to_str().unwrap().to_string();
        let file = root.path().join("lindera-ipadic").join(common::DA_DATA);
        let contents = fs::read(&file).unwrap();
        fs::remove_file(&file).unwrap();
        let result = load_dictionary_from_kind(&DictionaryKind::IPADIC, build_dir.clone(), vec![]);
        assert!(
            matches!(result, Err(TantivyBindingError::IOError(ref error)) if error.kind() == std::io::ErrorKind::NotFound)
        );
        fs::write(file, contents).unwrap();
        let character_file = root
            .path()
            .join("lindera-ipadic")
            .join(common::CHAR_DEFINITION_DATA);
        let character_contents = fs::read(&character_file).unwrap();
        fs::write(&character_file, []).unwrap();
        assert!(matches!(
            load_dictionary_from_kind(&DictionaryKind::IPADIC, build_dir.clone(), vec![]),
            Err(TantivyBindingError::InternalError(_))
        ));
        fs::write(character_file, character_contents).unwrap();
        let dictionary =
            load_dictionary_from_kind(&DictionaryKind::IPADIC, build_dir.clone(), vec![]).unwrap();
        let again = load_dictionary_from_kind(&DictionaryKind::IPADIC, build_dir, vec![]).unwrap();
        assert!(Arc::ptr_eq(&dictionary, &again));
    }
}

pub fn load_dictionary_from_kind(
    kind: &DictionaryKind,
    build_dir: String,
    download_url: Vec<String>,
) -> Result<Arc<Dictionary>> {
    let requested_key = dictionary_key(kind, &build_dir)?;
    if requested_key.1.is_some() {
        if let Some(dictionary) = LOADED_PATHS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&requested_key)
            .cloned()
        {
            return Ok(dictionary);
        }
    }
    let mut key = requested_key.clone();
    if let Some(path) = &key.1 {
        std::fs::create_dir_all(path)?;
        key.1 = Some(std::fs::canonicalize(path)?);
    }
    let build_dir = match &key.1 {
        Some(path) => path
            .to_str()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Lindera dictionary path is not UTF-8",
                )
            })?
            .to_string(),
        None => build_dir,
    };
    let cell = DICTIONARIES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(key)
        .or_default()
        .clone();
    let dictionary = cell
        .get_or_try_init(|| -> Result<Arc<Dictionary>> {
            let dictionary = match kind {
                DictionaryKind::IPADIC => ipadic::load_ipadic(build_dir, download_url),
                DictionaryKind::CcCedict => cc_cedict::load_cc_cedict(build_dir, download_url),
                DictionaryKind::KoDic => ko_dic::load_ko_dic(build_dir, download_url),
                DictionaryKind::IPADICNEologd => {
                    ipadic_neologd::load_ipadic_neologd(build_dir, download_url)
                }
                DictionaryKind::UniDic => unidic::load_unidic(build_dir, download_url),
            }?;
            Ok(Arc::new(dictionary))
        })
        .cloned()?;
    if requested_key.1.is_some() {
        return Ok(LOADED_PATHS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(requested_key)
            .or_insert(dictionary)
            .clone());
    }
    Ok(dictionary)
}
