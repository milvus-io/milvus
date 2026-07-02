use super::common::*;
use crate::error::{Result, TantivyBindingError};
use once_cell::sync::Lazy;
use serde_json as json;
use std::collections::HashMap;
use std::sync::RwLock;

static GLOBAL_OPTIONS: Lazy<RuntimeOption> = Lazy::new(RuntimeOption::new);

pub fn set_options(params: &String) -> Result<()> {
    GLOBAL_OPTIONS.set_json(params)
}

pub fn get_options(key: &str) -> Option<json::Value> {
    GLOBAL_OPTIONS.get(key)
}

pub fn get_lindera_download_url(kind: &str) -> Option<Vec<String>> {
    GLOBAL_OPTIONS.get_lindera_download_urls(kind)
}

struct RuntimeOption {
    inner: RwLock<RuntimeOptionInner>,
}

impl RuntimeOption {
    fn new() -> Self {
        RuntimeOption {
            inner: RwLock::new(RuntimeOptionInner::new()),
        }
    }

    fn set_json(&self, json_params: &String) -> Result<()> {
        let mut w = self.inner.write().unwrap();
        w.set_json(json_params)
    }

    fn get(&self, key: &str) -> Option<json::Value> {
        let r = self.inner.read().unwrap();
        r.params.get(key).cloned()
    }

    fn get_lindera_download_urls(&self, kind: &str) -> Option<Vec<String>> {
        let r = self.inner.read().unwrap();
        r.lindera_download_urls.get(kind).cloned()
    }
}

struct RuntimeOptionInner {
    params: HashMap<String, json::Value>,
    lindera_download_urls: HashMap<String, Vec<String>>,
}

impl RuntimeOptionInner {
    fn new() -> Self {
        RuntimeOptionInner {
            params: HashMap::new(),
            lindera_download_urls: HashMap::new(),
        }
    }

    fn set_json(&mut self, json_params: &String) -> Result<()> {
        let v =
            json::from_str::<json::Value>(json_params).map_err(TantivyBindingError::JsonError)?;

        let m = v.as_object().ok_or(TantivyBindingError::InternalError(
            "analyzer params should be json map".to_string(),
        ))?;

        for (key, value) in m.to_owned() {
            self.set(key, value)?;
        }
        Ok(())
    }

    fn set(&mut self, key: String, value: json::Value) -> Result<()> {
        if key == LINDERA_DOWNLOAD_KEY {
            self.lindera_download_urls = HashMap::new();

            let m = value.as_object().ok_or(TantivyBindingError::InternalError(
                "lindera download urls should be a json map".to_string(),
            ))?;

            for (key, value) in m {
                let array = value.as_array().ok_or(TantivyBindingError::InternalError(
                    "lindera download urls should be list".to_string(),
                ))?;

                if !array.iter().all(|v| v.is_string()) {
                    return Err(TantivyBindingError::InternalError(
                        "all elements in lindera download urls must be string".to_string(),
                    ));
                }

                let urls = array
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                self.lindera_download_urls.insert(key.to_string(), urls);
            }
            return Ok(());
        }

        self.params.insert(key, value);
        Ok(())
    }
}
