use super::common::*;
use super::resource_info::{FileResourcePathBuilder, FileResourcePathHelper, ResourceInfo};
use crate::error::{Result, TantivyBindingError};
use once_cell::sync::Lazy;
use serde_json as json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

static GLOBAL_OPTIONS: Lazy<Arc<RuntimeOption>> = Lazy::new(|| Arc::new(RuntimeOption::new()));

pub fn set_options(params: &String) -> Result<()> {
    GLOBAL_OPTIONS.set_json(params)
}

pub fn get_options(key: &str) -> Option<json::Value> {
    GLOBAL_OPTIONS.get(key)
}

pub fn get_lindera_download_url(kind: &str) -> Option<Vec<String>> {
    GLOBAL_OPTIONS.get_lindera_download_urls(kind)
}

pub fn get_global_file_resource_helper() -> FileResourcePathHelper {
    FileResourcePathHelper::new(GLOBAL_OPTIONS.clone())
}

// analyzer options
struct RuntimeOption {
    inner: RwLock<RuntimeOptionInner>,
}

impl RuntimeOption {
    fn new() -> Self {
        return RuntimeOption {
            inner: RwLock::new(RuntimeOptionInner::new()),
        };
    }

    fn set_json(&self, json_params: &String) -> Result<()> {
        let mut w = self.inner.write().unwrap();
        w.set_json(json_params)
    }

    fn get(&self, key: &str) -> Option<json::Value> {
        let r = self.inner.read().unwrap();
        r.params.get(key).map(|v| v.clone())
    }

    fn get_lindera_download_urls(&self, kind: &str) -> Option<Vec<String>> {
        let r = self.inner.read().unwrap();
        r.lindera_download_urls.get(kind).map(|v| v.clone())
    }
}

// file resource
impl FileResourcePathBuilder for RuntimeOption {
    fn get_resource_file_path(
        &self,
        resource_name: &str,
        file_name: &str,
    ) -> Result<(i64, PathBuf)> {
        let r = self.inner.read().unwrap();
        return r
            .resource_info
            .get_resource_file_path(resource_name, file_name);
    }
}

struct RuntimeOptionInner {
    params: HashMap<String, json::Value>,
    resource_info: ResourceInfo, // resource name -> resource id
    lindera_download_urls: HashMap<String, Vec<String>>, // dict name -> url
}

impl RuntimeOptionInner {
    fn new() -> Self {
        RuntimeOptionInner {
            params: HashMap::new(),
            resource_info: ResourceInfo::new(),
            lindera_download_urls: HashMap::new(),
        }
    }

    fn set_json(&mut self, json_params: &String) -> Result<()> {
        let v = json::from_str::<json::Value>(json_params)
            .map_err(|e| TantivyBindingError::JsonError(e))?;

        let m = v.as_object().ok_or(TantivyBindingError::InternalError(
            "analyzer params should be json map".to_string(),
        ))?;

        for (key, value) in m.to_owned() {
            self.set(key, value)?;
        }

        return Ok(());
    }

    fn set(&mut self, key: String, value: json::Value) -> Result<()> {
        // cache linera download map
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

        if key == RESOURCE_MAP_KEY {
            self.resource_info = ResourceInfo::from_global_json(&value)?;
            return Ok(());
        }

        self.params.insert(key, value);
        return Ok(());
    }
}
