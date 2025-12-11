// resource options
use super::common::*;
use super::runtime_option::get_options;
use crate::error::{Result, TantivyBindingError};
use log::warn;
use serde_json as json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub struct ResourceInfo {
    storage_name: String,
    resource_map: HashMap<String, i64>,
}

impl ResourceInfo {
    pub fn new() -> Self {
        Self {
            storage_name: "default".to_string(),
            resource_map: HashMap::new(),
        }
    }

    pub fn from_json(value: &json::Value) -> Result<Self> {
        let mut resource_map = HashMap::new();
        let m = value.as_object().ok_or(TantivyBindingError::InternalError(
            "extra info should be a json map".to_string(),
        ))?;

        if let Some(v) = m.get(RESOURCE_MAP_KEY) {
            let kv = v.as_object().ok_or(TantivyBindingError::InternalError(
                "file resource map should be a json map".to_string(),
            ))?;
            for (key, value) in kv {
                let url = value.as_i64().ok_or(TantivyBindingError::InternalError(
                    "file resource id shoud be integer".to_string(),
                ))?;
                resource_map.insert(key.to_string(), url);
            }
        }

        let storage_name = m
            .get(RESOURCE_STORAGE_NAME_KEY)
            .ok_or(TantivyBindingError::InternalError(
                "storage_name config not init success".to_string(),
            ))?
            .as_str()
            .ok_or("storage_name must set as string")?
            .to_string();

        Ok(Self {
            storage_name: storage_name,
            resource_map,
        })
    }

    pub fn get_resource_id(&self, resource_name: &str) -> Option<i64> {
        self.resource_map.get(resource_name).cloned()
    }
}

impl FileResourcePathBuilder for ResourceInfo {
    fn get_resource_file_path(
        &self,
        resource_name: &str,
        file_name: &str,
    ) -> Result<(i64, PathBuf)> {
        let resource_id =
            self.get_resource_id(resource_name)
                .ok_or(TantivyBindingError::InternalError(format!(
                    "file resource: {} not found in local resource list",
                    resource_name
                )))?;

        let base_value =
            get_options(RESOURCE_PATH_KEY).ok_or(TantivyBindingError::InternalError(
                "local_resource_path config not init success".to_string(),
            ))?;

        let base = base_value
            .as_str()
            .ok_or("local_resource_path must set as string")?;

        return Ok((
            resource_id,
            PathBuf::new()
                .join(base)
                .join(self.storage_name.as_str())
                .join(resource_id.to_string())
                .join(file_name),
        ));
    }
}

pub trait FileResourcePathBuilder {
    fn get_resource_file_path(
        &self,
        resource_name: &str,
        file_name: &str,
    ) -> Result<(i64, PathBuf)>;
}

pub struct FileResourcePathHelper {
    builder: Arc<dyn FileResourcePathBuilder>,
    ids: Vec<i64>,
}

impl FileResourcePathHelper {
    pub fn new(builder: Arc<dyn FileResourcePathBuilder>) -> Self {
        Self {
            builder,
            ids: Vec::new(),
        }
    }

    pub fn from_json(value: &json::Value) -> Result<Self> {
        let info = ResourceInfo::from_json(value)?;
        let builder: Arc<dyn FileResourcePathBuilder> = Arc::new(info);
        Ok(Self {
            builder,
            ids: Vec::new(),
        })
    }

    pub fn get_resource_file_path(
        &mut self,
        resource_name: &str,
        file_name: &str,
    ) -> Result<PathBuf> {
        let (resource_id, path) = self
            .builder
            .get_resource_file_path(resource_name, file_name)?;
        self.ids.push(resource_id);
        warn!("test-- path: {:?}", path);
        Ok(path)
    }

    pub fn get_resource_ids(&self) -> Vec<i64> {
        self.ids.clone()
    }
}
