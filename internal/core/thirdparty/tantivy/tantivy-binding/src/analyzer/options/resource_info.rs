// resource options
use super::common::*;
use super::runtime_option::get_options;
use crate::error::{Result, TantivyBindingError};
use serde_json as json;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

pub struct ResourceInfo {
    storage_name: Option<String>,
    resource_map: HashMap<String, i64>,
}

impl ResourceInfo {
    pub fn new() -> Self {
        Self {
            storage_name: None,
            resource_map: HashMap::new(),
        }
    }

    pub fn debug(&self) -> String {
        format!(
            "storage_name: {:?}, resource_map: {:?}",
            self.storage_name, self.resource_map
        )
    }

    pub fn from_global_json(value: &json::Value) -> Result<Self> {
        let mut resource_map = HashMap::new();
        let kv = value
            .as_object()
            .ok_or(TantivyBindingError::InternalError(format!(
                "file resource map should be a json map, but got: {}",
                json::to_string(value).unwrap()
            )))?;
        for (key, value) in kv {
            let url = value
                .as_i64()
                .ok_or(TantivyBindingError::InternalError(format!(
                    "file resource id should be integer, but got: {}",
                    json::to_string(value).unwrap()
                )))?;
            resource_map.insert(key.to_string(), url);
        }
        Ok(Self {
            storage_name: None,
            resource_map,
        })
    }

    pub fn from_json(value: &json::Value) -> Result<Self> {
        let mut resource_map = HashMap::new();
        let m = value
            .as_object()
            .ok_or(TantivyBindingError::InternalError(format!(
                "extra info should be a json map, but got: {}",
                json::to_string(value).unwrap()
            )))?;

        if let Some(v) = m.get(RESOURCE_MAP_KEY) {
            let kv = v
                .as_object()
                .ok_or(TantivyBindingError::InternalError(format!(
                    "file resource map should be a json map, but got: {}",
                    json::to_string(v).unwrap()
                )))?;
            for (key, value) in kv {
                let url = value
                    .as_i64()
                    .ok_or(TantivyBindingError::InternalError(format!(
                        "file resource id should be integer, but got: {}",
                        json::to_string(value).unwrap()
                    )))?;
                resource_map.insert(key.to_string(), url);
            }
        }

        let mut storage_name = None;
        if let Some(v) = m.get(RESOURCE_STORAGE_NAME_KEY) {
            let name = v
                .as_str()
                .ok_or(TantivyBindingError::InternalError(format!(
                    "storage_name must set as string, but got: {}",
                    json::to_string(v).unwrap()
                )))?
                .to_string();
            storage_name = Some(name)
        }

        Ok(Self {
            storage_name,
            resource_map,
        })
    }
}

impl FileResourcePathBuilder for ResourceInfo {
    fn get_resource_file_path(
        &self,
        resource_name: &str,
        file_name: &str,
    ) -> Result<(i64, PathBuf)> {
        let resource_id =
            self.resource_map
                .get(resource_name)
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
            .ok_or(TantivyBindingError::InternalError(
                "local_resource_path must set as string".to_string(),
            ))?;

        if let Some(storage_name) = &self.storage_name {
            return Ok((
                resource_id.clone(),
                PathBuf::new()
                    .join(base)
                    .join(storage_name)
                    .join(resource_id.to_string())
                    .join(file_name),
            ));
        } else {
            return Ok((
                resource_id.clone(),
                PathBuf::new()
                    .join(base)
                    .join(resource_id.to_string())
                    .join(file_name),
            ));
        }
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
    ids: HashSet<i64>,
}

impl FileResourcePathHelper {
    pub fn new(builder: Arc<dyn FileResourcePathBuilder>) -> Self {
        Self {
            builder,
            ids: HashSet::new(),
        }
    }

    pub fn from_json(value: &json::Value) -> Result<Self> {
        let info = ResourceInfo::from_json(value)?;
        let builder: Arc<dyn FileResourcePathBuilder> = Arc::new(info);
        Ok(Self {
            builder,
            ids: HashSet::new(),
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
        self.ids.insert(resource_id);
        Ok(path)
    }

    pub fn get_resource_ids(self) -> Vec<i64> {
        self.ids.into_iter().collect()
    }
}
