use serde_json as json;
use std::path::PathBuf;

use super::resource_file::check_remote_file_name;
use super::resource_info::FileResourcePathHelper;
use crate::error::{Result, TantivyBindingError};

pub fn get_resource_path(
    helper: &mut FileResourcePathHelper,
    v: &json::Value,
    resource_key: &str,
) -> Result<PathBuf> {
    if !v.is_object() {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "file config of {} must be object",
            resource_key
        )));
    }

    let params = v.as_object().unwrap();

    let file_type = params.get("type").ok_or_else(|| {
        TantivyBindingError::InvalidArgument(format!("file type of {} must be set", resource_key))
    })?;

    if !file_type.is_string() {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "file type of {} must be string",
            resource_key
        )));
    }

    match file_type.as_str().unwrap() {
        "local" => {
            let path = params.get("path").ok_or_else(|| {
                TantivyBindingError::InvalidArgument(format!(
                    "file path of local file `{}` must be set",
                    resource_key
                ))
            })?;

            if !path.is_string() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "file path of local file `{}` must be string",
                    resource_key
                )));
            }

            let path_str = path.as_str().unwrap();
            helper.resolve_local_path(path_str, resource_key)
        }
        "remote" => {
            let resource_name = params
                .get("resource_name")
                .ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!(
                        "resource name of remote file `{}` must be set",
                        resource_key
                    ))
                })?
                .as_str()
                .ok_or(TantivyBindingError::InvalidArgument(format!(
                    "remote file resource name of remote file `{}` must be string",
                    resource_key
                )))?;

            let file_name = params
                .get("file_name")
                .ok_or_else(|| {
                    TantivyBindingError::InvalidArgument(format!(
                        "file name of remote file `{}` must be set",
                        resource_key
                    ))
                })?
                .as_str()
                .ok_or(TantivyBindingError::InvalidArgument(format!(
                    "remote file resource name of {} must be string",
                    resource_key
                )))?;

            let path = helper.get_resource_file_path(resource_name, file_name)?;
            // Keep runtime lookups, which do not check the registered name,
            // inside the resource directory.
            check_remote_file_name(file_name, resource_key)?;
            Ok(path)
        }
        other => Err(TantivyBindingError::InvalidArgument(format!(
            "unsupported file type {} of {}",
            other, resource_key
        ))),
    }
}
