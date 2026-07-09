mod char_filter;
mod mapping_char_filter;

use serde_json as json;

#[cfg(test)]
pub(crate) use self::char_filter::CharFilter;
pub(crate) use self::char_filter::{BoxCharFilter, FilteredText};
use self::mapping_char_filter::MappingCharFilter;
use crate::error::{Result, TantivyBindingError};

pub(crate) fn build_char_filters(params: &json::Value) -> Result<Vec<BoxCharFilter>> {
    let filters = params.as_array().ok_or_else(|| {
        TantivyBindingError::InvalidArgument("char_filter params should be array".to_string())
    })?;

    let mut result = Vec::with_capacity(filters.len());
    for filter in filters {
        let filter = filter.as_object().ok_or_else(|| {
            TantivyBindingError::InvalidArgument("char_filter item should be object".to_string())
        })?;
        result.push(create_char_filter(filter)?);
    }
    Ok(result)
}

fn create_char_filter(params: &json::Map<String, json::Value>) -> Result<BoxCharFilter> {
    let type_ = params
        .get("type")
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument("char_filter type must be set".to_string())
        })?
        .as_str()
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument("char_filter type should be string".to_string())
        })?;

    match type_ {
        "mapping" => MappingCharFilter::from_json(params).map(|filter| Box::new(filter) as _),
        other => Err(TantivyBindingError::InvalidArgument(format!(
            "unsupported char_filter type: {}",
            other
        ))),
    }
}
