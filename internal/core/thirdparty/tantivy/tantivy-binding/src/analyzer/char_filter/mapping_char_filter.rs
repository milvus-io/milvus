use std::collections::HashMap;

use serde_json as json;

use super::char_filter::{BoxCharFilter, CharFilter, FilteredText};
use crate::error::{Result, TantivyBindingError};

#[derive(Clone)]
pub(crate) struct MappingCharFilter {
    mappings: HashMap<char, Vec<(String, String)>>,
}

impl MappingCharFilter {
    pub(crate) fn from_json(params: &json::Map<String, json::Value>) -> Result<Self> {
        let mappings = params
            .get("mappings")
            .ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "mapping char_filter must set mappings".to_string(),
                )
            })?
            .as_array()
            .ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "mapping char_filter mappings must be array".to_string(),
                )
            })?;

        let mut parsed = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            let mapping = mapping.as_str().ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "mapping char_filter mapping item must be string".to_string(),
                )
            })?;
            parsed.push(parse_mapping(mapping)?);
        }

        let mut mappings: HashMap<char, Vec<(String, String)>> = HashMap::new();
        for (source, target) in parsed {
            let first_char = source.chars().next().unwrap();
            mappings
                .entry(first_char)
                .or_default()
                .push((source, target));
        }
        for rules in mappings.values_mut() {
            rules.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        }

        Ok(MappingCharFilter { mappings })
    }
}

impl CharFilter for MappingCharFilter {
    fn apply(&self, input: FilteredText) -> FilteredText {
        let mut replacements = Vec::new();
        let mut cursor = 0;

        while cursor < input.text.len() {
            let next = input.text[cursor..].chars().next().unwrap();
            if let Some((source, target)) = self
                .mappings
                .get(&next)
                .into_iter()
                .flatten()
                .find(|(source, _)| input.text[cursor..].starts_with(source))
            {
                replacements.push((cursor, cursor + source.len(), target.clone()));
                cursor += source.len();
                continue;
            }

            cursor += next.len_utf8();
        }

        input.replace_ranges(replacements)
    }

    fn box_clone(&self) -> BoxCharFilter {
        Box::new(self.clone())
    }
}

fn parse_mapping(mapping: &str) -> Result<(String, String)> {
    let separator = find_unescaped_separator(mapping).ok_or_else(|| {
        TantivyBindingError::InvalidArgument(format!(
            "invalid mapping char_filter mapping: {}",
            mapping
        ))
    })?;

    let mut source = &mapping[..separator];
    let mut target = &mapping[(separator + 2)..];
    // Preserve one-sided whitespace so rules can map to or from a space.
    if has_separator_padding(source, target) {
        source = source.trim_end();
        target = target.trim_start();
    }

    let source = unescape_mapping_side(source)?;
    let target = unescape_mapping_side(target)?;
    if source.is_empty() {
        return Err(TantivyBindingError::InvalidArgument(
            "mapping char_filter source must not be empty".to_string(),
        ));
    }

    Ok((source, target))
}

fn find_unescaped_separator(mapping: &str) -> Option<usize> {
    let mut escaped = false;
    for (offset, ch) in mapping.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if mapping[offset..].starts_with("=>") {
            return Some(offset);
        }
    }
    None
}

fn unescape_mapping_side(input: &str) -> Result<String> {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }

        let escaped = chars.next().ok_or_else(|| {
            TantivyBindingError::InvalidArgument(
                "mapping char_filter escape sequence must not end with backslash".to_string(),
            )
        })?;
        match escaped {
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            other => output.push(other),
        }
    }
    Ok(output)
}

fn has_separator_padding(source: &str, target: &str) -> bool {
    matches!(source.chars().next_back(), Some(ch) if ch.is_whitespace())
        && matches!(target.chars().next(), Some(ch) if ch.is_whitespace())
}

#[cfg(test)]
mod tests {
    use serde_json as json;

    use super::MappingCharFilter;
    use crate::analyzer::char_filter::{CharFilter, FilteredText};

    #[test]
    fn test_mapping_char_filter() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["&=>and", "--=>-"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("a&b--c"));

        assert_eq!(output.text, "aandb-c");
        assert_eq!(output.correct_offset(0), 0);
        assert_eq!(output.correct_offset(1), 1);
        assert_eq!(output.correct_offset(4), 2);
        assert_eq!(output.correct_offset(6), 5);
        assert_eq!(output.correct_offset(7), 6);
    }

    #[test]
    fn test_mapping_char_filter_uses_longest_match() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["a=>x", "aa=>y"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("aa"));

        assert_eq!(output.text, "y");
        assert_eq!(output.correct_offset(0), 0);
        assert_eq!(output.correct_offset(1), 2);
    }

    #[test]
    fn test_mapping_char_filter_accepts_es_style_separator_padding() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["& => and", ":) => _happy_"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("a&b :)"));

        assert_eq!(output.text, "aandb _happy_");
    }

    #[test]
    fn test_mapping_char_filter_can_delete() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["-=>"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("中-文"));

        assert_eq!(output.text, "中文");
        assert_eq!(output.correct_offset(0), 0);
        assert_eq!(output.correct_offset(3), 4);
        assert_eq!(output.correct_offset(6), 7);
    }

    #[test]
    fn test_mapping_char_filter_preserves_whitespace() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["-=> ", " =>_"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("a-b c"));

        assert_eq!(output.text, "a b_c");
    }

    #[test]
    fn test_mapping_char_filter_supports_escaped_whitespace_and_separator() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["\\t=>\\ ", "\\=\\>=>arrow"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("\t=>"));

        assert_eq!(output.text, " arrow");
    }

    #[test]
    fn test_mapping_char_filter_rejects_trailing_escape() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["a=>b\\"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();

        assert!(MappingCharFilter::from_json(&params).is_err());
    }
}
