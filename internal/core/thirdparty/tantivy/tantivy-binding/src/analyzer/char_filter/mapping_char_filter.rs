use std::collections::{hash_map::Entry, HashMap};

use serde_json as json;

use super::char_filter::{BoxCharFilter, CharFilter, FilteredText};
use crate::error::{Result, TantivyBindingError};

/// Rewrites tokenizer input with greedy longest-match `source => target` rules.
///
/// The filter scans its input once from left to right. Replacement output is
/// not processed again by this filter, but can be processed by a later char
/// filter in the analyzer chain. `FilteredText` composes the replacements with
/// existing offset corrections.
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

        let mut parsed = HashMap::with_capacity(mappings.len());
        for mapping in mappings {
            let mapping = mapping.as_str().ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "mapping char_filter mapping item must be string".to_string(),
                )
            })?;
            let (source, target) = parse_mapping(mapping)?;
            match parsed.entry(source) {
                Entry::Vacant(entry) => {
                    entry.insert(target);
                }
                Entry::Occupied(entry) => {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "mapping char_filter source must be unique: {:?}",
                        entry.key()
                    )));
                }
            }
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
                replacements.push((cursor, cursor + source.len(), target.as_str()));
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
    let separator = mapping.rfind("=>").ok_or_else(|| {
        TantivyBindingError::InvalidArgument(format!(
            "invalid mapping char_filter mapping: {}",
            mapping
        ))
    })?;

    let source = trim_mapping_side(&mapping[..separator]);
    let target = trim_mapping_side(&mapping[(separator + 2)..]);

    let source = unescape_mapping_side(source)?;
    let target = unescape_mapping_side(target)?;
    if source.is_empty() {
        return Err(TantivyBindingError::InvalidArgument(
            "mapping char_filter source must not be empty".to_string(),
        ));
    }

    Ok((source, target))
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
            '\\' => output.push('\\'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            'b' => output.push('\u{0008}'),
            'f' => output.push('\u{000c}'),
            'u' => output.push(parse_unicode_escape(&mut chars)?),
            other => output.push(other),
        }
    }
    Ok(output)
}

// Elasticsearch trims characters up to U+0020 before parsing escapes.
fn trim_mapping_side(input: &str) -> &str {
    input.trim_matches(|ch| ch <= '\u{0020}')
}

fn parse_unicode_escape(chars: &mut std::str::Chars<'_>) -> Result<char> {
    let first = parse_unicode_code_unit(chars)?;
    let code_point = if (0xd800..=0xdbff).contains(&first) {
        if chars.next() != Some('\\') || chars.next() != Some('u') {
            return Err(invalid_unicode_escape());
        }
        let second = parse_unicode_code_unit(chars)?;
        if !(0xdc00..=0xdfff).contains(&second) {
            return Err(invalid_unicode_escape());
        }
        0x10000 + (((first as u32 - 0xd800) << 10) | (second as u32 - 0xdc00))
    } else if (0xdc00..=0xdfff).contains(&first) {
        return Err(invalid_unicode_escape());
    } else {
        first as u32
    };

    char::from_u32(code_point).ok_or_else(invalid_unicode_escape)
}

fn parse_unicode_code_unit(chars: &mut std::str::Chars<'_>) -> Result<u16> {
    let mut value = 0u16;
    for _ in 0..4 {
        let digit = chars
            .next()
            .and_then(|ch| ch.to_digit(16))
            .ok_or_else(invalid_unicode_escape)?;
        value = (value << 4) | digit as u16;
    }
    Ok(value)
}

fn invalid_unicode_escape() -> TantivyBindingError {
    TantivyBindingError::InvalidArgument(
        "mapping char_filter contains an invalid unicode escape".to_string(),
    )
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
        assert_eq!(output.correct_offsets(1, 4), (1, 2));
        assert_eq!(output.correct_offsets(4, 5), (2, 3));
        assert_eq!(output.correct_offsets(5, 6), (3, 5));
        assert_eq!(output.correct_offsets(0, 7), (0, 6));
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
        assert_eq!(output.correct_offsets(0, 1), (0, 2));
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
        assert_eq!(output.correct_offsets(0, 3), (0, 3));
        assert_eq!(output.correct_offsets(3, 6), (4, 7));
        assert_eq!(output.correct_offsets(0, 6), (0, 7));
    }

    #[test]
    fn test_mapping_char_filter_uses_unicode_escape_for_whitespace() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["-=>\\u0020", "\\u0020=>_"]
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
            "mappings": ["\\t=>\\u0020", "\\=\\>=>arrow"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("\t=>"));

        assert_eq!(output.text, " arrow");
    }

    #[test]
    fn test_mapping_char_filter_uses_last_raw_separator() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["a=>b=>c"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("a=>b"));

        assert_eq!(output.text, "c");
    }

    #[test]
    fn test_mapping_char_filter_rejects_backslash_before_raw_separator() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["a=>b\\=>c"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();

        assert!(MappingCharFilter::from_json(&params).is_err());
    }

    #[test]
    fn test_mapping_char_filter_rejects_duplicate_source() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["a=>x", "\\u0061=>y"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();

        assert!(MappingCharFilter::from_json(&params).is_err());
    }

    #[test]
    fn test_mapping_char_filter_trims_each_mapping_side() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["a => b", "-=> "]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("a-"));

        assert_eq!(output.text, "b");
    }

    #[test]
    fn test_mapping_char_filter_supports_unicode_surrogate_pairs() {
        let params = r#"{
            "type": "mapping",
            "mappings": ["\\u4e2d=>zhong", "\\ud83d\\ude00=>smile"]
        }"#;
        let params = json::from_str::<json::Map<String, json::Value>>(params).unwrap();
        let filter = MappingCharFilter::from_json(&params).unwrap();
        let output = filter.apply(FilteredText::new("中😀"));

        assert_eq!(output.text, "zhongsmile");
    }

    #[test]
    fn test_mapping_char_filter_rejects_invalid_unicode_escape() {
        for mapping in ["\\u123=>x", "\\ud83d=>x", "\\ud83d\\u0041=>x"] {
            let params = json::json!({
                "type": "mapping",
                "mappings": [mapping]
            });

            assert!(MappingCharFilter::from_json(params.as_object().unwrap()).is_err());
        }
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
