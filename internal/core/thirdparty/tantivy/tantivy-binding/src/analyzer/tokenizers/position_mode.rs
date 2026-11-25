use serde_json::Value;

use crate::error::{Result, TantivyBindingError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PositionMode {
    Char,
    Token,
}

impl PositionMode {
    pub(super) fn from_json(value: Option<&Value>) -> Result<Self> {
        match value {
            None => Ok(Self::Char),
            Some(value) => match value.as_str() {
                Some("char") => Ok(Self::Char),
                Some("token") => Ok(Self::Token),
                _ => Err(TantivyBindingError::InvalidArgument(
                    "tokenizer position_mode must be \"char\" or \"token\"".to_string(),
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use tantivy::tokenizer::{TextAnalyzer, Token, TokenStream};

    use crate::analyzer::{create_analyzer, validate_analyzer};
    use crate::error::TantivyBindingError;

    fn collect(analyzer: &mut TextAnalyzer, input: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        analyzer
            .token_stream(input)
            .process(&mut |token| tokens.push(token.clone()));
        tokens
    }

    fn analyzer(params: &Value) -> TextAnalyzer {
        create_analyzer(&params.to_string(), "").unwrap()
    }

    #[test]
    fn test_position_mode_preserves_tokens_and_defaults() {
        for tokenizer in [
            json!({"type": "icu"}),
            json!({"type": "thai"}),
            json!({"type": "jieba"}),
            json!({"type": "jieba", "mode": "exact"}),
        ] {
            let mut params = json!({"tokenizer": tokenizer});
            let mut legacy = analyzer(&params);
            params["tokenizer"]["position_mode"] = json!("char");
            let mut chars = analyzer(&params);
            let supports_token = tokenizer["type"] != "jieba" || tokenizer["mode"] == "exact";
            params["tokenizer"]["position_mode"] = json!("token");
            let mut tokens = supports_token.then(|| analyzer(&params));
            for input in [
                "Dubai, skincare!",
                "南京市长江大桥",
                "  สวัสดี,  ครับ!",
                "café e\u{301} 👩🏽‍💻 🇹🇭",
                " \t!?",
                "",
            ] {
                let expected = collect(&mut legacy, input);
                assert_eq!(collect(&mut chars, input), expected, "{tokenizer}");
                if let Some(tokens) = &mut tokens {
                    let actual = collect(tokens, input);
                    assert_eq!(actual.len(), expected.len(), "{tokenizer}: {input}");
                    for (position, (actual, mut expected)) in
                        actual.into_iter().zip(expected).enumerate()
                    {
                        expected.position = position;
                        expected.position_length = 1;
                        assert_eq!(actual, expected, "{tokenizer}: {input}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_position_mode_icu_keeps_separators() {
        let mut chars = analyzer(&json!({"tokenizer": "icu"}));
        let mut tokens = analyzer(&json!({"tokenizer": {
            "type": "icu", "position_mode": "token"
        }}));
        let input = "Dubai, skincare!";
        let char_tokens = collect(&mut chars, input);
        assert_eq!(
            char_tokens.iter().map(|t| t.position).collect::<Vec<_>>(),
            [0, 5, 6, 7, 15]
        );
        let token_tokens = collect(&mut tokens, input);
        assert_eq!(
            token_tokens
                .iter()
                .map(|t| t.text.as_str())
                .collect::<Vec<_>>(),
            ["Dubai", ",", " ", "skincare", "!"]
        );
        assert_eq!(
            token_tokens.iter().map(|t| t.position).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn test_position_mode_clone_and_stream_reset() {
        for name in ["icu", "thai", "jieba"] {
            let mut original = analyzer(&json!({"tokenizer": {
                "type": name, "position_mode": "token", "mode": "exact"
            }}));
            let mut stream = original.token_stream("hello world");
            assert!(stream.advance());
            drop(stream);
            let mut cloned = original.clone();
            let expected = collect(&mut original, "hello world");
            assert_eq!(expected[0].position, 0);
            assert_eq!(collect(&mut cloned, "hello world"), expected);
            assert_eq!(collect(&mut original, "hello world"), expected);
        }
    }

    #[test]
    fn test_position_mode_before_filters() {
        let params = json!({
            "tokenizer": {"type": "icu", "position_mode": "token"},
            "filter": [{"type": "regex", "expr": "^[a-z]+$"}]
        });
        let tokens = collect(&mut analyzer(&params), "a, b");
        assert_eq!(
            tokens
                .iter()
                .map(|t| (t.text.as_str(), t.position))
                .collect::<Vec<_>>(),
            [("a", 0), ("b", 3)]
        );
        let params = json!({
            "tokenizer": {"type": "thai", "position_mode": "token"},
            "filter": [{"type": "stop", "stop_words": ["and"]}]
        });
        let tokens = collect(&mut analyzer(&params), "red and blue");
        assert_eq!(
            tokens
                .iter()
                .map(|t| (t.text.as_str(), t.position))
                .collect::<Vec<_>>(),
            [("red", 0), ("blue", 2)]
        );
    }

    #[test]
    fn test_position_mode_nested_analyzer_and_char_filter() {
        let inner = json!({
            "tokenizer": {"type": "icu", "position_mode": "token"},
            "char_filter": [{"type": "mapping", "mappings": ["&=>,and,"]}]
        });
        let expected = collect(&mut analyzer(&inner), "hello&world");
        let nested = json!({"tokenizer": {
            "type": "language_identifier",
            "analyzers": {"default": inner}
        }});
        assert_eq!(collect(&mut analyzer(&nested), "hello&world"), expected);
        assert_eq!(expected.last().unwrap().position, 4);
        assert_eq!(expected.last().unwrap().offset_from, 6);
    }

    #[test]
    fn test_position_mode_validation() {
        for name in ["icu", "thai", "jieba"] {
            for value in [
                json!(null),
                json!(0),
                json!(true),
                json!([]),
                json!({}),
                json!("word"),
            ] {
                let params =
                    json!({"tokenizer": {"type": name, "position_mode": value}}).to_string();
                assert!(
                    matches!(
                        create_analyzer(&params, ""),
                        Err(TantivyBindingError::InvalidArgument(_))
                    ),
                    "{params}"
                );
                assert!(
                    matches!(
                        validate_analyzer(&params, ""),
                        Err(TantivyBindingError::InvalidArgument(_))
                    ),
                    "{params}"
                );
            }
        }
        for mode in [None, Some("search")] {
            let mut tokenizer = json!({"type": "jieba", "position_mode": "token"});
            if let Some(mode) = mode {
                tokenizer["mode"] = json!(mode);
            }
            let params = json!({"tokenizer": tokenizer}).to_string();
            for result in [
                create_analyzer(&params, "").map(|_| ()),
                validate_analyzer(&params, "").map(|_| ()),
            ] {
                let err = result.err().expect("search + token must be rejected");
                assert!(matches!(err, TantivyBindingError::InvalidArgument(_)));
                assert!(err.to_string().contains("requires mode \"exact\""));
            }
        }
    }
}
