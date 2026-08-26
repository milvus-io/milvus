use tantivy::tokenizer::TokenStream;
use tantivy_binding::analyzer::create_analyzer;

#[test]
fn test_mapping_char_filter_pipeline() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["-=>\\u0020"]
            }
        ],
        "tokenizer": "standard",
        "filter": ["lowercase"]
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("FOO-BAR");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "foo");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 3);

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "bar");
    assert_eq!(token.offset_from, 4);
    assert_eq!(token.offset_to, 7);

    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_expansion_offsets() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["&=>and"]
            }
        ],
        "tokenizer": "standard",
        "filter": ["lowercase"]
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("A&B");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "aandb");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 3);

    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_accepts_es_style_separator_padding() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["& => and"]
            }
        ],
        "tokenizer": "standard",
        "filter": ["lowercase"]
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("A&B");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "aandb");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 3);

    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_trims_each_mapping_side() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["a => b", "-=> "]
            }
        ],
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("a-");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "b");
    assert_eq!((token.offset_from, token.offset_to), (0, 1));
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_supports_unicode_surrogate_pair_escape() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["\\ud83d\\ude00=>smile"]
            }
        ],
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("😀");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "smile");
    assert_eq!((token.offset_from, token.offset_to), (0, 4));
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_rejects_invalid_unicode_escape() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["\\ud83d=>smile"]
            }
        ],
        "tokenizer": "standard"
    }"#;

    assert!(create_analyzer(params, "").is_err());
}

#[test]
fn test_mapping_char_filter_uses_last_unescaped_separator() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["a=>b=>c"]
            }
        ],
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("a=>b");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "c");
    assert_eq!((token.offset_from, token.offset_to), (0, 4));
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_rejects_duplicate_source() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["a=>x", "\\u0061=>y"]
            }
        ],
        "tokenizer": "standard"
    }"#;

    assert!(create_analyzer(params, "").is_err());
}

#[test]
fn test_mapping_char_filter_preserves_source_spans_when_expanding() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["ab=>x y"]
            }
        ],
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("ab");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "x");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 2);

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "y");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 2);
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_single_character_expansion_uses_full_source_span() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["a=>x y"]
            }
        ],
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("a");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "x");
    assert_eq!((token.offset_from, token.offset_to), (0, 1));

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "y");
    assert_eq!((token.offset_from, token.offset_to), (0, 1));
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_boundary_mode_preserves_character_boundaries() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["ab=>x y"]
            }
        ],
        "char_filter_offset_mode": "boundary",
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("ab");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "x");
    assert_eq!((token.offset_from, token.offset_to), (0, 1));

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "y");
    assert_eq!((token.offset_from, token.offset_to), (1, 2));
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_boundary_mode_returns_utf8_byte_offsets() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["中=>x y"]
            }
        ],
        "char_filter_offset_mode": "boundary",
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("中");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "x");
    assert_eq!((token.offset_from, token.offset_to), (0, 0));

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "y");
    assert_eq!((token.offset_from, token.offset_to), (0, 3));
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_supports_escaped_whitespace() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["\\t=>\\u0020"]
            }
        ],
        "tokenizer": {
            "type": "char_group",
            "delimiters": [" "]
        }
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("foo\tbar");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "foo");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 3);

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "bar");
    assert_eq!(token.offset_from, 4);
    assert_eq!(token.offset_to, 7);
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_chain_composes_offsets() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["-=>\\u0020and\\u0020"]
            },
            {
                "type": "mapping",
                "mappings": ["\\u0020and\\u0020=>x"]
            }
        ],
        "tokenizer": "standard",
        "filter": ["lowercase"]
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("A-B");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "axb");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 3);
    assert!(!stream.advance());
}

#[test]
fn test_mapping_char_filter_deletion_preserves_utf8_offsets() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["-=>"]
            }
        ],
        "tokenizer": "standard"
    }"#;

    let mut analyzer = create_analyzer(params, "").unwrap();
    let mut stream = analyzer.token_stream("中-文");

    assert!(stream.advance());
    let token = stream.token();
    assert_eq!(token.text, "中文");
    assert_eq!(token.offset_from, 0);
    assert_eq!(token.offset_to, 7);
    assert!(!stream.advance());
}
