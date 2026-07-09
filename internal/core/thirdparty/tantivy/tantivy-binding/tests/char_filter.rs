use tantivy::tokenizer::TokenStream;
use tantivy_binding::analyzer::create_analyzer;

#[test]
fn test_mapping_char_filter_pipeline() {
    let params = r#"{
        "char_filter": [
            {
                "type": "mapping",
                "mappings": ["-=> "]
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
