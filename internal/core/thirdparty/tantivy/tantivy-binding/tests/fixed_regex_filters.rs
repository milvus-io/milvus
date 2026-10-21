use regex::Regex;
use serde_json::{json, Value};
use tantivy::tokenizer::{TextAnalyzer, Token};
use tantivy_binding::analyzer::create_analyzer;

const FILTERS: [(&str, &str, bool); 3] = [
    ("removepunct", r"[\p{Punct}\s]+", false),
    ("cncharonly", r"\p{Han}+", true),
    ("cnalphanumonly", r"[\p{Han}a-zA-Z0-9]+", true),
];

fn collect(analyzer: &mut TextAnalyzer, text: &str) -> Vec<Token> {
    let mut stream = analyzer.token_stream(text);
    let mut tokens = Vec::new();
    while stream.advance() {
        tokens.push(stream.token().clone());
    }
    tokens
}

fn corpus() -> Vec<String> {
    vec![
        "".into(),
        "，。！？ \t\n\u{a0}\u{2003}".into(),
        "中文分词测试，南京市长江大桥。向量数据库".into(),
        "Milvus中文 ABC123 e-mail A&B foo_bar C++ １２３ café İ ΟΣ 😀".into(),
        "中文Repeat，ABC123! 空白\tİ ΟΣ e\u{301} 😀 ".repeat(1024),
        "中文".repeat(128),
    ]
}

#[test]
fn fixed_regex_filters_preserve_complete_tokens() {
    // The oracle applies the old per-document regex predicate to the complete
    // upstream stream. Token equality checks text, byte offsets and positions.
    let configs = [
        json!({"tokenizer": "jieba"}),
        json!({"tokenizer": "whitespace"}),
        json!({"tokenizer": "standard"}),
        json!({"tokenizer": {
            "type": "jieba", "dict": ["_default_", "向量数据库"],
            "mode": "exact", "hmm": false
        }}),
        json!({"tokenizer": "jieba", "filter": ["lowercase"]}),
        json!({"tokenizer": "whitespace", "char_filter": [
            {"type": "mapping", "mappings": ["&=>and", "-=>\\u0020"]}
        ]}),
    ];
    for config in configs {
        for (name, pattern, keep_match) in FILTERS {
            let mut baseline = create_analyzer(&config.to_string(), "").unwrap();
            let mut filtered_config = config.clone();
            if filtered_config.get("filter").is_none() {
                filtered_config["filter"] = json!([]);
            }
            filtered_config["filter"]
                .as_array_mut()
                .unwrap()
                .push(json!(name));
            let mut optimized = create_analyzer(&filtered_config.to_string(), "").unwrap();
            // Reuse the same analyzer across documents and across two passes.
            for _ in 0..2 {
                for text in corpus() {
                    let regex = Regex::new(pattern).unwrap();
                    let expected: Vec<_> = collect(&mut baseline, &text)
                        .into_iter()
                        .filter(|token| regex.is_match(&token.text) == keep_match)
                        .collect();
                    assert_eq!(
                        collect(&mut optimized, &text),
                        expected,
                        "{filtered_config}"
                    );
                }
            }
        }
    }
}

fn texts(config: Value, input: &str) -> Vec<String> {
    collect(
        &mut create_analyzer(&config.to_string(), "").unwrap(),
        input,
    )
    .into_iter()
    .map(|token| token.text)
    .collect()
}

#[test]
fn fixed_regex_filters_preserve_substring_matching() {
    // Matching any substring accepts/rejects the WHOLE token. The Chinese
    // predicates do not require every character to be Chinese/alphanumeric.
    let input = "foo-bar foo_bar plain 中文! !中? é １２３ 😀";
    assert_eq!(
        texts(
            json!({"tokenizer":"whitespace", "filter":["removepunct"]}),
            input
        ),
        ["plain", "é", "１２３", "😀"]
    );
    assert_eq!(
        texts(
            json!({"tokenizer":"whitespace", "filter":["cncharonly"]}),
            input
        ),
        ["中文!", "!中?"]
    );
    assert_eq!(
        texts(
            json!({"tokenizer":"whitespace", "filter":["cnalphanumonly"]}),
            input
        ),
        ["foo-bar", "foo_bar", "plain", "中文!", "!中?"]
    );
}

#[test]
fn fixed_regex_filters_preserve_filter_order() {
    assert_eq!(
        texts(
            json!({"tokenizer":"whitespace", "filter":["asciifolding", "cnalphanumonly"]}),
            "É 中文"
        ),
        ["E", "中文"]
    );
    assert_eq!(
        texts(
            json!({"tokenizer":"whitespace", "filter":["cnalphanumonly", "asciifolding"]}),
            "É 中文"
        ),
        ["中文"]
    );
}

#[test]
fn fixed_regex_filters_preserve_builtin_chinese_and_chaining() {
    let han = Regex::new(r"\p{Han}+").unwrap();
    let han_alphanum = Regex::new(r"[\p{Han}a-zA-Z0-9]+").unwrap();
    let punct = Regex::new(r"[\p{Punct}\s]+").unwrap();
    let mut baseline = create_analyzer(r#"{"tokenizer":"jieba"}"#, "").unwrap();
    let mut chinese = create_analyzer(r#"{"type":"chinese"}"#, "").unwrap();
    let mut chained = create_analyzer(
        r#"{"tokenizer":"jieba","filter":["removepunct","cncharonly","cnalphanumonly"]}"#,
        "",
    )
    .unwrap();
    for text in corpus() {
        let tokens = collect(&mut baseline, &text);
        let expected_chinese: Vec<_> = tokens
            .iter()
            .filter(|token| han_alphanum.is_match(&token.text))
            .cloned()
            .collect();
        let expected_chain: Vec<_> = tokens
            .into_iter()
            .filter(|token| !punct.is_match(&token.text))
            .filter(|token| han.is_match(&token.text))
            .filter(|token| han_alphanum.is_match(&token.text))
            .collect();
        assert_eq!(collect(&mut chinese, &text), expected_chinese);
        assert_eq!(collect(&mut chained, &text), expected_chain);
    }
}

#[test]
fn fixed_regex_filter_clones_are_independent() {
    for (name, _, _) in FILTERS {
        let config = json!({"tokenizer":"jieba", "filter":["lowercase", name]});
        let mut original = create_analyzer(&config.to_string(), "").unwrap();
        let inputs = corpus();
        let expected: Vec<_> = inputs
            .iter()
            .map(|text| collect(&mut original, text))
            .collect();
        let workers: Vec<_> = (0..4).map(|_| original.clone()).collect();
        drop(original);
        std::thread::scope(|scope| {
            for mut worker in workers {
                let inputs = &inputs;
                let expected = &expected;
                scope.spawn(move || {
                    for _ in 0..4 {
                        for (text, tokens) in inputs.iter().zip(expected) {
                            assert_eq!(&collect(&mut worker, text), tokens);
                        }
                    }
                });
            }
        });
    }
}
