use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tantivy::tokenizer::TextAnalyzer;
// use tantivy_binding::analyzer::tokenizers;
use tantivy_binding::analyzer::create_analyzer;

fn test_analyzer(tokenizer: &mut TextAnalyzer) {
    let text = "(AI Focus evaluates subject movement, automatically sets either one-shot AF or AI Servo AF automatically.) Drive mode is set to Single Shot, ISO is set to Auto, and the metering mode is set to Evaluative";
    tokenizer.token_stream(text);
}

fn bench_lindua_language_identifier_tokenizer(c: &mut Criterion) {
    let params = r#"
        {
            "tokenizer": {
                "type": "language_identifier",
                "analyzers": {
                    "default": {
                        "tokenizer": "standard"
                    },
                    "en": {
                            "type": "english"
                        },
                    "jieba": {
                        "tokenizer": "jieba"
                    }
                },
                "mapping": {
                    "Chinese": "jieba",
                    "English": "en"
                },
                "identifier": "lingua"
            }
        }
    "#;
    let mut analyzer = create_analyzer(params);
    assert!(analyzer.is_ok(), "error: {}", analyzer.err().unwrap());

    c.bench_function("test", |b| {
        b.iter(|| test_analyzer(black_box(&mut analyzer.as_mut().unwrap())))
    });
}

fn bench_whatlang_language_identifier_tokenizer(c: &mut Criterion) {
    let params = r#"
        {
            "tokenizer": {
                "type": "language_identifier",
                "analyzers": {
                    "default": {
                        "tokenizer": "standard"
                    },
                    "en": {
                            "type": "english"
                        },
                    "jieba": {
                        "tokenizer": "jieba"
                    }
                },
                "mapping": {
                    "Chinese": "jieba",
                    "English": "en"
                },
                "identifier": "whatlang"
            }
        }
    "#;
    let mut analyzer = create_analyzer(params);
    assert!(analyzer.is_ok(), "error: {}", analyzer.err().unwrap());

    c.bench_function("test", |b| {
        b.iter(|| test_analyzer(black_box(&mut analyzer.as_mut().unwrap())))
    });
}

criterion_group!(
    benches,
    bench_lindua_language_identifier_tokenizer,
    bench_whatlang_language_identifier_tokenizer
);
criterion_main!(benches);
