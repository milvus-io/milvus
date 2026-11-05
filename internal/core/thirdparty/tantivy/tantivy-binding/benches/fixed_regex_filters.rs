use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use std::sync::Barrier;
use std::time::Instant;
use tantivy::tokenizer::TextAnalyzer;
use tantivy_binding::analyzer::create_analyzer;

fn consume(analyzer: &mut TextAnalyzer, text: &str) -> usize {
    let mut stream = analyzer.token_stream(black_box(text));
    let mut count = 0;
    while stream.advance() {
        black_box(stream.token());
        count += 1;
    }
    black_box(count)
}

fn bench_fixed_regex_filters(c: &mut Criterion) {
    // Use the identical harness on both revisions. No analyzer construction or
    // cloning in the timed region; BM25 clones once per worker, not per document.
    let cases = [
        ("short_zh", "向量数据库，中文分词测试。".to_string()),
        (
            "mixed",
            "Milvus中文 ABC123 café İ ΟΣ 😀 foo-bar\t空白。".repeat(8),
        ),
        ("punctuation", "，。！？ ,.;:! \t\n\u{a0}\u{2003}".repeat(8)),
        (
            "long_zh",
            "南京市长江大桥，向量数据库支持全文检索。".repeat(256),
        ),
    ];
    for filter in ["removepunct", "cncharonly", "cnalphanumonly"] {
        let mut group = c.benchmark_group(format!("fixed_regex/{filter}"));
        for (name, text) in &cases {
            let config = json!({"tokenizer":"jieba", "filter":["lowercase", filter]});
            let mut analyzer = create_analyzer(&config.to_string(), "").unwrap();
            let tokens = consume(&mut analyzer, text);
            // Document counts remain comparable for fully filtered input.
            eprintln!(
                "{filter}/{name}: {} input bytes, {tokens} output tokens/document",
                text.len()
            );
            for workers in [1, 4] {
                group.throughput(Throughput::Elements(workers as u64));
                group.bench_with_input(
                    BenchmarkId::new(*name, workers),
                    &workers,
                    |b, &workers| {
                        if workers == 1 {
                            b.iter(|| consume(&mut analyzer, text));
                        } else {
                            b.iter_custom(|iterations| {
                                let ready = Barrier::new(workers + 1);
                                let start = Barrier::new(workers + 1);
                                std::thread::scope(|scope| {
                                    for _ in 0..workers {
                                        let mut worker = analyzer.clone();
                                        let ready = &ready;
                                        let start = &start;
                                        scope.spawn(move || {
                                            consume(&mut worker, text);
                                            ready.wait();
                                            start.wait();
                                            for _ in 0..iterations {
                                                consume(&mut worker, text);
                                            }
                                        });
                                    }
                                    ready.wait();
                                    let begin = Instant::now();
                                    start.wait();
                                    // Join inside the timed region so all work is
                                    // counted; thread creation and warmup are outside.
                                    begin
                                })
                                .elapsed()
                            });
                        }
                    },
                );
            }
        }
        group.finish();
    }
}

criterion_group!(benches, bench_fixed_regex_filters);
criterion_main!(benches);
