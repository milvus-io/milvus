use std::error::Error;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use serde::Deserialize;
use serde_json::json;
use tantivy::tokenizer::TokenStream;

use tantivy_binding::analyzer::create_analyzer;

#[derive(Clone, Deserialize)]
struct SmokeInput {
    text: String,
    tokens: Vec<SmokeToken>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SmokeInputs {
    One(SmokeInput),
    Many(Vec<SmokeInput>),
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct SmokeToken {
    text: String,
    offset_from: usize,
    offset_to: usize,
    position: usize,
    position_length: usize,
}

fn invalid(message: impl Into<String>) -> IoError {
    IoError::new(ErrorKind::InvalidData, message.into())
}

fn tokenize(
    analyzer: &mut tantivy::tokenizer::TextAnalyzer,
    smoke: &SmokeInput,
) -> Vec<SmokeToken> {
    let mut stream = analyzer.token_stream(&smoke.text);
    let mut tokens = Vec::new();
    while stream.advance() {
        let token = stream.token();
        tokens.push(SmokeToken {
            text: token.text.clone(),
            offset_from: token.offset_from,
            offset_to: token.offset_to,
            position: token.position,
            position_length: token.position_length,
        });
    }
    tokens
}

fn percentile(sorted: &[u64], percent: usize) -> f64 {
    sorted[(sorted.len() - 1) * percent / 100] as f64 / 1_000.0
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let package_dir = PathBuf::from(args.next().ok_or_else(|| {
        invalid(
            "usage: model_onnx_smoke MODEL_PACKAGE_DIR [WORKERS] [ITERATIONS] [SESSION_POOL_SIZE]",
        )
    })?);
    let workers = args
        .next()
        .map(|value| value.to_string_lossy().parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    let iterations = args
        .next()
        .map(|value| value.to_string_lossy().parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    let session_pool_size = args
        .next()
        .map(|value| value.to_string_lossy().parse::<usize>())
        .transpose()?;
    if workers == 0 || iterations == 0 || args.next().is_some() {
        return Err(invalid("WORKERS and ITERATIONS must be positive integers").into());
    }

    let inputs: SmokeInputs = serde_json::from_slice(&fs::read(package_dir.join("smoke.json"))?)?;
    let inputs = Arc::new(match inputs {
        SmokeInputs::One(input) => vec![input],
        SmokeInputs::Many(inputs) => inputs,
    });
    if inputs.is_empty() {
        return Err(invalid("smoke input must contain at least one case").into());
    }

    let mut analyzer_json = json!({
        "tokenizer": {
            "type": "model",
            "package": {
                "type": "local",
                "path": package_dir
            }
        }
    });
    if let Some(session_pool_size) = session_pool_size {
        analyzer_json["tokenizer"]["session_pool_size"] = json!(session_pool_size);
    }
    let load_started = Instant::now();
    let analyzer = create_analyzer(&analyzer_json.to_string(), "")?;
    let load_elapsed = load_started.elapsed();

    let warmup_started = Instant::now();
    let mut validator = analyzer.clone();
    for (case_index, smoke) in inputs.iter().enumerate() {
        let tokens = tokenize(&mut validator, smoke);
        if tokens != smoke.tokens {
            return Err(invalid(format!(
                "warmup case {case_index} differs: {tokens:?} != {:?}",
                smoke.tokens
            ))
            .into());
        }
    }
    let warmup_elapsed = warmup_started.elapsed();

    let barrier = Arc::new(Barrier::new(workers + 1));
    let (total_tokens, mut latencies, elapsed) = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let mut analyzer = analyzer.clone();
            let inputs = Arc::clone(&inputs);
            let barrier = Arc::clone(&barrier);
            handles.push(
                scope.spawn(move || -> std::result::Result<(usize, Vec<u64>), String> {
                    let mut total = 0;
                    let mut latencies = Vec::with_capacity(inputs.len() * iterations);
                    barrier.wait();
                    for _ in 0..iterations {
                        for offset in 0..inputs.len() {
                            let case_index = (offset + worker) % inputs.len();
                            let smoke = &inputs[case_index];
                            let started = Instant::now();
                            let actual = tokenize(&mut analyzer, smoke);
                            latencies.push(started.elapsed().as_nanos() as u64);
                            if actual != smoke.tokens {
                                return Err(format!(
                                    "worker {worker} case {case_index} differs: {actual:?} != {:?}",
                                    smoke.tokens
                                ));
                            }
                            total += actual.len();
                        }
                    }
                    Ok((total, latencies))
                }),
            );
        }

        let started = Instant::now();
        barrier.wait();
        let mut total_tokens = 0;
        let mut latencies = Vec::with_capacity(inputs.len() * workers * iterations);
        for handle in handles {
            let (tokens, mut worker_latencies) = handle
                .join()
                .map_err(|_| "model smoke worker panicked".to_string())??;
            total_tokens += tokens;
            latencies.append(&mut worker_latencies);
        }
        Ok::<_, String>((total_tokens, latencies, started.elapsed()))
    })
    .map_err(invalid)?;

    latencies.sort_unstable();
    let requests = inputs.len() * workers * iterations;
    let total_bytes =
        inputs.iter().map(|input| input.text.len()).sum::<usize>() * workers * iterations;
    let mean_latency =
        latencies.iter().map(|value| *value as f64).sum::<f64>() / latencies.len() as f64 / 1_000.0;
    println!(
        "matched model package; load={:.3}ms warmup={:.3}ms workers={workers} requests={requests} elapsed={:.3}s throughput={:.1} requests/s, {:.0} bytes/s, {:.0} tokens/s latency_us mean={mean_latency:.1} p50={:.1} p95={:.1} p99={:.1}",
        load_elapsed.as_secs_f64() * 1_000.0,
        warmup_elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_secs_f64(),
        requests as f64 / elapsed.as_secs_f64(),
        total_bytes as f64 / elapsed.as_secs_f64(),
        total_tokens as f64 / elapsed.as_secs_f64(),
        percentile(&latencies, 50),
        percentile(&latencies, 95),
        percentile(&latencies, 99),
    );
    Ok(())
}
