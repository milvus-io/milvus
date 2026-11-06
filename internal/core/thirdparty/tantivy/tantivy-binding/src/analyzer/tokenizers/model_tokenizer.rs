use std::collections::VecDeque;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Component, Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};

use log::warn;
use ort::{
    session::{
        builder::{GraphOptimizationLevel, PrepackedWeights},
        Session,
    },
    value::{Tensor, TensorElementType, ValueType},
};
use serde::Deserialize;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::analyzer::options::FileResourcePathHelper;
use crate::error::{Result, TantivyBindingError};

use super::model_weights::shared_initializers;
use super::output_decoder::OutputDecoder;

const PACKAGE_KEY: &str = "package";
const MANIFEST_FILE: &str = "manifest.json";
const MAX_SESSION_POOL_SIZE: usize = 128;
const DEFAULT_SESSION_POOL_SIZE: usize = 4;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    model: String,
    decoder: String,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum PackageConfig {
    Local { path: PathBuf },
    Remote { resource_name: String },
}

struct SessionPool<T> {
    state: Mutex<PoolState<T>>,
}

struct PoolState<T> {
    available: Vec<T>,
    waiting: VecDeque<mpsc::SyncSender<T>>,
}

struct SessionLease<'a, T> {
    pool: &'a SessionPool<T>,
    session: Option<T>,
}

impl<T> SessionPool<T> {
    fn new(sessions: Vec<T>) -> Self {
        assert!(!sessions.is_empty());
        Self {
            state: Mutex::new(PoolState {
                available: sessions,
                waiting: VecDeque::new(),
            }),
        }
    }

    fn acquire(&self) -> SessionLease<'_, T> {
        let mut state = self.state.lock().unwrap();
        let session = if let Some(session) = state.available.pop() {
            session
        } else {
            let (sender, receiver) = mpsc::sync_channel(1);
            state.waiting.push_back(sender);
            drop(state);
            receiver.recv().unwrap()
        };
        SessionLease {
            pool: self,
            session: Some(session),
        }
    }
}

impl<T> Deref for SessionLease<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.session.as_ref().unwrap()
    }
}

impl<T> DerefMut for SessionLease<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.session.as_mut().unwrap()
    }
}

impl<T> Drop for SessionLease<'_, T> {
    fn drop(&mut self) {
        let mut session = self.session.take().unwrap();
        let mut state = self.pool.state.lock().unwrap();
        // Hand off directly so new requests cannot overtake existing waiters.
        while let Some(waiter) = state.waiting.pop_front() {
            match waiter.send(session) {
                Ok(()) => return,
                Err(error) => session = error.0,
            }
        }
        state.available.push(session);
    }
}

struct Inference {
    sessions: SessionPool<Session>,
    decoder: OutputDecoder,
}

#[derive(Clone)]
pub struct ModelTokenizer {
    inference: Arc<Inference>,
}

pub struct ModelTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for ModelTokenStream {
    fn advance(&mut self) -> bool {
        if self.index == self.tokens.len() {
            false
        } else {
            self.index += 1;
            true
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

impl Inference {
    fn predict(&self, text: &str) -> Result<Vec<Token>> {
        if text.is_empty() {
            return Ok(Vec::new());
        }
        // Acquire only for inference: idle analyzer clones must not hold pool slots.
        let mut session = self.sessions.acquire();
        let input = Tensor::from_string_array(([1], &[text.to_owned()][..])).map_err(|error| {
            TantivyBindingError::InternalError(format!("model tokenizer input is invalid: {error}"))
        })?;
        let outputs = session
            .run(ort::inputs!["text" => input])
            .map_err(|error| {
                TantivyBindingError::InternalError(format!(
                    "model tokenizer inference failed: {error}"
                ))
            })?;
        let mut tokens = self
            .decoder
            .decode(&outputs, &[text])
            .map_err(TantivyBindingError::InternalError)?;
        Ok(tokens.remove(0))
    }
}

impl ModelTokenizer {
    pub fn from_json(
        params: &json::Map<String, json::Value>,
        helper: &mut FileResourcePathHelper,
    ) -> Result<Self> {
        let package = params.get(PACKAGE_KEY).ok_or_else(|| {
            TantivyBindingError::InvalidArgument("model tokenizer must set package".to_string())
        })?;
        let package: PackageConfig = serde_json::from_value(package.clone())?;
        let session_pool_size = params
            .get("session_pool_size")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or(DEFAULT_SESSION_POOL_SIZE);
        match package {
            PackageConfig::Local { path } => Self::from_package(path, session_pool_size),
            PackageConfig::Remote { resource_name } => Self::from_manifest(
                helper.get_resource_file_path(&resource_name, MANIFEST_FILE)?,
                session_pool_size,
            ),
        }
    }

    pub fn from_package(path: impl AsRef<Path>, session_pool_size: usize) -> Result<Self> {
        Self::from_manifest(path.as_ref().join(MANIFEST_FILE), session_pool_size)
    }

    fn from_manifest(path: impl AsRef<Path>, session_pool_size: usize) -> Result<Self> {
        if !(1..=MAX_SESSION_POOL_SIZE).contains(&session_pool_size) {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "model tokenizer session_pool_size must be between 1 and {MAX_SESSION_POOL_SIZE}"
            )));
        }
        let path = path.as_ref();
        let manifest: Manifest = serde_json::from_slice(&fs::read(path)?)?;
        let decoder_path = package_file(path, &manifest.decoder, "decoder")?;
        let decoder = OutputDecoder::from_file(&decoder_path)?;
        let model_path = package_file(path, &manifest.model, "model")?;
        let model = fs::read(&model_path).map_err(|error| {
            TantivyBindingError::InternalError(format!(
                "failed to load model tokenizer ONNX {}: {error}",
                model_path.display()
            ))
        })?;
        let runtime_path = std::env::var_os("ORT_DYLIB_PATH")
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                format!(
                    "{}onnxruntime{}",
                    std::env::consts::DLL_PREFIX,
                    std::env::consts::DLL_SUFFIX
                )
                .into()
            });
        ort::init_from(runtime_path)
            .map_err(|error| {
                TantivyBindingError::InternalError(format!(
                    "failed to load ONNX Runtime for model tokenizer: {error}"
                ))
            })?
            .commit();
        let weights = shared_initializers(&model)?;
        drop(model);
        let packed = PrepackedWeights::new();
        let mut sessions = Vec::with_capacity(session_pool_size);
        for _ in 0..session_pool_size {
            let session = (|| -> ort::Result<Session> {
            let mut builder = Session::builder()?
                .with_optimization_level(GraphOptimizationLevel::Level3)?
                .with_independent_thread_pool()?
                .with_intra_threads(1)?
                .with_inter_threads(1)?
                .with_prepacked_weights(&packed)?;
            for (name, value) in &weights {
                builder = builder.with_initializer(name, Arc::clone(value))?;
            }
            builder.commit_from_file(&model_path)
        })()
        .map_err(|error| {
            TantivyBindingError::InternalError(format!(
                "failed to load model tokenizer ONNX {} (set ORT_DYLIB_PATH to libonnxruntime when needed): {error}",
                model_path.display()
            ))
        })?;
            let [input] = session.inputs() else {
                return Err(TantivyBindingError::InvalidArgument(
                    "model tokenizer ONNX must have exactly one input named text".to_string(),
                ));
            };
            let input_is_compatible = matches!(
                input.dtype(),
                ValueType::Tensor {
                    ty: TensorElementType::String,
                    shape,
                    ..
                } if shape.len() == 1
                    && (shape[0] == -1 || shape[0] == 1)
            );
            if input.name() != "text" || !input_is_compatible {
                return Err(TantivyBindingError::InvalidArgument(
                    "model tokenizer input must be text: tensor(string)[dynamic_batch] or [1]"
                        .to_string(),
                ));
            }
            decoder.validate_outputs(session.outputs())?;
            sessions.push(session);
        }
        Ok(Self {
            inference: Arc::new(Inference {
                sessions: SessionPool::new(sessions),
                decoder,
            }),
        })
    }
}

impl Tokenizer for ModelTokenizer {
    type TokenStream<'a> = ModelTokenStream;

    fn token_stream(&mut self, text: &str) -> Self::TokenStream<'_> {
        let tokens = self.inference.predict(text).unwrap_or_else(|error| {
            warn!("model tokenizer failed: {error}");
            Vec::new()
        });
        ModelTokenStream { tokens, index: 0 }
    }
}

fn package_file(manifest: &Path, file: &str, kind: &str) -> Result<PathBuf> {
    let path = Path::new(file);
    if !matches!(
        path.components().collect::<Vec<_>>().as_slice(),
        [Component::Normal(_)]
    ) {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "model tokenizer {kind} must be a package-local file name"
        )));
    }
    Ok(manifest
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::options::FileResourcePathBuilder;

    struct PackageResource(PathBuf);

    impl FileResourcePathBuilder for PackageResource {
        fn get_resource_file_path(&self, name: &str, file: &str) -> Result<(i64, PathBuf)> {
            assert_eq!(name, "test_model");
            assert_eq!(file, MANIFEST_FILE);
            Ok((7, self.0.join(file)))
        }
    }

    #[test]
    fn test_local_and_remote_packages_resolve_root_manifest() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join(MANIFEST_FILE),
            r#"{"model":"model.onnx","decoder":"../decoder.json"}"#,
        )
        .unwrap();
        let mut helper =
            FileResourcePathHelper::new(Arc::new(PackageResource(directory.path().to_path_buf())));
        for package in [
            json::json!({"type": "local", "path": directory.path()}),
            json::json!({"type": "remote", "resource_name": "test_model"}),
        ] {
            let params = json::json!({"type": "model", "package": package});
            let result = ModelTokenizer::from_json(params.as_object().unwrap(), &mut helper);
            assert!(matches!(result,
                Err(TantivyBindingError::InvalidArgument(message))
                    if message == "model tokenizer decoder must be a package-local file name"
            ));
        }
        assert_eq!(helper.get_resource_ids(), vec![7]);
        assert!(ModelTokenizer::from_package(directory.path().join(MANIFEST_FILE), 8).is_err());
    }

    #[test]
    fn test_package_and_manifest_reject_unknown_fields() {
        let manifest = json::json!({"model": "model.onnx", "decoder": "decoder.json"});
        assert!(json::from_value::<Manifest>(manifest.clone()).is_ok());
        let mut unknown = manifest;
        unknown["unexpected"] = json::json!(1);
        assert!(json::from_value::<Manifest>(unknown).is_err());
        assert!(json::from_value::<PackageConfig>(json::json!({
            "type": "remote", "resource_name": "test_model", "file_name": "manifest.json"
        }))
        .is_err());
    }

    #[test]
    fn test_analyzer_session_pool_size_rejects_invalid_values() {
        for session_pool_size in [
            json::json!(0),
            json::json!(129),
            json::json!(-1),
            json::json!(1.5),
            json::json!("8"),
            json::Value::Null,
        ] {
            let mut helper = FileResourcePathHelper::new(Arc::new(PackageResource(PathBuf::new())));
            let params = json::json!({
                "type": "model",
                "package": {"type": "local", "path": "unused"},
                "session_pool_size": session_pool_size
            });
            assert!(matches!(
                ModelTokenizer::from_json(params.as_object().unwrap(), &mut helper),
                Err(TantivyBindingError::InvalidArgument(_))
                    | Err(TantivyBindingError::JsonError(_))
            ));
        }
    }

    #[test]
    fn test_session_pool_waits_and_returns_on_unwind() {
        use std::{sync::mpsc, time::Duration};
        let pool = SessionPool::new(vec![0, 1]);
        let first = pool.acquire();
        let second = pool.acquire();
        std::thread::scope(|scope| {
            let (started, start) = mpsc::channel();
            let (finished, finish) = mpsc::channel();
            let pool = &pool;
            scope.spawn(move || {
                started.send(()).unwrap();
                let session = pool.acquire();
                finished.send(*session).unwrap();
            });
            start.recv().unwrap();
            assert!(matches!(
                finish.recv_timeout(Duration::from_millis(20)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            let expected = *first;
            drop(first);
            assert_eq!(
                finish.recv_timeout(Duration::from_secs(5)).unwrap(),
                expected
            );
        });
        drop(second);
        let result = std::panic::catch_unwind(|| {
            let _session = pool.acquire();
            panic!("simulate a decoder panic");
        });
        assert!(result.is_err());
        assert_eq!(pool.state.lock().unwrap().available.len(), 2);
    }

    #[test]
    fn test_session_pool_exclusive_reuse_under_contention() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let pool = SessionPool::new(vec![0, 1]);
        let busy = [AtomicBool::new(false), AtomicBool::new(false)];
        let uses = [AtomicUsize::new(0), AtomicUsize::new(0)];
        std::thread::scope(|scope| {
            for _ in 0..16 {
                scope.spawn(|| {
                    for _ in 0..200 {
                        let session = pool.acquire();
                        assert!(!busy[*session].swap(true, Ordering::SeqCst));
                        uses[*session].fetch_add(1, Ordering::SeqCst);
                        std::thread::yield_now();
                        assert!(busy[*session].swap(false, Ordering::SeqCst));
                    }
                });
            }
        });
        assert_eq!(
            uses.iter()
                .map(|count| count.load(Ordering::SeqCst))
                .sum::<usize>(),
            3200
        );
        assert_eq!(pool.state.lock().unwrap().available.len(), 2);
    }

    #[test]
    fn test_session_pool_hands_off_in_wait_order() {
        use std::time::{Duration, Instant};
        let pool = SessionPool::new(vec![0]);
        let held = pool.acquire();
        let (sender, receiver) = mpsc::channel();
        std::thread::scope(|scope| {
            for index in 0..4 {
                let (pool, sender) = (&pool, sender.clone());
                scope.spawn(move || {
                    let _session = pool.acquire();
                    sender.send(index).unwrap();
                });
                let deadline = Instant::now() + Duration::from_secs(5);
                while pool.state.lock().unwrap().waiting.len() != index + 1 {
                    assert!(Instant::now() < deadline, "waiter failed to enqueue");
                    std::thread::yield_now();
                }
            }
            drop(held);
            for expected in 0..4 {
                assert_eq!(
                    receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
                    expected
                );
            }
        });
        assert_eq!(pool.state.lock().unwrap().available.len(), 1);
        assert!(pool.state.lock().unwrap().waiting.is_empty());
    }

    #[test]
    fn test_session_pool_recovers_abandoned_handoff() {
        let pool = SessionPool::new(vec![7]);
        let session = pool.acquire();
        let (sender, receiver) = mpsc::sync_channel(1);
        pool.state.lock().unwrap().waiting.push_back(sender);
        drop(receiver);
        drop(session);
        assert_eq!(*pool.acquire(), 7);
    }
}
