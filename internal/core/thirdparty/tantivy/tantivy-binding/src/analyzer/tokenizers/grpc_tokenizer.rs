use std::vec::Vec;
use std::sync::{Arc,Mutex};
use std::collections::HashMap;
use serde_json as json;
use once_cell::sync::Lazy;
use tokio::runtime::{Runtime};
use tantivy::tokenizer::{Token, Tokenizer, TokenStream};
use crate::error::TantivyBindingError;

pub mod tokenizer {
    include!("../gen/milvus.proto.tokenizer.rs");
}

use tokenizer::tokenizer_client::TokenizerClient;
use tokenizer::tokenization_request::Parameter;
use tokenizer::TokenizationRequest;

static TOKIO_RT: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create Tokio runtime")
});

#[derive(Clone)]
pub struct GrpcTokenizer {
    token_cache: Arc<Mutex<HashMap<String, Vec<Token>>>>,
    endpoint: String,
    parameters: Vec<Parameter>,
}

#[derive(Clone)]
pub struct GrpcTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

const ENDPOINTKEY: &str = "endpoint";
const PARAMTERSKEY: &str = "parameters";

impl TokenStream for GrpcTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

async fn grpc_tokenize_async(
    endpoint: String,
    text: String,
    parameters: Vec<Parameter>,
) -> Option<Vec<Token>> {
    let mut client = TokenizerClient::connect(endpoint).await.ok()?;
    let request = tonic::Request::new(TokenizationRequest {
        text: text.clone(),
        parameters,
    });

    match client.tokenize(request).await {
        Ok(response) => {
            let response = response.into_inner();
            let tokens = response.tokens.into_iter().map(|t| Token {
                offset_from: t.offset_from as usize,
                offset_to: t.offset_to as usize,
                position: t.position as usize,
                text: t.text,
                position_length: t.position_length as usize,
            }).collect();
            Some(tokens)
        }
        Err(e) => {
            eprintln!("gRPC request failed: {}", e);
            None
        }
    }
}

impl GrpcTokenizer {
    pub fn from_json(params: &json::Map<String, json::Value>) -> crate::error::Result<GrpcTokenizer> {
        let endpoint = params
            .get(ENDPOINTKEY)
            .ok_or(TantivyBindingError::InvalidArgument(
                "grpc tokenizer must set endpoint".to_string(),
            ))?
            .as_str()
            .ok_or(TantivyBindingError::InvalidArgument(
                "grpc tokenizer endpoint must be string".to_string(),
            ))?;
        if endpoint.is_empty() {
            return Err(TantivyBindingError::InvalidArgument(
                "grpc tokenizer endpoint must not be empty".to_string(),
            ));
        }
        // validate endpoint
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(TantivyBindingError::InvalidArgument(
                "grpc tokenizer endpoint must start with http:// or https://".to_string(),
            ));
        }

        let mut parameters = vec![];
        if let Some(val) = params.get(PARAMTERSKEY) {
            if !val.is_array() {
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "grpc tokenizer parameters must be array"
                )));
            }
            for param in val.as_array().unwrap() {
                if !param.is_object() {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "grpc tokenizer parameters item must be object"
                    )));
                }
                let param = param.as_object().unwrap();
                let key = param
                    .get("key")
                    .ok_or(TantivyBindingError::InvalidArgument(
                        "grpc tokenizer parameters item must have key".to_string(),
                    ))?
                    .as_str()
                    .ok_or(TantivyBindingError::InvalidArgument(
                        "grpc tokenizer parameters item key must be string".to_string(),
                    ))?;
                let mut values: Vec<String> = vec![];
                let ori_values = param
                    .get("values")
                    .ok_or(TantivyBindingError::InvalidArgument(
                        "grpc tokenizer parameters item must have values".to_string(),
                    ))?
                    .as_array()
                    .ok_or(TantivyBindingError::InvalidArgument(
                        "grpc tokenizer parameters item values must be array".to_string(),
                    ))?;

                for v in ori_values {
                    if !v.is_string() {
                        return Err(TantivyBindingError::InvalidArgument(format!(
                            "grpc tokenizer parameters item value {} is not string",
                            v,
                        )));
                    }
                    values.push(v.as_str().unwrap().to_string());
                }

                parameters.push(Parameter {
                    key: key.to_string(),
                    values: values,
                });
            }
        }

        Ok(GrpcTokenizer {
            token_cache: Arc::new(Mutex::new(HashMap::new())),
            endpoint: endpoint.to_string(),
            parameters: parameters,
        })
    }

    fn poll_tokens(&self, text: &str) -> Option<Vec<Token>> {
        let mut cache = self.token_cache.lock().unwrap();
        cache.get(text).cloned()
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let newtext = text.to_string();
        let endpoint = self.endpoint.clone();
        let parameters = self.parameters.clone();
        let cache = self.token_cache.clone();

        // gRPC client works asynchronously using the Tokio runtime.
        // It requires the Tokio runtime to create a gRPC client and send requests.
        // Use the Tokio runtime to send gRPC requests asynchronously and wait for responses.
        TOKIO_RT.spawn(async move {
            if let Some(tokens) = grpc_tokenize_async(endpoint, newtext.clone(), parameters).await {
                let mut guard = cache.lock().unwrap();
                guard.insert(newtext, tokens);
            } else {
                let mut guard = cache.lock().unwrap();
                // If the gRPC call fails, we can still insert an empty vector to avoid repeated calls
                eprintln!("gRPC call failed for text: {}", newtext);
                guard.insert(newtext, vec![]);
            }
        });

        loop {
            if let Some(tokens) = self.poll_tokens(text) {
                return tokens;
            } else {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
}


impl Tokenizer for GrpcTokenizer {
    type TokenStream<'a> = GrpcTokenStream;

    fn token_stream(&mut self, text: &str) -> GrpcTokenStream {
        let tokens = self.tokenize(text);
        GrpcTokenStream { tokens, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tantivy::tokenizer::{Tokenizer, TokenStream};

    #[test]
    fn test_grpc_tokenizer_from_json_success() {
        let params = json!({
            "endpoint": "http://localhost:50051",
            "parameters": [
                {
                    "key": "lang",
                    "values": ["en"]
                }
            ]
        });

        let map = params.as_object().unwrap();
        let tokenizer = GrpcTokenizer::from_json(map);

        assert!(tokenizer.is_ok(), "from_json failed: {:?}", tokenizer.err());
    }

    #[test]
    fn test_grpc_tokenizer_from_json_fail_missing_endpoint() {
        let params = json!({
            "parameters": []
        });

        let map = params.as_object().unwrap();
        let tokenizer = GrpcTokenizer::from_json(map);

        assert!(tokenizer.is_err());
    }

    #[test]
    fn test_grpc_tokenizer_token_stream() {
        let params = json!({
            "endpoint": "http://localhost:50051",
            "parameters": [
                {
                    "key": "lang",
                    "values": ["en"]
                }
            ]
        });

        let map = params.as_object().unwrap();
        let tokenizer = GrpcTokenizer::from_json(map).unwrap();

        let mut tokenizer_clone = tokenizer.clone();
        let mut stream = tokenizer_clone.token_stream("hello world");

        // There is no runnig gRPC server in the test environment,
        // so the result is likely to be an empty vector.
        let mut tokens = vec![];
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }

        // Check if it runs without error
        println!("tokenized: {:?}", tokens);
    }
}