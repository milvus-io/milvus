use std::vec::Vec;
use serde_json as json;
use tantivy::tokenizer::{Token, Tokenizer, TokenStream};
use crate::error::TantivyBindingError;

pub mod tokenizer {
    include!("../gen/milvus.proto.tokenizer.rs");
}

use tokenizer::tokenizer_client::TokenizerClient;
use tokenizer::tokenization_request::Parameter;
use tokenizer::TokenizationRequest;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

static TOKIO_RT: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create Tokio runtime")
});

#[derive(Clone)]
pub struct GrpcTokenizer {
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
            endpoint: endpoint.to_string(),
            parameters: parameters,
        })
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let request = tonic::Request::new(TokenizationRequest {
            text: text.to_string(),
            parameters: self.parameters.clone(),
        });

        // gRPC client works asynchronously using the Tokio runtime.
        // It requires the Tokio runtime to create a gRPC client and send requests.
        // Use the Tokio runtime to send gRPC requests asynchronously and wait for responses.
        let response = TOKIO_RT.block_on(async {
            let mut client = match TokenizerClient::connect(self.endpoint.clone()).await {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("gRPC tokenizer connect error: {}", e);
                    return None;
                }
            };
            match client.tokenize(request).await {
                Ok(resp) => Some(resp),
                Err(e) => {
                    eprintln!("gRPC tokenizer request error: {}", e);
                    None
                }
            }
        });

        let response = match response {
            Some(resp) => resp,
            None => return vec![],
        };

        let ori_tokens = response.into_inner().tokens;
        let mut tokens = Vec::with_capacity(ori_tokens.len());

        for token in ori_tokens {
            tokens.push(Token {
                offset_from: token.offset_from as usize,
                offset_to: token.offset_to as usize,
                position: token.position as usize,
                text: token.text,
                position_length: (token.offset_to - token.offset_from) as usize,
            });
        }
        tokens
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