use std::vec::Vec;

use log::warn;
use once_cell::sync::Lazy;
use serde_json as json;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use tokio::runtime::Runtime;
use tonic::transport::Channel;
#[cfg(feature = "analyzer-remote-tls")]
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

use tokenizer::tokenization_request::Parameter;
use tokenizer::tokenizer_client::TokenizerClient;
use tokenizer::TokenizationRequest;

#[cfg(feature = "analyzer-remote-tls")]
use crate::analyzer::options::read_resource_file;
use crate::analyzer::options::FileResourcePathHelper;
use crate::error::TantivyBindingError;

pub mod tokenizer {
    include!("../gen/milvus.proto.tokenizer.rs");
}

static TOKIO_RT: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

#[derive(Clone)]
pub struct GrpcTokenizer {
    endpoint: String,
    parameters: Vec<Parameter>,
    client: TokenizerClient<Channel>,
    default_tokens: Vec<Token>,
}

#[derive(Clone)]
pub struct GrpcTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

const ENDPOINTKEY: &str = "endpoint";
const PARAMTERSKEY: &str = "parameters";
#[cfg(feature = "analyzer-remote-tls")]
const TLSKEY: &str = "tls";
const DEFAULTTOKENSKEY: &str = "default_tokens";

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

// Certificate files named in the tls params. They are local paths and follow the
// same directory policy as other local analyzer resources.
#[cfg(feature = "analyzer-remote-tls")]
#[derive(Default)]
struct TlsFiles {
    ca_cert: Option<Vec<u8>>,
    identity_requested: bool,
    client_cert: Option<Vec<u8>>,
    client_key: Option<Vec<u8>>,
}

#[cfg(feature = "analyzer-remote-tls")]
fn read_tls_file(
    helper: &FileResourcePathHelper,
    path: &str,
    name: &str,
) -> crate::error::Result<Option<Vec<u8>>> {
    let resource_key = format!("grpc tokenizer tls {}", name);
    if path.is_empty() {
        // An empty path could never be read; keep skipping it as before.
        warn!("grpc tokenizer tls {} is empty, skip loading it", name);
        return Ok(None);
    }
    let result = helper
        .resolve_local_path(path, &resource_key)
        .and_then(|path| read_resource_file(&path, &resource_key));
    match result {
        Ok(data) => Ok(Some(data)),
        // Not allowed, not a regular file, or too large.
        Err(e @ TantivyBindingError::InvalidArgument(_)) => Err(e),
        // Other read failures keep skipping the file as before.
        Err(e) => {
            warn!("grpc tokenizer tls {} read error: {}", name, e);
            Ok(None)
        }
    }
}

#[cfg(feature = "analyzer-remote-tls")]
fn read_tls_files(
    tls_val: &json::Value,
    helper: &FileResourcePathHelper,
) -> crate::error::Result<TlsFiles> {
    let mut files = TlsFiles::default();
    if let Some(ca_cert_path) = tls_val.get("ca_cert") {
        match ca_cert_path.as_str() {
            Some(path) => files.ca_cert = read_tls_file(helper, path, "ca_cert")?,
            None => {
                warn!("grpc tokenizer tls ca_cert must be a string. skip loading CA certificate.")
            }
        }
    }
    if let (Some(client_cert_path), Some(client_key_path)) = (
        tls_val.get("client_cert").and_then(|v| v.as_str()),
        tls_val.get("client_key").and_then(|v| v.as_str()),
    ) {
        files.identity_requested = true;
        files.client_cert = read_tls_file(helper, client_cert_path, "client_cert")?;
        files.client_key = read_tls_file(helper, client_key_path, "client_key")?;
    }
    Ok(files)
}

impl GrpcTokenizer {
    pub fn from_json(
        params: &json::Map<String, json::Value>,
        helper: &FileResourcePathHelper,
    ) -> crate::error::Result<GrpcTokenizer> {
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
        #[cfg(not(feature = "analyzer-remote-tls"))]
        if endpoint.starts_with("https://") {
            return Err(TantivyBindingError::InvalidArgument(
                "grpc tokenizer https endpoints require the analyzer-remote-tls feature, which is disabled in this build".to_string(),
            ));
        }

        let default_tokens = if let Some(val) = params.get(DEFAULTTOKENSKEY) {
            if let Some(arr) = val.as_array() {
                let mut offset = 0;
                let mut position = 0;
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|text| {
                        let start = offset;
                        let end = start + text.len();
                        offset = end + 1;
                        let token = Token {
                            offset_from: start,
                            offset_to: end,
                            position,
                            text: text.to_string(),
                            position_length: text.chars().count(),
                        };
                        position += 1;
                        token
                    })
                    .collect()
            } else {
                warn!("grpc tokenizer default_tokens must be an array. ignoring.");
                vec![]
            }
        } else {
            vec![]
        };

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

        #[cfg(feature = "analyzer-remote-tls")]
        let tls_files = match params.get(TLSKEY) {
            Some(tls_val) if endpoint.starts_with("https://") => read_tls_files(tls_val, helper)?,
            _ => TlsFiles::default(),
        };
        #[cfg(not(feature = "analyzer-remote-tls"))]
        let _ = helper;

        let channel = match TOKIO_RT.block_on(async {
            #[cfg(feature = "analyzer-remote-tls")]
            {
                let endpoint_domain = url::Url::parse(endpoint)
                    .ok()
                    .and_then(|u| u.host_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| endpoint.to_string());
                // if the endpoint starts with "https://", we need to configure TLS
                if endpoint.starts_with("https://") {
                    let tls_config = match params.get(TLSKEY) {
                        Some(tls_val) => {
                            let domain = tls_val.get("domain")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| endpoint_domain);

                            let mut tls = ClientTlsConfig::new()
                                .domain_name(domain);

                            if let Some(ca_cert) = &tls_files.ca_cert {
                                tls = tls.ca_certificate(Certificate::from_pem(ca_cert));
                            }

                            if tls_files.identity_requested {
                                match (&tls_files.client_cert, &tls_files.client_key) {
                                    (Some(cert), Some(key)) if !cert.is_empty() && !key.is_empty() => {
                                        tls = tls.identity(Identity::from_pem(cert, key));
                                    }
                                    _ => warn!("grpc tokenizer tls client_cert or client_key is empty. skip loading client identity."),
                                }
                            }
                            tls
                        }
                        None => ClientTlsConfig::new()
                            .domain_name(endpoint_domain),
                    };

                    return tonic::transport::Endpoint::new(endpoint.to_string())?
                        .tls_config(tls_config)?
                        .connect()
                        .await;
                }
            }

            tonic::transport::Endpoint::new(endpoint.to_string())?
                .connect()
                .await
        }) {
            Ok(client) => client,
            Err(e) => {
                warn!("failed to connect to gRPC server: {}, error: {}", endpoint, e);
                return Err(TantivyBindingError::InvalidArgument(format!(
                    "failed to connect to gRPC server: {}, error: {}",
                    endpoint, e
                )));
            }
        };

        // Create a new gRPC client using the channel
        let client = TokenizerClient::new(channel);

        Ok(GrpcTokenizer {
            endpoint: endpoint.to_string(),
            parameters: parameters,
            client: client,
            default_tokens: default_tokens,
        })
    }

    fn tokenize(&self, text: &str) -> Vec<Token> {
        let request = tonic::Request::new(TokenizationRequest {
            text: text.to_string(),
            parameters: self.parameters.clone(),
        });

        let mut client = self.client.clone();

        // gRPC client works asynchronously using the Tokio runtime.
        // It requires the Tokio runtime to create a gRPC client and send requests.
        // Use the Tokio runtime to send gRPC requests asynchronously and wait for responses.
        tokio::task::block_in_place(|| {
            TOKIO_RT.block_on(async {
                match client.tokenize(request).await {
                    Ok(resp) => {
                        let ori_tokens = resp.into_inner().tokens;
                        let mut tokens = Vec::with_capacity(ori_tokens.len());
                        for token in ori_tokens {
                            tokens.push(Token {
                                offset_from: token.offset_from as usize,
                                offset_to: token.offset_to as usize,
                                position: token.position as usize,
                                text: token.text,
                                position_length: token.position_length as usize,
                            });
                        }
                        tokens
                    }
                    Err(e) => {
                        warn!("gRPC tokenizer request error: {}", e);
                        self.default_tokens.clone()
                    }
                }
            })
        })
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
    use crate::analyzer::options::ResourceInfo;
    use serde_json::json;
    use std::sync::Arc;

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
        let tokenizer = GrpcTokenizer::from_json(
            map,
            &FileResourcePathHelper::new(Arc::new(ResourceInfo::new())),
        );

        assert!(tokenizer.is_err()); // This test is expected to fail because the endpoint is not valid for testing
    }

    #[test]
    fn test_grpc_tokenizer_from_json_fail_missing_endpoint() {
        let params = json!({
            "parameters": []
        });

        let map = params.as_object().unwrap();
        let tokenizer = GrpcTokenizer::from_json(
            map,
            &FileResourcePathHelper::new(Arc::new(ResourceInfo::new())),
        );

        assert!(tokenizer.is_err());
    }

    #[cfg(not(feature = "analyzer-remote-tls"))]
    #[test]
    fn test_grpc_tokenizer_https_requires_remote_tls() {
        let params = json!({
            "endpoint": "https://localhost:50051"
        });

        let map = params.as_object().unwrap();
        let tokenizer = GrpcTokenizer::from_json(
            map,
            &FileResourcePathHelper::new(Arc::new(ResourceInfo::new())),
        );
        assert!(tokenizer.is_err());
        let err = tokenizer.err().unwrap().to_string();
        assert!(
            err.contains("analyzer-remote-tls"),
            "unexpected error: {err}"
        );
    }

    #[cfg(feature = "analyzer-remote-tls")]
    #[test]
    fn test_grpc_tokenizer_tls_files_follow_local_policy() {
        use crate::analyzer::options::test_util::{assert_not_allowed, helper_allowing};

        let allowed = tempfile::tempdir().unwrap();
        for tls in [
            json!({"ca_cert": "/etc/passwd"}),
            json!({"ca_cert": "relative/ca.pem"}),
            json!({"client_cert": "/proc/self/environ", "client_key": "/etc/hostname"}),
            json!({"client_cert": "/dev/zero", "client_key": "/dev/zero"}),
        ] {
            let params = json!({"endpoint": "https://localhost:50051", "tls": tls});
            let err = GrpcTokenizer::from_json(
                params.as_object().unwrap(),
                &helper_allowing(&[allowed.path()]),
            )
            .err()
            .unwrap();
            assert_not_allowed(&err, &tls);
            assert!(
                err.to_string().contains("grpc tokenizer tls"),
                "{}: {}",
                tls,
                err
            );
        }

        // Empty paths are still skipped; only the connection fails here.
        let params = json!({
            "endpoint": "https://localhost:50051",
            "tls": {"ca_cert": "", "client_cert": "", "client_key": ""}
        });
        let err = GrpcTokenizer::from_json(
            params.as_object().unwrap(),
            &helper_allowing(&[allowed.path()]),
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("failed to connect"), "{}", err);
    }
}
