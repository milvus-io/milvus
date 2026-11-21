//! TLS for gRPC tokenizer endpoints through OpenSSL instead of rustls.
//!
//! Compiled only with the `analyzer-remote-tls-openssl` feature. It reads the
//! same `tls` settings as the rustls path and keeps its behaviour:
//!
//! - Only the configured `ca_cert` is trusted. The system trust store is not
//!   used, so an endpoint without `ca_cert` fails verification.
//! - The server name is checked against `domain`, or the endpoint host.
//! - The server must select HTTP/2 through ALPN.
//!
//! Protocol settings, including the P-256 and P-384 key exchange pin that
//! refuses an X25519-only server, come from `crate::analyzer::tls_openssl`.

use std::future::Future;
use std::io;
use std::pin::Pin;

use hyper_util::rt::TokioIo;
use log::warn;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslConnectorBuilder};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::X509;
use serde_json as json;
use tokio::net::TcpStream;
use tokio_openssl::SslStream;
use tonic::transport::Uri;
use tower::{service_fn, Service};

use crate::analyzer::tls_openssl::{self, BoxError, ALPN_H2};
use crate::error::{Result, TantivyBindingError};

const DEFAULT_HTTPS_PORT: u16 = 443;

type ConnectFuture = Pin<
    Box<dyn Future<Output = std::result::Result<TokioIo<SslStream<TcpStream>>, BoxError>> + Send>,
>;

fn invalid(what: &str, e: impl std::fmt::Display) -> TantivyBindingError {
    TantivyBindingError::InvalidArgument(format!("grpc tokenizer tls {what}: {e}"))
}

fn load_ca_cert(builder: &mut SslConnectorBuilder, tls: Option<&json::Value>) -> Result<()> {
    // SslConnector::builder() trusts the system store. Replace that store so
    // that only the configured CA is trusted, even when none is configured.
    let mut store = X509StoreBuilder::new().map_err(|e| invalid("create trust store", e))?;

    if let Some(ca_cert_path) = tls.and_then(|t| t.get("ca_cert")) {
        if let Some(ca_cert_path) = ca_cert_path.as_str() {
            match std::fs::read(ca_cert_path) {
                Ok(pem) => {
                    let certs =
                        X509::stack_from_pem(&pem).map_err(|e| invalid("parse ca_cert", e))?;
                    if certs.is_empty() {
                        return Err(invalid("parse ca_cert", "no certificate found"));
                    }
                    for cert in certs {
                        store
                            .add_cert(cert)
                            .map_err(|e| invalid("add ca_cert", e))?;
                    }
                }
                Err(_) => warn!("grpc tokenizer tls ca_cert read error: {}", ca_cert_path),
            }
        } else {
            warn!("grpc tokenizer tls ca_cert must be a string. skip loading CA certificate.");
        }
    }

    builder.set_cert_store(store.build());
    Ok(())
}

fn load_client_identity(
    builder: &mut SslConnectorBuilder,
    tls: Option<&json::Value>,
) -> Result<()> {
    let (Some(cert_path), Some(key_path)) = (
        tls.and_then(|t| t.get("client_cert"))
            .and_then(|v| v.as_str()),
        tls.and_then(|t| t.get("client_key"))
            .and_then(|v| v.as_str()),
    ) else {
        return Ok(());
    };

    let cert = std::fs::read(cert_path).unwrap_or_else(|e| {
        warn!("grpc tokenizer tls client_cert read error: {}", e);
        vec![]
    });
    let key = std::fs::read(key_path).unwrap_or_else(|e| {
        warn!("grpc tokenizer tls client_key read error: {}", e);
        vec![]
    });
    if cert.is_empty() || key.is_empty() {
        warn!(
            "grpc tokenizer tls client_cert or client_key is empty. skip loading client identity."
        );
        return Ok(());
    }

    let mut chain = X509::stack_from_pem(&cert)
        .map_err(|e| invalid("parse client_cert", e))?
        .into_iter();
    let leaf = chain
        .next()
        .ok_or_else(|| invalid("parse client_cert", "no certificate found"))?;
    builder
        .set_certificate(&leaf)
        .map_err(|e| invalid("use client_cert", e))?;
    for intermediate in chain {
        builder
            .add_extra_chain_cert(intermediate)
            .map_err(|e| invalid("use client_cert chain", e))?;
    }
    let key = PKey::private_key_from_pem(&key).map_err(|e| invalid("parse client_key", e))?;
    builder
        .set_private_key(&key)
        .map_err(|e| invalid("use client_key", e))?;
    builder
        .check_private_key()
        .map_err(|e| invalid("client_key does not match client_cert", e))?;
    Ok(())
}

fn build_ssl_connector(tls: Option<&json::Value>) -> Result<SslConnector> {
    let mut builder =
        tls_openssl::base_builder(ALPN_H2).map_err(|e| invalid("create OpenSSL context", e))?;
    load_ca_cert(&mut builder, tls)?;
    load_client_identity(&mut builder, tls)?;
    Ok(builder.build())
}

async fn connect(
    connector: SslConnector,
    domain: String,
    uri: Uri,
) -> std::result::Result<TokioIo<SslStream<TcpStream>>, BoxError> {
    let host = uri
        .host()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "endpoint has no host"))?;
    let port = uri.port_u16().unwrap_or(DEFAULT_HTTPS_PORT);

    let stream = match tls_openssl::connect(&connector, host, port, &domain).await {
        Ok(stream) => stream,
        Err(e) => {
            warn!("grpc tokenizer tls handshake with {} failed: {}", uri, e);
            return Err(e);
        }
    };
    if stream.ssl().selected_alpn_protocol() != Some(ALPN_H2) {
        return Err(
            format!("grpc tokenizer endpoint {uri} did not select HTTP/2 through ALPN").into(),
        );
    }
    Ok(TokioIo::new(stream))
}

/// Returns a connector for `Endpoint::connect_with_connector`. It is called
/// again on every reconnect, and each call does a full handshake.
pub(super) fn connector(
    endpoint_domain: String,
    tls: Option<&json::Value>,
) -> Result<
    impl Service<
            Uri,
            Response = TokioIo<SslStream<TcpStream>>,
            Error = BoxError,
            Future = ConnectFuture,
        > + Send
        + 'static,
> {
    let domain = tls
        .and_then(|t| t.get("domain"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or(endpoint_domain);
    let ssl_connector = build_ssl_connector(tls)?;

    Ok(service_fn(move |uri: Uri| -> ConnectFuture {
        Box::pin(connect(ssl_connector.clone(), domain.clone(), uri))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::asn1::Asn1Time;
    use openssl::bn::{BigNum, MsbOption};
    use openssl::ec::{EcGroup, EcKey};
    use openssl::hash::MessageDigest;
    use openssl::nid::Nid;
    use openssl::pkey::Private;
    use openssl::ssl::{select_next_proto, AlpnError, Ssl, SslAcceptor, SslMethod, SslVerifyMode};
    use openssl::x509::extension::{BasicConstraints, SubjectAlternativeName};
    use openssl::x509::{X509Builder, X509NameBuilder};
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    // Wire-format ALPN list the test servers accept.
    const ALPN_PROTOS: &[u8] = b"\x02h2";

    struct Issued {
        cert: X509,
        key: PKey<Private>,
    }

    fn issue(cn: &str, san: Option<&str>, issuer: Option<&Issued>) -> Issued {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", cn).unwrap();
        let name = name.build();
        let mut serial = BigNum::new().unwrap();
        serial.rand(64, MsbOption::MAYBE_ZERO, false).unwrap();

        let mut b = X509Builder::new().unwrap();
        b.set_version(2).unwrap();
        b.set_serial_number(&serial.to_asn1_integer().unwrap())
            .unwrap();
        b.set_subject_name(&name).unwrap();
        b.set_pubkey(&key).unwrap();
        b.set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        b.set_not_after(&Asn1Time::days_from_now(2).unwrap())
            .unwrap();
        if let Some(san) = san {
            let ext = SubjectAlternativeName::new()
                .dns(san)
                .build(&b.x509v3_context(issuer.map(|i| i.cert.as_ref()), None))
                .unwrap();
            b.append_extension(ext).unwrap();
        } else {
            b.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                .unwrap();
        }
        match issuer {
            Some(i) => {
                b.set_issuer_name(i.cert.subject_name()).unwrap();
                b.sign(&i.key, MessageDigest::sha256()).unwrap();
            }
            None => {
                b.set_issuer_name(&name).unwrap();
                b.sign(&key, MessageDigest::sha256()).unwrap();
            }
        }
        Issued {
            cert: b.build(),
            key,
        }
    }

    fn ca() -> Issued {
        issue("test-ca", None, None)
    }

    fn write_cert(dir: &TempDir, name: &str, i: &Issued) -> String {
        let path = dir.path().join(name);
        std::fs::write(&path, i.cert.to_pem().unwrap()).unwrap();
        path.to_str().unwrap().to_string()
    }

    fn write_key(dir: &TempDir, name: &str, i: &Issued) -> String {
        let path = dir.path().join(name);
        std::fs::write(&path, i.key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[derive(Default)]
    struct ServerOptions {
        no_alpn: bool,
        groups: Option<&'static str>,
        client_ca: Option<X509>,
    }

    /// Accepts one TLS connection and returns its port and the handshake result.
    async fn serve_once(
        leaf: &Issued,
        opts: ServerOptions,
    ) -> (
        u16,
        tokio::task::JoinHandle<std::result::Result<bool, String>>,
    ) {
        let mut b = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).unwrap();
        b.set_certificate(&leaf.cert).unwrap();
        b.set_private_key(&leaf.key).unwrap();
        if let Some(groups) = opts.groups {
            b.set_groups_list(groups).unwrap();
        }
        if !opts.no_alpn {
            b.set_alpn_select_callback(|_, protos| {
                select_next_proto(ALPN_PROTOS, protos).ok_or(AlpnError::NOACK)
            });
        }
        if let Some(ca) = opts.client_ca {
            let mut store = X509StoreBuilder::new().unwrap();
            store.add_cert(ca).unwrap();
            b.set_verify_cert_store(store.build()).unwrap();
            b.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
        }
        let acceptor = b.build();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let ssl = Ssl::new(acceptor.context()).map_err(|e| e.to_string())?;
            let mut stream = SslStream::new(ssl, tcp).map_err(|e| e.to_string())?;
            Pin::new(&mut stream)
                .accept()
                .await
                .map_err(|e| e.to_string())?;
            // Keep the connection open long enough for the client to finish.
            let mut buf = [0u8; 1];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
            Ok(stream.ssl().peer_certificate().is_some())
        });
        (port, task)
    }

    async fn dial(port: u16, tls: json::Value) -> std::result::Result<(), String> {
        let mut c = connector("localhost".to_string(), Some(&tls)).map_err(|e| e.to_string())?;
        let uri: Uri = format!("https://localhost:{port}").parse().unwrap();
        c.call(uri).await.map(|_| ()).map_err(|e| e.to_string())
    }

    #[tokio::test]
    async fn connects_with_configured_ca() {
        let dir = TempDir::new().unwrap();
        let ca = ca();
        let leaf = issue("localhost", Some("localhost"), Some(&ca));
        let (port, _server) = serve_once(&leaf, ServerOptions::default()).await;
        let tls = json!({"ca_cert": write_cert(&dir, "ca.pem", &ca)});
        dial(port, tls).await.unwrap();
    }

    #[tokio::test]
    async fn refuses_without_configured_ca() {
        let ca = ca();
        let leaf = issue("localhost", Some("localhost"), Some(&ca));
        let (port, _server) = serve_once(&leaf, ServerOptions::default()).await;
        assert!(dial(port, json!({})).await.is_err());
    }

    #[tokio::test]
    async fn refuses_untrusted_ca() {
        let dir = TempDir::new().unwrap();
        let (ca, other) = (ca(), issue("other-ca", None, None));
        let leaf = issue("localhost", Some("localhost"), Some(&ca));
        let (port, _server) = serve_once(&leaf, ServerOptions::default()).await;
        let tls = json!({"ca_cert": write_cert(&dir, "other.pem", &other)});
        assert!(dial(port, tls).await.is_err());
    }

    #[tokio::test]
    async fn checks_server_name_and_honors_domain() {
        let dir = TempDir::new().unwrap();
        let ca = ca();
        let leaf = issue("tokenizer.test", Some("tokenizer.test"), Some(&ca));
        let ca_path = write_cert(&dir, "ca.pem", &ca);

        let (port, _server) = serve_once(&leaf, ServerOptions::default()).await;
        assert!(dial(port, json!({"ca_cert": ca_path})).await.is_err());

        let (port, _server) = serve_once(&leaf, ServerOptions::default()).await;
        let tls = json!({"ca_cert": ca_path, "domain": "tokenizer.test"});
        dial(port, tls).await.unwrap();
    }

    #[tokio::test]
    async fn refuses_server_without_http2_alpn() {
        let dir = TempDir::new().unwrap();
        let ca = ca();
        let leaf = issue("localhost", Some("localhost"), Some(&ca));
        let opts = ServerOptions {
            no_alpn: true,
            ..Default::default()
        };
        let (port, _server) = serve_once(&leaf, opts).await;
        let err = dial(port, json!({"ca_cert": write_cert(&dir, "ca.pem", &ca)}))
            .await
            .unwrap_err();
        assert!(err.contains("ALPN"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn refuses_x25519_only_server() {
        let dir = TempDir::new().unwrap();
        let ca = ca();
        let leaf = issue("localhost", Some("localhost"), Some(&ca));
        let opts = ServerOptions {
            groups: Some("X25519"),
            ..Default::default()
        };
        let (port, _server) = serve_once(&leaf, opts).await;
        assert!(
            dial(port, json!({"ca_cert": write_cert(&dir, "ca.pem", &ca)}))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn presents_client_certificate() {
        let dir = TempDir::new().unwrap();
        let ca = ca();
        let leaf = issue("localhost", Some("localhost"), Some(&ca));
        let client = issue("client", Some("client"), Some(&ca));
        let opts = ServerOptions {
            client_ca: Some(ca.cert.clone()),
            ..Default::default()
        };
        let (port, server) = serve_once(&leaf, opts).await;
        let tls = json!({
            "ca_cert": write_cert(&dir, "ca.pem", &ca),
            "client_cert": write_cert(&dir, "client.pem", &client),
            "client_key": write_key(&dir, "client.key", &client),
        });
        let mut c = connector("localhost".to_string(), Some(&tls)).unwrap();
        let uri: Uri = format!("https://localhost:{port}").parse().unwrap();
        let stream = c.call(uri).await.unwrap();
        drop(stream);
        assert_eq!(server.await.unwrap(), Ok(true));
    }

    #[test]
    fn rejects_invalid_settings() {
        let dir = TempDir::new().unwrap();
        let ca = ca();
        let other = issue("other", Some("other"), Some(&ca));
        let bad = dir.path().join("bad.pem");
        std::fs::write(&bad, "not a pem").unwrap();
        let bad = bad.to_str().unwrap();

        let err = |tls: json::Value| connector("localhost".to_string(), Some(&tls)).err();
        assert!(err(json!({"ca_cert": bad})).is_some());
        assert!(
            err(json!({"client_cert": bad, "client_key": write_key(&dir, "k", &ca)})).is_some()
        );
        // A key that does not belong to the certificate.
        assert!(err(json!({
            "client_cert": write_cert(&dir, "c.pem", &other),
            "client_key": write_key(&dir, "k2", &ca),
        }))
        .is_some());
    }
}
