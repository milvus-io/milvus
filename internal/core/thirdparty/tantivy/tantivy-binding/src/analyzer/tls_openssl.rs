//! OpenSSL TLS client settings for gRPC tokenizers.
//!
//! Compiled only with the `analyzer-remote-tls-openssl` feature. Callers get
//! these protocol settings:
//!
//! - TLS 1.2 or later, with ECDHE and AEAD cipher suites.
//! - Key exchange limited to P-256 and P-384, so a server that only offers
//!   X25519 is refused. The rustls path accepts such a server.
//! - Server certificate and host name verification, with the trust store
//!   chosen by the caller.
//!
//! Beyond that, the OpenSSL provider the process has loaded decides what can
//! be negotiated. A FIPS provider removes ChaCha20-Poly1305 and leaves AES-GCM.
//! The group list has to be set here because the provider does not remove
//! X25519: OpenSSL 3.1.2 registers it with fips=yes, although the module's
//! Security Policy (certificate #4985, Table 8) lists it as not allowed in
//! the Approved mode.

use std::pin::Pin;

use openssl::error::ErrorStack;
use openssl::ssl::{SslConnector, SslConnectorBuilder, SslMethod, SslVersion};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) const ALPN_H2: &[u8] = b"h2";

// TLS 1.2 suites matching what the rustls path can negotiate. TLS 1.3 suites
// are left at the OpenSSL default, which is also AEAD only.
const TLS12_CIPHERS: &str = "ECDHE+AESGCM:ECDHE+CHACHA20";
const KEY_EXCHANGE_GROUPS: &str = "P-256:P-384";

/// A client builder with the shared protocol settings, offering `alpn` as the
/// only application protocol. The trust store is OpenSSL's default; callers
/// replace it.
pub(crate) fn base_builder(alpn: &[u8]) -> Result<SslConnectorBuilder, ErrorStack> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    builder.set_min_proto_version(Some(SslVersion::TLS1_2))?;
    builder.set_cipher_list(TLS12_CIPHERS)?;
    builder.set_groups_list(KEY_EXCHANGE_GROUPS)?;
    builder.set_alpn_protos(&alpn_wire(alpn))?;
    Ok(builder)
}

fn alpn_wire(alpn: &[u8]) -> Vec<u8> {
    debug_assert!(!alpn.is_empty() && alpn.len() <= 255);
    let mut wire = Vec::with_capacity(alpn.len() + 1);
    wire.push(alpn.len() as u8);
    wire.extend_from_slice(alpn);
    wire
}

pub(crate) fn strip_ipv6_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Opens a TCP connection to `host:port` and completes a TLS handshake that
/// verifies the certificate against `server_name`, which is also sent as SNI.
/// The caller checks the negotiated ALPN protocol.
pub(crate) async fn connect(
    connector: &SslConnector,
    host: &str,
    port: u16,
    server_name: &str,
) -> Result<SslStream<TcpStream>, BoxError> {
    let tcp = TcpStream::connect((strip_ipv6_brackets(host), port)).await?;
    tcp.set_nodelay(true)?;

    let ssl = connector
        .configure()?
        .into_ssl(strip_ipv6_brackets(server_name))?;
    let mut stream = SslStream::new(ssl, tcp)?;
    Pin::new(&mut stream).connect().await?;
    Ok(stream)
}
