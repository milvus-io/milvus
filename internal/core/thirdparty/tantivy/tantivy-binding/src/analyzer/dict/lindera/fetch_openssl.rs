//! Lindera dictionary download through OpenSSL, for the
//! `analyzer-remote-tls-openssl` feature.
//!
//! - One HTTP/1.1 GET per connection through hyper's connection API, without
//!   a pool or proxy support (`HTTPS_PROXY` and similar are ignored).
//! - Up to `MAX_REDIRECTS` redirects are followed. A redirect from `https` to
//!   `http` is refused.
//! - `https` uses the shared settings in `crate::analyzer::tls_openssl`, so an
//!   X25519-only server is refused. The server is verified against the system
//!   CA certificates that `openssl-probe` finds. `SSL_CERT_FILE` and
//!   `SSL_CERT_DIR` replace them when either is set. The trust store used for
//!   gRPC tokenizers is separate.
//! - `http` mirrors still work, as on the rustls path. The archive's SHA-256
//!   is checked either way.
//! - SHA-256 is computed through EVP (`openssl::hash`), so it comes from the
//!   provider the process has loaded and a failed fetch is an error.
//!   `openssl::sha::sha256` ignores that failure: with OpenSSL 3.3.2 and no
//!   provider offering SHA-256, it returns an unset buffer as the digest.

use std::env;
use std::error::Error;
use std::path::PathBuf;

use http_body_util::{BodyExt, Empty, Limited};
use hyper::body::Bytes;
use hyper::client::conn::http1;
use hyper::{header, Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use openssl::hash::{hash, MessageDigest};
use openssl::ssl::{SslConnector, SslConnectorBuilder, SslFiletype};
use openssl::x509::store::{X509Lookup, X509StoreBuilder};
use tokio::net::TcpStream;

use crate::analyzer::tls_openssl::{self, BoxError, ALPN_HTTP11};

const MAX_REDIRECTS: usize = 5;
// The largest dictionary archive (unidic) is about 141 MB.
const MAX_BODY_BYTES: usize = 1 << 30;

enum Response {
    Body(Vec<u8>),
    Redirect(Uri),
    Status(StatusCode),
}

pub(super) struct Client {
    tls: SslConnector,
    user_agent: String,
}

impl Client {
    /// A client that trusts the system CA certificates, or those named by
    /// `SSL_CERT_FILE` / `SSL_CERT_DIR` when either is set.
    pub(super) fn new(user_agent: String) -> Result<Client, Box<dyn Error>> {
        let (file, dir) = trust_locations(
            env::var_os("SSL_CERT_FILE").map(PathBuf::from),
            env::var_os("SSL_CERT_DIR").map(PathBuf::from),
            || {
                let probe = openssl_probe::probe();
                (probe.cert_file, probe.cert_dir)
            },
        );
        Self::with_trust(user_agent, file, dir)
    }

    fn with_trust(
        user_agent: String,
        ca_file: Option<PathBuf>,
        ca_dir: Option<PathBuf>,
    ) -> Result<Client, Box<dyn Error>> {
        let mut builder = tls_openssl::base_builder(ALPN_HTTP11)?;
        load_trust(&mut builder, ca_file, ca_dir)?;
        Ok(Client {
            tls: builder.build(),
            user_agent,
        })
    }

    /// GETs `url`, following redirects, and returns the body. Errors are
    /// log-ready messages that name the URL.
    pub(super) async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        let mut uri: Uri = url
            .parse()
            .map_err(|e| format!("Request error from {}: invalid URL: {}", url, e))?;
        for _ in 0..=MAX_REDIRECTS {
            match self.get_once(&uri).await {
                Ok(Response::Body(body)) => return Ok(body),
                Ok(Response::Redirect(next)) => {
                    if uri.scheme_str() == Some("https") && next.scheme_str() != Some("https") {
                        return Err(format!(
                            "Request error from {}: refused redirect from {} to non-https {}",
                            url, uri, next
                        ));
                    }
                    uri = next;
                }
                Ok(Response::Status(status)) => {
                    return Err(format!(
                        "HTTP download failed from {}: HTTP {}",
                        url, status
                    ))
                }
                Err(e) => return Err(format!("Request error from {}: {}", url, e)),
            }
        }
        Err(format!(
            "Request error from {}: more than {} redirects",
            url, MAX_REDIRECTS
        ))
    }

    async fn get_once(&self, uri: &Uri) -> Result<Response, BoxError> {
        let host = uri.host().ok_or("URL has no host")?;
        match uri.scheme_str() {
            Some("https") => {
                let server_name = host.to_ascii_lowercase();
                let port = uri.port_u16().unwrap_or(443);
                let stream = tls_openssl::connect(&self.tls, host, port, &server_name).await?;
                match stream.ssl().selected_alpn_protocol() {
                    None => {}
                    Some(p) if p == ALPN_HTTP11 => {}
                    Some(p) => {
                        return Err(format!(
                            "server selected unexpected ALPN protocol {:?}",
                            String::from_utf8_lossy(p)
                        )
                        .into())
                    }
                }
                self.exchange(TokioIo::new(stream), uri).await
            }
            Some("http") => {
                let port = uri.port_u16().unwrap_or(80);
                let tcp =
                    TcpStream::connect((tls_openssl::strip_ipv6_brackets(host), port)).await?;
                tcp.set_nodelay(true)?;
                self.exchange(TokioIo::new(tcp), uri).await
            }
            other => Err(format!("unsupported URL scheme {:?}", other.unwrap_or("")).into()),
        }
    }

    async fn exchange<IO>(&self, io: IO, uri: &Uri) -> Result<Response, BoxError>
    where
        IO: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    {
        let authority = uri.authority().ok_or("URL has no host")?.as_str();
        let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
        let request = Request::get(path)
            .header(header::HOST, authority)
            .header(header::USER_AGENT, self.user_agent.as_str())
            .header(header::ACCEPT, "*/*")
            .header(header::CONNECTION, "close")
            .body(Empty::<Bytes>::new())?;

        let (mut sender, connection) = http1::handshake(io).await?;
        let connection = tokio::spawn(connection);
        let result = async {
            let response = sender.send_request(request).await?;
            let status = response.status();
            if status.is_success() {
                let body = Limited::new(response.into_body(), MAX_BODY_BYTES)
                    .collect()
                    .await?
                    .to_bytes();
                return Ok(Response::Body(body.to_vec()));
            }
            if is_followed_redirect(status) {
                let location = response
                    .headers()
                    .get(header::LOCATION)
                    .ok_or_else(|| format!("HTTP {} without a Location header", status))?
                    .to_str()?;
                return Ok(Response::Redirect(resolve_location(uri, location)?));
            }
            Ok(Response::Status(status))
        }
        .await;
        connection.abort();
        result
    }
}

fn is_followed_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

/// Resolves an absolute, scheme-relative or path-absolute `Location`.
/// Path-relative locations are not used by dictionary mirrors and are refused.
fn resolve_location(base: &Uri, location: &str) -> Result<Uri, BoxError> {
    let location = location.split('#').next().unwrap_or_default();
    let scheme = base.scheme_str().ok_or("URL has no scheme")?;
    let absolute = if location.starts_with("//") {
        format!("{}:{}", scheme, location)
    } else if location.starts_with('/') {
        let authority = base.authority().ok_or("URL has no host")?;
        format!("{}://{}{}", scheme, authority, location)
    } else {
        location.to_string()
    };
    let uri: Uri = absolute.parse()?;
    if uri.scheme().is_none() || uri.authority().is_none() {
        return Err(format!("unsupported redirect location {:?}", location).into());
    }
    Ok(uri)
}

/// Picks the CA certificate locations: the environment variables when either
/// is set, otherwise what `probe` finds.
fn trust_locations(
    env_file: Option<PathBuf>,
    env_dir: Option<PathBuf>,
    probe: impl FnOnce() -> (Option<PathBuf>, Option<PathBuf>),
) -> (Option<PathBuf>, Option<PathBuf>) {
    if env_file.is_some() || env_dir.is_some() {
        (env_file, env_dir)
    } else {
        probe()
    }
}

fn load_trust(
    builder: &mut SslConnectorBuilder,
    ca_file: Option<PathBuf>,
    ca_dir: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    // SslConnector::builder() trusts OpenSSL's build-time default locations.
    // Replace them so that only the locations chosen here are used.
    builder.set_cert_store(X509StoreBuilder::new()?.build());
    if ca_file.is_none() && ca_dir.is_none() {
        return Err(
            "no CA certificates found for Lindera dictionary download; set SSL_CERT_FILE".into(),
        );
    }
    if let Some(file) = ca_file {
        builder
            .set_ca_file(&file)
            .map_err(|e| format!("load CA certificates from {}: {}", file.display(), e))?;
    }
    if let Some(dir) = ca_dir {
        let dir_str = dir
            .to_str()
            .ok_or_else(|| format!("CA directory {} is not UTF-8", dir.display()))?;
        builder
            .cert_store_mut()
            .add_lookup(X509Lookup::hash_dir())?
            .add_dir(dir_str, SslFiletype::PEM)
            .map_err(|e| format!("use CA directory {}: {}", dir.display(), e))?;
    }
    Ok(())
}

/// Checks `content` against the pinned SHA-256. The message names `url`.
pub(super) fn check_sha256(content: &[u8], expected: &str, url: &str) -> Result<(), String> {
    let digest = hash(MessageDigest::sha256(), content)
        .map_err(|e| format!("SHA-256 unavailable for {}: {}", url, e))?;
    let actual = hex(&digest);
    if actual.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(format!(
            "SHA-256 mismatch from {}! Expected {}, got {}",
            url, expected, actual
        ))
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use openssl::asn1::Asn1Time;
    use openssl::bn::{BigNum, MsbOption};
    use openssl::ec::{EcGroup, EcKey};
    use openssl::nid::Nid;
    use openssl::pkey::{PKey, Private};
    use openssl::ssl::{Ssl, SslAcceptor, SslMethod};
    use openssl::x509::extension::{BasicConstraints, SubjectAlternativeName};
    use openssl::x509::{X509Builder, X509NameBuilder, X509};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    pub(crate) struct Issued {
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
        match (san, issuer) {
            (Some(san), Some(issuer)) => {
                let ext = SubjectAlternativeName::new()
                    .dns(san)
                    .build(&b.x509v3_context(Some(&issuer.cert), None))
                    .unwrap();
                b.append_extension(ext).unwrap();
                b.set_issuer_name(issuer.cert.subject_name()).unwrap();
                b.sign(&issuer.key, MessageDigest::sha256()).unwrap();
            }
            _ => {
                b.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                    .unwrap();
                b.set_issuer_name(&name).unwrap();
                b.sign(&key, MessageDigest::sha256()).unwrap();
            }
        }
        Issued {
            cert: b.build(),
            key,
        }
    }

    pub(crate) struct Pki {
        dir: TempDir,
        ca: Issued,
        pub(crate) server: Issued,
    }

    impl Pki {
        pub(crate) fn new() -> Pki {
            let ca = issue("fetch-test-ca", None, None);
            let server = issue("localhost", Some("localhost"), Some(&ca));
            Pki {
                dir: tempfile::tempdir().unwrap(),
                ca,
                server,
            }
        }

        fn ca_file(&self, ca: &Issued) -> PathBuf {
            let path = self.dir.path().join(format!("ca-{}.pem", rand_suffix()));
            std::fs::write(&path, ca.cert.to_pem().unwrap()).unwrap();
            path
        }

        pub(crate) fn client(&self) -> Client {
            Client::with_trust("test".into(), Some(self.ca_file(&self.ca)), None).unwrap()
        }
    }

    fn rand_suffix() -> u64 {
        let mut b = [0u8; 8];
        openssl::rand::rand_bytes(&mut b).unwrap();
        u64::from_le_bytes(b)
    }

    /// One canned HTTP response per connection. `routes` maps a request path
    /// to the raw head and body. Returns the port and a request counter.
    pub(crate) async fn serve(
        tls: Option<(&Issued, Option<&str>)>,
        routes: Vec<(&'static str, String, Vec<u8>)>,
    ) -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = tls.map(|(server, groups)| {
            let mut b = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).unwrap();
            b.set_certificate(&server.cert).unwrap();
            b.set_private_key(&server.key).unwrap();
            if let Some(groups) = groups {
                b.set_groups_list(groups).unwrap();
            }
            b.build()
        });
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                let routes = routes.clone();
                let counter = counter.clone();
                tokio::spawn(async move {
                    let mut io: Box<dyn Stream> = match acceptor {
                        Some(acceptor) => {
                            let ssl = Ssl::new(acceptor.context()).unwrap();
                            let mut s = tokio_openssl::SslStream::new(ssl, tcp).unwrap();
                            if std::pin::Pin::new(&mut s).accept().await.is_err() {
                                return;
                            }
                            Box::new(s)
                        }
                        None => Box::new(tcp),
                    };
                    let mut buf = vec![0u8; 4096];
                    let mut req = Vec::new();
                    while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                        match io.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => req.extend_from_slice(&buf[..n]),
                        }
                    }
                    counter.fetch_add(1, Ordering::SeqCst);
                    let line = String::from_utf8_lossy(&req);
                    let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
                    let (head, body) = routes
                        .iter()
                        .find(|(p, _, _)| *p == path)
                        .map(|(_, h, b)| (h.clone(), b.clone()))
                        .unwrap_or(("HTTP/1.1 404 Not Found".into(), vec![]));
                    let msg = format!(
                        "{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        head,
                        body.len()
                    );
                    let _ = io.write_all(msg.as_bytes()).await;
                    let _ = io.write_all(&body).await;
                    let _ = io.shutdown().await;
                });
            }
        });
        (port, hits)
    }

    trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
    impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Stream for T {}

    pub(crate) fn ok(body: &[u8]) -> (String, Vec<u8>) {
        ("HTTP/1.1 200 OK".into(), body.to_vec())
    }

    fn redirect(to: String) -> (String, Vec<u8>) {
        (format!("HTTP/1.1 302 Found\r\nLocation: {}", to), vec![])
    }

    pub(crate) fn sha256_hex(data: &[u8]) -> String {
        hex(&hash(MessageDigest::sha256(), data).unwrap())
    }

    #[tokio::test]
    async fn downloads_over_https() {
        let pki = Pki::new();
        let (r, b) = ok(b"archive");
        let (port, _) = serve(Some((&pki.server, None)), vec![("/a.tar.gz", r, b)]).await;
        let body = pki
            .client()
            .get(&format!("https://localhost:{port}/a.tar.gz"))
            .await
            .unwrap();
        assert_eq!(body, b"archive");
    }

    #[tokio::test]
    async fn follows_redirects_and_refuses_downgrade() {
        let pki = Pki::new();
        let (r, b) = ok(b"archive");
        let (plain, _) = serve(None, vec![("/a.tar.gz", r.clone(), b.clone())]).await;
        let (port, _) = serve(
            Some((&pki.server, None)),
            vec![
                ("/a.tar.gz", r, b),
                ("/rel", redirect("/a.tar.gz".into()).0, vec![]),
                (
                    "/down",
                    redirect(format!("http://localhost:{plain}/a.tar.gz")).0,
                    vec![],
                ),
                ("/loop", redirect("/loop".into()).0, vec![]),
            ],
        )
        .await;
        let client = pki.client();
        let url = |p: &str| format!("https://localhost:{port}{p}");

        assert_eq!(client.get(&url("/rel")).await.unwrap(), b"archive");
        let err = client.get(&url("/down")).await.unwrap_err();
        assert!(err.contains("non-https"), "unexpected error: {err}");
        let err = client.get(&url("/loop")).await.unwrap_err();
        assert!(err.contains("redirects"), "unexpected error: {err}");
        // http -> https is allowed, and plain http mirrors still work.
        assert_eq!(
            client
                .get(&format!("http://localhost:{plain}/a.tar.gz"))
                .await
                .unwrap(),
            b"archive"
        );
    }

    #[tokio::test]
    async fn refuses_untrusted_ca_and_x25519_only_server() {
        let pki = Pki::new();
        let (r, b) = ok(b"archive");
        let (port, hits) = serve(
            Some((&pki.server, None)),
            vec![("/a", r.clone(), b.clone())],
        )
        .await;
        let other = issue("other-ca", None, None);
        let untrusted = Client::with_trust("test".into(), Some(pki.ca_file(&other)), None).unwrap();
        assert!(untrusted
            .get(&format!("https://localhost:{port}/a"))
            .await
            .is_err());

        let (x_port, x_hits) = serve(Some((&pki.server, Some("X25519"))), vec![("/a", r, b)]).await;
        assert!(pki
            .client()
            .get(&format!("https://localhost:{x_port}/a"))
            .await
            .is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert_eq!(x_hits.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn checks_sha256() {
        let expected = sha256_hex(b"archive");
        assert!(check_sha256(b"archive", &expected, "u").is_ok());
        assert!(check_sha256(b"archive", &expected.to_uppercase(), "u").is_ok());
        let err = check_sha256(b"tampered", &expected, "u").unwrap_err();
        assert!(err.contains("SHA-256 mismatch"), "unexpected error: {err}");
    }

    #[test]
    fn environment_replaces_probed_trust() {
        let probed = || {
            (
                Some(PathBuf::from("/probe.pem")),
                Some(PathBuf::from("/probe")),
            )
        };
        assert_eq!(
            trust_locations(Some("/env.pem".into()), None, probed),
            (Some(PathBuf::from("/env.pem")), None)
        );
        assert_eq!(
            trust_locations(None, None, probed),
            (
                Some(PathBuf::from("/probe.pem")),
                Some(PathBuf::from("/probe"))
            )
        );
        assert!(Client::with_trust("t".into(), None, None).is_err());
        assert!(Client::with_trust("t".into(), Some("/missing.pem".into()), None).is_err());
    }

    #[test]
    fn resolves_locations() {
        let base: Uri = "https://a.example:8443/x/y.tar.gz".parse().unwrap();
        let r = |l: &str| resolve_location(&base, l).map(|u| u.to_string());
        assert_eq!(r("/z.tar.gz").unwrap(), "https://a.example:8443/z.tar.gz");
        assert_eq!(r("//b.example/z").unwrap(), "https://b.example/z");
        assert_eq!(r("http://c.example/z#frag").unwrap(), "http://c.example/z");
        assert!(r("z.tar.gz").is_err());
    }
}
