# FIPS builds on the Go Cryptographic Module

Related issue: #55850.

`MILVUS_FIPS_ENABLED=ON` used BoringCrypto (`GOEXPERIMENT=boringcrypto`) for
Go cryptography and loaded OpenSSL's FIPS provider for C++. This change moves
the Go side to Go's native FIPS 140-3 module and makes a FIPS build refuse to
start outside FIPS mode.

## Why

- **The module is validated.** The Go Cryptographic Module v1.0.0 has CMVP
  certificate #5247, and its approved algorithms include PBKDF. When
  BoringCrypto was adopted (#48331), the native module was not yet validated.
- **BoringCrypto is on its way out.** Go documents it as an unsupported
  mechanism that will be removed in a future release.
- **BoringCrypto runs hashing through cgo.** PBKDF2 makes hundreds of
  thousands of cgo calls into BoringSSL assembly per password. A signal that
  lands in that assembly makes cgosymbolizer unwind through code without unwind
  information, and Milvus segfaults (#55557). The native module is Go code, so
  hashing makes no cgo calls.
- **A FIPS build could run without FIPS.** When OpenSSL's FIPS provider failed
  to load, Milvus logged it and started anyway.

## Design

**Build.** `MILVUS_FIPS_ENABLED=ON` exports `GOFIPS140=$(MILVUS_FIPS_GO_MODULE)`
(default `v1.0.0`) to every Go command `make` runs, and adds the `milvus_fips`
build tag to `MILVUS_GO_BUILD_TAGS`. `GOFIPS140` is exported rather than added
to one command, because a Go plugin only loads into a binary built with the
same value. Plugins built outside this Makefile must use both the same
`GOFIPS140` and the `milvus_fips` tag.

**OpenSSL.** `pkg/util/fips` enables the OpenSSL FIPS provider under the
`milvus_fips` tag instead of `boringcrypto`. Nothing else in it changes: the
same configuration file, the same checks, and `MaybeEnableOpenSSLFIPS` still
drives CRC32C for S3.

**Startup.** `fips.Require` runs first in `milvus run`. In a `milvus_fips`
build it returns an error, and Milvus exits with status 1, when:

- the binary was not built against a frozen module version
  (`crypto/fips140.Version()` is `latest`);
- Go FIPS mode is off at run time (`GODEBUG=fips140=off`);
- the OpenSSL FIPS provider could not be enabled.

Outside a `milvus_fips` build it returns nil.

**PBKDF2.** `pkg/util/crypto` uses `crypto/pbkdf2` instead of its own HMAC
loop. The loop existed only because `crypto/pbkdf2` bypasses BoringCrypto.
Stored hashes are unchanged; the RFC known-answer tests still pass.

## Behaviour changes for MILVUS_FIPS_ENABLED=ON

- **Startup is fail-closed.** A deployment whose OpenSSL FIPS configuration
  does not load now fails to start instead of running without it.
- **Go TLS is restricted.** In FIPS mode `crypto/tls` negotiates only approved
  versions, cipher suites and key exchanges. Peers that offer only TLS 1.0 or
  1.1, only ChaCha20-Poly1305, or only X25519 key exchange fail the handshake,
  and TLS 1.2 peers must support Extended Master Secret. This covers etcd,
  object storage, the SDK and gRPC between components. `etcd.ssl.tlsMinVersion`
  and `minio.ssl.tlsMinVersion` values of `1.0` or `1.1` act as `1.2`.
- **The banner line changes** from `Milvus FIPS in Go: BoringCrypto <bool>` to
  `FIPS: on (Go Cryptographic Module v1.0.0, OpenSSL FIPS provider)`.

Default builds do not change.

## Not covered

- OpenSSL's FIPS provider is still built from Conan's OpenSSL 3.3.2, which is
  not a validated version, and `openssl-fips.cnf` still activates the default
  provider. The C++ part of #55086 addresses both.
- Rust crates and other libraries in the image that bundle their own
  cryptography (#55083 and related issues).
- Go tools built from this repository (`binlog`, `meta-migration`, ...) now
  use the Go module but do not enable OpenSSL's FIPS provider.
- bcrypt stays the default password hash; FIPS deployments should select
  `pbkdf2-sha512`.
