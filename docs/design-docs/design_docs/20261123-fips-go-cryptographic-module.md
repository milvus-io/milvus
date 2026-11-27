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

**Build.** `MILVUS_FIPS_ENABLED=ON` pins `GOFIPS140=v1.0.0` for all Go
commands run by Make and adds `milvus_fips` to the server build tags, including
custom tag lists. Go 1.26.10 resolves this selector to the certified snapshot
`v1.0.0-c2097c7c`. External Go plugins must use the server's Go toolchain,
module snapshot, and matching tags (including `milvus_fips`). External packaging
builds can query the selector with
`make -s --no-print-directory MILVUS_FIPS_ENABLED=ON print-fips-go-module`.

**OpenSSL.** `pkg/util/fips` enables the OpenSSL FIPS provider under the
`milvus_fips` tag. After loading the configuration, it checks the default
`fips=yes` property and the provider backing the shared primary random generator
and the startup thread's public and private generators. Both public and private
random generation must succeed. Embedding hosts must configure FIPS before
initializing the OpenSSL random generators, since OpenSSL retains generators
across changes to default properties. `MaybeEnableOpenSSLFIPS` still drives
CRC32C for S3.

**Startup.** `milvus run` calls `fips.Require` before starting service roles. In a `milvus_fips`
build it returns an error, and Milvus exits with status 1, when:

- Go build metadata is missing or its resolved `GOFIPS140` value differs
  from `v1.0.0-c2097c7c`;
- Go FIPS mode is off at run time (`GODEBUG=fips140=off`);
- the OpenSSL FIPS provider could not be enabled.

With `--run-with-subprocess`, the supervisor finishes session cleanup and
propagates an explicit nonzero exit status from the run child in both build
modes. A FIPS startup failure therefore reaches the caller as exit status 1.
Outside a `milvus_fips` build, `fips.Require` returns nil.

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

Default builds continue to use Go's standard cryptography with their existing FIPS mode defaults.

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
