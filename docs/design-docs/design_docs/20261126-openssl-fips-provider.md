# OpenSSL provider and native TLS in FIPS builds

Milvus FIPS builds use the OpenSSL provider covered by
[CMVP certificate #4985](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/4985).
This design carries forward the provider and native TLS work from #55086
for issue #55082, on top of the [Go FIPS build](20261123-fips-go-cryptographic-module.md)
introduced by #55851.

## Build and packaging

Run the following command inside a Linux Milvus build environment and package
its installed files using the Milvus container layout.

```sh
make install MILVUS_FIPS_ENABLED=ON
```

The build sets the `milvus_fips` Go tag and `GOFIPS140=v1.0.0`.
External Go plugins must match the host's build tags, toolchain, and Go module
snapshot. Image builders that stage their own artifacts can use
`make build-fips-provider MILVUS_FIPS_ENABLED=ON` to generate the provider
and its integrity configuration in `cmake_build/fips_provider/artifacts/`.

The build verifies the OpenSSL 3.1.2 source archive by SHA-256 and builds the
provider separately from Milvus's Conan OpenSSL libraries. It uses the module
build procedure in the
[OpenSSL FIPS Provider Security Policy](https://csrc.nist.gov/CSRC/media/projects/cryptographic-module-validation-program/documents/security-policies/140sp4985.pdf)
and generates and verifies the integrity configuration with the 3.1.2
`fipsinstall -pedantic` command. The installer stages the module and its
configuration together. Runtime images carry them into
`/milvus/lib/ossl-modules/fips.so` and
`/milvus/configs/ssl/fipsmodule-validated.cnf`, respectively. RPM and Debian
packages currently use different layouts and reject this build mode.

The provider pin and the application-facing OpenSSL libraries have separate
version requirements. This build uses the 3.1.2 provider with the current
Conan libraries. An upgrade to OpenSSL 3.5 LTS can target the 3.5.9 provider
covered by [CMVP certificate #5586](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5586).

## Startup and TLS

Milvus loads `openssl-fips.cnf` before starting service roles.
Startup requires `fips=yes`, the OpenSSL `fips` provider at version 3.1.2,
and the `base` provider for encoding and decoding. It rejects other active
providers and checks both public and private OpenSSL random generation.

Native TLS connections that inherit OpenSSL's default context policy use
TLS 1.2 or later, OpenSSL security level 2, and the P-256, P-384 and P-521
key exchange groups. Go TLS continues to follow the Go FIPS module.
Libraries that override the OpenSSL context policy or bring their own TLS
implementation require separate verification.

## Validation

Verify the module and its generated integrity configuration together.
Exercise startup with the 3.1.2 provider, the standard Conan provider, and a
missing integrity configuration. Check native TLS handshakes with TLS 1.3
and P-256, TLS 1.2 and P-384, and an X25519-only peer. Run the standard
installer tests to check FIPS artifact staging and that an ordinary build
restores the Conan provider when reusing the installation directory.

Certificate applicability depends on the deployed operational environment
and compliance with the module's Security Policy.
