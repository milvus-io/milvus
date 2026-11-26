# Analyzer TLS file security

Request-selected TLS files use the generic startup-only `tls.allowedPath`
base directory, defaulting to `<localStorage.path>/tls` (normally
`/var/lib/milvus/data/tls`). The gRPC tokenizer uses it for `ca_cert`,
`client_cert` and `client_key`. Relative paths resolve beneath the base;
absolute paths must be descendants. Reads reject traversal, symlinks and
non-regular files and enforce a fixed 1 MiB limit per file before TLS parsing.
Read/configuration failures abort before connecting, without echoing file contents.
Client certificates and private keys must be supplied together.

Operators must provision this directory separately from every dictionary or
file-resource source directory; never place private keys in those readable
resource trees. Only provision credentials intended for this consumer: directory
confinement does not authorize endpoints or restrict which caller can select a
credential. Existing server/internal TLS path settings are unchanged. This fix
preserves custom CA and mTLS support using confined files; it does not introduce
public CA roots. TLS-disabled builds continue to reject HTTPS endpoints.
