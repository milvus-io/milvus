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

Directory and byte limits are admission rules: new or changed schema analyzers
and every inline analyzer request are checked. Persisted definitions restore
without applying the current operator directory or byte limit, including after
restart and index rebuild. Errors remain redacted. New relative TLS paths are
resolved to absolute paths before validation and schema WAL publication, so a
later base change cannot retarget them. Existing definitions are unchanged;
legacy relative paths retain their process-working-directory meaning. Altering
unrelated fields does not revalidate existing analyzers.

Admission does not freeze file contents. Operators must retain the admitted
files and protect them from replacement; missing or invalid files can still
fail recovery. CDC carries the resolved paths in the existing schema messages;
corresponding paths and contents must be provisioned on each cluster. No public
RPC or protobuf field is added. The 2.6 internal tokenizer C interface carries
an explicit restoration flag because it has no resource-extra-info argument.
