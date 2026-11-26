# Analyzer file security

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

Local dictionary paths are relative to, or absolute descendants of,
`function.analyzer.local_resource_path`. This startup-only directory must be
absolute and cannot be `/`. When unset or empty, it uses
`<localStorage.path>/analyzer` (default `/var/lib/milvus/data/analyzer`).
An explicit absolute path overrides this default. Deployments using the former
`/var/lib/milvus/analyzer` default must set that path explicitly or move their
dictionaries, including any cached Lindera dictionaries, before upgrading.
Reads reject traversal, symlinks and non-regular
files, then take a bounded snapshot before parsing. `function.analyzer.max_dict_size`
limits each local or downloaded dictionary to 64 MiB by default. Parser errors
must not echo dictionary contents, and native errors must not append analyzer
parameters. Lindera's path-only parser receives a private temporary snapshot.

`common.fileResource.allowedPath` is a startup-only source boundary. For S3 and
other remote storage, empty uses `<storage-root>/file_resource`, normally
`<minio.rootPath>/file_resource` (default `files/file_resource`). This is a
bucket-relative object-key prefix, unrelated to `localStorage.path`. For example,
`AddFileResource(path="files/file_resource/dict.txt")` reads that object in the
configured bucket. An explicit prefix overrides the default. Remote request
paths must be complete keys under the prefix; they are never rebased or
path-cleaned.

With a local storage backend, empty uses `<localStorage.path>/file_resource`
(default `/var/lib/milvus/data/file_resource`). Relative request paths resolve
beneath this base; absolute paths must already be descendants of it. Source
files are operator-provisioned, separate from downloaded resource caches under
`<localStorage.path>/cache/<nodeID>/file_resource/`.

Configure an appropriate source boundary before upgrading; move existing
resources into it and update registrations as necessary. Admission and downloads reject
out-of-range paths before calling the storage backend. Resource filenames in
analyzer parameters must be single basenames, so they cannot select another
resource's cached files. `common.fileResource.maxFileSize` limits both admission
and actual downloads; the default is 64 MiB and zero no longer disables it.

These are deployment-local access policies, not replicated metadata. CDC still
replicates resource registration through the existing WAL message. Every
cluster must provision compatible allowed resource prefixes and local dictionary
files; replay does not grant permission to read outside its local policy.
