# Analyzer file security

Local dictionary paths are relative to, or absolute descendants of,
`function.analyzer.local_resource_path`. This startup-only directory must be
absolute and cannot be `/`. Reads reject traversal, symlinks and non-regular
files, then take a bounded snapshot before parsing. `function.analyzer.max_dict_size`
limits each local or downloaded dictionary to 64 MiB by default. Parser errors
must not echo dictionary contents, and native errors must not append analyzer
parameters. Lindera's path-only parser receives a private temporary snapshot.

`common.fileResource.allowedPath` is a startup-only complete object-key prefix
(remote storage) or absolute directory (local storage). Empty disables resource
access. Configure it before upgrading; move existing resources into this range
and update their registrations as necessary. Admission and downloads reject
out-of-range paths before calling the storage backend. Resource filenames in
analyzer parameters must be single basenames, so they cannot select another
resource's cached files. `common.fileResource.maxFileSize` limits both admission
and actual downloads; the default is 64 MiB and zero no longer disables it.

These are deployment-local access policies, not replicated metadata. CDC still
replicates resource registration through the existing WAL message. Every
cluster must provision compatible allowed resource prefixes and local dictionary
files; replay does not grant permission to read outside its local policy.

Request-supplied gRPC tokenizer TLS file paths (`ca_cert`, `client_cert`,
`client_key`) are rejected before reading files or connecting.
