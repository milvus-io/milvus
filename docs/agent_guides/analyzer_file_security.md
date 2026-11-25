# Analyzer file security

Request-supplied gRPC tokenizer TLS file paths (`ca_cert`, `client_cert`,
`client_key`) are rejected before reading files or connecting.

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

`common.fileResource.allowedPath` is a startup-only absolute base directory for
local file resources. Empty uses `<localStorage.path>/file_resource` (default
`/var/lib/milvus/data/file_resource`). Relative request paths resolve beneath
this base; absolute paths must already be descendants of it. These source
directories are operator-provisioned, separate from downloaded resource caches
under `<localStorage.path>/cache/<nodeID>/file_resource/`.

For remote storage, `common.fileResource.allowedPath` remains a complete
object-key prefix, and empty disables access. Remote request paths must be
complete keys under that prefix; they are never rebased or path-cleaned.
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
