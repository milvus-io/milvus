# Analyzer file security

Directory and byte-limit policies apply at admission: new collection analyzers,
changed analyzer definitions, inline RunAnalyzer/ComputePhraseMatchSlop, and new
file-resource registrations. They do not retroactively revoke persisted schemas
or registered resources when operators change configuration, including on restart,
index rebuild, WAL recovery, or cache eviction. Unchanged named analyzers retain
their resource IDs when other schema fields or analyzers change.

Request-selected TLS files use the generic startup-only `tls.allowedPath`,
defaulting to `<localStorage.path>/tls`. New gRPC tokenizer `ca_cert`, `client_cert`
and `client_key` reads use this directory and a fixed 1 MiB limit per file.
The client certificate and key must be supplied together. Custom CA and mTLS
remain supported; no new HTTPS trust roots are installed.

Local dictionaries use the startup-only `function.analyzer.local_resource_path`,
defaulting to `<localStorage.path>/analyzer`. New dictionary reads are limited by
`function.analyzer.max_dict_size` (64 MiB by default). Admission resolves relative
paths against the appropriate base and requires absolute paths to be descendants.
Descriptor-relative reads reject traversal, symlinks and non-regular files before
reading. Text dictionaries stream through a bounded reader; TLS and binary
Lindera dictionaries use byte snapshots, and Lindera CSV uses a temporary file.

Create/Alter resolves new relative dictionary and TLS paths to absolute paths in
the schema before validation and WAL publication. Old definitions remain unchanged,
including legacy relative paths that use the process working directory. Recovery
uses the persisted definition, without the current admission directory or limit.
Only internal schema consumers select this restoration path; client-supplied
analyzer JSON cannot select it. Native validation always enforces admission even
if its server extra info contains a restoration flag. Parser errors remain redacted
on both paths and must not append dictionary contents or analyzer parameters.

`common.fileResource.allowedPath` bounds new source registrations. Remote sources
use complete bucket-relative keys, defaulting to `<storage-root>/file_resource`
(normally `files/file_resource`); they are not rebased or cleaned. Local sources
use `<localStorage.path>/file_resource` by default, and relative request paths are
resolved before persisting registration. Registration checks both reported size
and actual streamed bytes against `common.fileResource.maxFileSize` (64 MiB by
default). Already registered sources download from their recorded path without
reapplying the operator directory/size policy. Downloads still bound reads to the
reported object size and reject size changes without publishing a partial cache.
The per-node cache remains separate from source directories.

Admission is not a content snapshot or a guarantee that files cannot be replaced.
Operators must preserve admitted paths and control their contents and permissions;
TLS credentials should remain outside dictionary/resource trees. Lowering a limit
or changing a base does not revoke old definitions. Missing or invalid files can
still fail recovery. CDC carries resolved schema paths and registrations through
the existing WAL messages; each cluster must provision those paths and contents.
No new metadata write outside WAL or public protocol field is introduced.
