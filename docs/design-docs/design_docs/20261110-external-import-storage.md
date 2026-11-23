# MEP: Per-request source storage for ordinary bulk import

- **Created:** 2026-11-10
- **Author(s):** @Sallery-X
- **Status:** Under Review
- **Component:** DataNode, Coordinator, Proxy, Storage
- **Related Issues:** #55301
- **Released:** Not released

## Summary

An ordinary bulk import can read files directly from a caller's S3-compatible
bucket without copying them into Milvus storage. Its source connection belongs
to that import job. Segment, index, intermediate fragment, and result writes
continue to use the cluster's configured storage.

This covers JSON, JSONL, CSV, Parquet, and NumPy imports through both Import V2
and V3. Snapshot, backup/binlog, and L0 imports keep their separate contracts.
No protobuf changes or cluster storage configuration changes are required.

## Motivation

Data owners currently have to copy ordinary import files into the cluster's
configured bucket. Large datasets incur an extra transfer and temporary storage,
and the copy requires write access to the cluster's internal bucket. Per-request
source storage allows the existing import pipeline to read those files directly.

## Public interfaces

The existing import `options` map/repeated key-value field accepts:

| Option | Requirement/default |
| --- | --- |
| `storage.type` | Optional; `remote` is the only supported value and the default |
| `storage.endpoint` | Required; host or host:port, without scheme, credentials, path, query, or fragment |
| `storage.bucket` | Required; valid S3 bucket name |
| `storage.accessKeyID` | Required; nonblank static access key |
| `storage.secretAccessKey` | Required; nonblank static secret key |
| `storage.useSSL` | Optional boolean; defaults to `true` |
| `storage.region` | Optional; SDK region discovery applies when omitted |
| `storage.useVirtualHost` | Optional boolean; defaults to `false`, using the existing SDK/provider lookup behavior |

Booleans use Go's `strconv.ParseBool` convention. Option names are case
sensitive; unknown `storage.*` options, including incorrectly cased names, are
rejected. Supplying any storage option requires a complete independent source
configuration. Missing, duplicate, and malformed values fail with parameter
errors before scheduling; error messages do not echo supplied option values.

Without storage options, import retains its existing cluster-storage behavior.
The source never inherits cluster credentials, IAM, bucket, or root path.
It uses the existing S3-compatible client factory and static credentials.

> Note: `storage.endpoint` is validated for host/port syntax only. Milvus does
> not filter private, loopback, or cloud metadata addresses or perform DNS/SSRF
> checks on caller-supplied endpoints. Operators in shared or untrusted
> environments should restrict worker egress at the network/infrastructure layer.

Example REST request body for the existing import endpoint:

```json
{
  "collectionName": "target",
  "files": [["exports/part-000.parquet"]],
  "options": {
    "storage.endpoint": "s3.us-east-1.amazonaws.com",
    "storage.bucket": "source-bucket",
    "storage.accessKeyID": "<source-access-key>",
    "storage.secretAccessKey": "<source-secret-key>",
    "storage.region": "us-east-1"
  }
}
```

File names are complete bucket-relative object keys validated against S3 object
name rules (nonblank UTF-8 up to 1024 bytes, without a leading `/` or `://`
scheme; valid S3 keys containing colons are preserved). Milvus does not prepend
a source root, normalize dot components, or collapse repeated separators. The
cluster's internal-layout denylist does not apply to an explicitly authenticated
source bucket; the source credentials determine object access. There is no
implicit fallback to cluster storage after a source failure.

A source client does not create buckets or perform an initialization-time
bucket-existence probe. Object GET/HEAD permissions are sufficient when the
region is supplied; omitted regions can require SDK bucket-location discovery.
Readers issue only source reads. Destination writers keep their original
chunk manager, storage config, and encryption context.

## Design details: execution and recovery

Snapshot import and external source storage remain orthogonal: snapshot/backup/L0
imports use `ValidateImportSourceRequest`, `ValidateImportSourceOptions`, and
`ValidateImportPlan` unchanged, while ordinary external-storage imports
parse options into a typed `SourceStorageConfig` via `GetSourceStorageConfig` and
validate object keys via `ValidateSourceStorageFilePaths` at Proxy admission
and in DataCoord (`ValidateImportFilePaths`) before WAL broadcast. Because only
validated messages enter the Import WAL, the ACK callback records the job
directly without mutating or re-validating durable source options.

The job's options travel unchanged in preimport/import requests and V3 reshard
plans. Each DataNode worker task entry point (`importv2.PreImportTask`,
`importv2.ImportTask`, `importv3.preImportTask`, and
`importv3.reshardPlanExecutor`) resolves its source once via
`NewSourceChunkManager(ctx, options, fallback) -> (sourceCM, err)`:
- When no `storage.*` options are present, `sourceCM` aliases the cluster
  `ChunkManager`.
- When `storage.*` options are present, `NewSourceChunkManager` builds one
  read-only external S3-compatible `ChunkManager` and shares it across all file
  readers and V3 Parquet/NumPy metadata counters in that task attempt.

`ReaderFactory`, `NewReader`, and `simpleRowCount` stay agnostic to `storage.*`
options and simply read from the `sourceCM` passed to them, while destination
writers continue to use the worker's cluster `ChunkManager`.

No coordinator writes a new state outside the existing broadcast/ACK path.
All segment publication and import completion behavior remains in that path.

## Compatibility, deprecation, and migration plan

External-storage imports are rejected on a cluster with a replicate topology,
even when ordinary imports on replicating clusters are enabled and manual
commit is requested. The existing admission check and its recheck under the
shared-cluster broadcast lock enforce this. This feature does not establish a
cross-cluster source-credential or source-lifetime contract.

All proxies, coordinators, and import workers must be upgraded before clients
use these options. Older workers may ignore unknown options and read cluster
storage. No wire-level capability negotiation is added. Source objects and
credentials must remain available through preimport, import, and retries;
this feature does not snapshot source object versions or rotate credentials.

## Credentials and diagnostics

Request logging uses `RedactOptions`, which redacts every `storage.*` value,
including case variants and unknown future keys, without mutating the live
request. This covers REST/gRPC import trace logging and DataCoord submission
and ACK option logs. Existing public progress/list responses do not return job
options or storage configs. Bucket names and file keys may still appear in
ordinary storage diagnostics.

Active job catalog entries and the Import WAL body contain source credentials
because recovery needs them. Internal task RPCs and in-memory V3 reshard plans
also carry them. On `Completed`/`Failed` transitions,
`RemoveSourceStorageCredentials` removes secret (`storage.accessKeyID`,
`storage.secretAccessKey`) and any unknown `storage.*` options from the retained
job record in the same catalog update while preserving non-secret structural
options (`storage.endpoint`, `storage.bucket`, etc.). Preserving structural
options ensures a terminal external-storage job retains its external-source
identity and cannot silently fall back to cluster storage. This
cleanup does not erase historical WAL entries, metadata backups, or
already-dispatched worker memory; their existing retention/lifecycle rules still
apply. Protect these internal systems as credential-bearing data. Encryption at
rest and secret references are outside this change.

Storage errors retain the existing object-storage/reader error handling.
Request-option redaction does not sanitize arbitrary messages returned by a
source server; a server reflecting sensitive values in its error body remains
a limitation of the existing storage diagnostics. This change does not claim
to solve that broader error-sanitization problem.

## Test plan and verification

Regression tests cover independent source credentials, unchanged destination
configuration, every ordinary file format, V2 destination sync selection,
V3 metadata counting and reshard reads, request-log redaction, durable terminal
credential cleanup, replication rejection, and option/path validation.
Source fault tests cover missing objects/buckets, access denial, invalid access
keys, malformed JSON, a transient SlowDown response, and cancellation before
client initialization. Destination mocks reject any accidental fallback reads.

Standalone integration validation passed for both Import V2 and V3 using the
current-branch binary and two independent MinIO instances. A source account
with GetObject permission but no write or ListBucket permission imported all
five formats; imported row counts and exact primary-key/vector values matched.
After stopping the source, releasing and loading each collection still returned
the expected data. Admission failures, wrong credentials, a missing source
object with a same-name destination decoy, malformed JSON, ordinary imports,
and detailed request-log/public-response redaction also passed. Runtime logs
confirmed that each instance used its intended V2 or V3 implementation.

Tests requiring Milvus native libraries must run in a configured Milvus build
environment. The source-selection change makes no new general retry or
error-classification guarantee beyond the existing readers and SDK. Sustained
outages, credential rotation, process crash recovery, a full replication
topology, and arbitrary source-server error reflection remain unverified by
these integration checks. Terminal credential cleanup is covered by catalog
mock regressions, not a deployed-catalog recovery test.

## Rejected alternatives

- Copying source files into the cluster bucket retains the extra transfer,
  storage cost, and destination write-permission requirement.
- Changing the cluster's global storage configuration affects existing segment
  and index access and cannot isolate the source to one import request.
- Replacing the task's destination chunk manager with the source client would
  also redirect segment or intermediate writes. Readers instead resolve a
  separate client while writers retain their original configuration.

## References

- Issue: https://github.com/milvus-io/milvus/issues/55301
- Storage path contract: `docs/agent_guides/storage/path_contract.md`
