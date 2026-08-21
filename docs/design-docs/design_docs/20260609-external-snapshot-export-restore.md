# MEP: External Snapshot Export and Restore

- **Created:** 2026-06-09
- **Status:** Proposed

This document is the single design record for external snapshot restore,
cross-bucket snapshot export/restore, and root relocation of exported snapshot
bundles.

## 1. Problem & Goals

Milvus snapshots were originally scoped to one cluster and one object-storage
bucket. `RestoreSnapshot` restores a snapshot that already exists in the target
cluster metadata. `RestoreExternalSnapshot` must restore from a metadata URI and
support both snapshot layouts that Milvus can store: the normal referenced
layout written by `CreateSnapshot`, and the self-contained bundle layout written
by `ExportSnapshot`.

The feature has five goals:

- Support external snapshot restore from a metadata URI instead of from the
  target cluster snapshot registry.
- Support both referenced snapshots and exported self-contained snapshots as
  first-class `RestoreExternalSnapshot` inputs.
- Support `ExportSnapshot` to the source bucket or another bucket, and support
  `RestoreExternalSnapshot` across buckets when the object-storage provider can
  perform provider-side copy. A same-bucket export must not overwrite any
  object that belongs to the source snapshot.
- Support moving a complete exported bundle to any new root prefix when the
  bundle internal layout is unchanged.
- Do not add any extra root-rewrite parameter; the restore metadata URI is the
  only root-relocation input.

The feature has explicit non-goals:

- No streaming copy. Milvus must not download object bytes to a node and upload
  them again just to cross buckets.
- No cross-provider copy, cross-endpoint copy, or provider-specific source-auth
  extension.
- No external collection export. Its StorageV3 manifests may reference lake
  fragments outside the snapshot file set. Full support requires copying those
  fragments, rewriting the manifest references, and clearing
  `external_source` and `external_spec` from the exported schema.
- No arbitrary metadata layout. The restore metadata URI must still expose
  `<root>/snapshots/{collectionID}/metadata/{snapshotID}.json`.

## 2. User Scenarios

Same-bucket external restore:

1. A source cluster creates a normal snapshot or exports a self-contained
   snapshot under a path readable by the target cluster.
2. The target cluster calls `RestoreExternalSnapshot` with the snapshot metadata
   URI.
3. DataCoord reads the metadata and creates the normal asynchronous restore job.

Referenced snapshot restore:

1. The source cluster calls `CreateSnapshot`.
2. The target cluster calls `RestoreExternalSnapshot` with the returned
   `s3_location`.
3. Restore reads the snapshot metadata and manifest files in place, then copies
   the original referenced segment/index files into the target collection.
4. The source snapshot and referenced files must stay readable until the restore
   job finishes. If the source snapshot is dropped and GC removes referenced
   files, restore fails.

Manual bundle relocation:

1. The caller supplies `export-root` and Export writes the bundle under its
   persisted namespace:
   `export-root/exports/<export-id>/snapshots/100/metadata/1.json` and
   `export-root/exports/<export-id>/files/...`.
2. An operator copies the entire bundle to:
   `restored/x/snapshots/100/metadata/1.json` and `restored/x/files/...`.
3. Restore receives the new metadata URI. Milvus derives `oldRoot` from the
   export-time metadata and `newRoot` from the restore-time metadata URI, then
   rebases self-contained paths from `oldRoot` to `newRoot`.

Export to the source or a foreign bucket:

1. The caller invokes `ExportSnapshot` with a `target_s3_path` in the configured
   source bucket or a foreign bucket.
2. DataCoord validates the request, generates a random export namespace, pins
   the source snapshot, persists the effective
   `<target_s3_path>/exports/<export-id>` root in a `Pending` export job, and
   immediately returns its `job_id`.
3. A background worker resolves the target storage config from the instance
   credential or request `external_spec`.
4. For a same-bucket export, Milvus rejects the job before copying if any
   generated target metadata, segment manifest, or data object key would
   overwrite an object used by the source snapshot.
5. Milvus rejects external collections before enumerating or copying snapshot
   objects because their lake fragments are not yet included in the bundle.
6. The provider performs object copy without streaming through Milvus. The
   caller polls `GetExportSnapshotState` until the job completes or fails.

Restore from a foreign bucket:

1. The caller invokes `RestoreExternalSnapshot` with a foreign metadata URI. The
   URI may point to either a referenced snapshot or a self-contained exported
   snapshot.
2. DataCoord reads metadata and manifests through a foreign-source storage
   manager.
3. DataNode copies segment data into the local bucket using a credential that
   can read the source and write the destination.

Unsupported arbitrary layout:

```text
restored/x/meta.json
restored/x/metadata/1.json
```

These paths do not contain the `snapshots` anchor. Without adding a new request
parameter or persisting a bundle-root field in the metadata, Milvus cannot infer
whether the root is `restored`, `restored/x`, or another ancestor. The request
must fail closed.

## 3. Public API Contract

### 3.1 gRPC APIs

The public request carrier for foreign storage information is only
`external_spec`.

`RestoreExternalSnapshotRequest` contains:

- `db_name`: database routing and namespace context.
- `target_collection_name`: collection created by the restore job.
- `snapshot_metadata_uri`: complete metadata file URI, including scheme and
  host, for either a referenced snapshot or a self-contained exported
  snapshot. Object-key-only restore inputs are rejected.
- `external_spec`: optional JSON storage spec for the foreign source.

`RestoreExternalSnapshotResponse.job_id` is the asynchronous restore job ID. The
caller uses it with `GetRestoreSnapshotState`.

`ExportSnapshotRequest` contains:

- `db_name`: database routing and namespace context.
- `collection_name`: local source collection.
- `snapshot_name`: local snapshot to export.
- `target_s3_path`: destination base root. Each accepted job writes its
  self-contained bundle under `<target_s3_path>/exports/<export-id>`.
- `external_spec`: optional JSON storage spec for the foreign target.

`ExportSnapshotResponse.job_id` identifies the accepted asynchronous export
job. Field 2, `snapshot_metadata_uri`, remains reserved as a deprecated
compatibility field and is empty on submission.

`GetExportSnapshotStateRequest` contains the export `job_id`.
`GetExportSnapshotStateResponse.info` contains the job identity, state,
checkpoint-based progress, copied and total file counts, timing, sanitized
failure reason, total bundle bytes, and the completed bundle metadata URI.
`total_bytes` is exposed for Completed jobs and sums the unique copied data
objects plus generated segment manifests and final metadata. DataCoord computes
and persists it before entering `Publishing`, but both it and the metadata URI
remain hidden until the state is `Completed`.

DataCoord persists an internal `Publishing` state only after all data objects
are copied, final segment manifests are written, and a private
`_staging/metadata.json` object has been written and read back successfully.
The public API maps this state to `Executing` with progress `99`; it does not add
another public enum value or expose the metadata URI before `Completed`.

For remote object storage, `DescribeSnapshot.s3_location` and the completed
export metadata location are credential-free, complete URIs. Standard
S3-compatible providers use
`https://<endpoint>/<bucket>/<object-key>`, native GCS uses `gs://`, and Azure
uses `azure://<account-endpoint>/<container>/<object-key>`. This keeps the
provider endpoint available when the snapshot is restored by another cluster.

The final API does not include `foreign_storage_spec`,
`foreign_credential_ref`, or `external_credential_ref`. Splitting storage config
and credential reference would create two credential models for one provider
copy request, so the API keeps one `external_spec` field aligned with external
table `extfs` shape and snapshot-specific validation.

### 3.2 Go SDK APIs

The Go SDK exposes:

- `ExportSnapshot(ctx, NewExportSnapshotOption(...).WithExternalSpec(...))`
- `GetExportSnapshotState(ctx, NewGetExportSnapshotStateOption(jobID))`
- `RestoreExternalSnapshot(ctx, NewRestoreExternalSnapshotOption(...).WithExternalSpec(...))`
- `GetRestoreSnapshotState(ctx, NewGetRestoreSnapshotStateOption(jobID))`

`WithExternalSpec` is optional. Empty `external_spec` means Layer 1 instance
credential resolution.

### 3.3 REST APIs

REST exposes:

```text
POST /v2/vectordb/jobs/snapshot/export
POST /v2/vectordb/jobs/snapshot/export/describe
POST /v2/vectordb/jobs/snapshot/restore_external
POST /v2/vectordb/jobs/snapshot/describe
POST /v2/vectordb/jobs/snapshot/list
```

REST uses camelCase request fields. The JSON field is `externalSpec`, and the
handler forwards it to the gRPC `external_spec` field.

### 3.4 API Demos

Go SDK export:

```go
exportJobID, err := client.ExportSnapshot(
    ctx,
    milvusclient.NewExportSnapshotOption(
        "snapshot_20260608",
        "source_collection",
        "s3://foreign-bucket/export-root",
    ).WithExternalSpec(`{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`),
)
```

Go SDK export status:

```go
exportInfo, err := client.GetExportSnapshotState(
    ctx,
    milvusclient.NewGetExportSnapshotStateOption(exportJobID),
)
metadataURI := exportInfo.GetSnapshotMetadataUri() // Completed only
```

Go SDK external restore:

```go
jobID, err := client.RestoreExternalSnapshot(
    ctx,
    milvusclient.NewRestoreExternalSnapshotOption(
        "restored_collection",
        "s3://foreign-bucket/export-root/exports/<export-id>/snapshots/100/metadata/1.json",
    ).WithExternalSpec(`{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`),
)
```

Go SDK restore status:

```go
info, err := client.GetRestoreSnapshotState(
    ctx,
    milvusclient.NewGetRestoreSnapshotStateOption(jobID),
)
```

REST export:

```bash
curl -X POST "$MILVUS_ADDR/v2/vectordb/jobs/snapshot/export" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "dbName": "default",
    "collectionName": "source_collection",
    "snapshotName": "snapshot_20260608",
    "targetS3Path": "s3://foreign-bucket/export-root",
    "externalSpec": "{\"extfs\":{\"cloud_provider\":\"aws\",\"region\":\"us-west-2\",\"use_iam\":\"true\"}}"
  }'
```

REST external restore:

```bash
curl -X POST "$MILVUS_ADDR/v2/vectordb/jobs/snapshot/restore_external" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "dbName": "default",
    "targetCollectionName": "restored_collection",
    "snapshotMetadataURI": "s3://foreign-bucket/export-root/exports/<export-id>/snapshots/100/metadata/1.json",
    "externalSpec": "{\"extfs\":{\"cloud_provider\":\"aws\",\"region\":\"us-west-2\",\"use_iam\":\"true\"}}"
  }'
```

REST export status:

```bash
curl -X POST "$MILVUS_ADDR/v2/vectordb/jobs/snapshot/export/describe" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"jobId":"12345"}'
```

REST restore status:

```bash
curl -X POST "$MILVUS_ADDR/v2/vectordb/jobs/snapshot/describe" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"jobId":"12345"}'
```

### 3.5 RBAC and `db_name`

`RestoreExternalSnapshot`, `ExportSnapshot`, and `GetExportSnapshotState` are
Global RBAC operations. The source collection for external restore belongs to
another cluster, and the target collection may not exist when the request
enters Proxy. Authorization must therefore check a global privilege instead of
treating either collection name as the permission object. Export submission and
state query both use `PrivilegeExportSnapshot`.

`db_name` remains in both requests because the database interceptor and
namespace routing still need database context. It is not the RBAC object.

## 4. Credential Model

Snapshot storage credentials must align with Milvus instance storage config.
The API does not add a generic credential abstraction.

Layer 1: instance credential plus bucket policy.

- Empty `external_spec` uses the Milvus instance object-storage credential.
- The same principal must be granted the missing bucket permission: read the
  foreign source for restore, or write the foreign target for export.
- No request secret is persisted in this layer.

Layer 2: request `external_spec.extfs`.

- The request may provide storage-config-compatible fields such as provider,
  region, endpoint, TLS mode, virtual-host mode, `use_iam`, access key ID/value,
  GCP service-account JSON through `credential_json` for native GCS, or Azure
  account key fields when those fields map to the same config structs Milvus
  already uses. Request-level `ssl_ca_cert` is accepted for external-spec
  compatibility but ignored; custom CA trust must come from the Milvus instance
  storage configuration.
- The resolved config must still represent one principal/config that can satisfy
  the provider-side copy request.
- An explicit request credential mode replaces inherited instance credentials.
  `use_iam=true`, raw AK/SK, and `credential_json` are mutually exclusive.
- Snapshot validation is stricter than a generic external spec parser. It must
  reject generic `role_arn`, `gcp_target_service_account`, SAS, anonymous auth,
  source-auth URLs, and independent dual credentials.
- Endpoint, provider, region, and TLS information encoded by the metadata URI
  is authoritative. Conflicting `external_spec` values are rejected. Standard
  AWS, Aliyun, Tencent, Huawei, GCP, and Azure endpoints are recognized;
  unknown custom endpoints still require explicit provider configuration and
  the existing endpoint compatibility checks.

Provider notes:

- S3-compatible storage supports static AK/SK and ambient identity through the
  existing `use_iam` path. Runtime AWS role mechanisms may be provided by the
  environment, but the snapshot request must not contain a generic `role_arn`.
- GCP native storage supports service-account JSON and Application Default
  Credentials through the existing config model.
- Azure storage supports account key mode and the existing workload/managed
  identity path. Request-level account key fields take precedence over the
  process-level `AZURE_STORAGE_CONNECTION_STRING`; a request SAS token is not
  supported.

Restore persistence red line:

`RestoreExternalSnapshot` must propagate the source storage config from Proxy to
DataCoord, through WAL/meta, restore job state, copy segment job state, and
DataNode task execution. Raw secrets inside `external_spec` are therefore
persisted through WAL/meta/job/task state. Operators should prefer Layer 1 or
ambient identity fields such as `use_iam=true`. Logs and errors must use
redacted specs.

## 5. Snapshot Layouts & Root Relocation

`RestoreExternalSnapshot` supports two snapshot layouts.

Referenced snapshots are the normal output of `CreateSnapshot`:

```text
<root>/snapshots/{collectionID}/metadata/{snapshotID}.json
<root>/snapshots/{collectionID}/manifests/...
<root>/insert_log|stats_log|delta_log|index_files|...
```

The metadata and manifests reference the original segment and index files in
place. Restore does not rebase referenced snapshot paths. It derives the source
storage root from the metadata URI and uses that root only to remap copied files
into the target cluster root. Referenced restore is valid only while every
referenced object remains readable.

Exported self-contained bundles use this layout:

```text
<root>/snapshots/{collectionID}/metadata/{snapshotID}.json
<root>/snapshots/{collectionID}/manifests/...
<root>/files/...
```

For self-contained bundles, the `snapshots` directory is the root anchor.
Restore derives:

- `oldRoot` from the export-time metadata path stored in snapshot metadata.
- `newRoot` from the restore-time `snapshot_metadata_uri`.

When the layout is self-contained and `oldRoot != newRoot`, restore rebases
paths from `oldRoot` to `newRoot`. The copied data source root is
`<newRoot>/files`. This safely supports:

```text
old:
export-root/exports/<export-id>/snapshots/100/metadata/1.json
export-root/exports/<export-id>/files/...

new:
restored/x/snapshots/100/metadata/1.json
restored/x/files/...
```

Self-contained root relocation is two-stage:

1. Rebase metadata manifest paths before manifest reads. Otherwise restore would
   attempt to load manifest files from `oldRoot`.
2. Rebase loaded segment/index/binlog data after manifest reads. StorageV3
   manifest paths carry a base path; rebasing that base path is enough for
   manifest-relative data and LOB listing.

Both layouts require metadata URIs that contain the
`snapshots/.../metadata/...` structure. Referenced snapshots need that anchor to
derive the source storage root. Self-contained snapshots additionally need it to
derive the bundle root for relocation. Supporting arbitrary layouts would require
a new request parameter or a new persisted root field, and this feature
explicitly avoids both.

## 6. Cross-Bucket Copy Design

Cross-bucket copy is a provider-side copy capability. There is no streaming
fallback.

The core invariant is:

> There must exist one provider-side copy request whose credential can read the
> source object and write the destination object.

For restore, the destination is the local bucket. The copy credential must read
the foreign source and write the local target. For export, the destination is
the foreign bucket. The copy credential must read the local source and write the
foreign target.

Provider limitations fail closed:

- Different providers cannot be copied by one server-side request.
- Different endpoints or independent MinIO/S3-compatible services cannot be
  copied by one server-side request.
- Request-only source-auth mechanisms such as SAS are outside the snapshot API.
- If provider, endpoint, region, or credential probing shows that copy cannot be
  expressed as one provider-side request, Milvus rejects the request before
  scheduling work.

Metadata reads/writes and large object copy can use different helper objects,
but the large object move itself must be one provider-side copy request.
Export schedules object copies with the refreshable DataCoord configuration
`dataCoord.snapshot.exportCopyConcurrency`, which defaults to `16`. Each export
worker reads the limit once when it starts, so configuration changes affect new
worker attempts without changing an active attempt. Invalid or non-positive
values fall back to `16`. `dataCoord.snapshot.exportMaxConcurrentJobs` defaults
to `1`, `dataCoord.snapshot.exportJobTimeout` defaults to 12 hours including
queue wait, and `dataCoord.snapshot.exportJobRetention` keeps terminal state for
3 hours after pin cleanup. Public snapshot metadata is written only after every
object copy and final segment manifest write succeeds. Each accepted export
receives a random namespace that is stored
in the durable job before object-store work begins. Therefore two clusters can
use the same requested target root without sharing metadata, manifest, or data
object keys, and correctness does not depend on an `Exist` preflight or a
single-DataCoord lock. A failed attempt may leave isolated, unreferenced
objects; Milvus does not remove them automatically.

Same-bucket export is supported, but source protection is an object-level
invariant. Before copy starts, DataCoord builds the complete source object set:
the source metadata file, snapshot segment manifests, StorageV2 manifests, and
all concrete data/index objects. It also builds the destination object set for
the exported metadata, manifests, and `files/...` data. If the sets intersect,
the request fails with an input error. Equal object keys in different buckets
do not intersect and must still be copied.

## 7. Internal Architecture

Proxy:

- Accepts gRPC and REST requests.
- Fills `db_name` through the database interceptor.
- Performs Global RBAC checks for external snapshot APIs.
- Forwards `external_spec` without logging raw secrets.

DataCoord:

- Owns snapshot metadata parsing, validation, export layout generation, restore
  job creation, and WAL restore message emission.
- Owns a durable `SnapshotExportManager`. Submission persists one constant-size
  job record before returning. Reconciliation schedules `Pending`, recovered
  `Executing`, and recovered internal `Publishing` jobs, enforces the configured
  deadline and concurrency limit, retries pin cleanup, and removes
  credential-free terminal jobs after retention.
- Builds a deterministic ordered copy plan and persists its version,
  fingerprint, total file count, and copy cursor. A recovered job resumes only
  when the rebuilt plan matches; otherwise it fails closed.
- Advances public progress only after an entire copy batch is durably
  checkpointed. Uncheckpointed batches may be replayed to the same deterministic
  destination keys after restart.
- For external restore, reads metadata/manifests from the foreign source before
  broadcasting the restore message.
- The WAL ACK callback retries transient source failures. If the source is
  permanently unavailable after the preflight read, it persists a failed
  restore job and returns successfully so broadcaster resource locks are
  released.
- Persists enough external storage information for restore jobs and DataNode
  copy tasks.
- For export, resolves the target in the background, prevents same-bucket
  source-object overwrite, copies data, writes final segment manifests, and
  serializes final metadata to `<bundle-root>/_staging/metadata.json`. It reads
  the staging object back before persisting `Publishing`, the deterministic
  final metadata URI, prepared total bytes, and progress `99`.
- A recovered `Publishing` job resolves only the target storage and reads the
  staging object. It never reads the source snapshot or rebuilds the export
  plan, so source pin expiration or source snapshot deletion cannot invalidate
  publication after this state is durable.
- Final metadata publication is idempotent. If the final object already equals
  staging, publication is complete. Otherwise DataCoord writes it and reads it
  back. A write error is accepted when read-back proves the final bytes equal
  staging; a different final object fails with a data-integrity error. Transient
  or ambiguous storage results remain in `Publishing` for reconciliation retry.
  A missing or corrupt staging object and permanent target-access errors fail
  the job because publication can no longer make progress.
  A separate durable update records `Completed` and end time, then staging is
  removed best-effort. `external_spec` is retained only while a job is
  non-terminal, and the first terminal update clears it atomically.
- Publication replay does not persist a second metadata checksum. It compares
  staging and final metadata bytes directly; the staging path is derived from
  the durable target root, so no additional proto field is required.
- Computes a deterministic fingerprint of external snapshot metadata and loaded
  segment manifests after preflight. The fingerprint is carried through WAL and
  copy-job state so ACK and task assembly reject metadata that changed between
  phases. It does not hash referenced object contents.

DataNode:

- Executes copy segment tasks.
- Accepts external restore copies through the `ExternalCopySegment` worker task
  type. This task type is the capability handshake: workers that predate
  foreign-source copy support reject it before decoding or executing the
  `CopySegmentRequest`.
- Keeps local restore on the existing `CopySegment` task type so it remains
  compatible with older workers. Capability detection does not depend on the
  Milvus version returned by a slot endpoint, which may represent a pooled
  gateway rather than the worker that executes the task.
- Rebuilds source storage config for external restore tasks.
- Copies StorageV1 PB paths, treats a StorageV2 manifest as a concrete object,
  and enumerates StorageV3 manifest objects and LOB files before copying them
  into local target paths.
- Gives each object copy a refreshable
  `dataNode.import.copyObjectTimeout` deadline. Provider SDKs own request-level
  retries; DataNode does not replay the whole copy operation.
- Azure starts an asynchronous copy once. If the SDK observes an existing
  pending copy, the provider resumes polling that operation and validates its
  source URL and copy ID instead of starting another copy.

`snapshotstorage`:

- Parses `external_spec`.
- Validates snapshot-specific allowlists.
- Resolves Layer 1 or Layer 2 object-storage config.
- Builds Go chunk-manager config and internal storage config used by V3/loon
  paths.

Internal proto and WAL propagation:

- Internal DataCoord requests carry `external_spec` for export and external
  restore.
- Restore WAL message headers carry external restore source information.
- Copy segment job/task state carries the resolved external spec for DataNode.

Data flow:

```text
ExportSnapshot:
Proxy -> DataCoord durable Pending job -> background plan/checkpoint loop ->
provider-side copies -> final manifests -> verified staging metadata -> durable
Publishing state -> final metadata byte comparison/publication -> Completed job

GetExportSnapshotState:
Proxy -> DataCoord in-memory cache backed by persisted export job metadata

RestoreExternalSnapshot:
Proxy -> DataCoord -> foreign metadata/manifests -> WAL restore message ->
copy segment job -> DataNode -> provider-side copies into local bucket
```

## 8. Validation & Security

Path validation:

- Reject URI userinfo, query parameters, fragments, unsupported schemes, empty
  object keys, and path traversal forms. Presigned URLs and SAS URLs are not
  accepted credential mechanisms.
- Require restore metadata locations to be complete URIs with a scheme and
  host. Export targets may still use object keys in the instance bucket.
- Require metadata URIs to expose `snapshots/.../metadata/...`.
- Validate self-contained metadata after root relocation against `newRoot`.

Endpoint/provider compatibility validation:

- Parse source and destination locations into provider, endpoint, bucket, and
  object key.
- Reject different providers, incompatible endpoints, and unsupported
  server-side copy combinations.

Access probing:

- Probe source read access before restore scheduling.
- Export does not issue a separate target write probe. The first provider-side
  copy request is the end-to-end check for source read, target write, copy API,
  and KMS permissions.
- A permission failure before `Publishing` transitions the export job to
  `Failed` before public metadata is written.
- The configured export deadline applies to queueing, planning, data-copy,
  manifest, and staging preparation work. Once `Publishing` is durable, the job
  is not downgraded to `Failed` solely because that original deadline elapsed.
  If metadata publication succeeds but the `Completed` catalog update fails,
  reconciliation compares final metadata with staging and retries only the
  completion commit; it does not read the source snapshot.
- A failed attempt may leave unreferenced data or manifest objects. Export does
  not delete them because object paths may already be shared by an older
  published bundle; deleting them could corrupt that bundle. A later retry can
  safely overwrite the immutable snapshot objects.

Secret handling:

- Redact `external_spec` in logs and errors.
- Do not include raw secrets in task labels, metric labels, or user-facing
  failure messages.
- Persist export `external_spec` only for non-terminal restart recovery and
  clear it in the first durable `Completed` or `Failed` update.
- Treat restore raw secret persistence through WAL/meta as an operational red
  line.

Fail-closed behavior:

- If parsing, compatibility validation, access probing, metadata read,
  manifest read, path validation, or provider-side copy resolution is ambiguous,
  reject the request.
- After a restore has entered WAL processing, permanent source errors produce a
  terminal failed job; transient errors continue through broadcaster retry.
- Do not silently fall back to streaming.

## 9. Test Plan

API contract tests:

- gRPC request builders and Proxy forwarding include `external_spec`.
- `RestoreExternalSnapshot` uses Global restore RBAC; `ExportSnapshot` and
  `GetExportSnapshotState` use Global `PrivilegeExportSnapshot` RBAC.
- `db_name` is filled by the database interceptor and is not treated as the RBAC
  object.
- REST `externalSpec` is forwarded, and describe/list snapshot job routes map to
  restore job state APIs.

Resolver and validator tests:

- Empty `external_spec` resolves Layer 1 instance credential.
- `external_spec.extfs` resolves allowed storage-config-compatible fields.
- Request-level `ssl_ca_cert` does not override the instance CA configuration.
- Native GCS `credential_json` maps to the object-storage service-account JSON
  field, while `role_arn`, `gcp_target_service_account`, SAS, anonymous auth,
  and dual credentials are rejected.
- Azure request-level account keys override an ambient connection string while
  Layer 1 instance configuration preserves the existing environment behavior.
- URI query parameters and fragments are rejected and removed from defensive
  log redaction output.
- Redacted spec output never contains secret values.

Root relocation tests:

- `oldRoot -> newRoot` rebase updates metadata manifest paths before reads.
- Loaded segment binlogs, index files, and StorageV3 manifest base paths rebase
  after reads.
- Metadata URI without a `snapshots/.../metadata/...` anchor is rejected.

Restore tests:

- DataCoord external restore reads foreign metadata and creates restore job
  state with `external_spec`.
- DataNode copy tasks rebuild source storage config and copy into local target
  paths.
- During a rolling upgrade, DataCoord submits external restore work as
  `ExternalCopySegment`. An older worker rejects the unknown task type without
  side effects; DataCoord recognizes that capability error and fails the
  restore immediately instead of retrying it until the job timeout. Other
  transient DataNode errors remain retryable. Local restore, import, index, and
  compaction keep their existing task types.
- DataNode invokes each provider copy once within one bounded object-copy
  deadline; provider SDKs retain their request-level retries.
- Azure retries transient copy-status polling without replaying
  `StartCopyFromURL`, resumes a matching pending copy, and rejects source URL or
  copy ID mismatches.
- A copy implementation blocked on object storage exits when
  `dataNode.import.copyObjectTimeout` expires.
- Failure to read source metadata or manifests fails before scheduling unsafe
  work.
- Go-client e2e covers both referenced restore from `DescribeSnapshot.s3Location`
  and self-contained restore from the `ExportSnapshot` metadata URI.

Export tests:

- Submission returns a durable job ID without object-store access; state query
  hides the metadata URI until `Completed`.
- Copy progress advances only after a complete persisted batch, remains
  non-decreasing after restart, and fails closed if the rebuilt plan changes.
- Queue timeout, active-worker timeout, shutdown, finalization replay, terminal
  credential clearing, pin cleanup retry, and retention are covered.
- Internal `Publishing` remains schedulable after the original deadline, maps
  to public `Executing` at `99`, and survives a failed `Completed` catalog write
  without exposing the metadata URI or total bytes early.
- Publishing recovery succeeds after source data is unavailable, treats an
  already matching final object as committed, and verifies an ambiguous final
  metadata write by reading the object back.
- Export to the same bucket succeeds when destination objects do not overlap the
  source snapshot and fails before copy when metadata, manifest, or data objects
  would overlap.
- Export to a foreign target copies equal object keys instead of treating them
  as already present.
- StorageV2 manifests are copied and rewritten as ordinary objects; StorageV3
  manifest objects and LOB files remain manifest-owned.
- Provider/endpoint mismatch rejects before copying.
- Object copies use bounded concurrency, and any copy failure prevents metadata
  from being written without deleting objects that may belong to an older
  published bundle.

Standalone client build:

- Root module and standalone `client/` module both build against the published
  milvus-proto version that contains the snapshot APIs.
