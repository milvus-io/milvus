# Design: External Snapshot Export and Restore

Status: Proposed

This document is the single design record for external snapshot restore,
cross-bucket snapshot export/restore, and root relocation of exported snapshot
bundles.

## 1. Problem & Goals

Milvus snapshots were originally scoped to one cluster and one object-storage
bucket. `RestoreSnapshot` restores a snapshot that already exists in the target
cluster metadata. `ExportSnapshot` writes a self-contained bundle, but the copy
path and path validation were still built around one configured bucket.

The feature has four goals:

- Support external snapshot restore from a metadata URI instead of from the
  target cluster snapshot registry.
- Support `ExportSnapshot` and `RestoreExternalSnapshot` across buckets when the
  object-storage provider can perform provider-side copy.
- Support moving a complete exported bundle to any new root prefix when the
  bundle internal layout is unchanged.
- Do not add any extra root-rewrite parameter; the restore metadata URI is the
  only root-relocation input.

The feature has explicit non-goals:

- No streaming copy. Milvus must not download object bytes to a node and upload
  them again just to cross buckets.
- No cross-provider copy, cross-endpoint copy, or provider-specific source-auth
  extension.
- No arbitrary metadata layout. The restore metadata URI must still expose
  `<root>/snapshots/{collectionID}/metadata/{snapshotID}.json`.

## 2. User Scenarios

Same-bucket external restore:

1. A source cluster exports a snapshot under the same bucket or a path readable
   by the target cluster.
2. The target cluster calls `RestoreExternalSnapshot` with the exported metadata
   URI.
3. DataCoord reads the metadata and creates the normal asynchronous restore job.

Manual bundle relocation:

1. Export writes:
   `export-root/snapshots/100/metadata/1.json` and `export-root/files/...`.
2. An operator copies the entire bundle to:
   `restored/x/snapshots/100/metadata/1.json` and `restored/x/files/...`.
3. Restore receives the new metadata URI. Milvus derives `oldRoot` from the
   export-time metadata and `newRoot` from the restore-time metadata URI, then
   rebases self-contained paths from `oldRoot` to `newRoot`.

Export to a foreign bucket:

1. The caller invokes `ExportSnapshot` with a `target_s3_path` in a foreign
   bucket.
2. Milvus resolves a foreign target storage config from the instance credential
   or request `external_spec`.
3. The provider performs copy from the local source bucket to the foreign target
   bucket without streaming through Milvus.

Restore from a foreign bucket:

1. The caller invokes `RestoreExternalSnapshot` with a foreign metadata URI.
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
- `snapshot_metadata_uri`: metadata file URI or object key.
- `external_spec`: optional JSON storage spec for the foreign source.

`RestoreExternalSnapshotResponse.job_id` is the asynchronous restore job ID. The
caller uses it with `GetRestoreSnapshotState`.

`ExportSnapshotRequest` contains:

- `db_name`: database routing and namespace context.
- `collection_name`: local source collection.
- `snapshot_name`: local snapshot to export.
- `target_s3_path`: destination root for the self-contained bundle.
- `external_spec`: optional JSON storage spec for the foreign target.

`ExportSnapshotResponse.snapshot_metadata_uri` is the metadata URI of the
exported bundle.

The final API does not include `foreign_storage_spec`,
`foreign_credential_ref`, or `external_credential_ref`. Splitting storage config
and credential reference would create two credential models for one provider
copy request, so the API keeps one `external_spec` field aligned with external
table `extfs` shape and snapshot-specific validation.

### 3.2 Go SDK APIs

The Go SDK exposes:

- `ExportSnapshot(ctx, NewExportSnapshotOption(...).WithExternalSpec(...))`
- `RestoreExternalSnapshot(ctx, NewRestoreExternalSnapshotOption(...).WithExternalSpec(...))`
- `GetRestoreSnapshotState(ctx, NewGetRestoreSnapshotStateOption(jobID))`

`WithExternalSpec` is optional. Empty `external_spec` means Layer 1 instance
credential resolution.

### 3.3 REST APIs

REST exposes:

```text
POST /v2/vectordb/jobs/snapshot/export
POST /v2/vectordb/jobs/snapshot/restore_external
POST /v2/vectordb/jobs/snapshot/describe
POST /v2/vectordb/jobs/snapshot/list
```

REST uses camelCase request fields. The JSON field is `externalSpec`, and the
handler forwards it to the gRPC `external_spec` field.

### 3.4 API Demos

Go SDK export:

```go
metadataURI, err := client.ExportSnapshot(
    ctx,
    milvusclient.NewExportSnapshotOption(
        "snapshot_20260608",
        "source_collection",
        "s3://foreign-bucket/export-root",
    ).WithExternalSpec(`{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`),
)
```

Go SDK external restore:

```go
jobID, err := client.RestoreExternalSnapshot(
    ctx,
    milvusclient.NewRestoreExternalSnapshotOption(
        "restored_collection",
        "s3://foreign-bucket/export-root/snapshots/100/metadata/1.json",
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
    "snapshotMetadataURI": "s3://foreign-bucket/export-root/snapshots/100/metadata/1.json",
    "externalSpec": "{\"extfs\":{\"cloud_provider\":\"aws\",\"region\":\"us-west-2\",\"use_iam\":\"true\"}}"
  }'
```

REST restore status:

```bash
curl -X POST "$MILVUS_ADDR/v2/vectordb/jobs/snapshot/describe" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"jobId":"12345"}'
```

### 3.5 RBAC and `db_name`

`RestoreExternalSnapshot` and `ExportSnapshot` are Global RBAC operations. The
source collection for external restore belongs to another cluster, and the
target collection may not exist when the request enters Proxy. Authorization
must therefore check a global privilege instead of treating either collection
name as the permission object.

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
  region, endpoint, TLS, virtual-host mode, `use_iam`, access key ID/value,
  GCP service-account JSON for native GCS, or Azure account key fields when
  those fields map to the same config structs Milvus already uses.
- The resolved config must still represent one principal/config that can satisfy
  the provider-side copy request.
- Snapshot validation is stricter than a generic external spec parser. It must
  reject generic `role_arn`, `gcp_target_service_account`, SAS, anonymous auth,
  source-auth URLs, and independent dual credentials.

Provider notes:

- S3-compatible storage supports static AK/SK and ambient identity through the
  existing `use_iam` path. Runtime AWS role mechanisms may be provided by the
  environment, but the snapshot request must not contain a generic `role_arn`.
- GCP native storage supports service-account JSON and Application Default
  Credentials through the existing config model.
- Azure storage supports account key mode and the existing workload/managed
  identity path. A request SAS token is not supported.

Restore persistence red line:

`RestoreExternalSnapshot` must propagate the source storage config from Proxy to
DataCoord, through WAL/meta, restore job state, copy segment job state, and
DataNode task execution. Raw secrets inside `external_spec` are therefore
persisted through WAL/meta/job/task state. Operators should prefer Layer 1 or
ambient identity fields such as `use_iam=true`. Logs and errors must use
redacted specs.

## 5. Bundle Layout & Root Relocation

Exported self-contained bundles use this layout:

```text
<root>/snapshots/{collectionID}/metadata/{snapshotID}.json
<root>/snapshots/{collectionID}/manifests/...
<root>/files/...
```

The `snapshots` directory is the root anchor. Restore derives:

- `oldRoot` from the export-time metadata path stored in snapshot metadata.
- `newRoot` from the restore-time `snapshot_metadata_uri`.

When the layout is self-contained and `oldRoot != newRoot`, restore rebases
paths from `oldRoot` to `newRoot`. This safely supports:

```text
old:
export-root/snapshots/100/metadata/1.json
export-root/files/...

new:
restored/x/snapshots/100/metadata/1.json
restored/x/files/...
```

Root relocation is two-stage:

1. Rebase metadata manifest paths before manifest reads. Otherwise restore would
   attempt to load manifest files from `oldRoot`.
2. Rebase loaded segment/index/binlog data after manifest reads. StorageV3
   manifest paths carry a base path; rebasing that base path is enough for
   manifest-relative data and LOB listing.

The design intentionally rejects metadata URIs that do not contain the
`snapshots/.../metadata/...` structure. Supporting arbitrary layouts would
require a new request parameter or a new persisted bundle-root field, and this
feature explicitly avoids both.

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

## 7. Internal Architecture

Proxy:

- Accepts gRPC and REST requests.
- Fills `db_name` through the database interceptor.
- Performs Global RBAC checks for external snapshot APIs.
- Forwards `external_spec` without logging raw secrets.

DataCoord:

- Owns snapshot metadata parsing, validation, export layout generation, restore
  job creation, and WAL restore message emission.
- For external restore, reads metadata/manifests from the foreign source before
  broadcasting the restore message.
- Persists enough external storage information for restore jobs and DataNode
  copy tasks.
- For export, resolves the foreign target and writes the self-contained bundle
  metadata.

DataNode:

- Executes copy segment tasks.
- Rebuilds source storage config for external restore tasks.
- Enumerates StorageV2/V3 files and copies referenced data into local target
  paths.

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
Proxy -> DataCoord -> local snapshot metadata -> provider-side copies ->
foreign target bundle -> snapshot_metadata_uri

RestoreExternalSnapshot:
Proxy -> DataCoord -> foreign metadata/manifests -> WAL restore message ->
copy segment job -> DataNode -> provider-side copies into local bucket
```

## 8. Validation & Security

Path validation:

- Reject URI userinfo, unsupported schemes, empty object keys, and path traversal
  forms.
- Require metadata URIs to expose `snapshots/.../metadata/...`.
- Validate self-contained metadata after root relocation against `newRoot`.

Endpoint/provider compatibility validation:

- Parse source and destination locations into provider, endpoint, bucket, and
  object key.
- Reject different providers, incompatible endpoints, and unsupported
  server-side copy combinations.

Access probing:

- Probe source read access before restore scheduling.
- Probe target write access before export writes.
- Prefer failing during request handling over failing after a WAL message or
  long-running copy job is created.

Secret handling:

- Redact `external_spec` in logs and errors.
- Do not include raw secrets in task labels, metric labels, or user-facing
  failure messages.
- Treat restore raw secret persistence through WAL/meta as an operational red
  line.

Fail-closed behavior:

- If parsing, compatibility validation, access probing, metadata read,
  manifest read, path validation, or provider-side copy resolution is ambiguous,
  reject the request.
- Do not silently fall back to streaming.

## 9. Test Plan

API contract tests:

- gRPC request builders and Proxy forwarding include `external_spec`.
- `RestoreExternalSnapshot` and `ExportSnapshot` use Global RBAC.
- `db_name` is filled by the database interceptor and is not treated as the RBAC
  object.
- REST `externalSpec` is forwarded, and describe/list snapshot job routes map to
  restore job state APIs.

Resolver and validator tests:

- Empty `external_spec` resolves Layer 1 instance credential.
- `external_spec.extfs` resolves allowed storage-config-compatible fields.
- `role_arn`, `gcp_target_service_account`, SAS, anonymous auth, and dual
  credentials are rejected.
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
- Failure to read source metadata or manifests fails before scheduling unsafe
  work.

Export tests:

- Export to a foreign target writes self-contained metadata and data mappings.
- Provider/endpoint mismatch rejects before copying.
- Target write probe failure aborts the request.

Standalone client build:

- Root module and standalone `client/` module both build against the published
  milvus-proto version that contains the snapshot APIs.
