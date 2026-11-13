# Snapshot copying

`github.com/milvus-io/milvus/pkg/v3/snapshotio` shares snapshot decoding,
physical file inventory, path rewriting and publication with Milvus Export.
Restore uses the same inventory and bounded copy executor while retaining its
own ID allocation, segment registration and visibility rules.

```go
targetStore, err := objectstore.NewS3Store(client, objectstore.DefaultS3Options())
// Check err. client is an existing *minio.Client, owned by the caller.
result, err := snapshotio.CopySnapshot(ctx, sourceReader, targetStore, snapshotio.CopyRequest{
    SourceMetadataURI: metadataURI,
    TargetRoot: exclusiveAttemptURI,
}, snapshotio.Options{Concurrency: 128})
// Check err before exposing result.MetadataURI.
```

`objectstore.ObjectReader.Read` and `objectstore.ObjectStore.Write` receive
an `objectstore.Object{Bucket, Key}`. Import `pkg/v3/objectstorage/store` as
`objectstore`. ObjectStore also provides `Copy(CopyRequest) (int64, error)`.
Keys are complete bucket-relative keys, without automatic root-prefix insertion.
Local adapters use absolute paths and an empty bucket. Stores return Milvus I/O
errors or the provider SDK's errors (classified by the adapter); a missing object
must be distinguishable from permission or network failure. `ObjectStore.Copy`
returns the nonnegative object size on success. S3Store reuses its source
HEAD request; server adapters query the copied destination when the backend
does not return a size. This also applies to Restore and metadata copies,
although metadata is excluded from exported byte totals. A size-query failure
returns `CopySizeError`, which preserves the underlying error and confirms that
the destination was written. Other errors may still leave partially written
objects; failed-attempt cleanup remains caller-owned.
Source only needs Read; Target implements Read, Write and Copy. Both must
support concurrent calls. Clients, credentials, source pinning and cleanup remain
caller-owned.

## Native manifests

Storage V3 inventory reads the pinned manifest bytes through Source, then calls
`DecodeManifest(data, basePath)`. The base is the original segment base key;
the decoder must return Go-owned inventory with complete resolved paths.
The decoder uses milvus-storage's backup/restore-only `loon_manifest_deserialize`
API, then expands relative paths using the native section layout. It preserves
external URIs and absolute local paths. Corrupt or unsupported manifests fail;
there is no empty-inventory or native-I/O fallback. Milvus server Export/Restore
retain their transaction reader; normal reads and commits never use raw serde.

`TargetRoot` must be a complete, credential-free URI (for example,
`https://s3.us-west-2.amazonaws.com/backup-bucket/backups/attempt-123`). Output
and staging URIs retain its scheme, endpoint and bucket. Source and target
storage configurations are not copy arguments; callers bind their injected
stores to the URI locations. Target.Copy must have access to both copy endpoints;
it does not receive credentials from Source. Absolute paths remain supported for
local adapters. Remote URI roots with repeated separators are rejected.

Copies retain index definitions, build IDs and index files. Native manifests
are copied unchanged, and Avro paths are rewritten into the target layout.
The output uses the upstream V6 snapshot format (Avro OCF with an embedded writer schema).

The native adapter is internal. Consumers need CGO and a compatible
`milvus-storage.pc`/library; production dependencies do not include Milvus
`internal/` packages or `libmilvus_core`. Pin the pkg revision and matching native
library together. Milvus integration fixtures remain under `internal/snapshotio/storage` and
additionally use Milvus core. Library unit tests have no server-package imports.
Run both from the repository root with `-tags dynamic,test`.

## Copy lifecycle

Each CopySnapshot call owns its concurrency budget and publishes metadata only
when the complete copy succeeds. Use an exclusive target root per attempt and
keep the source immutable throughout copying. The caller owns source leases,
task scheduling and cleanup of unpublished objects after a failed attempt.

`objectstorage/store` can also be used independently of snapshots. Its S3 store
supports provider-side cross-bucket copy on one compatible service with a client
authorized for both buckets. It does not stream between unrelated services.
Multipart concurrency is per object; CopySnapshot concurrency bounds operations, not
the total HTTP request count. Set `DisableMultipart` for GCS's XML API.

## Shared segment execution

`EnumerateSegmentFiles` returns raw typed references and the pinned manifest
inventory. It preserves URI spelling and duplicates for caller selection and
validation. External Collection Restore excludes native URI references before
normalization; it still copies the manifest and snapshot-selected indexes.
No directory listing is used. Ordinary segments keep all enumerated references.

Export, Restore and CopySnapshot use `PlanFileCopies` for destination mapping and
collision checks, and `ExecuteFileCopies` for bounded copies and confirmed-write
accounting. Copy adapters report confirmation separately from errors so Restore
retains cleanup paths even when a successful provider write races a timeout.
Copy always returns a size on success. A `CopySizeError` preserves a confirmed
destination for Restore cleanup when size lookup fails; its bytes are not counted.
Export/CopySnapshot retain Avro rewriting and bundle publication. Restore retains ID
mapping, native index commits, metadata results and cleanup policy. Segment
scheduling remains outside the file executor; it does not merge their lifecycles.
