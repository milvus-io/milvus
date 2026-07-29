## Context

`ExportSnapshot` currently executes synchronously across Proxy and DataCoord.
DataCoord resolves the destination storage, pins the source snapshot, lists all
referenced objects, issues provider-side copy requests, rewrites manifests, and
writes the exported metadata before returning `snapshot_metadata_uri`. Large
exports therefore keep the client RPC and snapshot resource-key lock open for
the complete copy duration and cannot expose durable progress.

The existing exporter already provides important correctness properties that
must remain unchanged:

- copy is provider-side and does not stream snapshot data through Milvus;
- source objects are immutable while protected by a snapshot pin;
- exported object paths are deterministic;
- source/target overlap is rejected before copying;
- segment manifests are written before the metadata file, so metadata is the
  publication marker for a complete self-contained bundle; and
- failed exports do not delete copied objects because target roots may contain
  data shared by another exported snapshot.

DataCoord already has durable job implementations for restore and external
collection refresh. Restore jobs are coupled to target collection creation,
segment-ID mappings, and DataNode tasks. External collection refresh is closer
to the required submission/query model, but its inspector/task/checker stack is
designed for DataNode-dispatched work. Export copy requests are lightweight
coordinator-side provider API calls, so neither existing task model is a clean
fit.

The public beta Go SDK currently returns `(string, error)` from
`ExportSnapshot`. This change is allowed to break that beta signature. The
request `external_spec` may contain raw credentials and is allowed to be
persisted while a job is active so that DataCoord restart recovery works.

## Goals / Non-Goals

**Goals:**

- Return an export job ID quickly after durable job acceptance.
- Expose durable state, file progress, failure reason, timing, and the final
  metadata URI through `GetExportSnapshotState`.
- Resume accepted jobs after DataCoord restart without decreasing externally
  reported progress.
- Preserve source data with a persisted snapshot pin from submission through
  terminal cleanup.
- Preserve provider-side copy, current provider/endpoint restrictions, overlap
  validation, and self-contained bundle layout.
- Bound coordinator concurrency and job lifetime.
- Persist raw `external_spec` only while needed and remove it at the first
  durable terminal transition.

**Non-Goals:**

- Export job listing or cancellation APIs.
- Client-side or DataNode-mediated object streaming.
- Cross-provider export.
- Intra-object byte progress for one provider copy request.
- Automatic deletion of partially copied target objects.
- Changing referenced or self-contained restore behavior.
- A general-purpose encrypted secret store for DataCoord metadata.

## Decisions

### 1. Change the existing ExportSnapshot API to asynchronous semantics

`ExportSnapshotResponse` will retain field 2 as the deprecated
`snapshot_metadata_uri` field and add `job_id` with a new field number. New
servers leave the deprecated URI empty on submission. The Go SDK changes
`ExportSnapshot` to return `(int64, error)`, and REST returns `jobId`.

Add a separate `GetExportSnapshotState` RPC and REST endpoint. Its job info
contains:

- job ID, snapshot name, database name, and collection name;
- Pending, Executing, Completed, or Failed state;
- progress percentage, total files, and copied files;
- safe failure reason and timing; and
- `snapshot_metadata_uri`, populated only for Completed jobs.

Submission and state query both use Global `PrivilegeExportSnapshot`. The state
query does not require a database name because authorization is global and the
job ID is globally allocated.

Alternatives considered:

- Add `StartExportSnapshot` and retain synchronous `ExportSnapshot`. This
  preserves compatibility but creates two long-term API semantics for one
  operation. The beta API is explicitly allowed to change, so this is rejected.
- Reuse `GetRestoreSnapshotState`. Export and restore have different result
  fields and lifecycle ownership; overloading a restore-named API is rejected.

### 2. Use a dedicated DataCoord-local SnapshotExportManager

DataCoord will own a `SnapshotExportManager` containing:

- `snapshotExportMeta`: etcd-backed job metadata plus an in-memory cache;
- a bounded local worker scheduler;
- a running-job registry used to prevent duplicate execution in one process;
- a target-root key lock preserving current export serialization; and
- a reconciliation loop for restart recovery, timeout, terminal pin cleanup,
  credential clearing, and retention GC.

The manager starts with the other DataCoord loops and must stop and wait for
workers before snapshot metadata is closed. Worker contexts derive from the
manager/DataCoord lifecycle context, never from the submission RPC context.

No WAL message is required. Export does not mutate collection metadata and the
job record itself is the durable acceptance point. No DataNode task is required
because the provider performs the data copy and DataCoord only issues API
requests.

Alternatives considered:

- Reuse `CopySegmentJob`: rejected because it requires target collection and
  segment task semantics.
- Reuse the external collection refresh task framework: rejected because it
  would dispatch coordinator-owned provider calls through DataNodes.
- Start an untracked goroutine in `snapshotManager`: rejected because it loses
  progress, credentials, pins, and recovery state on restart.

### 3. Make job persistence the acceptance boundary

Submission follows this order:

1. Proxy and DataCoord validate required fields and request-level path/spec
   syntax without constructing an object-storage client or issuing a bucket or
   object request.
2. DataCoord resolves collection identity and acquires the shared snapshot
   resource key.
3. DataCoord allocates a globally unique job ID.
4. DataCoord pins the named source snapshot with a TTL covering the job
   deadline plus a safety margin.
5. DataCoord persists a Pending export job containing the pin ID.
6. DataCoord releases the resource key, signals the export manager, and returns
   the job ID.

If job persistence fails after pin creation, submission uses an independent,
bounded cleanup context to unpin before returning an error. A process crash in
that narrow interval can leave an orphan pin, but the pin TTL bounds it.

Once a job ID is returned, later storage or copy failures are represented by a
Failed job rather than by the original RPC.

The submission path must use a parse-only storage validation helper. The worker
constructs the request-scoped chunk manager and copier after acceptance. This
keeps endpoint/provider/spec shape errors synchronous while permission,
connectivity, bucket, and object failures remain asynchronous job results.

### 4. Persist one job record, not one record per file

The internal `ExportSnapshotJob` stores at least:

- identity: job, database, collection, and source snapshot;
- request inputs: target URI and raw external spec;
- lifecycle: state, safe reason, start/end/deadline, and cleanup timestamp;
- progress: total files, copied files, and copy cursor;
- plan identity: export plan version and plan fingerprint;
- result: completed snapshot metadata URI; and
- protection: source snapshot pin ID.

The DataCoord catalog adds Save/List/Drop operations under a dedicated export
job prefix. Job updates use clone, persist, then cache-swap semantics under a
job-scoped lock so state and progress queries never observe partially mutated
objects.

Per-file etcd tasks are rejected because a snapshot may reference very large
numbers of objects. The single job record remains constant-size regardless of
file count.

### 5. Use deterministic copy batches as durable progress checkpoints

`ListSnapshotDataFiles` already deduplicates references and sorts them by
normalized object path. The executor filters out prefix-only references such as
StorageV3 manifest roots and constructs a deterministic sequence of concrete
copy mappings.

Before the first copy, it computes an export plan fingerprint over:

- an explicit export plan version;
- the source snapshot fingerprint;
- normalized target bucket and root;
- each ordered source object path and file type; and
- each ordered destination object path.

The job persists the plan fingerprint, total file count, and cursor before
copying. Files are then processed in bounded batches with the existing
`snapshot.exportCopyConcurrency` applied inside each batch.

For each batch:

1. issue provider-side copies concurrently;
2. wait until every object in the batch succeeds;
3. persist `copy_cursor` and `copied_files` at the batch end; and
4. continue to the next batch.

If DataCoord stops after only part of a batch completes, the persisted cursor
does not advance. Restart repeats that batch. Provider copies overwrite the
same deterministic target keys, so replay is idempotent.

The checkpoint batch size is an internal constant derived to amortize etcd
writes; it is not a new user-facing parameter. It must be substantially larger
than per-job copy concurrency.

### 6. Fail closed when a recovered export plan changes

On recovery, the worker re-reads the pinned source snapshot, rebuilds the
ordered plan, and compares its fingerprint and total count with the persisted
values.

- A matching plan resumes from `copy_cursor`.
- A job without a persisted plan starts from zero and persists one before its
  first copy.
- A mismatching plan transitions to Failed with a data-integrity reason.

The manager must not silently reset progress or continue an old cursor against
a different plan. This preserves monotonic progress and prevents skipped
objects after binary or metadata changes.

### 7. Define progress from durable checkpoints

Public progress is derived only from persisted state:

- Pending: 0;
- planning complete: 5;
- copying: `5 + copied_files * 90 / total_files`, capped at 95;
- finalizing manifests and metadata: 99; and
- Completed: 100.

An empty valid snapshot moves from planning directly to finalization. Prefix
anchors are excluded from `total_files`; only concrete provider-copy requests
are counted.

The visible value may temporarily under-report copies completed inside the
current uncommitted batch, but it never decreases across polling or DataCoord
restart. True intra-object progress is unavailable because the storage
interface completes one object copy as a single operation.

### 8. Publish metadata last and make terminal transitions replay-safe

After every data object checkpoint is complete, the worker rewrites the
snapshot and invokes the existing snapshot writer. Segment manifests are
written first and metadata is written last. Only after metadata write succeeds
does the job persist Completed, progress 100, and the metadata URI.

Relevant crash cases are replay-safe:

- copied objects but no checkpoint: repeat the current batch;
- all objects copied but no metadata: rewrite manifests and metadata;
- metadata written but Completed not persisted: rewrite the deterministic
  metadata and persist Completed;
- Completed persisted but unpin failed: expose Completed while cleanup retries
  the remaining pin release.

Failed jobs leave copied objects in place and never publish a new metadata file
after failure. Automatic deletion remains prohibited because target data may be
shared with another published bundle.

### 9. Keep pins through queued and executing lifetime

Queue wait time counts toward the export job deadline. The pin TTL is computed
as the greater of the existing snapshot job pin TTL and the configured export
job timeout plus a safety margin.

Terminal transition and pin release are separate durable concerns:

- Completed or Failed is persisted first so user-visible state is not blocked
  on a temporary catalog failure during unpin.
- The job retains `pin_id` until unpin succeeds.
- Reconciliation retries unpin for terminal jobs with a non-zero pin ID and
  clears the field only after success.
- A job is not removed by retention GC while it still owns a pin.

Graceful DataCoord shutdown cancels workers without marking jobs Failed or
releasing their pins. Recovered jobs continue under the same deadline.

### 10. Persist credentials only for active recovery

Raw `external_spec` is persisted in the internal job because request-level
credentials may be required after the submission RPC returns or after a
DataCoord restart. It is never included in public job info, logs, metric labels,
or persisted failure reasons.

The same durable job update that first writes Completed or Failed also clears
`external_spec`. If that update fails, the job remains non-terminal and retains
the credential required for safe replay. Terminal retention therefore keeps
status and result fields but not request credentials.

All target URIs are validated to reject userinfo, query credentials, fragments,
and traversal before persistence. Failure reasons are sanitized and length
bounded before being stored.

At-rest encryption is not introduced in this change. Operators that do not
accept raw request credentials in DataCoord metadata must use instance
credentials and bucket policy instead.

### 11. Bound worker concurrency, timeout, and retention

Add refreshable DataCoord settings for:

- export job timeout, default 12 hours;
- terminal export job retention, default 3 hours; and
- maximum concurrent export jobs, default 1.

The existing `snapshot.exportCopyConcurrency` continues to bound provider copy
requests inside one active job. A default of one active job prevents the two
concurrency dimensions from multiplying unexpectedly; different target roots
can be enabled concurrently by increasing the job-level setting.

The reconciliation loop wakes on submission and also runs periodically so it
can recover jobs, enforce deadlines, retry terminal cleanup, and remove expired
terminal jobs.

Timeout and worker completion are serialized through the job-scoped lock. When
reconciliation times out a running job, it persists Failed and cancels that
worker. A provider request that ignores cancellation may still finish remotely,
but the worker must re-read the persisted state before checkpointing or
publication and must not advance, publish metadata, or overwrite a terminal
state.

### 12. Treat storage failures as asynchronous terminal results

The submission RPC rejects deterministic request problems synchronously:

- missing or invalid fields;
- invalid path, provider, endpoint, or external spec structure;
- missing collection or source snapshot; and
- pin or job persistence failure.

Failures requiring object-store access occur asynchronously and transition the
job to Failed, including permission denial, missing source objects, copy
failure, plan mismatch, manifest write failure, and timeout.

Storage errors retain their existing typed code and retriability while the
worker is running. When persisted as a terminal job reason, they are sanitized
and bounded rather than flattened into a new public error code; the public
state query communicates failure through the job state and reason.

The first version does not add a new job-level retry policy. Provider SDK and
existing storage retries remain responsible for transient request retries. A
DataCoord lifecycle interruption is recoverable and is not classified as a job
failure.

### 13. Add job-oriented observability without secret or cardinality growth

Logs bind the stable job ID, collection ID, and snapshot name through `mlog`.
They record state transitions, checkpoint counts, duration, and safe errors,
but never raw external spec or unredacted credential-bearing paths.

Metrics include active export jobs, terminal job count by state, and export job
duration. Job ID, bucket, root, and snapshot name are not metric labels. Existing
persistent data operation metrics continue to account for individual provider
copy requests.

Worker traces are coordinator-owned asynchronous spans identified by job ID.
They do not require WAL trace propagation or persistence of request trace
context.

## Risks / Trade-offs

- **[Breaking beta API]** Existing Go clients expect a metadata URI directly.
  -> Bump milvus-proto and all Milvus Go modules together, update SDK examples,
  and document polling as the required migration.
- **[Raw credentials in etcd while active]** A compromised metadata store can
  expose request credentials. -> Recommend instance credentials, redact every
  output boundary, clear the raw spec atomically at terminal transition, and
  keep retention records credential-free.
- **[Progress is batch-granular]** A large current batch may make progress look
  temporarily stalled. -> Keep batches bounded and report copied/total file
  counts alongside percentage.
- **[Plan mismatch after upgrade]** A changed file collection rule can prevent
  resume. -> Version and fingerprint the plan and fail closed rather than risk
  an incomplete bundle.
- **[Provider copy continues after cancellation]** Some providers may finish an
  already-started server-side copy after DataCoord stops. -> Replay the same
  deterministic keys and rely on metadata-last publication.
- **[Orphan pin or partial target objects]** Crashes can occur between durable
  operations. -> Use pin TTL, terminal cleanup reconciliation, idempotent copy,
  and no metadata publication until all data is complete.
- **[etcd update load]** Persisting every file would overload metadata. -> Write
  one checkpoint per large batch and keep one constant-size job record.
- **[Single active job default]** Multiple queued exports may wait. -> Make the
  job-level concurrency refreshable while keeping the conservative default.

## Migration Plan

1. Update milvus-proto with the asynchronous response, export state/info
   messages, `GetExportSnapshotState`, message type, and Global privilege
   options; publish and bump every Milvus module that consumes go-api.
2. Add the internal DataCoord job proto, catalog persistence, Proxy/MixCoord
   forwarding, Go SDK, and REST contract.
3. Add the DataCoord export manager, checkpointed executor, lifecycle wiring,
   configuration, metrics, and focused unit tests.
4. Update the existing snapshot export/restore end-to-end test to submit,
   poll until Completed, consume the returned metadata URI, and restore it.
5. Update the snapshot design and user guide to describe asynchronous failure
   boundaries, progress semantics, credential persistence, and restart resume.

There is no existing export-job metadata to migrate. During rollback, operators
must stop new submissions and allow active export jobs to finish or fail before
running an older binary. An older binary ignores export-job catalog keys and
will not resume them; their source pins remain bounded by TTL and partial target
objects remain unpublished.

## Open Questions

None. The compatibility break, monotonic checkpointed progress, and temporary
raw external-spec persistence have been explicitly accepted for this change.
