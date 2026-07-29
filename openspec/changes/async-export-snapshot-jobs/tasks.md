## 1. Public And Internal API Contracts

- [x] 1.1 Update milvus-proto so `ExportSnapshotResponse` returns a new `job_id`, retains field 2 as deprecated `snapshot_metadata_uri`, and defines export job state/info plus `GetExportSnapshotState` request and response messages, including the completed bundle `total_bytes`.
- [x] 1.2 Register the new public RPC message type and Global `PrivilegeExportSnapshot` request option, regenerate milvus-proto outputs, and add contract tests for field compatibility and RBAC scope.
- [x] 1.3 Publish the milvus-proto revision and use `scripts/update-api-version.sh` to bump every Milvus Go module that consumes go-api, including the standalone client and Go client test modules.
- [x] 1.4 Extend `pkg/proto/data_coord.proto` with the internal export job state, constant-size persisted job record, submission response job ID, and state-query RPC, then regenerate internal proto outputs.
- [x] 1.5 Regenerate only generator-owned mocks affected by the RPC interfaces and verify that no unrelated handwritten mock or compatibility shim is introduced.

## 2. Export Job Persistence

- [x] 2.1 Add a dedicated DataCoord catalog key prefix and catalog interface methods to save, list, and drop export job records.
- [x] 2.2 Implement the KV catalog serialization and batch-loading behavior for export jobs, including compatibility with an empty catalog on upgrade.
- [x] 2.3 Add catalog tests for save, overwrite, list, drop, malformed records, and storage error propagation.
- [x] 2.4 Implement `snapshotExportMeta` with an in-memory cache, per-job locking, cloned updates, persist-before-cache-swap semantics, and defensive copies for readers.
- [x] 2.5 Add metadata tests proving atomic state/progress visibility, terminal credential clearing, and safe behavior when persistence fails.

## 3. Durable Submission And Query Path

- [x] 3.1 Introduce `SnapshotExportManager` as a DataCoord-owned component with start, wake, close, worker wait, running-job registry, and target-root key locking.
- [x] 3.2 Wire manager construction, catalog loading, startup, shutdown ordering, and restart reconciliation into the DataCoord server lifecycle.
- [x] 3.3 Split parse-only target storage validation from client construction, then replace synchronous DataCoord export execution with collection and snapshot lookup, job ID allocation, source pin creation, Pending job persistence, manager wake-up, and immediate job ID return without object-store I/O.
- [x] 3.4 Compute the export pin TTL from the greater of the existing snapshot job pin TTL and the export deadline plus safety margin, and use an independent bounded context to clean up a pin when job persistence fails.
- [x] 3.5 Implement DataCoord `GetExportSnapshotState` lookup and public job-info conversion, including not-found handling, terminal result URI rules, bounded sanitized reasons, and exclusion of `external_spec`.
- [x] 3.6 Add service tests for synchronous request rejection, durable acceptance ordering, submission-context cancellation after acceptance, pin cleanup on persistence failure, and state-query responses.

## 4. Deterministic Export Planning

- [x] 4.1 Refactor the current exporter into a deterministic plan builder that reuses snapshot file collection and path mapping, excludes prefix-only references, and produces ordered concrete source/destination mappings.
- [x] 4.2 Define an explicit export plan version and compute the plan fingerprint from the source snapshot fingerprint, normalized target storage identity, ordered source paths and file types, and ordered destination paths.
- [x] 4.3 Persist plan version, fingerprint, total file count, zero cursor, and planning progress before issuing any provider copy request.
- [x] 4.4 Add plan tests for deterministic ordering, deduplication, URI normalization, StorageV3 prefix exclusion, empty snapshots, target overlap rejection, and fingerprint changes for every plan-defining input.

## 5. Checkpointed Copy And Publication

- [x] 5.1 Implement bounded copy batches over the persisted plan while retaining `snapshot.exportCopyConcurrency` as the per-job provider request limit.
- [x] 5.2 Advance `copy_cursor`, `copied_files`, and public progress only after every copy in a batch succeeds; leave the checkpoint unchanged when a batch is partially completed.
- [x] 5.3 Resume a matching recovered plan from its persisted cursor and replay an uncheckpointed batch against the same deterministic destination keys.
- [x] 5.4 Fail the job with a data-integrity reason when recovery rebuilds a different plan version, fingerprint, or total file count instead of resetting progress.
- [x] 5.5 Reuse the snapshot writer to rewrite segment manifests and publish metadata last, recheck that the job remains Executing before publication, and persist Completed with progress 100 and the metadata URI only after metadata publication succeeds.
- [x] 5.6 Add executor tests for successful batches, partial-batch failure, crash points before and after checkpoints, finalization replay, metadata-last visibility, empty plans, and non-decreasing progress after restart.

## 6. Terminal Lifecycle And Reconciliation

- [x] 6.1 Implement the reconciliation loop to schedule Pending and recoverable Executing jobs up to the configured job concurrency while preventing duplicate workers in one process.
- [x] 6.2 Add refreshable DataCoord parameters for export job timeout, terminal retention, and maximum concurrent jobs with defaults of 12 hours, 3 hours, and 1.
- [x] 6.3 Enforce deadlines from durable acceptance time, including queue wait, serialize timeout with job updates, cancel the active worker, and prevent late provider completions from checkpointing, publishing metadata, or replacing Failed with Completed.
- [x] 6.4 Make the first Completed or Failed persistence clear raw `external_spec` in the same job update while retaining `pin_id` until cleanup succeeds.
- [x] 6.5 Retry terminal unpin operations during reconciliation, clear `pin_id` only after success, and prevent retention GC from removing a job that still owns a pin.
- [x] 6.6 Remove credential-free terminal jobs only after retention expires and leave all partially copied target objects untouched.
- [x] 6.7 Add lifecycle tests for concurrency limits, queued-job timeout, timeout racing with an uncooperative provider request, DataCoord shutdown without false failure, restart recovery, terminal credential clearing, retryable unpin, and retention GC.

## 7. Proxy, SDK, REST, And Security Boundaries

- [x] 7.1 Forward asynchronous submission and `GetExportSnapshotState` through MixCoord and Proxy while preserving Global RBAC and existing rate-limit classification.
- [x] 7.2 Update Proxy response handling so gRPC `ExportSnapshot` returns the job ID and state queries expose only the public export job fields.
- [x] 7.3 Update REST submission to return `jobId` and add the export-state endpoint with the same Global authorization path and response semantics as gRPC.
- [x] 7.4 Change the Go SDK `ExportSnapshot` result to `int64`, add export-state query options and methods, and update SDK tests for Pending, Executing, Completed, Failed, transport-error, and status-error responses.
- [x] 7.5 Audit request trace logging, job conversion, persisted reasons, metrics, and URI errors so raw `external_spec`, URI userinfo, query credentials, and fragments are never emitted.
- [x] 7.6 Add API tests covering generated request options, database interceptor behavior, Global RBAC, REST validation, REST response fields, and standalone `client/` module compilation.

## 8. Observability

- [x] 8.1 Add low-cardinality metrics for active export jobs, terminal transitions by state, and job duration without job ID, bucket, root, snapshot name, or collection name labels.
- [x] 8.2 Add `mlog` state-transition and checkpoint logs using the worker context plus job and collection IDs, with rate limiting where reconciliation may repeat.
- [x] 8.3 Add coordinator-owned asynchronous worker spans keyed by job ID without persisting request trace context or introducing WAL spans.
- [x] 8.4 Add focused tests for metric transitions and verify logging and tracing helpers receive non-nil lifecycle contexts and redacted fields.

## 9. Documentation And End-To-End Validation

- [x] 9.1 Update the external snapshot design document and user guide with asynchronous submission, polling examples, progress semantics, synchronous versus asynchronous errors, restart behavior, credential persistence, timeout, retention, and partial-object cleanup policy.
- [x] 9.2 Update the Go client snapshot end-to-end workflow to submit export, poll `GetExportSnapshotState` to Completed, consume the returned metadata URI, and restore both referenced and self-contained snapshot layouts.
- [x] 9.3 Add restart or fault-injection coverage proving accepted jobs recover with non-decreasing progress and fail closed on plan mismatch.
- [x] 9.4 Run targeted unit tests with `-tags dynamic,test -gcflags="all=-N -l"`, verify at least 99 percent coverage for newly introduced job-manager logic, and record any applicable macOS-only known skip.
- [ ] 9.5 Run proto freshness checks, standalone client tests, `make check-commit`, and the snapshot export/restore E2E suite after resetting the local Milvus environment.
