## ADDED Requirements

### Requirement: Asynchronous export submission
The system SHALL accept a valid `ExportSnapshot` request as an asynchronous
export job and SHALL return a globally unique job ID without waiting for object
copy or metadata publication to finish.

#### Scenario: Submit an export job
- **WHEN** a caller submits a valid snapshot name, collection, target storage URI, and optional external storage spec
- **THEN** the server durably creates a Pending export job and returns its job ID before copying snapshot objects

#### Scenario: Preserve the deprecated response field
- **WHEN** a new server accepts an `ExportSnapshot` request
- **THEN** it returns the new `job_id` field and leaves the deprecated `snapshot_metadata_uri` submission field empty

#### Scenario: Expose job-oriented SDK and REST results
- **WHEN** a Go SDK or REST caller submits an export
- **THEN** the SDK returns the job ID and the REST endpoint returns `jobId` instead of a completed metadata URI

### Requirement: Export job state query and authorization
The system SHALL expose `GetExportSnapshotState` as a Global
`PrivilegeExportSnapshot` operation and SHALL return persisted export job
state without exposing request credentials.

#### Scenario: Query an existing export job
- **WHEN** an authorized caller queries a known export job ID
- **THEN** the response contains the job identity, snapshot and collection identity, state, progress, copied and total file counts, timing, and a sanitized failure reason

#### Scenario: Query a completed export job
- **WHEN** an authorized caller queries a Completed export job
- **THEN** the response reports progress 100 and includes the completed snapshot metadata URI and total bundle bytes

#### Scenario: Report completed bundle size
- **WHEN** an export publishes its data objects, segment manifests, and metadata
- **THEN** the Completed job reports `total_bytes` as the sum of the unique published object sizes

#### Scenario: Query a non-completed export job
- **WHEN** an authorized caller queries a Pending, Executing, or Failed export job
- **THEN** the response leaves the snapshot metadata URI empty

#### Scenario: Authorize submission and query globally
- **WHEN** a caller invokes `ExportSnapshot` or `GetExportSnapshotState`
- **THEN** the request is authorized against Global `PrivilegeExportSnapshot` rather than a database or collection privilege object

#### Scenario: Query an unknown export job
- **WHEN** an authorized caller queries a job ID that does not exist or has expired from retention
- **THEN** the server returns a not-found error without revealing any other job information

### Requirement: Durable job acceptance
The system SHALL treat successful persistence of a Pending export job as the
acceptance boundary and SHALL preserve every accepted job across DataCoord
restart.

#### Scenario: Persist acceptance before returning
- **WHEN** submission validation, source snapshot pinning, and Pending job persistence all succeed
- **THEN** the server returns the job ID and executes the job from a DataCoord lifecycle context independent of the submission RPC context

#### Scenario: Job persistence fails after pinning
- **WHEN** source snapshot pinning succeeds but Pending job persistence fails
- **THEN** submission returns an error, returns no job ID, and attempts to release the pin with an independent bounded cleanup context

#### Scenario: DataCoord restarts after acceptance
- **WHEN** DataCoord restarts after a job ID has been returned but before the job becomes terminal
- **THEN** the export manager reloads the persisted job and resumes or safely fails it according to the recovery requirements

### Requirement: Export job state machine and error boundary
The system SHALL use Pending, Executing, Completed, and Failed states, SHALL
make Completed and Failed terminal, and SHALL distinguish submission errors
from asynchronous execution failures.

#### Scenario: Execute an accepted job
- **WHEN** worker capacity becomes available for a Pending job
- **THEN** the manager transitions the job to Executing and never transitions it back to Pending

#### Scenario: Complete an export job
- **WHEN** every required object, manifest, and metadata file has been published and the terminal update is persisted
- **THEN** the job transitions once to Completed and remains terminal

#### Scenario: Fail an export job
- **WHEN** an accepted job encounters a permanent validation, storage, plan-integrity, publication, or deadline failure
- **THEN** the job transitions once to Failed with a bounded sanitized reason and remains terminal

#### Scenario: Reject deterministic submission errors synchronously
- **WHEN** required fields, URI or external-spec syntax, provider compatibility, collection existence, snapshot existence, pin creation, or job persistence validation fails before acceptance
- **THEN** `ExportSnapshot` returns the error synchronously and does not return a job ID

#### Scenario: Avoid object-store access during submission
- **WHEN** request structure and source snapshot identity are valid enough for durable acceptance
- **THEN** submission does not create a request-scoped storage client or issue a bucket, permission, or object probe before returning the job ID

#### Scenario: Report storage execution errors asynchronously
- **WHEN** permission denial, missing source data, provider copy failure, plan mismatch, manifest write failure, metadata write failure, or timeout occurs after acceptance
- **THEN** `ExportSnapshot` remains accepted and the failure is reported through the Failed export job state

#### Scenario: Stop DataCoord during execution
- **WHEN** DataCoord shutdown cancels an active export worker
- **THEN** the manager leaves the job non-terminal for restart recovery rather than marking it Failed solely because of shutdown

### Requirement: Monotonic checkpointed progress
The system SHALL derive public progress only from persisted deterministic copy
checkpoints so that reported progress never decreases across polls or DataCoord
restart.

#### Scenario: Persist the export plan before copying
- **WHEN** an export worker finishes collecting and ordering concrete object-copy mappings
- **THEN** it persists the plan version, plan fingerprint, total file count, and initial copy cursor before issuing the first copy batch

#### Scenario: Checkpoint a successful copy batch
- **WHEN** every provider copy request in the current bounded batch succeeds
- **THEN** the worker atomically advances the persisted copy cursor and copied file count to the end of that batch

#### Scenario: Part of a copy batch succeeds
- **WHEN** at least one object in a batch succeeds but the complete batch does not succeed
- **THEN** the persisted cursor does not advance and recovery replays the batch using the same deterministic destination keys

#### Scenario: Report copying progress
- **WHEN** a job has a persisted non-zero total file count and is copying files
- **THEN** its progress is `5 + copied_files * 90 / total_files`, capped at 95

#### Scenario: Report lifecycle progress boundaries
- **WHEN** a job is Pending, has completed planning, is finalizing metadata, or is Completed
- **THEN** its persisted progress is respectively 0, 5, 99, or 100

#### Scenario: Export an empty valid snapshot
- **WHEN** the deterministic export plan contains no concrete provider-copy requests
- **THEN** the job advances from planning to finalization without dividing by zero or inventing copied files

### Requirement: Restart recovery and plan validation
The system SHALL rebuild and verify a recovered export plan before using a
persisted copy cursor and SHALL fail closed when the rebuilt plan differs.

#### Scenario: Resume a matching persisted plan
- **WHEN** recovery rebuilds a plan whose version, fingerprint, and total file count match the persisted job
- **THEN** the worker resumes from the persisted copy cursor without decreasing copied files or progress

#### Scenario: Recover a job without an initialized plan
- **WHEN** recovery loads an accepted job that has no persisted plan fingerprint or cursor
- **THEN** the worker builds and persists a new deterministic plan before copying from the beginning

#### Scenario: Reject a changed recovery plan
- **WHEN** the rebuilt plan version, fingerprint, or total file count differs from the persisted job
- **THEN** the job transitions to Failed with a data-integrity reason instead of resetting progress or continuing from the old cursor

#### Scenario: Prevent duplicate local execution
- **WHEN** reconciliation observes a job that is already owned by a running worker in the same DataCoord process
- **THEN** it does not start a second worker for that job

### Requirement: Provider-side copy and metadata-last publication
The system SHALL preserve provider-side object copy and the existing
self-contained bundle layout, and SHALL use the metadata file as the final
publication marker.

#### Scenario: Copy snapshot objects
- **WHEN** an export job transfers a concrete snapshot object
- **THEN** DataCoord issues the supported provider-side copy operation without streaming the object through the client, Proxy, or DataNode

#### Scenario: Finalize a self-contained bundle
- **WHEN** all concrete data objects have durable copy checkpoints
- **THEN** the worker writes rewritten segment manifests before writing the snapshot metadata file

#### Scenario: Publish successful output
- **WHEN** the metadata file write succeeds and the Completed job update is persisted
- **THEN** the metadata URI becomes visible through `GetExportSnapshotState` as the result of a complete self-contained bundle

#### Scenario: Replay finalization after a crash
- **WHEN** DataCoord restarts after writing some or all deterministic manifests or metadata but before persisting Completed
- **THEN** recovery safely rewrites the deterministic publication objects and then persists Completed

#### Scenario: Fail before publication
- **WHEN** an export job becomes Failed before a metadata file is successfully published
- **THEN** the job exposes no metadata URI and does not publish a metadata file after entering Failed

### Requirement: Source snapshot pin lifecycle
The system SHALL protect source snapshot objects for the complete queued and
executing lifetime and SHALL durably track pin cleanup after terminal state.

#### Scenario: Pin before job persistence
- **WHEN** DataCoord accepts an export submission
- **THEN** it creates a source snapshot pin before persisting the Pending job and stores the pin ID in that job

#### Scenario: Size the pin lifetime
- **WHEN** DataCoord creates the export pin
- **THEN** its TTL covers at least the configured export deadline plus a safety margin and is not shorter than the existing snapshot job pin TTL

#### Scenario: Reach terminal state before unpinning
- **WHEN** an export finishes successfully or fails
- **THEN** DataCoord first persists Completed or Failed while retaining the pin ID and then attempts to release the pin

#### Scenario: Retry terminal pin cleanup
- **WHEN** pin release fails for a terminal job
- **THEN** reconciliation retries release and clears the persisted pin ID only after unpin succeeds

#### Scenario: Retain jobs that still own a pin
- **WHEN** a terminal job has exceeded its normal retention period but still contains a pin ID
- **THEN** retention cleanup keeps the job until the pin has been released

### Requirement: Active-job credential persistence
The system SHALL persist raw request `external_spec` only while an export job
is non-terminal and SHALL prevent that value from crossing public or
observability boundaries.

#### Scenario: Persist credentials for recovery
- **WHEN** an accepted non-terminal job uses request-level external storage credentials
- **THEN** the internal job record retains the raw external spec so a restarted DataCoord can continue the job

#### Scenario: Clear credentials at terminal transition
- **WHEN** DataCoord persists the first Completed or Failed state for a job
- **THEN** the same durable job update clears the raw external spec

#### Scenario: Terminal update persistence fails
- **WHEN** an attempted terminal update cannot be persisted
- **THEN** the job remains non-terminal and retains the external spec required for replay

#### Scenario: Return or observe a job
- **WHEN** a job is returned by a public API or recorded in logs, traces, metrics, or failure reasons
- **THEN** the raw external spec is absent and credential-bearing paths are redacted

#### Scenario: Reject credential-bearing target URIs
- **WHEN** a target URI contains userinfo, query parameters, fragments, or traversal segments
- **THEN** submission rejects the URI before it or its credentials can be persisted

### Requirement: Timeout, retention, and bounded scheduling
The system SHALL bound export job execution and retention while leaving
partially copied target objects untouched after failure.

#### Scenario: Apply the export deadline
- **WHEN** the elapsed time since durable job acceptance exceeds the configured export job timeout, including queue wait time
- **THEN** reconciliation transitions the non-terminal job to Failed with a timeout reason

#### Scenario: Timeout races with an active worker
- **WHEN** reconciliation persists Failed for a timed-out job while a provider request is still completing
- **THEN** the worker cannot checkpoint more files, publish metadata, or replace the terminal state with Completed

#### Scenario: Use conservative defaults
- **WHEN** operators do not override export job settings
- **THEN** DataCoord uses a 12-hour job timeout, a 3-hour terminal retention period, and at most one concurrently executing export job

#### Scenario: Bound nested copy concurrency
- **WHEN** an export job executes a copy batch
- **THEN** provider copy concurrency inside that job remains bounded by the existing `snapshot.exportCopyConcurrency` setting

#### Scenario: Retain terminal query state
- **WHEN** a Completed or Failed job has no remaining pin and has not exceeded the configured retention period
- **THEN** it remains queryable with credentials removed

#### Scenario: Remove an expired terminal job
- **WHEN** a terminal job has no remaining pin and exceeds the configured retention period
- **THEN** reconciliation removes its job record from persistent metadata and cache

#### Scenario: Leave partial target objects after failure
- **WHEN** an export fails after copying one or more target objects
- **THEN** DataCoord leaves those objects in place and does not automatically delete them
