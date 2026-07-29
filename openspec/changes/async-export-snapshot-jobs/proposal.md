## Why

`ExportSnapshot` currently performs every provider-side object copy and writes
the exported metadata before returning, so large exports hold the RPC open and
provide no durable status or progress after client timeout or DataCoord
restart. Export needs the same job-oriented user experience as snapshot
restore while preserving the existing self-contained bundle contract.

## What Changes

- **BREAKING**: change `ExportSnapshot` to return an export job ID instead of a
  completed `snapshot_metadata_uri`; update the Go SDK and REST response to
  expose that job ID.
- Add `GetExportSnapshotState` for querying job state, progress, failure reason,
  file counts, timing, and the completed snapshot metadata URI.
- Persist export jobs in DataCoord with Pending, Executing, Completed, and
  Failed terminal behavior so accepted jobs survive DataCoord restart.
- Report progress from durably completed file-copy batches. A restarted job
  resumes from its last validated checkpoint without decreasing externally
  reported progress.
- Keep source snapshot data pinned for the complete queued and executing job
  lifetime, and release the pin through retryable terminal cleanup.
- Persist request `external_spec` only while it is needed by a non-terminal job
  and clear it when the job reaches Completed or Failed. Never return or log the
  raw value.
- Keep provider-side copy, metadata-last publication, overlap validation, and
  the existing self-contained bundle layout unchanged.
- Apply Global `PrivilegeExportSnapshot` authorization to submission and state
  queries.
- Do not add export-job listing, cancellation, client-side streaming, or
  cross-provider copy in this change.

## Capabilities

### New Capabilities

- `async-snapshot-export`: Durable asynchronous snapshot export submission,
  monotonic progress queries, restart recovery, credential lifecycle, and
  terminal metadata publication.

### Modified Capabilities

None.

## Impact

- Public milvus-proto snapshot RPCs, message types, request privilege options,
  generated Go APIs, Go SDK behavior, and REST snapshot job endpoints.
- Internal DataCoord proto and MixCoord/Proxy RPC forwarding.
- DataCoord catalog keys, export-job metadata cache, lifecycle management,
  source snapshot pin handling, progress persistence, timeout, and retention.
- Existing snapshot exporter copy batching and progress reporting, without
  moving data transfer to clients or DataNodes.
- Snapshot design/user documentation and Go client end-to-end workflows, which
  must poll export completion before starting external restore.
