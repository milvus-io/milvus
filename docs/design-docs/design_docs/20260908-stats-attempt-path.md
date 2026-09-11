# StorageV3 stats attempt directories

StorageV3 TextIndex and JSON stats jobs can outlive a failed Drop RPC. Giving a
replacement a fresh task ID prevents an old metadata commit but does not stop
an old uploader overwriting the replacement's files in a shared directory.

New DataCoords request `UseV3StatsAttemptPath`. A supporting DataNode writes:

```
<segment-base>/_stats/<type>.<fieldID>/<taskID>/<file>
```

This also applies to stats baked into Sort output. Text results carry complete
object keys; standalone JSON results carry field-relative keys including the
task ID. The manifest commit preserves those paths. QueryNode's stats resolver
uses the actual compound text-file directory and preserves nested JSON paths.
The JSON stats file format is unchanged.

## Capability and upgrade contract

DataNodes advertise `supports_v3_stats_attempt_path` in QuerySlot. The scheduler
skips incompatible workers without consuming slots or blocking unrelated tasks.
CreateStats rechecks the selected worker's capability before sending CreateTask,
including for bound index nodes. An old worker produces ServiceUnimplemented at
this boundary; its unknown protobuf flag is never relied on for fencing.

QueryNodes advertise `V3StatsAttemptPath` in their sessions. DataCoord requires
all discovered QueryNodes to advertise support before dispatching StorageV3
stats jobs, and checks again before publishing worker results. An empty reader
set is treated as unknown capability. V2 jobs and empty-segment completion keep
their existing behavior. New DataNodes honor the flag independent of their
release string; requests from old coordinators that omit it retain the legacy
layout.

Deploy the new coordinator, supporting QueryNodes, and supporting DataNodes as a
monotonic upgrade. Stats work waits during mixed-reader operation. Once isolated
paths have been published, unsupported QueryNodes must not be rolled back or
admitted later. Session capability checks gate new work; they are not a durable
cluster-wide admission fence for an old reader reading already-published data.
Likewise, rolling back to a coordinator that writes shared directories gives up
the new retry isolation guarantee.

## Completion and recovery

Before any catalog publication, DataCoord validates each returned Text/JSON
stats file against the current task's directory and checks the build ID. A
legacy result recovered after upgrade is dropped best effort and marked Retry;
the existing inspector replaces it with a new task ID. Shared legacy files are
not deleted because another old task can still use them. Failed Drop does not
permit publication. A valid result is held InProgress while reader capability
is missing, rather than treating capability unavailability as an ambiguous
catalog failure and terminating the coordinator.

The layout does not introduce a new GC policy. Existing rejected-result cleanup
understands the nested paths; dropped-segment GC eventually removes the segment
directory. Unpublished files from lost workers in otherwise live segments still
need the existing orphan-cleanup follow-up. This patch does not delete them by
checking only the current manifest, which could invalidate historical snapshots.

## Verification

Focused Go tests cover capability serialization and rewatch, writer capability
propagation and direct-dispatch rejection, scheduler slot accounting, native
build parameters and result paths for two attempt IDs, manifest-backed nested
path resolution, rejection before catalog writes even when Drop times out, and
publication after readers become capable. Native index building is mocked in
the writer parameter tests; this is not a mixed-version cluster e2e run.
