# Incremental Talon prewarming after segment manifest publication

## Goal and scope

Warm newly published data, delta logs, indexes, stats, LOBs and the new manifest revision for sealed StorageV3 segments. Sealed includes Sealed, Flushing and Flushed states after publication. Growing segments are excluded. Cache population is local to each cluster; CDC secondaries warm their own derived files through their own commits. No WAL or replication contract changes.

## Configuration

`dataCoord.talonPrewarm.incrementalEnabled` defaults to false and requires restart. It is independent of `dataCoord.talonPrewarm.enabled`, which retains periodic manifest-only warming. Local storage skips warming. Remote deployments require a Talon coordinator and a supported cloud provider.

## Publication and execution

Both CommitSegmentManifest and CommitSegmentManifests enqueue only after successful catalog publication. The batch path excludes skipped segments. The commit path copies newly added object keys and known on-disk sizes directly from its mutation, including writer-owned column-group and LOB descriptors before the caller releases them. Each event owns its Go slice; it retains no caller-owned FFI writer output. The worker never opens a manifest or issues Stat. BatchLoad receives the known size and an empty version, which the native bridge maps to Talon's reserved unchecked version, matching normal known-size readers.

Structured commits carry the manifest size from the existing native commit callback. Prepared Noop producers can supply ManifestSize and NewObjects. Column-group sizes come from the writer's file_size property; LOB sizes come from its output descriptor. Delta-log and bloom/BM25 stat producers forward their known on-disk sizes. The stats-result adapter forwards insert/stats/BM25 keys and sizes. Missing per-object sizes are skipped with a warning, never guessed from memory size, an aggregate size for multiple files, or zero. Existing producer gaps remain: prepared revisions without a manifest size, multi-file indexes without per-file sizes, stats descriptors without per-file sizes, and artifacts not returned by a legacy producer (including unreported LOBs). These require producer metadata propagation for complete coverage.

The queue holds 1024 object lists. Admission never waits for queue capacity: a full queue skips the new event with a rate-limited warning, so slow prewarming cannot stall a manifest commit or hold its publication locks waiting for I/O. This bounded best-effort policy can miss prewarming under overload. A single worker sends batches of 1024 objects; failure in one batch is reported after continuing the remaining batches. Cancellation stops further submissions; shutdown cancels I/O and drains native resources. Pending events are ephemeral; restart does not replay them. Prewarm failures do not roll back committed metadata; there is no automatic retry or durable delivery guarantee.

Periodic and incremental prewarming share one Talon client with a single native admission slot. Stat and BatchLoad operations from both producers compete for that same slot per operation; neither producer reserves it for an entire round or commit event. Admission remains held until the native callback completes, including when the Go caller cancels. Each producer owns a client lease; the native client closes only after both producers have stopped and released their leases. The configuration switches remain independent. There is no cross-round deduplication: periodic warming still repeats loads to maintain its existing behavior.

## Planned PR breakdown

One independently reviewable layer contains configuration, single/batch hooks, object enumeration, worker lifecycle, tests and this document. No remote submission is performed as part of implementation.

## Validation

Cover disabled and independent switches, growing/sealed transitions, catalog failure and skipped batch members, mutation object extraction across data/delta/stats/index/LOB categories, removal-only commits, same-pointer no-op, nonblocking admission under a full queue, no-Stat known-size loads, continuation after failed batches, missing-size handling, shutdown and Talon failures. Run focused DataCoord and packed Go tests in the repository development container with dynamic,test tags and disabled optimization/inlining. No live Talon validation is claimed by source inspection or mocked tests.
