# Row budgets for Parquet refresh Function materialization

Issue: https://github.com/milvus-io/milvus/issues/55447

## Problem and scope

A file count is a poor proxy for BM25/embedding/MinHash materialization cost.
1024 compacted Parquet files containing over a billion rows produced one task
under the default 10,000-file target. Functions ran on one DataNode until the
one-hour job timeout. Merely increasing node count does not split that task.

This change refines base task ranges for Parquet collections with Functions.
It leaves DataNode execution, scheduling, the persisted task protocol and the
job-level apply/ownership contract in place. It does not change CDC behavior or
introduce a WAL message: physical task planning remains local to this cluster.

## Input provenance

In the pinned milvus-storage revision
`5c2ea329889e30d94b4e65209c0bc782256e7a95`, `PlainFormat::explore` writes -1 for
both row endpoints. `ReadFileInfosFromManifestPath` exposes `end_index` through
`FileInfo.NumRows`; those values must not be used as estimates.

`loon_exttable_get_file_info` instead creates a format reader and returns the
last row-group end offset. The Parquet reader derives those offsets from footer
row counts. DataCoord reuses DataNode's existing concurrent row-count resolver
before planning. Its concurrency is sixteen per planning attempt, not sixteen
per file. It does not execute Functions or read full data batches.

The explore manifest is unchanged. DataNode retains its authoritative metadata
read and source validation. This deliberately adds a metadata pass; persisting
counts in a new cross-component format is outside this change.

## Planning

`dataCoord.externalCollection.functionRowsPerTask` defaults to 16,000,000;
zero disables this behavior. The setting is refreshable and captured once per
new planning attempt. At default segment sizing this groups roughly sixteen
segments, matching one wave of manifest/Function workers. This is a tunable
row proxy, not a latency or memory bound and not an estimate of text length,
number of Functions, embedding service latency, or tokenizer complexity.

1. Keep the existing balanced file-count base boundaries.
2. Insert boundaries within those ranges before another file would exceed the
   row budget. Use subtraction/capped accumulation to avoid integer overflow.
3. A file is indivisible; a file larger than the target gets its own range.
4. Map files to the refined base ranges and run the existing transitive
   ownership closure. An old segment must still have exactly one task that can
   see all of its surviving source files. Closure can exceed either target.
5. Persist the complete plan before scheduling, then use the existing task
   results and single aggregate segment apply. Recovery reuses that plan.

Without Functions, for other formats, or with the budget disabled, no extra
metadata pass is added and file-count planning is retained.

## Failure and cancellation

Counts are estimates for a performance optimization. An estimation failure
while the planning context is live falls back to `min(filesPerTask,64)` before
ownership closure. This also avoids treating an unavailable count as zero.
DataNode remains responsible for normal source validation and error handling;
this planning change does not claim to repair upstream storage error taxonomy.

That distinction matters: the native FFI preserves some extended codes but
still has generic Arrow/exception fallbacks. The coordinator must not introduce
a new permanent-error decision based only on such a category.

Cancellation/expiry of the planning context returns without publishing a task
plan. Queued metadata work checks the context before entering native code;
already-running native reads drain before the resolver returns. The change
does not make synchronous native I/O forcibly cancellable. Late plan publication
is additionally rejected by the existing terminal-job guard.

## Documentation reconciliation

The older design described a scheduler that updated source/schema before job
submission. Current master already calls this a Manager and describes applying
segments before schema publication. The current implementation is the basis
here: the WAL callback records an Init job, background planning creates tasks,
and the job finish path publishes source/spec. Update stale package paths and
make the asynchronous phase explicit; do not restore the obsolete flow.

## Verification boundaries

- Pure range tests: compacted billion-row shape, file-only compatibility, skew,
  indivisible oversized files, zero rows, overflow, and randomized coverage.
- Ownership tests: crossed refined boundaries, transitive merge and missing
  baseline files; the existing Paimon/revision identity tests stay in place.
- Manager regression: actual Explore -1 inputs through the real row resolver,
  task-plan persistence, disabled/no-Function paths, estimation-error fallback,
  and canceled planning without task publication.
- Metadata regression: a barrier establishes sixteen active readers, then
  cancellation prevents queued native reads and drains in-flight work.

These tests distinguish planned tasks from completed refreshes. Multi-node
throughput, memory, native metadata behavior and end-to-end refresh completion
require native dependencies and a running cluster. Per-segment progress,
sub-file distributed tasks, resource-aware placement and row weighting for
other formats are follow-ups rather than benefits claimed by this change.
