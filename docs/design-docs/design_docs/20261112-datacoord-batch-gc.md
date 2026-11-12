# DataCoord batch garbage collection

Related issue: #54143. Builds on segment writer locks in #54675.

## Scope

Batch the existing dropped-segment, field-index and record-only segment-index
cleanup paths. Eligibility remains governed by retention, loaded segments,
snapshots, DataView compaction lineage, index readiness and the physical
StreamingNode checkpoint. Missing or unreadable checkpoints retain segments.
Manifest-resident index retirement keeps its existing publication transaction.
Physical segment and index artifacts are local to each cluster; this does not
add a logical state transition or WAL/CDC message.

## Deletion and retry

The dropped-segment pipeline marks index tasks deleted, removes exact file
keys, removes V3 segment directories, deletes segment-index metadata, then
removes the segment metadata. Only candidates that complete a stage enter the
next. Exact files go before V3 directories so an index deletion failure retains
the manifest that identifies the files to retry. Existing path ownership and
local-root validation are unchanged.

Catalog segment deletion has two phases. First remove the slash-terminated
insert, delta, stats and BM25 binlog prefixes; after every prefix batch succeeds,
remove segment main records by exact key. Batches obey the backend transaction
operation limit. No old CAS persistence layer is used. This is deliberately not
one atomic transaction: an interrupted prefix phase retains every main record;
an interrupted record phase can leave a subset of main records. All files and
binlog prefixes are already gone for records removed in that final phase.
Retries are idempotent. In-memory removals publish only after the catalog call
succeeds; a restart rediscovers remaining durable markers.

GC rechecks immutable segment identity under the manifest locks before file
removal and under segment writer locks before catalog removal. Index metadata
batches acquire BuildID locks in order and skip records whose identity, version,
state or file keys changed. Publication retains the master lock order:
manifest locks, BuildID locks, segment writer locks, then MetaStore shards.
Pause acknowledgements occur outside deletion batches; queued candidates are
filtered again before deletion starts.

## Object storage

S3/MinIO uses the SDK's Multi-Delete API, with at most 1000 unique objects per
request. Other backends use bounded individual deletes. Each requested path
has an outcome: NotFound is success; missing, unknown or failed results retain
its later metadata. Prefix deletion lists each prefix separately with bounded
concurrency and coalesces discovered object keys into bounded delete batches.
It does not assume a server-side multi-prefix delete operation. Cancellation
stops subsequent metadata stages; partial object deletion is retried.

`dataCoord.gc.droppedSegment.batchDelete.batchSize` and
`dataCoord.gc.indexFileBatchDelete.batchSize` default to 1000 and accept 1–1000.
They bound admission by candidates and estimated object paths; one oversized
segment or index is processed alone. SDK requests remain bounded. The existing
`removeConcurrent` budget limits listing and individual-delete fallback.
Metric names and label sets are unchanged.

## Verification boundaries

Regression coverage includes partial and missing object results, prefix failures,
NotFound, cancellation, paused collections, stale metadata, manifest rollback,
snapshot protection, exact-key sibling preservation, both catalog failure phases
and idempotent retries. A real SDK HTTP test verifies request splitting and
per-object error mapping. Local fault injection and allocation/request-count
benchmarks do not establish production throughput, distributed upgrade behavior,
or a deployed TiKV result.
