# Per-field logical sizes in Storage V3 segments

This design uses commit `163abc5e40` on `milvus-io/milvus` `master` as its baseline. Go V3 flush, Import V3, and compaction record the size of persisted insert values by `FieldID` for each output segment. DataCoord publishes the counters in `SegmentInfo.stats` together with the segment's manifest and row count. Query metering can use these counters to measure the size of the fields a query touches. Query pricing, QueryNode direct flush, and V1/V2 writes are outside the scope of this design.

A delete log leaves the values in the original segment, so its field sizes still include them. When compaction removes deleted rows or rows whose time to live (TTL) has expired, each output counts only the rows it retains. The same values must produce the same totals across compression codecs, column groups, and batch boundaries. **For the same inputs, routing, and LOB policy, metering must add zero LOB body read requests and zero bytes read.**

## What the counters measure

Each field with a known size has two unsigned counters. `payload_bytes` counts the bytes in its persisted values. `validity_bits` counts the positions that can be null under the schema. To combine a field across batches or segments, sum each counter before converting bits to bytes. The logical size in bytes is `sum(payload_bytes) + ceil(sum(validity_bits) / 8)`. Rounding validity bits per batch would make the result depend on the batch boundaries.

| Field type | `payload_bytes` per non-null value |
| --- | --- |
| `Bool`, `Int8` | 1 B |
| `Int16` | 2 B |
| `Int32`, `Float` | 4 B |
| `Int64`, `Double`, `Timestamptz` | 8 B |
| `FloatVector` | `4d` B for dimension `d`. |
| `Float16Vector`, `BFloat16Vector` | `2d` B. |
| `Int8Vector` | `d` B. |
| `BinaryVector` | `ceil(d/8)` B. |
| `SparseFloatVector`, including BM25 output | Each stored entry uses 8 B for a 4 B dimension index and a 4 B float value. The two rows `{2: 0.5, 100000: 1.0}` and `{7: 0.8}` total 24 B. |
| `VarChar`, legacy `String`, `Text` | UTF-8 bytes in the body. The character `U+732B` occupies 3 B. An empty string occupies 0 B. |
| `JSON`, including the dynamic field `$meta` | Bytes in the JSON text as written, including whitespace and escapes. |
| `Geometry` | Bytes in the well-known binary (WKB) value. |
| `Array`, nested arrays, `ArrayOfVector` | Sum the leaf elements recursively. When V3 encodes a `ScalarField` in a binary column, use the logical widths of its elements. |
| `StructArray` | Each persisted leaf uses its own field ID. The parent field ID carries 0 B of payload and counts the shared parent-row validity bits once. |

A `NULL` value contributes zero payload bytes. A nullable field contributes one validity bit per row, even when every value is present. A non-null array also contributes one bit for each nullable element position. A null parent has no child positions. For a nullable `Array<Int16?>`, the three rows `[7, NULL]`, `[]`, and `NULL` contribute 2 B of payload and 5 validity bits.

The writer counts the primary key, RowID, timestamp, dynamic JSON, function inputs, materialized function outputs, and persisted default values under their respective field IDs. A default supplied only at read time has no entry. The counters exclude Arrow offsets, buffer capacity, alignment, protobuf tags, column group wrappers, compression, indexes, and statistics files. [Field measurement](../../../internal/storage/field_logical_size.go) · [Array and vector encoding](../../../internal/storage/serde.go)

## Metadata and coverage

`Statistics.field_logical_sizes` uses protobuf field number 17. `FieldLogicalSizes` contains `version`, `row_count`, and a `fields` map keyed by schema `FieldID`. `FieldLogicalSize` contains `uint64 payload_bytes` and `uint64 validity_bits`. In version 1, every field entry must cover all `row_count` rows in the same commit. The segment record supplies the authoritative row count. [Protocol definitions](../../../pkg/proto/data_coord.proto)

DataCoord stores the committed manifest's base directory and revision in `SegmentInfo.field_logical_sizes_manifest` (field 44). The base directory must be nonempty and the revision must be greater than zero. During recovery, DataCoord checks both values and the segment's row count. An older DataCoord can preserve unknown protobuf fields while replacing segment data with the same number of rows. After an upgrade, a missing, unparseable, or mismatched manifest identifier invalidates the old counters. Import, snapshot copy, and CDC bind the counters to the local segment's manifest at the destination. [Catalog recovery](../../../internal/datacoord/meta/meta.go) · [Manifest identity](../../../internal/datacoord/meta/field_logical_sizes.go)

| Metadata | Meaning |
| --- | --- |
| The version and row count match, and the field ID has an entry. | The field total is exact, including an explicit `{0, 0}`. |
| The version and row count match, but the field ID has no entry. | The size of that field is unknown. Other fields can still have exact totals. |
| The statistics message is missing, the version is unknown, or the row count differs. | Field sizes are unknown for this snapshot. |

When appending two nonempty batches, sum a field only if both batches have an exact entry for it. If a segment has 100 unmeasured rows, measuring the next 20 rows gives no exact total for all 120 rows. A sync with zero insert rows preserves the existing totals. The accumulator checks each counter for overflow. `StatisticsCollector` returns independent data from `Clone` and `Publish`, so a failed attempt cannot alter confirmed statistics. [Accumulation](../../../internal/storage/field_logical_size_snapshot.go) · [Statistics collector](../../../internal/storage/statistics.go)

## Writing and publishing the counters

### Go V3 flush

`packedRecordBatchWriter` or `packedTextBatchWriter` measures the Arrow columns after projection. After write and close succeed, `BulkPackWriterV3` obtains the batch counters. Once the manifest commit succeeds, `finalizeStats` calls `DigestWithFieldSizes` on a copy of the collector. The sync task installs the copy after DataCoord acknowledges the result. [V3 flush](../../../internal/flushcommon/syncmgr/pack_writer_v3.go) · [Collector wrapper](../../../internal/flushcommon/metacache/segment_stats.go)

### WAL L1 checkpoint

`flushPack.PreviousRows` captures the last acknowledged row count from `durableMeta`. The writer then adds the current chunk's rows to `Meta.Stat.ModifiedRows` for the new report. Recovery initializes the collector with `PreviousRows`. Using the updated row count would count the current batch as part of the historical prefix on the second flush.

`pendingFlushChunk.persisted` holds the written manifest and cumulative statistics. If the acknowledgment times out, the writer resends the same result. Recovery skips any TimeTick the checkpoint already covers. `CommitL1Segment` carries the final cumulative statistics. [Segment view](../../../internal/streamingnode/server/wal/vchannel/segment/view.go) · [WAL writer](../../../internal/streamingnode/server/wal/vchannel/segment/pack_writer.go) · [Commit and recovery](../../../internal/streamingnode/server/wal/vchannel/segment/lifecycle_writer.go)

### Import V3

`executeImportPlan` takes field totals only from a writer that closes successfully and produces a final manifest. It reports those totals in `ImportTaskV3Result.statistics`. Intermediate sharding and merge files do not contribute to the result. DataCoord checks the manifest and the reported row count before saving the segment statistics. [Import execution](../../../internal/datanode/importv3/import_plan_executor.go) · [Result commit](../../../internal/datacoord/bulkimport/result.go)

### Full compaction

Each output writer accumulates its own totals. Mix, merge sort, and clustering filter deleted and expired rows first, then count each row in the segment that receives it. Sort compaction, namespace rewrites, and full schema rewrites obtain their snapshots from their final outputs. Column group rotation and file flushes do not change which segment owns a row. `CompactionSegment.stats` carries the totals for that segment. [Segment writer](../../../internal/datanode/compactor/segment_writer.go) · [Sort compaction](../../../internal/datanode/compactor/sort_compaction.go) · [Output statistics](../../../internal/datanode/compactor/stats.go)

### In-place field materialization

A partial writer receives only `outputFields`. `SetFieldLogicalSizeSchema` supplies the parent-child relationships in the target schema. The writer uses the original physical column order to count leaf values and attribute shared row validity to the parent. `field_logical_size_update_ids` (field 18) lists every affected ID. DataCoord replaces entries with exact values, removes entries for rewritten fields whose sizes are unknown, and preserves other fields. A schema-version-only change, a delete-log-only update, or an L0 write preserves the insert-field totals for the original segment. [In-place materialization](../../../internal/datanode/compactor/bump_schema_version_compactor.go) · [Merging field updates](../../../internal/datacoord/meta/stats_delta.go)

## TEXT and external LOBs

`Text` is a field type. Large object (LOB) storage holds long text bodies. The default inline threshold is 65,536 B. An inline value consists of `0x00` followed by the body. An external reference occupies 24 B and contains `0x01`, 3 B of padding, a 16 B file UUID, and a 4 B row number within that file. The row number locates the body but carries no length. Metering uses the existing `TextLobRef` encoding. [Storage format](20260407-text_lob_storage.md) · [Threshold and policy](../../../internal/compaction/lob_compaction.go)

**New writes, import, and `REWRITE_ALL`.** The `LobColumnWriter` in `milvus-storage` already accumulates `total_bytes` per column for both inline and external bodies. After `SegmentWriter` closes successfully and before destroying the handle, Go calls `loon_segment_writer_get_text_column_stats` once for all columns. It checks `total_entries == rows_written` for coverage and computes validity bits from the target schema. Milvus pins the `milvus-storage` revision that exposes these counters. [Storage API change](https://github.com/milvus-io/milvus-storage/pull/838) · [Dependency revision](../../../internal/core/thirdparty/milvus-storage/CMakeLists.txt) · [Go FFI](../../../internal/storagev2/packed/segment_writer_ffi.go)

**`REUSE_ALL`.** The writer first checks the Arrow validity bit for `NULL`. For a valid value, it reads the first byte to identify an inline value and excludes that tag byte from the payload. An inline value with a 23 B body and an external reference both occupy 24 B. The tag distinguishes them.

If an output contains external references, it can inherit a source column's total only when all three conditions hold.

- The source segment has materialized the field.
- The source snapshot has an exact total for that field.
- Every row and value in that source column reaches the same output segment unchanged.

Merge sort tracks each source row it removes from an input and appends to the output batch. It attributes the row to an output segment after the write succeeds. An output can combine a complete column from one input with directly measurable inline values from another. The two sets of rows must be disjoint. If any retained external value still has an unknown size, the writer omits that field from that output segment. [Source rows and inheritance](../../../internal/datanode/compactor/text_size_lineage.go) · [Merge sort](../../../internal/storage/sort.go)

Regular clustering compaction and Mix compaction that expects multiple outputs select `REWRITE_ALL`. When Mix expects at most one output, it computes the hole ratio `1 − ΣValidRows / ΣTotalRows` per field. The denominator counts each LOB file path once. With the default threshold, a ratio below 30% selects reuse, and a ratio of 30% or more selects rewrite. A zero denominator selects reuse. Sort compaction and schema rewrites also check the input and output partition storage directories and rewrite across directories. The expected number of outputs guides policy selection. Inheritance depends on where the rows actually go. File sizes and the fraction of surviving rows cannot establish the size of the remaining bodies. [Mix policy](../../../internal/datanode/compactor/mix_compactor.go) · [LOB policy selection](../../../internal/compaction/lob_compaction.go)

## DataCoord, recovery, and CDC

- A flush commit updates the manifest, checkpoint, row count, and cumulative statistics in one metadata change. An older report cannot replace a newer checkpoint. If two reports for the same commit disagree on a known value, DataCoord keeps the committed result and returns an error. Full compaction installs the absolute totals for each actual output. In-place compaction preserves untouched fields and uses the existing schema or manifest concurrency checks. [Flush commit](../../../internal/datacoord/services.go) · [Coverage and replay](../../../internal/datacoord/meta/field_logical_sizes.go) · [Compaction commit](../../../internal/datacoord/meta/meta.go)
- If an older writer appends unmeasured rows, the existing totals lose full coverage. If an in-place update can identify the affected fields from the column group, DataCoord invalidates only those fields. DataCoord also clears unverifiable totals when Spark commits a prebuilt manifest, an external refresh replaces a segment, or a legacy sort task adopts a new manifest that can filter rows. [External backfill](../../../internal/datacoord/services_commit_backfill.go) · [External refresh](../../../internal/datacoord/externalrefresh/task.go) · [Legacy sort](../../../internal/datacoord/stats/task.go)
- Snapshots carry optional counters through `SegmentDescription`, Avro `ManifestEntry`, `CopySegmentSource`, and `CopySegmentResult`. Avro stores each counter as an 8 B unsigned integer in big-endian order. The target inherits verifiable entries only when the V3 insert manifest, row set, and field identities match. A copy from an older snapshot without counters has unknown field sizes. [Snapshot format](../../../internal/snapshotio/snapshot.go) · [Copy commit](../../../internal/datacoord/copysegment/task.go)
- With change data capture (CDC), each cluster derives its counters when it writes local segments. Clusters can use different segment IDs and split points. Before comparing a field across clusters, check that both sides cover the same logical rows in full. [Cluster replication](../../agent_guides/streaming-system/replication/replicate.md)

## Verification examples

| Primary key | `name` | `body` payload | Compaction result |
| --- | --- | ---: | --- |
| 1 | Character `U+732B` | 1 B | Segment A |
| 2 | Empty string | 65,536 B | Segment B |
| 3 | `NULL` | 90,000 B | Removed by a delete record |
| 4 | `ab` | 7 B | Removed by TTL |

A Go V3 flush wrote these four rows to one segment. `name` had **5 B of payload and 4 validity bits**, and `body` had **155,544 B of payload**. Splitting the same input into segments of one and three rows gave the same sum for each counter of every field. Compaction removed rows 3 and 4. Segment A had **3 B** of `name` payload and **1 B** of `body` payload. Segment B had an **explicit 0 B** for `name` and **65,536 B** for `body`. [Cross-segment flush test](../../../internal/flushcommon/syncmgr/pack_writer_v3_test.go) · [Delete and TTL test](../../../internal/datanode/compactor/clustering_compactor_storage_v3_test.go)

Another test wrote the same three rows twice. The first write used gzip, one column group, and one batch. The second used Zstandard, two column groups, and two batches. The test checked the codecs in the actual Parquet footers. In both outputs, the string primary key totaled 5 B. The nullable string totaled 4,096 B of payload and 3 validity bits. [Compression and column group test](../../../internal/storage/record_writer_test.go)

A same-directory Mix test used two external bodies of 65,536 B and 90,000 B. The source column totaled **155,536 B**. When both values survived, the output inherited all 155,536 B. After a delete removed the longer body, the output omitted `body` and still reported known fields such as the primary key. Nine reuse cases covering Mix, merge sort, sort compaction, and schema rewrites passed with the LOB bodies unreadable. [Reuse tests](../../../internal/datanode/compactor/clustering_compactor_storage_v3_test.go) · [V3 writer tests](../../../internal/storage/record_writer_test.go)

The Go regression tests used `-tags dynamic,test -gcflags="all=-N -l" -count=1`. The V3 flush tests injected failures during write, close, statistics-file creation, and delete-log writes, after statistics finalization, and at manifest commit. Each retry counted only the three successfully written rows. After a WAL L1 checkpoint acknowledgment timed out, the writer resent the same statistics. Recovery and sealing from that checkpoint produced two rows with 16 B of primary keys and 64 B of vectors. A regular import and a row-by-row backup import each produced three physical fragments and performed an intermediate merge. Each final manifest counted three rows. [Checkpoint tests](../../../internal/streamingnode/server/wal/vchannel/segment/pack_writer_v3_test.go) · [Flush retry tests](../../../internal/flushcommon/syncmgr/task_v3_retry_test.go) · [Import tests](../../../internal/datanode/importv3/import_plan_executor_test.go)

- **DataCoord commits.** Two flush RPC tests used isolated local etcd and MinIO instances. A request to seal a segment at an old checkpoint while it still accepted writes returned a retriable error. After DataView publication recovered, the statistics and the new row count took effect together. A replay that changed a known zero to a nonzero value for the same manifest returned an error. An empty TEXT segment remained `Dropped` after a repeat seal request. [Flush RPC tests](../../../internal/datacoord/services_test.go)
- **Round trip through an older version.** A local test used the official baseline's Go protobuf bindings to read and serialize a new `SegmentInfo`. The old bindings preserved both unknown fields. When the new DataCoord reloaded the same revision from the catalog, it recovered the explicit zero and nonzero counts. When the old bindings changed only the manifest revision, DataCoord cleared the stale counters and preserved the existing file size statistics. [Catalog recovery tests](../../../internal/datacoord/meta/field_logical_sizes_test.go)
- **Snapshots and column replacement.** Snapshot export included only verifiable counters. Both reference-based and self-contained V6 snapshots preserved explicit zero and nonzero counts on disk. Both direct and staged copy kept the installed target when counts conflicted for the same revision. Targeted tests also verified invalidation after Spark replaced an indexed field, after an external refresh, and after a legacy sort. [Snapshot export tests](../../../internal/datacoord/handler_test.go) · [Snapshot persistence tests](../../../internal/snapshotio/storage/snapshot_data_test.go) · [Copy replay tests](../../../internal/datacoord/copysegment/task_test.go)

The `internal/storage` regression tests ran in batches. One run with a local filesystem, MinIO, and Azurite passed 520 top-level tests and 2,794 subtests. A separate run passed the Azure connection failure case. Two GCS cases passed with a local `fake-gcs-server`. Two existing cases did not run. One writes a 512 MiB value, and the other requires external TLS credentials.

## Local overhead measurements

The benchmarks ran on an Apple M5 arm64 with a local filesystem, `GOMAXPROCS=2`, and the Go compiler flags above. Both versions used the same `milvus_core`, inputs, and parameters. The storage baseline was `95215819ea9094f7b96be0f9503346dfee0b62e5`. The comparison added the TEXT counter interface to that revision. The runs followed the order baseline, implementation, implementation, baseline, with six samples per version. The table gives the median and the minimum to maximum range.

| Operation and timing interval | Official `master` baseline | With field measurement |
| --- | ---: | ---: |
| Go V3 flush, 4,096 rows, from `BulkPackWriterV3.Write` through manifest commit | 12.23 ms (11.91–12.68) | 12.42 ms (11.64–13.21) |
| V3 Mix, 1 million rows, from entry to return of `Compact` | 337.81 ms (333.63–365.01) | 333.73 ms (328.42–344.69) |

Each flush sample performed 15 writes. The input had two column groups, a 128-dimensional float vector, and a string field with one eighth of its values null. Each Mix sample processed one input segment and produced one output segment. It used an Int64 primary key, a 64-dimensional binary vector, and `mergeSplit`. The sample included no deletes. The timing ranges overlap for both operations, so these samples do not establish a consistent speedup or slowdown. [Mix benchmark](../../../internal/datanode/compactor/mix_compactor_bench_test.go)

- **LOB body I/O.** Both versions used the same bodies, inline threshold, and policy. New writes read 0 B in zero requests in both versions. Normal `REWRITE_ALL` read 20,376 B in one request in each version. On both paths, measurement added zero body reads and zero bytes read. [Native write and rewrite tests](../../../internal/storagev2/packed/segment_writer_ffi_test.go)
- **In-memory measurement loop.** Each batch had 4,096 rows and three columns, an Int64 primary key, a nullable string, and a 128-dimensional float vector. With all strings valid, column group measurement took 116 ns per batch. Adding field measurement increased this to 941 ns per batch. With one eighth of the strings null, the times were 115 ns and 31.95 µs per batch, respectively. Both inputs added 912 B and 13 Go allocations per batch. This benchmark measures only the in-memory column loop. [Microbenchmark](../../../internal/storage/field_logical_size_benchmark_test.go)
- **Segment metadata.** The test serialized a `SegmentInfo` with 1 million rows. Each field had 8 million payload bytes and 1 million validity bits. Field IDs started at 100. Including the manifest identifier, 4, 16, 64, and 256 fields added 118, 300, 1,056, and 4,128 B, respectively. The size depends on the field IDs, counts, and manifest path length. [Metadata protocol](../../../pkg/proto/data_coord.proto)

The timings and body read counts come from local writers. Remote object-store server request counts, live-cluster throughput, and reconciliation of totals between two CDC clusters remain unmeasured.
