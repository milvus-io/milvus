# Unified Insert and Upsert WAL format

## Motivation and scope

When enabled, the write-ahead log (WAL) uses one Upsert format for Insert, Upsert, and partial update. Ordinary Insert carries rows without delete effects. Upsert and partial update carry prepared rows and the producer's explicit delete effects. Consumers do not consult a local primary key (PK) index to infer replacements. This format does not enable PRIMARY_KEY indexes or implement their decision algorithm.

An autocommit write is one logical WAL record with one MessageID and one final visibility TimeTick. Its rows and delete effects use that TimeTick. Backend chunking preserves this logical identity. A request that produces multiple messages still uses the existing per-VChannel transaction protocol. There is no cross-VChannel atomicity.

Logical WAL identity and visibility do not imply an in-memory transaction around native execution. Consumers apply rows and deletes through their existing execution paths. There is no rollback of partially completed native mutations. An execution failure stops progress rather than publishing completion for the failed operation.

Historical Insert and Delete records remain readable. Independent Delete messages remain supported. Public API semantics, including batch duplicate behavior, remain unchanged by the format name.

## Wire contract

The outer properties carry VChannel, TimeTick, transaction context, and the optional idempotency key. `UpsertMessageHeader` contains:

| Field number | Field | Meaning |
| --- | --- | --- |
| 1 | `collection_id` | Collection owning all rows and effects |
| 2 | `partitions` | Insert partition table with counts, binary sizes, and receiving-cluster segment assignments |
| 3 | `schema_version` | Present schema version, including explicit zero |
| 4 | `delete_rows` | Sum of explicit delete key counts |

Each `PartitionSegmentAssignment` contains `partition_id` (1), `rows` (2), `binary_size` (3), and `segment_assignment` (4). Partition IDs are positive and unique. Each entry has a positive row count. Segment IDs describe physical placement within the receiving cluster.

`UpsertMessageBody` contains:

| Field number | Field | Meaning |
| --- | --- | --- |
| 1 | `primary_keys` | Resolved Int64 or VarChar PKs in logical row order |
| 2 | `row_ids` | Internal row identifier (RowID) for each inserted row |
| 3 | `partition_indexes` | Index into the header partition table for each row |
| 4 | `columns` | Ordinary columns and materialized function outputs |
| 5 | `delete_effects` | Explicit keys with independent deletion scopes |
| 6 | `partial_update_cas` | Optional compare-and-swap (CAS) proof |
| 7 | `request_row_mapping` | Original request offsets for retry results |

PK, RowID, and Timestamp are not repeated in `columns`. The final message TimeTick supplies inserted-row and delete timestamps. A transaction uses Commit's final TimeTick. The body does not embed an InsertRequest or DeleteRequest and stores no duplicate retry result.

With one partition, omit `partition_indexes`: every row uses table entry zero. With multiple partitions, provide one valid index for each logical row. Index frequencies must match header counts. For example:

```text
primary_keys:        [101, 102, 103]
row_ids:             [9001, 9002, 9003]
partition table:     [partition 10: rows 2, partition 20: rows 1]
partition_indexes:   [0, 1, 0]
request row offsets: [4, 0, 7]
```

Partition 10 receives logical rows 0 and 2, with keys 101 and 103. Partition 20 receives logical row 1, with key 102. The offsets refer to positions 4, 0, and 7 in the original request, not positions within this message. Every column uses the same logical selection.

Each `UpsertDeleteEffect` has exactly one scope: `all_partitions` (field 1) or a positive `partition_id` (field 2). Its nonempty `primary_keys` (field 3) must match the collection's PK type. All-partitions scope covers only this collection's partitions within the message's VChannel. A targeted partition need not receive inserted rows. Delete keys need not equal or be a subset of inserted keys. For example, the message above can independently delete key 99 from partition 30. `delete_rows` counts all explicit key occurrences.

Deletes affect only versions strictly earlier than the final visibility TimeTick. They cannot delete this message's newly inserted rows. An absent older version is a no-op. Repeated PKs within a message or across transaction bodies retain existing equal-TimeTick semantics. The format does not promise last-write-wins among those rows.

AutoID values must be resolved before packing. Namespace requests and namespace-enabled collections are unsupported by this format. Proxy rejects them before producing Upsert records, and StreamingNode rejects namespace collection schemas for local and replicated appends. The legacy producer retains its existing namespace behavior when the format is disabled.

Producers validate ordinary column values. Shard admission validates collection ownership, schema version, placement, counts, PK types, column identities, delete scopes, mapping, and CAS shape. A schema-version mismatch precedes schema-dependent content checks. CAS proof presence must match the existing outer CAS marker.

## Producer and append flow

Proxy resolves API behavior, AutoID, partition placement, function preparation, and delete effects before encoding. Existing AutoID rows retain their PK and corresponding delete effects. Missing rows receive allocated keys without an old-version delete effect. Ordinary Insert supplies no deletes.

Proxy routes by the final PK to business VChannels. A specified partition supplies one target, while partition-key routing can select several targets within a message. Packing groups by VChannel and encoded size, not partition. Original row offsets survive routing and size splitting. The current Proxy packer associates each delete occurrence with the first inserted occurrence of its PK and preserves delete order across chunks. This packing policy does not narrow the wire contract's independent scopes and key sets.

Producers construct ordinary, persisted, replicable messages on the collection business VChannel. This is an internal producer contract, not an Upsert-specific flags validation requirement at each interceptor. Generic builder and replication property handling remains unchanged. The shard dispatcher keeps the existing CChannel skip rule and PChannel-level exception.

The common WAL Append entry checks activation. TimeTick and transaction handling retain their existing position in the interceptor chain. Partial-update locking covers the union of inserted keys and explicit delete keys. CAS uses the existing proof, conflict, retry, and version-tracking paths.

The shard handler checks schema compatibility and ownership, prepares function materialization when enabled, and assigns all insert targets as a batch. Every partition must be ready. Per-partition selection checks fencing, TimeTick, Growing state, and capacity. No rows or bytes are charged until all candidates are available. Ack references precede batch accounting, including capacity-triggered sealing. Failed accounting releases those references. Transaction cleanup retains its existing segment-reference lifecycle.

Each partition's `binary_size` describes its own selected rows, including PK and RowID. It excludes delete effects and the outer envelope. A later target requiring redo cannot leave a charged successful prefix. The receiving StreamingNode writes local segment assignments into the header before backend append.

## Transaction and retry

Local CAS writes retain the existing transaction wrapper even when only one body is needed. The normal producer sequence is:

1. Wait for Begin to succeed.
2. Send bodies and wait for every body to succeed.
3. Send Commit.

An Upsert is one successful logical body regardless of its partition count, effects, or backend chunks. The producer guarantees that Begin, bodies, and Commit use the same VChannel. TimeTick relies on this contract without an Upsert-specific membership check. Collection ownership and body identity are checked during shard admission. Commit does not retain and revalidate complete Begin/body messages.

A nonzero `body_cnt` must equal successful logical body appends. Zero keeps the existing compatibility behavior for all message types, including Upsert. A new empty transaction is still rejected. Sessions retain counts, state, in-flight admission, and a lightweight effects classification. Upsert without deletes is insert-only. Upsert with explicit deletes requires Transform consumption. Recovery rebuilds the same counts and classification from durable bodies.

Commit freezes admission and uses the existing wait for in-flight bodies. This does not extend the producer contract to sending Commit before body results. Rollback, expiry, rejection, cleanup callbacks, and lease behavior remain unchanged. Cancellation or fencing can leave a Commit outcome uncertain until recovery determines durable state.

`BuildUpsertResult` combines the original stored PKs with `request_row_mapping.row_offsets`. First append, transaction result buffering, Summary, and keyed retries share this path. Keyed writes require complete mapping and valid results. A keyed Commit cannot cache or return a partial merge. Missing buffered results return TransactionExpired so the existing producer can rebuild the transaction.

A duplicate returns the original AppendResult, including original MessageID, TimeTick, and available result IDs. It must not substitute newly allocated IDs from a retry request. Summary and WAL recovery reconstruct results from the original records. Retry suppression retains the existing bounded window and unknown-outcome behavior. Keyless re-appends remain separate writes. This format does not add unconditional exactly-once delivery.

## Consumption, persistence and recovery

Native consumers obtain an `InsertRowsReader` through `AsInsertRowsReader`. It reads selected rows into the existing storage InsertRequest representation. Partition selections are computed once and shared by all columns and receiving segments. Consumers restore the standalone PK column and supply system fields at the execution boundary. Nullable compact vectors use logical-to-physical positions. StructArray, VectorArray, and function outputs follow existing FieldData conversion rules.

`DeleteRequests` exposes only explicit effects at final visibility time. Native growing execution, L1 persistence, L0 materialization, Summary, and local flush replay use the same logical format. Transaction assembly supplies Commit visibility before these consumers apply the bodies.

Growing and Transform progress are independent. A row-only autocommit Upsert and an insert-only transaction require no new Transform work. Delete effects and explicit barriers retain their Transform semantics. Write-side QueryMVCC, TransformLog classification, and runtime publication use the same distinction.

L1 and L0 persistence can finish at different times. Completion and checkpoint publication must account for both required effects. Recovery compares each side's snapshot independently: a completed L1 must not suppress missing L0 work, and a completed L0 must not suppress missing L1 work. Replay uses the durable assignments of the receiving cluster and the final transaction visibility.

There is no whole-unit predecode or native rollback. Body decoding, decryption, materialization, conversion, and native apply errors propagate through the actual execution path. Live native apply failure stops with the existing panic before applied-frontier publication. Other consumer paths retain their error and completion handling. A later message cannot conceal a failed operation by advancing its completion or visibility frontier.

## Encoding and projections

The borrowed Proxy view encoder writes selected column values directly without first constructing copied destination columns. Size planning selects an accepted row prefix before final encoding. Conservative budgets include non-column fields, partition information, framing, CAS, delete effects, and envelope/encryption overhead. An indivisible plain row can use existing backend chunking. Keyed and CAS writes retain their size limits. These mechanisms require measurement before any performance claim.

The Upsert projection reads PKs, RowIDs, partition indexes, delete effects, mapping, CAS, and column descriptors. It scans wire fields and skips ordinary column values without calling full `Body()`. Descriptors contain field IDs, not scalar/vector values. This projection is distinct from the encoder's non-column fields, which participate in the complete body encoding.

Immutable readers share the projection through the bounded body cache. Projection and full-body entries share its budget and eviction policy. Accounting covers decoded projection data and reserved derived mappings. Row-selection and nullable-vector position caches belong to the projection and leave with its eviction. Projection entries do not retain the decrypted payload or ordinary column values.

Replacing a mutable payload invalidates its projection. Header-only segment assignment does not invalidate unchanged body data. Replacing delete effects copies other encoded fields, including columns, and encodes the replacement effects. Encryption still covers the complete payload. Projection reads may decrypt the whole payload but skip ordinary column decoding. No partial encryption is introduced.

## CDC, activation and compatibility

CDC forwards the primary's decided rows, partition identities, explicit effects, CAS proof, and request mapping. It does not repeat PK-index decisions on the secondary. The receiving cluster assigns its own segment IDs and uses its existing replication timestamp and transaction replay rules. Receiving-prefix recovery, local flush abort/replay, and switchover retain the same logical effects.

The native QueryView path from #52977 is a reader dependency and is included in the upstream base. Deployment still requires compatible readers. This change adds no legacy MsgPack pipeline or msgdispatcher support.

`streaming.wal.enableUpsertMessage` defaults to `auto` and uses the standard version gate:

| Setting | Behavior |
| --- | --- |
| `auto`, distributed deployment | Remain false until this cluster's registered online components satisfy version 3.1.0 and a one-minute stability window |
| `auto`, embedded-etcd deployment | Use the standard local-process version condition, since that process is the cluster |
| `false` | Produce legacy Insert/Delete records |
| `true` | Force the new format without the version condition |

The gate checks local cluster versions only. It does not verify CDC target compatibility or guarantee downgrade safety for retained WAL history. Deployment management must ensure compatible CDC targets before activation.

Format selection occurs before packing. Append failure does not trigger a second append in another format. Already packed requests retain their format during producer retries. StreamingNode rejects new Upsert appends while disabled. Disabling during an in-flight request can therefore fail that append. Retained Upsert decoding and Commit/Rollback for durable bodies remain available. Disabling generation does not make old readers safe to read retained new-format records.

## Validation requirements

Validation must inspect old and new field values and physical rows where query deduplication could hide stale versions. Cover ordinary Insert, Upsert, partial update, AutoID, CAS, duplicates, independent delete scope, and single/multiple partitions. Exercise specified-partition and partition-key routing through actual producers.

Trace redo, accounting failure, cancellation, unknown append outcomes, duplicate requests, and bounded retry recovery. Verify original results after WAL reopen. Cover Commit, Rollback, expiry, body-count mismatch, producer VChannel grouping, and cleanup. Preserve independent L1/L0 recovery and test flush, restart, reload, compaction, and local flush replay.

Check decode/decrypt/conversion/apply failures at actual consumers and prove that later work cannot advance past a failed completion. Verify row-only versus delete-bearing frontier classification. Test CDC receiving-prefix replay, receiver restart, and switchover separately from local persistence.

Confirm legacy history reads, explicit gate overrides, version-gate registration and timing, and gate-off producer selection. Simulated session switching and compile-only checks do not establish a real mixed-version rollout or CDC acceptance.

Projection tests must prove ordinary column values are not decoded. Encoding tests must compare split boundaries, full payloads, and properties. Benchmarks should measure projection, Proxy encoding, scatter, allocations, and WAL bytes under stated encryption and function-materialization conditions. Do not infer end-to-end throughput from isolated encoder results.
