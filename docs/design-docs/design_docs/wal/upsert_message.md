# Unified Insert and Upsert WAL format

## Motivation and scope

The write-ahead log (WAL) uses Insert V2 for ordinary Insert and Upsert for Upsert and partial update. The message types have distinct headers and bodies, with a shared nested InsertRows encoding. Ordinary Insert carries rows without delete effects. Upsert and partial update carry prepared rows and the producer's explicit delete effects. Consumers do not consult a local primary key (PK) index to infer replacements. This format does not enable PRIMARY_KEY indexes or implement their decision algorithm.

An autocommit write is one logical WAL record with one MessageID and one final visibility TimeTick. Its rows and delete effects use that TimeTick. Backend chunking preserves this logical identity. Routing creates one logical message per nonempty VChannel group. Local compare-and-swap (CAS) writes keep the existing per-VChannel transaction wrapper even for a single body. There is no cross-VChannel atomicity.

Logical WAL identity and visibility do not imply an in-memory transaction around native execution. Consumers apply rows and deletes through their existing execution paths. There is no rollback of partially completed native mutations. An execution failure stops progress rather than publishing completion for the failed operation.

Historical Insert and Delete records remain readable. Independent Delete messages remain supported. Public API semantics, including batch duplicate behavior, remain unchanged by the format name.

## Wire contract

The outer properties carry VChannel, TimeTick, transaction context, and the optional idempotency key. Insert V2 uses `InsertMessageHeaderV2`; Upsert keeps `UpsertMessageHeader`. Both carry fields 1 through 3 below. Only the Upsert header has field 4:

| Field number | Field | Meaning |
| --- | --- | --- |
| 1 | `collection_id` | Collection owning all rows and effects |
| 2 | `partitions` | Insert partition table with counts, binary sizes, and receiving-cluster segment assignments |
| 3 | `schema_version` | Present schema version, including explicit zero |
| 4 | `delete_rows` | Sum of explicit delete key counts |

Each `PartitionSegmentAssignment` contains `partition_id` (1), `rows` (2), `binary_size` (3), and `segment_assignment` (4). Partition IDs are positive and unique. Each entry has a positive row count. Segment IDs describe physical placement within the receiving cluster.

The body types remain distinct:

```proto
message InsertMessageBody {
    InsertRows rows = 1;
}
message UpsertMessageBody {
    InsertRows rows = 1;
    repeated UpsertDeleteEffect delete_effects = 2;
    PartialUpdateCAS partial_update_cas = 3;
}
```

`InsertRows` is shared:

| Field number | Field | Meaning |
| --- | --- | --- |
| 1 | `primary_keys` | Resolved Int64 or VarChar PKs in logical row order |
| 2 / 5 | `row_ids` / `delta_row_ids` | Internal row identifier for each inserted row |
| 3 | `partition_indexes` | Index into the header partition table for each row |
| 4 | `columns` | Ordinary columns and materialized function outputs |
| 7 | `request_row_mapping` | Original request offsets for retry results |

Insert V2 has no delete effects or header result. Its body directly owns one nested `InsertRows`. Upsert combines the same rows with optional explicit effects and CAS proof. Historical Insert V1 keeps its original header and `InsertRequest` body, including its optional header result. The nested Upsert layout replaces the unpublished flat draft; there is no flat-draft compatibility adapter.

Internal RowIDs use an `InsertRows` oneof: explicit `schema.LongArray row_ids` (field 2), or `DeltaEncodedRowIDs delta_row_ids` (field 5). The latter contains the first RowID (`base`, field 1) and packed signed ZigZag differences (`deltas`, field 2), preserving order including zero or negative differences. The encoder selects delta encoding only when the complete field is smaller and subtraction cannot overflow int64. Readers reject reconstruction overflow. Full-body and projection readers expose explicit logical values, and materialization reselects the wire encoding. Immutable readers share referenced PK values. Mutable Body readers detach delete-key containers so editing inserted keys cannot silently alter an independent delete effect; immutable string values remain shared. This replaces the unpublished draft representation without an adapter; historical formal Insert/Delete formats are unchanged.

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

Each `UpsertDeleteEffect` has exactly one scope: `all_partitions` (field 1) or a positive `partition_id` (field 2). Its key source is either nonempty `primary_keys` (field 3), matching the collection's PK type, or an explicit `all_inserted_keys` marker (field 4). The marker references the complete ordered inserted PK sequence, preserving duplicate occurrences; readers never infer delete keys from an absent source. Producers use the marker only for identical sequences. Independent keys retain their explicit representation. All-partitions scope covers only this collection's partitions within the message's VChannel. A targeted partition need not receive inserted rows. Delete keys need not equal or be a subset of inserted keys. For example, the message above can independently delete key 99 from partition 30. `delete_rows` counts all resolved key occurrences.

Deletes affect only versions strictly earlier than the final visibility TimeTick. They cannot delete this message's newly inserted rows. An absent older version is a no-op. Repeated PKs within a message or across transaction bodies retain existing equal-TimeTick semantics. The format does not promise last-write-wins among those rows.

AutoID values must be resolved before packing. Namespace requests and namespace-enabled collections are unsupported by this format. Proxy rejects them before producing Upsert records, and StreamingNode rejects namespace collection schemas for local and replicated appends. There is no legacy producer fallback in 3.1. Historical namespace-bearing Insert records remain readable; independent Delete retains its existing routing semantics.

Producers validate ordinary column values. Shard admission validates collection ownership, schema version, placement, counts, PK types, column identities, delete scopes, mapping, and CAS shape. A schema-version mismatch precedes schema-dependent content checks. CAS proof presence must match the existing outer CAS marker.

## Producer and append flow

Proxy resolves API behavior, AutoID, partition placement, function preparation, and delete effects before encoding. Existing AutoID rows retain their PK and corresponding delete effects. Missing rows receive allocated keys without an old-version delete effect. Ordinary Insert supplies no deletes.

Proxy routes by the final PK to business VChannels. A specified partition supplies one target, while partition-key routing can select several targets within a message. Packing creates one logical message per nonempty VChannel group, not per partition or backend record size. Original row offsets survive VChannel routing. The current Proxy packer groups delete occurrences by their preserved PK's VChannel and keeps the original delete order within each logical message. This packing policy does not narrow the wire contract's independent scopes and key sets.

Producers construct ordinary, persisted, replicable messages on the collection business VChannel. This is an internal producer contract, not an Upsert-specific flags validation requirement at each interceptor. Generic builder and replication property handling remains unchanged. The shard dispatcher keeps the existing CChannel skip rule and PChannel-level exception.

There is no producer format switch in 3.1. StreamingNode accepts supported Insert V1, Insert V2 and Upsert records. TimeTick and transaction handling retain their existing position in the interceptor chain. Partial-update locking covers the union of inserted keys and explicit delete keys. CAS uses the existing proof, conflict, retry, and version-tracking paths.

The shard handler checks schema compatibility and ownership, prepares function materialization when enabled, and assigns all insert targets as a batch. Every partition must be ready. Per-partition selection checks fencing, TimeTick, Growing state, and capacity. No rows or bytes are charged until all candidates are available. Ack references precede batch accounting, including capacity-triggered sealing. Failed accounting releases those references. Transaction cleanup retains its existing segment-reference lifecycle.

Within one admission attempt, the function adapter, column writeback, and partition sizing reuse one decoded body. The attempt owns this value; it is not a mutable-message body cache and is discarded on return or redo. Materialization updates that body and re-encodes only when outputs change. Projection data remains separate and does not retain column values.

Each partition's `binary_size` describes its own selected rows, including PK and explicit logical RowID representation; choosing a compact wire encoding does not reduce this accounting. It excludes delete effects and the outer envelope. A later target requiring redo cannot leave a charged successful prefix. The receiving StreamingNode writes local segment assignments into the header before backend append.

## Transaction and retry

Local CAS writes retain the existing transaction wrapper even when only one body is needed. The normal producer sequence is:

1. Wait for Begin to succeed.
2. Send bodies and wait for every body to succeed.
3. Send Commit.

An Upsert is one successful logical body regardless of its partition count, effects, or backend chunks. The producer guarantees that Begin, bodies, and Commit use the same VChannel. TimeTick relies on this contract without an Upsert-specific membership check. Collection ownership and body identity are checked during shard admission. Commit does not retain and revalidate complete Begin/body messages.

A nonzero `body_cnt` must equal successful logical body appends. Zero keeps the existing compatibility behavior for all message types, including Upsert. A new empty transaction is still rejected. Sessions retain counts, state, in-flight admission, and a lightweight effects classification. Upsert without deletes is insert-only. Upsert with explicit deletes requires Transform consumption. Recovery rebuilds the same counts and classification from durable bodies.

Commit freezes admission and uses the existing wait for in-flight bodies. This does not extend the producer contract to sending Commit before body results. Rollback, expiry, rejection, cleanup callbacks, and lease behavior remain unchanged. Cancellation or fencing can leave a Commit outcome uncertain until recovery determines durable state.

`BuildInsertOrUpsertResult` combines the original stored PKs with `rows.request_row_mapping.row_offsets`. First append, transaction result buffering, Summary, and keyed retries share this path. Keyed writes require complete mapping and valid results. A keyed Commit cannot cache or return a partial merge. Missing buffered results return TransactionExpired so the existing producer can rebuild the transaction.

A duplicate returns the original AppendResult, including original MessageID, TimeTick, and available result IDs. It must not substitute newly allocated IDs from a retry request. Summary and WAL recovery reconstruct results from the original records. Retry suppression retains the existing bounded window and unknown-outcome behavior. Keyless re-appends remain separate writes. This format does not add unconditional exactly-once delivery.

## Consumption, persistence and recovery

Native consumers obtain an `InsertRowsReader` through `AsInsertRowsReader`. It reads selected rows into the existing storage InsertRequest representation. Partition selections are computed once and shared by all columns and receiving segments. A single partition uses an implicit all-row selection, without an array of row numbers. All-row readers own execution metadata while borrowing immutable column values and RowIDs. Multi-partition selections preserve interleaving and use one exactly sized backing array divided into partition-local slices; they do not reorder the wire rows. Consumers restore the standalone PK column and supply system fields at the execution boundary. Nullable compact vectors use logical-to-physical positions. StructArray, VectorArray, and function outputs follow existing FieldData conversion rules.

`DeleteRequests` exposes only explicit effects at final visibility time. Native growing execution, L1 persistence, L0 materialization, Summary, and local flush replay use the same logical format. Transaction assembly supplies Commit visibility before these consumers apply the bodies.

Growing and Transform progress are independent. A row-only autocommit Upsert and an insert-only transaction require no new Transform work. Delete effects and explicit barriers retain their Transform semantics. Write-side QueryMVCC, TransformLog classification, and runtime publication use the same distinction.

L1 and L0 persistence can finish at different times. Completion and checkpoint publication must account for both required effects. Recovery compares each side's snapshot independently: a completed L1 must not suppress missing L0 work, and a completed L0 must not suppress missing L1 work. Replay uses the durable assignments of the receiving cluster and the final transaction visibility.

There is no whole-unit predecode or native rollback. Body decoding, decryption, materialization, conversion, and native apply errors propagate through the actual execution path. Live native apply failure stops with the existing panic before applied-frontier publication. Other consumer paths retain their error and completion handling. A later message cannot conceal a failed operation by advancing its completion or visibility frontier.

## Encoding and projections

The borrowed Proxy view encoder writes selected columns directly into the final payload without constructing copied destination columns. Insert and Upsert encoders reuse the same `InsertRows` codec. Each body adds its own nested protobuf framing. No temporary encoded row buffer is required solely for nesting.

Proxy no longer splits a VChannel batch to fit a physical backend record. The existing materialized Insert/Delete request limits and transport limits remain. SN chunks the final encoded, possibly encrypted payload when its backend has a configured chunk budget. Backend chunking does not decode rows or create extra transaction bodies. Keyed and CAS writes use the same physical mechanism within existing logical-request limits. Fewer logical records do not establish a throughput gain without measurement.

The Insert/Upsert projection reads PKs, RowIDs, partition indexes, delete effects, mapping, CAS, and column descriptors. It enters the nested rows field and skips ordinary column values without calling full `Body()`. Descriptors contain field IDs, not scalar/vector values. The projection decoder merges selected wire fields directly into their protobuf destinations rather than concatenating them into a temporary encoded buffer. Repeated embedded fields retain protobuf merge semantics. This projection is distinct from the encoder's non-column fields, which participate in complete body encoding.

Immutable readers share the projection through the bounded body cache. Projection and full-body entries share its budget and eviction policy. Accounting covers decoded projection data and reserved derived mappings. Row-selection and nullable-vector position caches belong to the projection and leave with its eviction. Projection entries do not retain the decrypted payload or ordinary column values.

Replacing a mutable payload invalidates its projection. Header-only segment assignment does not invalidate unchanged body data. Replacing delete effects copies other encoded fields, including columns, and encodes the replacement effects. Encryption still covers the complete payload. Projection reads may decrypt the whole payload but skip ordinary column decoding. No partial encryption is introduced.

## CDC, activation and compatibility

CDC forwards the primary's decided rows, partition identities, explicit effects, CAS proof, and request mapping. It does not repeat PK-index decisions on the secondary. The receiving cluster assigns its own segment IDs and uses its existing replication timestamp and transaction replay rules. Receiving-prefix recovery, local flush abort/replay, and switchover retain the same logical effects.

The native QueryView path from #52977 is a reader dependency and is included in the upstream base. Deployment still requires compatible readers. This change adds no legacy MsgPack pipeline or msgdispatcher support.

3.1 removes `streaming.wal.enableUpsertMessage`, `proxy.splitChunk`, and `streaming.splitChunkSN`. Insert always produces Insert V2. Upsert and partial update always produce Upsert. Proxy does not split by backend record size. SN always performs physical chunking when a payload exceeds its backend budget; an unbounded backend retains its single-record behavior. Readers always reassemble chunks independently of configuration.

Deployment first upgrades every component to the prerequisite latest 3.0 release with complete Insert V2/Upsert reception, consumption and chunk-reading support. The 3.0 compatibility backport is separate release work; this 3.1 implementation does not prove that backport complete. Existing 3.0 producers keep their formats. Deployment management must also upgrade CDC targets before sending new formats. There is no automatic remote compatibility negotiation or retained-history downgrade guarantee.

During a mixed 3.0/3.1 rollout, a new Proxy may send a large logical message to an old SN that cannot chunk writes. That SN can remain in its backend retry loop. After shutdown/fencing and reassignment, a still-live producer can resume on a new owner; its deadline may expire first. Temporary delay or timeout is accepted. No new version probe, config propagation wait, retry policy or format fallback is added.

Local CAS writes still use their existing transaction wrapper, even with one body. Ordinary writes no longer need a transaction solely because of size or partition grouping. Historical and replicated transactions remain readable. No cross-VChannel atomicity is introduced.

## Validation requirements

Validation must inspect old and new field values and physical rows where query deduplication could hide stale versions. Cover ordinary Insert, Upsert, partial update, AutoID, CAS, duplicates, independent delete scope, and single/multiple partitions. Exercise specified-partition and partition-key routing through actual producers.

Trace redo, accounting failure, cancellation, unknown append outcomes, duplicate requests, and bounded retry recovery. Verify original results after WAL reopen. Cover Commit, Rollback, expiry, body-count mismatch, producer VChannel grouping, and cleanup. Preserve independent L1/L0 recovery and test flush, restart, reload, compaction, and local flush replay.

Check decode/decrypt/conversion/apply failures at actual consumers and prove that later work cannot advance past a failed completion. Verify row-only versus delete-bearing frontier classification. Test CDC receiving-prefix replay, receiver restart, and switchover separately from local persistence.

Confirm legacy history reads, unconditional Insert V2/Upsert generation, no size-induced transaction controls, retained CAS transactions, and physical chunk identity across retries. Simulated session switching and compile-only checks do not establish a real mixed-version rollout or CDC acceptance.

Projection tests must prove ordinary column values are not decoded. Encoding tests must compare complete nested payloads and properties, and verify one logical message per VChannel despite physical chunking. Benchmarks should measure projection, Proxy encoding, scatter, allocations, and WAL bytes under stated encryption and function-materialization conditions. Do not infer end-to-end throughput from isolated encoder results.
