# Upsert WAL format

## Purpose

When enabled, Insert, Upsert, and partial update requests use one Upsert WAL format. A logical message has one MessageID and one final visibility TimeTick. Backend chunking does not create separately acknowledged Insert and Delete messages.

An ordinary Insert records rows without delete effects. An Upsert records the delete effects that the primary producer has already decided. Consumers execute these explicit effects. They do not query a local PK index to infer replacement behavior.

This format replaces the unreleased draft Upsert format. There is no draft adapter. Historical released Insert and Delete messages remain readable. Independent Delete messages remain supported.

## Wire contract

The header contains:

| Field number | Field | Meaning |
| --- | --- | --- |
| 1 | collection_id | One collection identity |
| 2 | partitions | Partition table with row counts, binary sizes, and receiving-cluster segment assignments |
| 3 | schema_version | Explicitly present schema version, including zero |
| 4 | delete_rows | Total explicit delete key count |

The body contains:

| Field number | Field | Meaning |
| --- | --- | --- |
| 1 | primary_keys | Inserted Int64 or VarChar keys |
| 2 | row_ids | Inserted RowIDs |
| 3 | partition_indexes | Partition table index for each logical row |
| 4 | columns | Ordinary columns and materialized function outputs |
| 5 | delete_effects | Explicit delete keys and independent deletion scope |
| 6 | partial_update_cas | Optional CAS proof |
| 7 | request_row_mapping | Original request offsets for retry results |

Ordinary columns exclude PK, RowID, and Timestamp. The final message TimeTick supplies row timestamps. The body does not embed InsertRequest or DeleteRequest. It has no result copy or namespace field.

One message addresses one collection and one VChannel. Partition IDs are positive and unique in the partition table. Each partition has a positive row count. With one partition, partition_indexes is absent. With multiple partitions, each logical row has one valid index. The frequencies must match the header counts.

Each delete effect has nonempty, correctly typed keys and exactly one scope: all partitions, or one positive partition ID. Deletion scope is independent of insertion placement. Delete keys can differ from inserted keys. delete_rows must match all explicit effects.

Delete effects remove only versions strictly earlier than the final visible TimeTick. The new rows cannot delete themselves. A missing older version is a no-op. Multiple rows or transaction bodies can contain the same PK. The protocol preserves existing equal-TimeTick semantics and does not promise last-write-wins. It provides no atomicity across VChannels.

Namespace requests and namespace-enabled collections are unsupported. Proxy rejects them before sending a WAL message. StreamingNode checks the collection schema for both local and replicated Upsert admission.

The producer validates ordinary column values. StreamingNode checks the compound contract, scope, identity, counts, PK types, placement, CAS proof shape, and schema version. Schema mismatch precedes schema-dependent content checks.

## Encoding and projections

The Proxy groups rows by VChannel and encoded size. It does not split by partition. The existing borrowed column encoder writes selected column values directly into the Upsert wire layout. It does not construct destination FieldData columns. A source-bound cursor selects a row prefix before column encoding. Incremental column plans share nullable positions and scratch across chunks. Conservative metadata budgets include PK, RowIDs, placement, request mapping, delete effects, CAS, and the envelope. A small envelope probe includes encryption metadata and fixed cipher expansion. A final physical-size check rejects unexpected cipher expansion without encoding the batch again. Each accepted chunk encodes ordinary columns once. An indivisible plain row can use existing backend chunking. Keyed and CAS writes retain their existing strict size bound.

Metadata readers scan protobuf tags and lengths. They decode PK, RowIDs, placement, explicit effects, CAS, and request mapping. For ordinary columns, they retain only field descriptors. They skip scalar and vector values without invoking full Body().

Immutable messages share a metadata projection through the existing bounded body cache. Projection loading has the same cancellation and concurrent-reader rules as full bodies. Metadata and full bodies have separate cache entries and share the cache budget and eviction policy. Projection entries account for decoded metadata and reserve space for derived row mappings. Row placement, validation, and nullable-vector positions are shared across independent segment readers. Eviction releases the metadata and these mappings together. They do not retain the decrypted payload. Historical messages do not allocate an Upsert projection entry.

Mutable payload replacement invalidates the metadata slot. Header-only segment assignment does not change body projections. Effect replacement copies unmodified encoded fields, including columns, and encodes only the new effects. Encryption still covers the complete payload. The implementation does not introduce partial encryption.

## Append and transaction flow

Proxy preparation resolves AutoID and existing API behavior before packing. Existing AutoID rows retain their PK and receive the corresponding explicit delete effect. Missing rows receive new IDs without old-version deletes. Original request offsets survive VChannel routing and size splitting. Each delete occurrence belongs to the first inserted occurrence of its PK. Matching visits inserted keys and delete occurrences once. Each chunk retains original delete order. Size planning does not consume effects.

The normal transaction contract remains:

1. Begin succeeds.
2. The producer sends bodies and waits for every successful response.
3. The producer sends Commit.

Upsert producers construct ordinary, persisted, replicable messages on the collection business VChannel. This is an internal producer contract. Append and individual interceptors do not add Upsert-specific flags validation. Existing generic property handling remains unchanged.

TimeTick checks transaction VChannel membership before Upsert body admission. Commit uses the existing logical body count: a nonzero body_cnt must equal successful body appends, while zero retains the legacy compatibility behavior. Sessions retain counts and a lightweight effects classification, not complete Begin/body messages for Commit revalidation. Recovery restores the same count and effects classification. Rollback, expiry, rejection, and completion retain the existing cleanup ownership.

All inserted target partitions must be ready. Batch assignment selects eligible Growing segments and holds Ack references before accounting. It validates the complete allocation set before charging rows or bytes. A later partition that requires redo cannot leave a charged successful prefix. Each partition accounts for its own PK, RowID, and selected ordinary columns. Delete effects and the outer envelope are excluded from insert pressure. Existing transaction cleanup releases segment transaction references.

RowLock and version tracking use the union of inserted and explicit delete keys. Lock-set deduplication does not change the encoded rows or effects. CAS retains its current term, snapshot, transaction, and recovery behavior. Consumers do not repeat CAS or PK-index decisions.

## Retry results and Summary

New Upsert messages have no IdempotentInsertResult field. The idempotency interceptor reconstructs that result from original PK and request_row_mapping. A keyed autocommit requires a complete mapping before it can acquire or return a cached result. Keyless writes can omit it.

Transaction bodies use the existing result buffer and merge path. An incomplete keyed Commit returns TransactionExpired so the producer rebuilds the transaction. This change does not support Commit before body responses complete. Duplicate requests return the original AppendResult and IDs, regardless of IDs supplied by the retry.

Summary uses the metadata projection to reconstruct retry results and explicit deletes. It does not decode ordinary columns for these records. Decode failures prevent publication of either Summary fact and stop frontier advancement. Retained WAL replay and Summary restore use the same original PK and mapping. The existing bounded idempotency window and unknown-outcome limitations remain unchanged.

## Consumption and persistence

The native WAL consumer builds logical row selections per partition. Every column uses the same selection. A shared row reader builds compact nullable-vector positions once and uses those positions for subsequent partition selections. PK is exposed as a storage column only at the execution boundary. RowID and final Timestamp enter the existing storage interfaces there.

L1 persistence, growing runtime, function-output handling, local flush replay, and recovery use this row interface. Historical Insert remains a separate readable input. Explicit delete effects feed the existing Transform and L0 paths. L1 and L0 recovery frontiers remain independent. Completion requires all effects that belong to that logical WAL position. A pure row write advances only the Growing frontier. An Upsert with explicit deletes advances Growing and Transform. Insert-only transactions follow the pure-row rule. Explicit query barriers retain their existing frontier behavior.

Each component decodes and converts through its actual apply path. There is no whole-unit predecode or rollback. A decode or apply failure stops processing before applied-frontier publication and cannot be hidden by a later message that advances completion. L0 projection errors poison the retained position and stop further scheduling. Query replay uses receiving-cluster segment assignments and final transaction visibility.

No support is added to the legacy MsgPack pipeline or msgdispatcher. Production activation requires the native query path from #52977, which is included in the upstream base. Deployment must still verify compatible native readers before enabling Upsert messages.

## CDC and activation

CDC forwards the primary's logical rows, placement partitions, CAS metadata, mapping, and explicit delete effects. The secondary preserves these logical effects and allocates its own segment IDs. It does not repeat PK-index decisions. Replicated messages bypass the local client idempotency window. Transaction replay uses the final Commit TimeTick.

Deployment uses reader-first activation. streaming.wal.enableUpsertMessage defaults to false. auto remains false because the new format has no assigned release capability. The old draft 3.0.7 threshold does not describe this format. The old draft switch cannot enable this new capability. Explicit enablement requires operators to verify all readers, CDC targets, and the native query-path dependency first.

When the switch is false, Proxy selects the existing WAL format before packing. Insert requests produce Insert messages. Upsert and partial-update requests produce the existing Insert/Delete transaction with the same CAS metadata. This path retains partition splitting, size splitting, and Insert-header retry results. When true, Proxy selects Upsert messages and uses the format described above.

The selection covers the complete packing attempt. An append error does not trigger a second append with another format. Requests already packed retain their format through producer retries. StreamingNode still rejects new Upsert appends while its switch is false. Configuration propagation or disabling the switch during an append can therefore fail that append. Operators must drain new-format writes before disabling it. Retained decoding and Commit/Rollback for durable bodies remain available. Disabling the switch does not remove retained WAL history or make older readers safe.

Client retries use the same per-VChannel idempotency key and original row offsets through either format. The existing bounded window and unknown-outcome contract still apply. Unkeyed requests do not gain an exactly-once guarantee from format selection.

Unknown-message skipping is not a compatibility strategy. Mixed-version operation and downgrade across retained new-format history are unsupported until the required reader capability is deployed. The operator-managed switch does not provide automatic mixed-version safety or a downgrade guarantee.

## Validation boundaries

Tests must check inserted and deleted rows, original retry results, accounting, cleanup, and recovery frontiers. Compilation and historical draft test results do not establish this contract. Benchmarks measure metadata decoding, borrowed encoding, scatter allocations, payload size, encryption, and materialized outputs separately. Performance claims require measured results.

This change does not enable PRIMARY_KEY indexes, import another branch's decision algorithm, change equal-TimeTick duplicate semantics, or implement cross-VChannel atomicity. Full deployment validation still requires the native query-path dependency and compatible primary and secondary readers.
