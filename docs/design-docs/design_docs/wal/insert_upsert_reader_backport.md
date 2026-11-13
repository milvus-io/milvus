# Insert V2, Upsert and chunk readers for Milvus 3.0

## Scope

This selective backport adds Insert V2 and Upsert readers from
[PR #55157](https://github.com/milvus-io/milvus/pull/55157), chunk readers from
[PR #52775](https://github.com/milvus-io/milvus/pull/52775), and the required
RecoveryBarrier and MVCCBarrier reader dependencies from
[PR #52943](https://github.com/milvus-io/milvus/pull/52943) and
[PR #54344](https://github.com/milvus-io/milvus/pull/54344). These readers are
adapted to the existing 3.0 consumption and recovery paths rather than copied
as whole commits. Existing 3.0
Proxy producers and StreamingNode append handlers keep their existing formats
and backend-size behavior. No chunk writer, new-format generation setting,
PK-index decision algorithm, or native QueryView architecture is backported.

A new 3.1 Proxy sending Insert V2 or Upsert directly to a 3.0 StreamingNode
append handler is not supported by this reader backport. The existing 3.0
Insert V1 handler and its admission behavior remain unchanged, and there is no
new Upsert append handler. Reader compatibility is not a writer-upgrade
orchestration guarantee; client retries are not asserted to make every such
mixed-version append succeed.

Deployment management must prevent switchover during upgrades and keep the
secondary cluster on a newer version than its primary. This change does not
support a 3.1 primary writing new-format messages into a 3.0 secondary. It does
not establish that all other 3.1 WAL messages or checkpoint formats can be read
by 3.0, and does not make downgrading a running 3.1 deployment safe.

Format compatibility covers the data types and functions already supported by
3.0. It does not add 3.1-specific types or nested-element nullability. Shared
row validity uses the legacy `FieldData.valid_data` representation, which the
3.1 producer preserves for older readers.

## Wire readers

Historical Insert and Delete remain readable. Insert V2 and Upsert share
`InsertRows`: inserted PKs, explicit or delta-encoded RowIDs, partition indexes,
ordinary columns, and optional original request mapping. Upsert separately
carries explicit delete effects and the primary's partial-update CAS proof.
Readers expand RowID deltas with overflow checking and resolve the explicit
all-inserted-keys reference without changing ordered duplicate occurrences.
No PK-index lookup or new decision is performed while reading.

Rows belong to one collection and VChannel. A header partition table carries
receiving segment assignments. One partition omits per-row indexes; multiple
partitions use indexes into that table. Delete scope and keys are independent
of inserted row placement. All effects use the logical message's final TimeTick;
transaction assembly supplies Commit's TimeTick. Existing equal-TimeTick duplicate
semantics remain unchanged; this backport adds no last-write-wins guarantee.

Persisted RecoveryBarrier and MVCCBarrier records are also recognized because
3.1 uses these as confirmation boundaries. Their native query-view content does
not become a 3.0 query architecture: the legacy consumer receives their TimeTick
and positions as confirmation progress.

## Physical chunk assembly

The durable catchup scanner assembles physical payload chunks before legacy
conversion, TimeTick ordering, filtering, or transaction assembly. It uses raw
ciphertext bytes, never decrypts individual pieces, removes chunk markers only
from complete records, and preserves the acknowledged head record's MessageID.
Interleaved runs, retried heads and seeks into an incomplete run are handled by
the same logical assembly contract as the source format. TimeTick and MVCCBarrier
confirmation boundaries discard incomplete runs in their confirmed prefix.

The write-ahead buffer already contains logical messages from the unchanged
local writer, so its tailing path does not split or reassemble payloads. Chunk
corruption terminates the scanner and preserves the error instead of allowing
later records to conceal the failure.

## Production consumption in 3.0

3.0 still uses MsgPack consumers. `delegatorMsgstreamAdaptor.Seek` includes
Upsert in its DML filter. The dispatcher adaptor retains new-format messages as
one lazy logical record until the destination business VChannel is selected;
messages for unrelated collections are not decrypted speculatively.

The selected record decodes once and expands into partition-specific Insert
messages followed by explicit Delete messages. Every column uses the same logical
selection. Compact nullable-vector storage uses logical-to-physical selections;
sparse vector dimensions are preserved, including all-null selections. Source
columns are not modified. The consumer supplies the schema identity for the
standalone PK column: QueryNode binds it before its insert filters, and the flush
write buffer binds it before storage conversion.

All conversions for a logical record or transaction complete before publishing
its MsgPack. This is decoding atomicity, not rollback of native mutations. Native
insert and delete retain the existing execution and error semantics. In particular,
deleting at the final TimeTick does not delete rows inserted at that same TimeTick.

Retryable KMS lookup failures use the existing dispatcher retry path and retain
the pending pack. Other new-format conversion failures stop dispatch without
advancing its TimeTick or consuming the following pack. A later resume retains
the failed pack. The flusher propagates conversion errors and does not observe the
failed record in RecoveryStorage or advance its dispatch progress.

## Persistence and recovery

The existing flusher routes new-format records through the same data service and
write-buffer pipeline as historical DML. Insert effects retain their receiving
partition and segment assignments. Explicit delete effects use the existing
Delete persistence path, including all-partitions scope.

Recovery reads new-format header assignments and reuses existing per-segment
row/byte accounting. As with legacy Delete, 3.0 RecoveryStorage does not separately
account delete payloads; the flusher owns their persistence. Transaction replay
uses the existing TxnBuffer: uncommitted, rolled-back and expired bodies do not
become visible; committed bodies share Commit's TimeTick. Physical chunks do not
increase the logical transaction body count.

Partial-update recovery reads pending Insert V2 and Upsert bodies to restore
inserted and explicit delete keys through the existing transaction write-set
tracking. It recognizes durable CAS markers and proofs using the existing
full-retry contract; it does not revalidate old CAS decisions or change normal
append-time PK extraction.

## Verification requirements

Verify actual 3.1-encoded fixtures, not only backported builders. Cover compact
RowIDs and independent/reference delete keys, multi-partition row selection,
nullable and nested columns, function outputs, and immutable source data. Exercise
transaction Commit/Rollback/expiry, decoder and KMS failures, and progress protection.

Use real WAL assembly and reopen tests in addition to scanner unit tests. Native
execution must check both old and replacement versions, including sparse vector
input. Verify storage conversion, flush/reload and recovery independently. Keep
historical Insert/Delete tests and confirm no production producer or chunk-writing
path was enabled by this reader-only backport.
