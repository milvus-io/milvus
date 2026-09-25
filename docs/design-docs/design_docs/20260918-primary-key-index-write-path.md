# MEP: Primary key index on the WAL write path

- **Created:** 2026-09-18
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** StreamingNode, WAL
- **Related Issues:** #52305

## Summary

Add a primary key index to the WAL write path of the streaming node. When an
insert carries a primary key that the index already holds, the interceptor
appends the insert and a companion delete of that key as one transaction. Both
take effect at the TimeTick of the CommitTxn, so the old row is masked and the
new row survives. The index is per vchannel and lives in memory. The feature is
off by default.

This document covers the first step of the LSM-based primary key index
(#52305): the write path. Persistence, recovery, the read path and the
`create_index` surface are later steps.

## Motivation

Milvus does not keep primary keys unique on the write path. An insert of an
existing primary key leaves the old row in place. The row count grows, and a
query returns both rows until a compaction or a delete from the client masks
the old one. A client that wants "insert equals upsert" has no way to ask for it.

The write path of one vchannel is a single-writer sequence: every message gets
a TimeTick at one point, in the timetick interceptor. A decision made at that
point is ordered with the messages. So the write path is the place where "does
this key exist" can be answered without a race, and where the answer can be
turned into a message that the consumers already understand.

## Goals

- An insert of an existing primary key masks the old row. The last write wins.
- Decisions of one key happen in TimeTick order.
- Consumers of the WAL need no change.
- The feature is behind a switch and off by default.

## Non-goals of this step

- A persistent index and recovery after a WAL open. The index starts empty.
- Filtering of deletes whose keys are absent. See "A delete is never narrowed".
- The `create_index(field=pk, index_type="PRIMARY_KEY")` surface. A global
  switch stands in for it.
- The read path: point query pruning and the binding of a lookup to an MVCC
  timestamp.
- Primary keys of rows written by bulk import, or written while the switch was
  off. They are not in the index.

## Why the companion delete must share the TimeTick of the insert

A delete masks the rows of its keys whose timestamp is strictly smaller than
the timestamp of the delete. Two different TimeTicks give two wrong results:

```
Delete@T1 < Insert@T2:   a read at MVCC timestamp in [T1, T2) sees no row for the key
Insert@T1 < Delete@T2:   the delete masks the new row too
Delete@T  = Insert@T:    the old row (ts < T) is masked, the new row (ts = T) survives
```

## Why a transaction and not two messages with one TimeTick

Every WAL message has a unique TimeTick since 2.6.0. Several components depend
on it:

- the consumer reconnects with a filter "TimeTick greater than the last one"
  (`internal/distributed/streaming/internal/consumer`),
- the message dispatcher seeks with "TimeTick less or equal" and drops the
  rest (`pkg/mq/msgdispatcher`),
- the write-ahead buffer panics on a non-increasing TimeTick
  (`wal/interceptors/wab/pending_queue.go`),
- the switchable scanner resumes from an exclusive TimeTick and would drop a
  second message with the same one (`wal/adaptor/scanner_switchable.go`).

A transaction is the existing way to give several messages one TimeTick. Each
sub-message gets its own TimeTick when it is appended. The consumer assembles
BeginTxn, the bodies and CommitTxn into one `Txn` message and rewrites the
TimeTick of every body to the TimeTick of the CommitTxn
(`pkg/streaming/util/message/builder.go`). The proxy already uses this path for
an upsert: the insert and the delete go to the same vchannel in one transaction.

So the interceptor does the same thing on the streaming node. An autocommit
insert that hits an existing key becomes BeginTxn, the insert, the companion
delete and CommitTxn. A client transaction gets its companion delete as one more
body before its CommitTxn.

## Position in the interceptor chain

```
idempotency → redo → lock → replicate → [pkindex] → timetick → shard → partialupdate
```

- Above `timetick`: every message of the group needs its own TimeTick, and the
  BeginTxn needs the smallest one.
- Below `lock`: the whole group runs under the one shared vchannel lock of the
  original append. An exclusive message waits for that lock, so it can not fail
  the transaction halfway.
- Below `replicate`: a replicated message passed the role check already. The
  interceptor lets it through untouched. A secondary cluster does not maintain
  the index in this step.

The helper `txn.AppendInTxn` (`wal/interceptors/txn/append_in_txn.go`) does the
transaction work. It knows nothing about the primary key index. Its contract is
in `docs/agent_guides/streaming-system/wal/timetick_and_txn.md`.

## Components

```
internal/streamingnode/server/wal/interceptors/pkindex   the interceptor (a thin shell)
        │ translates WAL messages into requests, appends what the decider asks for
        ▼
internal/pkindex/dedup                                   the write path decisions
        │ probe, striped locks, pending writes of transactions
        ▼
internal/pkindex/authority                               the index of one vchannel
        │ key-value semantics: Get, MultiGet, NewBatch, Write
        ▼
authority.Engine                                         the storage behind it
                                                         (in-memory in this step)
```

The two `internal/pkindex` packages do not depend on the streaming node. The
interceptor is the only place that knows both sides.

The primary key payload of a message is read by `wal/utility/primarykey`. It
checks the payload the way the write path needs it: a bad message gets an
error, never a lenient pass and never a panic.

## The write path

### An autocommit insert that hits an existing key

Key 1 is in the index in segment S0. Key 2 is not.

```
wal.Append(Insert pk=1,2)
 └ idempotency → redo → lock (shared lock of v0 held ●) → replicate
    └ pkindex
       ├ Decide(Insert [1,2]) ── lock the stripes of 1 and 2 ── MultiGet: 1 hits
       ├ build Delete(pk=1)
       ├ txn.AppendInTxn(Insert, [Delete])
       │    ├ BeginTxn      → timetick → shard → partialupdate → backend   tt=100
       │    ├ Insert (copy) → timetick → shard assigns S5 → …            tt=101
       │    ├ Delete(1)     → timetick → shard → …                       tt=102
       │    └ CommitTxn     → timetick → …                               tt=103
       ├ read S5 back from the appended insert
       └ Apply: index 1→S5, 2→S5, release the stripes ○
client ◀ MessageID and TimeTick of the CommitTxn (tt=103)
consumer: one Txn message at tt=103 with Insert and Delete. The row of key 1 in S0 is masked.
```

The caller's insert message is not modified. The helper appends a copy. When
the shard interceptor asks for a redo, the redo interceptor above runs the whole
chain again with the original message, and a new decision is made.

### A client transaction

The bodies of a transaction take effect at the TimeTick of the CommitTxn. So a
decision made when a body arrives can be wrong:

```
tt=11  Insert(pk=8) in T1     a decision now: key 8 is absent, no companion delete
tt=13  Insert(pk=8) autocommit  key 8 is absent (T1 is not committed), index 8→segA
tt=15  CommitTxn(T1)          the insert of T1 takes effect at tt=15: two rows of key 8
```

So a body is only recorded as a pending write of its transaction. The decision
happens when the CommitTxn arrives: the pending keys are probed, and the
companion delete is appended as one more body of the transaction, before the
CommitTxn. A key that the transaction deletes itself gets no companion delete.
The pending writes are dropped when the transaction commits, rolls back, or
expires.

The decision at CommitTxn is a snapshot of the pending writes. It relies on the
WAL contract that a producer appends the CommitTxn only after it received the
result of every body (`docs/agent_guides/streaming-system/wal/timetick_and_txn.md`,
"Transaction Lifecycle"). A body applied after that snapshot would leave the
index of its keys wrong, so the interceptor ends the process when it detects
one. Every producer in Milvus obeys the rule today.

### A delete is never narrowed

The index is a subset of the data. Rows written by bulk import, rows written
while the switch was off, and every row written before the current WAL open are
not in it. A key that the index does not know may still exist in the data.

So the index only ever adds deletes. A delete of a client reaches the WAL
unchanged. The decider still computes which keys of the delete exist, but that
result is used for the index update and the metrics only.

### Failure

```
Decide ✓ (stripes held ●) → BeginTxn ✓ → Insert ✓ → Delete ✗
→ the helper appends RollbackTxn
→ Discard: the index is unchanged, the stripes are released ○
→ client ◀ the error
```

A RollbackTxn is appended after every failed step, including a failed CommitTxn.
The timetick interceptor persists a rollback only while the session is still in
flight, so a rollback can not undo a commit that took effect. When the
transaction that the helper built ends with a transaction error, the helper
reports it as a retriable internal error, because the client asked for an
autocommit append and its retry starts a new transaction.

## Concurrency contract

Writes of one vchannel hold a shared lock and run concurrently. For one key,
the order of the decisions must equal the order of the TimeTicks, and a later
writer must see the index update of an earlier writer. So the critical section
runs from `Decide` through the downstream TimeTick allocation and the WAL write
to `Apply` or `Discard`.

- The decider owns the locks: keys are hashed onto a fixed number of stripes,
  and the stripes of one decision are locked in ascending order.
- The interceptor guarantees the sequence `Decide`, the WAL write, then exactly
  one of `Apply` and `Discard`. A panic in between ends the process, like every
  panic on the append path, so no decision is left unresolved in a live node.
- An exclusive DDL waits for the vchannel lock and never asks for a stripe, so
  it can not form a cycle with a write that holds stripes.
- Cost: the stripes stay held during the backend write. When the backend is
  slow, writers of the same stripe queue up.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `streaming.pkindex.enabled` | `false` | Whether the streaming node maintains the index. Experimental. |
| `streaming.pkindex.lockStripes` | `256` | The number of lock stripes per vchannel. |

Both are internal (`Export: false`). The only index engine compiles with the
`test` build tag. A production binary has no engine. When the switch is on in
such a binary, the streaming node panics at start with a message that names the
switch. A silent pass-through would hide a correctness difference: the operator
expects deduplication and does not get it.

## Metrics

- `pkindex_probed_keys_total{result="hit"|"miss"}`
- `pkindex_companion_delete_keys_total`
- `pkindex_decide_duration_seconds`
- `pkindex_lock_wait_duration_seconds`

One `wal.Append` metric record stands for the whole group of an upgraded
insert, so WAL record and byte counts under-report such an append.

## Compatibility

- Consumers see only messages that exist today: an assembled transaction with
  an insert and a delete. The delete covers all partitions
  (`PartitionID = AllPartitionsID`), which the proxy already produces.
- With the switch off, or in a binary without an engine, the write path is
  unchanged.
- With the switch on, an insert whose primary key field is missing, duplicated
  or of the wrong type is rejected as unrecoverable. Today such an insert
  reaches the WAL.
- With the switch on, the index of a collection is created before its
  CreateCollection message is appended. When the index can not be created, the
  CreateCollection is rejected as unrecoverable and the collection is not
  created. A collection that the WAL serves always has its index, or has an
  autoID primary key and needs none. At WAL open, a collection whose index can
  not be created makes the open panic.
- A retry of an idempotent insert after a WAL open still finds its record: the
  CommitTxn of the group carries the idempotency key.

## Known limitations and follow-ups

- The index starts empty on every WAL open. An insert of a key written before
  the open is not detected as a duplicate. Recovery is a later step.
- A transaction left uncommitted by a previous WAL lifetime can still be
  committed after the open, but its pending writes are not rebuilt.
- The index engine of this step is in memory and exists only in test builds.
- The integration suite needs a `milvus` binary built with the `test` tag. It
  runs only when `MILVUS_PKINDEX_TEST_BINARY` is set.
- A delete by expression (`DeleteRequest.SerializedExprPlan`) carries no keys.
  The field is reserved for the predicate delete feature (#50433), whose
  producer is not merged, so no delete on the WAL sets it today. The
  interceptor panics on such a delete: the index can not learn which keys it
  removes, and the producer must define that before it ships.
- Every redo of an upgraded insert writes one BeginTxn and one RollbackTxn.
- An index probe failure makes the append fail with a retriable error. The
  producer retries such errors without a limit, so a permanently broken engine
  stalls the writes of that collection instead of failing them.
- An index write failure after the WAL accepted the write ends the process.
  The engine write is a memory operation, so it fails only on a bug, and a
  stale index would let duplicates through in silence.
- Duplicate primary keys inside one insert are not detected.

## Test plan

- Unit tests of the helper, the authority, the decider and the interceptor,
  with a fake append operation.
- Tests on the real interceptor chain (`wal/adaptor`): the consumer sees one
  transaction with the insert and the companion delete at the TimeTick of the
  commit, a redo leaves no in-flight session, a client transaction gets its
  companion delete at commit, a delete reaches the WAL unchanged, and 320
  concurrent inserts of one key produce exactly one insert without a companion
  delete.
- Mutation checks: without the stripe locks, the serialization test fails
  in 10 of 10 runs. Without the rollback, the redo test fails.
- An integration test inserts the same keys twice and checks `count(*)`. With
  the switch off it reads 150 rows, with the switch on it reads 100.
