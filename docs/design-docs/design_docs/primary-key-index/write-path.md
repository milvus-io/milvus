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
new row survives. The index is per vchannel. The write path only reads it: the
writes reach the index when the WAL is applied to it after the append. The
feature is off by default.

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
        │ translates WAL messages into DecideRequests, appends what the Decider asks for
        │ creates the engine of each vchannel (in-memory in this step)
        ▼
internal/pkindex/dedup                                   the write path decision
        │ Decider: in-flight set (also the key lock), probe, pending writes of transactions
        │ WriteIntent: the decision until Committed, Aborted or InDoubt
        ▼
internal/pkindex/engine                                  the storage behind it
                                                         (engine.RW: MultiGet, Write)
```

The package `internal/pkindex/dedup` holds the index logic and does not depend
on the WAL. The interceptor is the only place that knows both sides. The
Decider only reads the engine of its vchannel. It does not own the engine: the
caller creates it, and closes it after the Decider.

The primary key payload of a message is read by `wal/utility/primarykey`. It
checks the payload the way the write path needs it: a bad message gets an
error, never a lenient pass and never a panic.

## The write path

### An autocommit insert that hits an existing key

Key 1 is in the index. Key 2 is not.

```
wal.Append(Insert pk=1,2)
 └ idempotency → redo → lock (shared lock of v0 held ●) → replicate
    └ pkindex
       ├ Decide(Insert [1,2])
       │    ├ register 1 and 2 in the in-flight set, wait for earlier writes of 1 and 2
       │    └ in-flight set: no earlier entry ── MultiGet on the engine: 1 hits
       ├ build Delete(pk=1)
       ├ txn.AppendInTxn(Insert, [Delete])
       │    ├ BeginTxn      → timetick → shard → partialupdate → backend   tt=100
       │    ├ Insert (copy) → timetick → shard → …                       tt=101
       │    ├ Delete(1)     → timetick → shard → …                       tt=102
       │    └ CommitTxn     → timetick → …                               tt=103
       └ Committed: set tt=103 in the entry. Later writes of 1 and 2 stop waiting.
client ◀ MessageID and TimeTick of the CommitTxn (tt=103)
consumer: one Txn message at tt=103 with Insert and Delete. The row of key 1 is masked.
observe: writes 1 and 2 into the engine at tt=103, then removes the entry.
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
result is used for the metrics only.

### Failure

```
Decide ✓ (entry registered ●) → BeginTxn ✓ → Insert ✓ → Delete ✗
→ the helper appends RollbackTxn
→ Aborted: the transaction can not have committed, so the entry is removed ○
→ client ◀ the error
```

A RollbackTxn is appended after every failed step, including a failed CommitTxn.
The timetick interceptor persists a rollback only while the session is still in
flight, so a rollback can not undo a commit that took effect. When the
transaction that the helper built ends with a transaction error, the helper
reports it as a retriable internal error, because the client asked for an
autocommit append and its retry starts a new transaction.

A failed append is one of two kinds:

- **Not written.** The failure is known to leave nothing in the WAL. The entry
  is removed. These are: no TimeTick was assigned to the message, a redo, and
  every failure that `AppendInTxn` reports with `NotCommitted`. That is a
  failure before the CommitTxn, a failed CommitTxn followed by a persisted
  RollbackTxn, and a failed extra body of a client transaction.
- **Unknown.** The message may be in the WAL. The backend can persist a
  message and still return an error, for example on a timeout. The entry stays
  in the set and its keys stay present. It stops blocking other writes of its
  keys. It leaves the set when observe passes the largest TimeTick that the
  failed append assigned.

That largest TimeTick is exact, for these reasons:

- The timetick interceptor writes the allocated TimeTick into the message in
  place, before the write.
- No interceptor between this interceptor and the timetick interceptor copies
  the message.
- Below the timetick interceptor, the append retries the same message and
  never allocates again.
- A redo runs the whole chain again, and this interceptor makes a new decision.

A message can carry the TimeTick of an earlier attempt, after a redo for
example. So the interceptor counts a TimeTick only when the failed call
changed it. `utility.AppendAndGetTimeTick` does this for a plain append, and
`txn.AllocatedTimeTick` returns it for `AppendInTxn`.

## The in-flight set and the key lock

The index engine has one writer: observe, the step that applies the WAL to the
engine after the append. It applies the messages in TimeTick order, for the
replay at WAL open and for live messages. The write path never writes the
engine. So a write is invisible in the engine from its decision until observe
applies it. The in-flight set covers that gap.

The in-flight set of a vchannel holds the decided writes that observe has not
applied yet. It answers one question: does a key exist. Two ordering rules keep
the set and the engine complete together:

- **Rule A (observe).** Write the engine first, then remove the entries.
- **Rule B (writer).** Read the set first, then the engine.

If a writer does not find a key in the set, the entry was removed before the
read. Rule A wrote the engine before that removal, so the engine read finds the
key.

### Entry life cycle

1. **Enqueue** before the append. The entry has no TimeTick yet.
2. **Commit**: set the TimeTick after the append returns. If observe already passed
   that TimeTick, the writer removes the entry itself.
3. **Remove** in one of three ways: observe applies the write, the writer finds
   that observe passed its TimeTick, or the append failed and the message was
   not written.

The entry is enqueued before the append because observe can apply a message
before its append returns to the writer. An entry enqueued after the append
could stay in the set for ever.

Steps 2 and 3 race. Observe raises the observed TimeTick and removes the
entries of that TimeTick under the mutex of the set. A writer stores its
TimeTick in the entry and reads the observed TimeTick under the same mutex. So
the side that comes second sees the other, and the entry is always removed.
A test runs both orders and checks that the entry leaves the set.

### Presence

A key is present for a write when the set holds an entry of the key that was
enqueued before the write, or when the engine holds the key. The write's own
entry, and the entries of later writes, do not count. A later write waits for
this one and gets a larger TimeTick.

### The lock

An entry without a TimeTick is the lock of its keys. The design follows the
partial update row lock (`wal/interceptors/partialupdate/row_lock.go`) and the
latch manager of CockroachDB (`spanlatch`):

- A write enqueues all its keys in one step under the one mutex of the set.
  So the enqueues of two overlapping writes are totally ordered.
- A write waits only for the last entry without a TimeTick of each key. That
  entry waited for the entries before it. The waits form no cycle, so writes of
  many keys can not deadlock.
- A write whose context ends while it waits keeps its place in line. It leaves
  after the writes it waited for, so a later write still waits for them.
- The commit stores the TimeTick under the mutex and closes a channel after
  it releases the mutex. A writer that finds that observe passed its TimeTick
  also removes its own entry under the mutex.
- The mutex is held only while the map changes, never across an append.

The set has one mutex and one map. A key of the set is a 64-bit number: the
value of an int64 primary key, or the 64-bit FNV-1a hash of a VarChar primary
key. Two
VarChar keys with the same hash share one entry. That only adds a wait and a
companion delete of a key that may not exist. Both are safe. The keys of one
write are deduplicated after the hash.

An exclusive DDL waits for the vchannel lock and never registers in the set, so
it can not form a cycle with a write that holds entries.

### Cost

One insert of 1000 new int64 keys, one writer, no contention, Intel i7-8700:

| Step | Time |
|---|---|
| Decode the body and the primary keys (key column only) | 15 µs |
| Decode the body and the primary keys (with a 128-dim float vector column) | 250 µs |
| Lock: enqueue, wait, commit, removal by observe | 173 µs |
| Decision with the in-memory test engine (lock, set read, probe) | 250 µs |
| Probe of 1000 keys on the Pebble engine (all miss, 1M keys in the memtable) | 1330 µs |

With the Pebble probe, the lock is about 11% of decode, decision and lock
(173 of about 1520 µs). With the in-memory test engine it is about 70%.

Under contention, 32 writers each hold the lock for a simulated 2 ms append.
The table gives messages per second at `GOMAXPROCS=2`:

| Keys per message | Row lock, exclusive mode | In-flight set |
|---|---|---|
| 1 | 14794 | 13692 |
| 16 | 14341 | 12804 |
| 100 | 13142 | 11742 |
| 1000 | 7700 | 4799 |

The registration of a write of 1000 keys holds the one mutex of the set for
all 1000 keys, which is the same as a table lock. The set
also does more work per key than the row lock. So large writes reach about half
of the row lock throughput. A vchannel-wide lock for writes above some number of
keys (lock escalation) is an open item.

### Writes that wait outside the WAL lock

`WaitUntil(T)` makes the writes of a vchannel wait until observe reaches
TimeTick `T`. It covers events whose earlier writes did not pass through the
in-flight set: the index is enabled for an existing collection, a secondary is
promoted to primary, or the engine recovers from a failure. After the wait, the
decision is exact, so an insert gets a companion delete only for the keys that
exist. This step adds the mechanism only. Nothing calls it yet.

A write of an index that is not ready waits the same way. `Config.Ready` is a
channel that the owner of the index closes when the engine holds every record
of the vchannel, after the install of its committed tables. Until then
`Decide` returns a `*WaitError` with `TimeTick` 0, and `Wait` returns when the
channel is closed. The decider has no degraded mode, where every key would
count as present: every decision is exact. A `WaitUntil` that comes after a
write passed this check, while the write waits for an earlier write of the
same key, also makes that write wait: the decider removes its registration and
returns a `*WaitError`.

The wait can not block inside the pkindex interceptor. That interceptor runs
under `glock.RLock` of the lock interceptor. An exclusive message waits for
`glock.Lock`. A TimeTick append also needs `glock.RLock`, and it queues behind
the exclusive message. So observe never reaches `T`, and the waiting write,
the exclusive message and the TimeTick block each other forever.

So the wait is in the redo interceptor, which is above the lock interceptor and
holds no lock:

1. `Decide` returns a `*dedup.WaitError` for an autocommit insert or delete,
   and for a CommitTxn with pending writes, while observe is below `T`. It
   registers nothing and probes nothing. A transaction body and a rollback do
   not wait.
2. The pkindex interceptor returns `interceptors.WaitRequiredError` with the
   `Wait` of that error. Nothing of the message is appended.
3. The lock interceptor releases `glock` when the inner chain returns.
4. The redo interceptor calls `Wait` without a lock. `Wait` returns when
   observe reaches `T`, when the context ends, or when the decider closes. The
   redo interceptor then runs the whole chain again, and `Decide` makes the
   decision anew.

### Contract of the interceptor

The interceptor calls `Decide`, the WAL append, then exactly one of
`Committed`, `Aborted` and `InDoubt` on the returned `WriteIntent`. A panic in
between ends the process, like every panic on the append path, so no decision
is left unresolved in a live node.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `streaming.pkindex.enabled` | `false` | Whether the streaming node maintains the index. Experimental. |

It is internal (`Export: false`). The only index engine compiles with the
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

- **Do not enable the index before observe exists.** This step has no observe.
  An entry of the in-flight set then never leaves the set, so the set grows
  with every write and every written key stays present. Observe comes with the
  recovery step (#54365).
- The total size of the in-flight set has no limit yet. The back pressure
  above a limit is open.
- Lock escalation for writes of many keys, and its threshold, are open.
- An empty map is set to nil so that it does not keep its peak memory.
  Keeping the maps would make the lock of a 1000-key write about 30% faster
  (140 µs instead of 193 µs, measured with the earlier 64-shard set).
- Rule A needs an engine write to be visible to reads as soon as it returns.
  A test must check this for the Pebble engine when observe writes it.
- Pending writes of a transaction are not rebuilt at WAL open.
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
- Duplicate primary keys inside one insert are not detected.

## Test plan

- Unit tests of the helper, the Decider and the interceptor,
  with a fake append operation.
- Tests on the real interceptor chain (`wal/adaptor`): the consumer sees one
  transaction with the insert and the companion delete at the TimeTick of the
  commit, a redo leaves no in-flight session, a client transaction gets its
  companion delete at commit, a delete reaches the WAL unchanged, and 320
  concurrent inserts of one key produce exactly one insert without a companion
  delete.
- Tests of the in-flight set: enqueue races with observe, the commit
  races with observe (a hook on each side), an append with an unknown outcome,
  two writers of one key, a canceled waiter, writes of many overlapping keys
  with no deadlock, a lock key collision, and a stress run against a simulated
  WAL with an observer.
- Mutation checks: each test fails when its rule is broken. That is: no removal
  at commit, the set is not read, no wait, a canceled waiter leaves at once,
  either order of the observe and commit race is reversed. Without the
  rollback, the redo test fails.
- Benchmarks: the lock under contention (hot key, batches of 1 to 1000 keys,
  mixed batches), and the cost of each step of one 1000-key write.
- An integration test inserts the same keys twice and checks `count(*)`. With
  the switch off it reads 150 rows, with the switch on it reads 100.
