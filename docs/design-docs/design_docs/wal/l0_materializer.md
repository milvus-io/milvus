# L0 Materializer Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Status:** Shared Summary reads, capacity admission, recoverable explicit
flush requests and Summary-owned materialization backlog requests are implemented.
Explicit completion waits for L1 final commit. Capacity work leaves small tails
for later accumulation. L0Materializer has no age-based flush timer.
[TransformLog](transform_log.md) remains a separate future subscription adaptor.

## 1. Ownership

`l0materializer` continuously converts one VChannel's Delete records from
[WALSummary](summary.md) into DataCoord-managed L0 segments. It is always active,
including when no external subscription exists, and has no enable switch.

```text
RecoveryStorage
  +-- WALSummary (PChannel record storage)
  +-- PChannelRecoveryManager
        +-- VChannelRecoveryModule
              +-- VChannelView
              +-- SegmentView*
              +-- L0Materializer -> Summary bounded reads -> L0 / DataCoord
```

It is an independent VChannel-owned component, not a new top-level recovery
module. It depends on a narrow Summary read interface, the L0 writer/registrar,
and the shared scheduler. It has no dependency on TransformLog, its streams, or
RPCs, and owns no separate catalog or record storage.

The VChannel module derives the L1 safety bound and aggregates materialization
progress into `VChannelMeta`. Its `transform_materialized_time_tick` stores the
durable L0 frontier. Unfinished explicit requests are reconstructed from WAL
replay and have no persisted field or reserved field number.

## 2. Window State

The runtime keeps three positions:

| Position | Meaning |
|---|---|
| M: materializedThrough | All Delete records through M have successfully completed L0 output and registration. |
| W: requestedThrough | The newest materialization boundary accepted by this component's ordered observation path. |
| L: upperBound | The inclusive L1 safety bound supplied by the VChannel owner. |

The outstanding window is `(M, W]`; `min(W, L)` is its safe upper bound,
not a sufficient reason to schedule output.
The persisted copy of M can lag the running value and is tracked by normal
VChannel snapshot bookkeeping. W and L are runtime state, not additional
persistent checkpoints or WAL replay positions.

There is no pending Delete-entry list, recovery-loaded payload window, or
`loadedThrough`. Window positions are constant-sized; only the current bounded
read/materialization batch holds Delete payloads. Explicit completion requests
add an ordered list of retained WAL handles until their boundaries are covered.
Coalescing targets does not discard the individual handles.

## 3. ObserveMessage

Observation classifies each valid, ordered, routed WAL message:

| Kind | Message | Window effect |
|---|---|---|
| DeleteEntry | Delete, or committed Txn containing Delete | Advance W to the outer message TimeTick. |
| None | Insert, or nonempty committed Txn containing only Inserts | No change. |
| BarrierEntry | Every other message, including empty or mixed non-Delete Txn | Advance W to the message TimeTick. |

Delete takes precedence inside a Txn. ObserveMessage performs no I/O, copies no
payload, and retains no Delete or ordinary Barrier handle. It monotonically
merges the requested boundary and asks the batching policy to re-evaluate when
relevant state changes. The owner separately calls `RequestFlush` with a
retained message for explicit completion operations before dispatch returns.
BarrierEntry is a classification, not an allocated queue entry or stored record.

PChannel-level messages, including persisted TimeTicks and RecoveryBarrier,
reach every affected VChannel. Non-persisted heartbeats remain filtered by
RecoveryStorage. Messages belonging only to another VChannel do not advance W.

Observation order is part of the integration contract:

```text
Summary.ObserveMessage
  -> install records and complete readable coverage in memory
VChannel.ObserveMessage
  -> update VChannel / Segment state and L1 upper bound
  -> L0Materializer.ObserveMessage advances W last
```

This is an in-memory ordering requirement, not a Summary flush requirement.
Summary must account for coverage of payload-free messages, including
RecoveryBarrier, before exposing their materialization boundary. A background
task must never interpret not-yet-observed data as an empty interval.

## 4. L1 Safety Bound

The owner derives:

```text
L = min(create_segment_time_tick of every Segment with l1_commit_done = false)
```

With no blocker, L is unbounded. The creation TimeTick is safe to include
because rows assigned to that Segment have later TimeTicks. A newly observed
CreateSegment installs its blocker before observation advances W.

Tasks cannot read beyond W even if Summary restored newer history. This prevents
materialization from passing Segment state not yet reconstructed by replay.
A later CreateSegment has a TimeTick beyond an already observed task target;
it cannot invalidate a correctly captured older interval.

An L1 final commit or lifecycle change recomputes L and independently
re-evaluates pending work. Raising L alone does not force a small output batch.
An explicit API flush waits for its related L1 Segment flushes to complete,
including output durability and final DataCoord commit, before L0 executes
through its requested boundary. Merely enqueuing a flush or marking a Segment
sealed is insufficient. All work still respects W and the VChannel-wide L.
Completion notifications must wake eligible requests without another WAL message.

## 5. Read And Materialize

### 5.1 Reasons To Materialize

The objective is to accumulate useful output batches and minimize physical L0
materializations. There are exactly three sources of requests:

| Source | Admission rule | Completion boundary |
|---|---|---|
| Capacity | Delete logical bytes in `(M,min(W,L)]` reach the configured byte target. | Process a bounded batch, then re-evaluate capacity; a small remaining tail waits. |
| Explicit completion | An API/lifecycle operation requires L0 progress through F, and its related L1 flushes are complete. | Complete the captured F subject to W and L, including a below-target tail. |
| Summary backlog | Summary requests progress through B for outstanding Delete consumption or retention pressure. | Complete the requested bounded prefix subject to W and L; coalesce repeated requests. |

Capacity uses `FlushL0MaxSize` (default 32 MiB). `FlushL0MaxRowNum`
(default 500,000 rows) limits output batches but is not an admission trigger.
Barrier counts, Insert sizes and Deletes beyond L do not count toward capacity.
Summary supplies section-level lower/upper byte bounds without I/O. Below-target
upper bounds do not schedule work. Uncertain partial sections are checked by a
bounded asynchronous byte probe before any physical output. A below-target
probe is remembered through its captured safe boundary to prevent a task loop;
new Delete coverage or a raised safety boundary may enable another check.
There is no per-entry permanent statistics index or copied payload backlog.

ManualFlush, FlushAll and lifecycle operations whose completion/cleanup needs
L0 output create explicit requests through the VChannel owner. Ordinary
TimeTicks, RecoveryBarrier, generic DDL and individual automatic Segment flushes
do not automatically create such requests. Barrier classification and the
semantic requirement to complete an API are separate decisions.

**There is no L0 maximum-age timer, idle timer, or periodic forced flush.**
`FlushL0MaxLifetime` does not supply a trigger to this component. Long-unmaterialized
Delete data is governed by Summary's backlog mechanism (§5.2). A raised L,
Summary upload completion or recovery checkpoint publication is not itself a
physical materialization trigger.

### 5.2 Summary-Owned Backlog Requests

Summary's backlog covers both records awaiting object persistence and retained
Delete records still awaiting consumer materialization. Persisting a chunk
moves records between storage states; it does not remove unmaterialized Deletes
from that backlog. This includes durable pre-checkpoint records after recovery,
even when `pending` is empty and no new WAL messages arrive.

Summary owns the decision to request a bounded consumption prefix and emits
that request through recovery/VChannel wiring. It does not import or execute
the L0 writer. The materializer merges requests, waits for safety/dependencies,
and carries out the output. An ordinary Summary flush need not request L0.
Retention pressure targets the oldest chunks whose release can actually be
unblocked; another consumer's retention need cannot be solved by extra L0 output.
The existing Summary backlog worker checks the earliest outstanding Delete's
WAL physical time against its existing backlog age budget. Upload and restart
do not reset that age. Retention pressure requests only the oldest blocking
chunk, then reassesses after metadata publication and GC. Successful output is
reported to Summary immediately to suppress redundant consumption requests;
this runtime report does not authorize GC. No independent per-materializer
deadline or age setting is added.

Range statistics and backlog inspection must include cold and hot records,
using Summary indexes and bounded reads without scanning all payloads on each
observation. A storage transition or restart cannot reset outstanding work.
Runtime requests from this policy can be reconstructed from Summary state;
explicit API requests are rebuilt by WAL replay as described in §6.

### 5.3 Execute A Captured Request

Each VChannel executes materialization batches serially:

1. Admit work for one of the reasons above; safety alone does not admit work.
2. Capture a finite target bounded by W and L. Explicit API work also waits
   for its related L1 flush completion; retain blocked requests without polling
   them as ready tasks.
3. Reject a fast-forward beyond M; missing required Delete history is an
   integrity failure. Read Delete records in `(M,target]` from Summary, bounded by rows/bytes.
4. Use returned `CoveredThrough`, not the requested target, as the possible
   commit position; group Deletes by partition/PK representation, write L0
   deltalogs and register all resulting output with DataCoord.
5. After the entire batch succeeds, update VChannelMeta through the owner
   callback and mark the snapshot dirty, then expose the new M and release
   retained explicit-request handles whose boundaries are covered. Keep later
   requests and failed/canceled work retained. Invoke finalizers outside locks.
6. For capacity work, continue only while capacity is still satisfied. For an
   explicit or backlog request, continue to its captured goal, then re-evaluate.
   New small arrivals do not indefinitely extend the running request's goal.

A capacity batch must not automatically drain a below-target remainder just
because W is still ahead of M. Coalesce requests arriving during execution and
keep one active task per VChannel. Rows/bytes also bound each execution and
physical output, with the complete-Entry rule below.

The reader covers durable chunks, sealed records, and the pending tail.
Materialization does not wait for Summary object persistence or manifest
publication. A Summary read failure or output/registration failure leaves M
unchanged and retries without skipping data.

An Entry, including a committed Txn's Delete children, is the smallest cursor
unit. A batch may exceed its soft row/byte limit for one oversized Entry; it
must not commit an Entry's TimeTick after processing only part of that Entry.
Physical L0 output may be split, but the batch frontier advances only after all
required outputs are registered.

A proven empty interval can advance M without producing an empty L0. Coalesce
this metadata work with required boundaries or normal snapshot handling rather
than scheduling a separate task for every TimeTick. An
incomplete read cannot. A read with no coverage progress must wait for the
relevant change or return an error, never spawn an endless continuation chain.
Task completion and new-window/L1-bound updates must be coordinated so a wakeup
arriving during task completion cannot strand work.

## 6. Persistence And GC

The publication sequence is:

```text
L0 output durable and DataCoord registration successful
  -> advance in-memory M
  -> owner updates VChannelMeta.transform_materialized_time_tick and marks dirty
  -> RecoveryStorage persists the captured VChannel snapshot
  -> report that snapshot's M to Summary as this consumer's release position
```

An in-memory M alone cannot authorize GC. Both full and base-only VChannel
snapshots must report their captured frontier after successful persistence.
Callbacks cannot substitute a newer in-memory value. Durable lifecycle cleanup
may provide an equivalent release position when no retained recovery/serving
state still needs the records. Restoring DROPPED or TOMBSTONED metadata alone
does not prove L0 completion: these states still use their persisted M for GC.
The catalog cleanup callback releases the remaining history only after cleanup
has satisfied its materialization dependency.

Summary owns actual chunk retention and deletion. Future subscription consumers
add their own history requirements; this consumer's release position is not
permission to override those requirements.

An explicit completion request retains its WAL message handle until L0 output
and registration through its boundary succeed and the owner installs dirty
materialization metadata. Each affected VChannel owns an independent clone.
Releasing the last consumer handle allows BroadcastAck/Tracker completion.
The checkpoint publisher then saves the captured component metadata before it
can publish a checkpoint past that request.

There is no persisted explicit-request field. A crash before completion leaves
the global checkpoint before the Flush, so WAL replay reconstructs the request.
If M was persisted before the crash but the checkpoint was not, replay can
recognize an already covered request without producing another L0. If neither
was persisted, physical output may repeat safely.

Ordinary Delete/capacity/backlog work retains no source WAL handles; Summary's
recoverable confirmation protects those records while materialization lags.
Explicit Flush/lifecycle requests do gate the checkpoint and, for broadcasts,
Coordinator Ack until L0 completion. This is intentional.

## 7. Recovery And Close

After restoring Summary and VChannel/Segment metadata:

1. Set M from the durable VChannel materialization field and initialize W = M.
2. Derive L from the restored Segment state before scheduling work.
3. Replay WAL once from the global checkpoint using the same Observe path as
   live consumption; Delete/Barrier messages advance W monotonically.
4. Route the startup RecoveryBarrier to each VChannel still requiring
   materialization. This ensures old Summary backlog is discovered even when
   no new Delete arrives.
5. Rebuild explicit completion requests from WAL replay and re-evaluate
   capacity and Summary backlog requests; read outstanding data lazily only
   when admitted.

For example, M=50 and global checkpoint=200 may coexist with an unmaterialized
Delete@100 already stored in Summary. WAL replay need not deliver that Delete
again: RecoveryBarrier@250 exposes a window through 250, and an admitted
capacity/API/Summary-backlog request reads Delete@100 from `(50,250]`, subject
to L. RecoveryBarrier itself does not force an undersized tail into L0.
No startup payload preload is needed.
Do not initialize W from Summary's largest position, which may be ahead of
VChannel replay. Restored M may itself be ahead of the global checkpoint;
older observations never move it backward or request already completed work.

Closing cancels reads/tasks without releasing unfinished Flush handles as
successful or requiring a final materialization. Restart reconstructs the
pending window from durable M and ordered replay. If L0 registration succeeds but the updated VChannel metadata
is lost in a crash, the batch may be repeated. This design does not promise
physical exactly-once output; output idempotency/reconciliation is separate.

## 8. Invariants And Validation

- Summary is the only owner of Delete record storage.
- ObserveMessage records boundaries only, after Summary and Segment observation.
- Explicit requests retain independent handles; dirty M precedes their release.
- Pending Flush requests remain in the WAL replay range, with no persisted F.
- A task never commits beyond W, L, or its complete read coverage.
- Per-VChannel batches advance through a continuous prefix of complete entries.
- Payload memory is bounded by active batches, not unmaterialized history.
- Successful L0 registration precedes M advancement; durable M precedes GC release.
- Materialization runs without subscribers and does not depend on TransformLog.
- Safety/Barrier progress alone cannot force output; L0 has no age timer.
- Capacity tails accumulate; explicit API output waits for related L1 completion.
- Summary backlog includes already-persisted, still-unmaterialized Delete records.

Validation must cover storage transitions during reads, empty windows,
row/byte-capped Txns, L1-bound release without new messages, observation/task
completion races, recovery with Summary ahead of replay, pre-checkpoint Delete
backlog, Barrier-only startup, and crashes between output registration and
metadata publication.

Batching validation must additionally cover low-rate Deletes with frequent
TimeTicks, below-target tails after full batches, no periodic L0 flush,
Summary backlog requests while pending storage is empty, blocked explicit
requests surviving restart, L1 completion without new input, and request
coalescing without continually extending an active goal.
