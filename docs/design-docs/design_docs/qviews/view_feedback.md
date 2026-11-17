# ViewFeedback: Replica and Global Access Feedback

Related feature: [milvus-io/milvus#55284](https://github.com/milvus-io/milvus/issues/55284).

StreamingNode collects best-effort planning access windows by ShardID and sends
those windows through the existing SyncQueryView streams. Coordinator publishes
an immutable aggregate per Replica: the latest access time and four request
rates, averaged over the Replica's full shard count. It retains received windows
for two minutes and expires them independently of reports and reads. A separate
immutable global rates snapshot sums the normalized Replica rates across all
collections and Replicas.

Feedback may be lost. This module does not implement reliable delivery,
persistent access history, loading priority, or scheduling scores.

## 1. Measurement and identities

`qviews.ShardID = (ReplicaID, VChannel)` has no QueryView version.
`GetQueryPlan` records one access after acquiring a serving View lease and
validating the schema and collection, before MVCC waiting and plan construction.
When the request does not name a Replica, the acquired lease supplies its actual
ReplicaID. Search, Retrieve, and Statistics planning attempts each count once.
A later failure does not undo the access; retries count again. Phase 2 does not
record another planning access. Counts are neither successful queries nor NQ.

Each raw shard sample contains a window request count and the SN's latest access
Unix timestamp in nanoseconds. All samples in a report share the Node's window
Start and End. The counters are increments within that window, not cumulative
counters. The final Replica snapshot does not expose these raw counters or a
ShardID.

## 2. StreamingNode collection

Each top-level `SNQueryViewHandler`, scoped to a PChannel/WAL instance, owns a
private `viewFeedback` child structure:

```text
viewFeedback
  mutex
  current window start
  ShardID -> (window request count, last access)
  capacity-one size notification
```

`RecordShardAccess` updates the current window under this structure's short
mutex. It does not acquire the handler's View map lock, a snShardView lock, or a
state-machine lock, and it does not encode protobuf or perform network I/O.

`TakeViewFeedback` detaches the current map in O(1) and starts a new window at
one nanosecond after the take time. This makes the next Start strictly greater
than the previous End, including across empty takes. The first access initializes
a window if none has started yet.
**Every take advances Start, even when the map is empty and no report is sent.**
Later accesses therefore do not inherit earlier skipped empty windows. The sender
owns the detached map; collection immediately continues in a new map. Encoding
and sending happen outside the collection mutex.

There is no cumulative ground truth, last-sent comparison cache, or historical
shard table on SN. View replacement, Down, and Dropped do not clear the current
window. Once taken, a window is consumed regardless of send success. SN restart
or WAL owner migration may lose the current window without reconstruction.

## 3. Persistent streams and reporting

At initialization, `qviewsRuntime` obtains the local PChannel inventory through
`GetClusterChannels(OptIncludeUnavailableInReplication())` and supplies it to
`ReliableSyncer`. The Syncer creates persistent resumable streams without waiting
for a View or an assignment owner to become ready. Later `SyncViews` calls add
previously unknown PChannels. Once created, a stream remains alive with no pending
Views or feedback. Newly added PChannels without Views have no planning accesses
to report yet. Existing HandlerClient assignment resolution handles owner changes
and reconnection; no additional discovery RPC or continuous inventory watcher is
introduced.

The existing node send loop takes feedback:

- When the stream starts, using only the currently unconsumed window.
- Five seconds after the previous take, or when the map reaches 1024 distinct
  ShardID entries. The threshold counts entries, not accesses; one hot shard
  cannot trigger frequent size-based reports.
- At least one second apart for size-triggered takes within the same stream.
  An early notification schedules that deadline while state and close reports
  remain serviceable. Repeated notifications do not postpone the deadline.

Taking a window drains the old size notification and resets the five-second
interval. Empty windows are not sent. The capacity-one notification needs no
additional SN goroutine or subscriber list. The 1024-entry threshold is not a
hard memory cap: collection continues while the sender is blocked.

The sender encodes up to 256 shards per batch and checks pending state/close
reports between feedback batches. Coordinator can accept each batch independently.
On failure, cancellation, or shutdown, unsent feedback is discarded. There is no
ACK, replay, sequence, epoch, or compensation queue. Reconnection resumes the
stream and existing View pending state, not already consumed feedback.

Detaching a map requires no full-table scan or change detection. Encoding and
network sending still consume resources; an in-progress gRPC Send cannot be
preempted by a later state report. This is not a strict latency isolation guarantee
under slow network conditions.

### Wire format

The protocol reuses `viewpb.ShardID` and `SyncResponse.view_feedback`:

```proto
message ViewFeedback {
    ShardID shard_id = 1;
    uint64 request_count = 2;
    int64 last_access_unix_nanos = 3;
}
message ViewFeedbackReport {
    repeated ViewFeedback shards = 1;
    int64 window_start_unix_nanos = 2;
    int64 window_end_unix_nanos = 3;
}
```

The views/close fields and RPC signatures are unchanged. Reports lacking valid
window times are ignored; an older cumulative counter must not be interpreted as
a window increment. Nodes without ViewFeedback continue ordinary state syncing.
QN does not produce feedback.

## 4. Coordinator ownership and topology

ReliableSyncer validates that each sample belongs to its stream's PChannel and
calls `ReplicaFeedbackStore.OnViewFeedback` directly. The store checks current
Replica membership and the collection encoded in the VChannel. It does not call
the Registry or a QueryView state machine.

`LoadConfigStore.RegisterLoadConfigListener` replays and tracks configured
Replicas. Keeping the same ReplicaID preserves feedback across resource-group,
field, and index changes. Withdrawing a Replica or collection load intent removes
its feedback immediately; late reports cannot resurrect it. Individual View
creation, replacement, and deletion do not own this lifetime.

The existing local `DataViewPublisher` supplies the collection's full shard count,
including shards with no feedback. Each Replica of the collection uses this count
as its divisor. The listener caches only the count, performs no I/O, and never
re-enters either metadata publisher. Ordinary DataView updates with unchanged
shard count do not republish feedback. A changed count republishes existing
Replica aggregates. Without a nonzero published count, the store returns no
snapshot rather than guessing the divisor from observed shards.

## 5. Windows, contributions, and expiration

Each shard appends accepted contributions in strictly increasing **Node
EndUnixNanos** order. A contribution contains Start, End, and request count.
Start must be positive and End must exceed Start. If Start is less than or equal
to that shard's last accepted End, discard the sample, including its count and
LastAccess. This rejects out-of-order, duplicate, overlapping and equal-boundary
windows without sorting or inserting into retained history. Admission is per
shard, so batches or distinct shards sharing window boundaries remain independent.
The last accepted End survives window expiration until Replica withdrawal;
Coordinator restart starts with no feedback or watermark.

There is no duration cap: a long window after stream disconnection retains its
full count and actual time span, including its recent LastAccess. It expires by
End under the same two-minute retention rule. Do not clip Start while retaining
all requests. There is no Coordinator receipt timestamp or correction of Node
timestamps. A sender clock rollback or owner change may cause windows to be
rejected until their Start exceeds the previous accepted End.

The store maintains four private recent-window counters per shard, for up to the
latest 2, 6, 12, and 24 accepted windows. Appending adds the request count to each
group and subtracts the window displaced from a full group. Strict non-overlap
makes the first selected window's Start the minimum and the last window's End
the maximum; no cached group start or boundary scan is needed.

One store-owned goroutine runs every second. It removes the expired prefix whose
End is at or before `now - 2 minutes`, subtracts those contributions from affected
recent counters, and refreshes the shard's rates. No report or read is needed to
drive expiration. Whole windows expire together; boundary windows are not split
or prorated. Empty window lists release their backing arrays.

Current Coordinator time only advances cleanup. Late reports already outside the
retention horizon are removed on the next sweep. Node clock skew directly affects
expiration. Reports are best effort. The timestamp watermark rejects stale or
overlapping samples across streams; there is no replay or loss compensation.

`LastAccessUnixNanos` is the maximum access timestamp in accepted samples for the
Replica. Its revert operation is a no-op. After all windows expire, RequestRates
becomes zero while LastAccess remains, until the Replica is withdrawn.

## 6. Replica rates and immutable snapshots

For each shard and each N in {2, 6, 12, 24}, select at most its latest N retained
windows by Node End and calculate:

```text
shardRate[N] = sum(selected request counts)
               / seconds(selected[last].End - selected[0].Start)
replicaRate[N] = sum(shardRate[N]) / total collection shard count
globalRate[N] = sum(replicaRate[N] across all configured Replicas)
replicaLastAccess = max(all accepted shard access timestamps)
```

The time denominator includes gaps between selected windows. It uses actual Node
Start and End, not an assumed five-second duration and not Coordinator receipt
time. Fewer than N windows use the available windows; no windows contribute zero.
The denominator does not extend beyond the latest received End toward the present.
Each shard selects its own N windows before averaging: reports from different
shards are not pooled into one sequence or one common time span.

Unreported shards contribute zero but still participate in the divisor. For two
shards, rates of 4/s and 0/s produce 2/s; rates of 4/s and 3/s produce 3.5/s.
Missing feedback can understate the result. A zero rate does not prove an absence
of real traffic.

The public read interface is:

```go
type Snapshot struct {
    LastAccessUnixNanos int64
    RequestRates        RequestRates
}

type RequestRates struct {
    Last2Windows  float64
    Last6Windows  float64
    Last12Windows float64
    Last24Windows float64
}

func (*ReplicaFeedbackStore) GetViewFeedback(replicaID int64) (*Snapshot, bool)
func (*ReplicaFeedbackStore) GetGlobalRequestRates() *RequestRates
```

Reports and actual expiration update private shard counters/rates, then publish
one newly allocated Replica snapshot per affected Replica per batch/sweep under
the store's write lock. Publication sums the four cached shard rates and divides
by the total shard count. It does not traverse historical windows again. A cleanup
pass that removes nothing keeps the same snapshot pointer.

Readers take a read lock for the Replica lookup and return the **same published
pointer**. Reads do not copy, allocate, divide, scan windows, or inspect a clock.
Callers must treat the snapshot as read-only; exported Go fields do not enforce
immutability. New reports, expiration, withdrawal, and Close never mutate an
already published snapshot, so a reader may safely retain it after releasing the
read lock.

Unknown/withdrawn Replicas, Replicas without feedback, or unavailable topology
return `(nil, false)`. A previously observed Replica whose windows all expired
still returns a snapshot with preserved LastAccess and zero rates. Expiration
visibility can lag by approximately one cleanup interval.

### Global rates

Global rates are a sum, not an average over Replicas or over all cluster shards.
Shard fanout is normalized within each Replica before summing. For example,
Replica rates of 2/s and 3.5/s contribute 5.5/s globally even if the collections
have different shard counts. The result estimates cluster-wide planning traffic;
it inherits the shard window, loss, and retry semantics above and is not an
exact count of distinct client requests.

Replacing a Replica snapshot subtracts its previous rates from the global
aggregate and adds its new rates under the same write lock. This adds O(1) work
per affected Replica, without traversing other Replicas or windows. Reports,
expiration, topology changes, and load-config withdrawal all use this path.
Unavailable topology contributes zero. If only LastAccess changes and the rates
stay identical, the global snapshot pointer stays unchanged.

Global publication is also copy-on-write. `GetGlobalRequestRates` returns the
published read-only pointer with no allocation, copying, or aggregation. It is
always non-nil, including before any feedback and after Close. With no nonzero
Replica contributions, the aggregate resets exactly to zero instead of retaining
floating-point subtraction residue. Close publishes zero without changing any
snapshot already held by a reader. Global and Replica reads are individually
consistent; separate getter calls do not constitute an atomic multi-value read.

Window admission, append and the four rate-group updates are amortized O(1)
per shard, with no sorted insertion or boundary scan. Replica publication is O(S)
over cached rates of its observed shards, once
per affected Replica per batch/sweep. Reads are O(1). Configuration callbacks,
reports, and cleanup share only the feedback store's lock, but their CPU,
allocation, and lock-wait costs can still indirectly delay synchronous metadata
publication. They do not acquire state-machine locks.

## 7. Startup and shutdown

After LoadConfig recovery, the store registers DataView and LoadConfig listeners,
replays their state, and starts cleanup before being passed to ReliableSyncer.
Shutdown stops Syncer first, unregisters both metadata listeners, stops and joins
the cleanup goroutine, and clears owned state. Initialization failure follows the
same cleanup path. Close is idempotent.

Coordinator restart does not recover windows or request replay from SN. New
accesses provide new feedback. Persistent SN streams remain alive while idle.

## 8. Verification scope

- Concurrent observation and O(1) takes; detached windows remain independent.
- Observation/takes while View locks are held; View cleanup does not clear feedback.
- Timer/size triggers, one-second size cooldown, notification coalescing, empty
  takes advancing Start without a report, and state/close handling between batches.
- Dropped feedback is not replayed after failure or reconnection; idle streams
  remain active without pending Views.
- Actual-span rates for variable windows, gaps and long reconnect windows;
  partial groups, prefix reversion, and autonomous expiration.
- Reject old, duplicate, overlapping and equal-boundary samples; preserve the
  last accepted End after expiration; accept consecutive real SN windows.
- Long reconnect windows retain recent LastAccess and use their full duration.
- Replica means include silent shards, preserve independent shard time spans,
  isolate Replica identities, and follow DataView shard counts.
- Copy-on-write pointer identity, retained snapshot stability, zero-allocation
  reads, concurrent readers, LastAccess preservation, and lifecycle removal.
- Global sums across collections/Replicas, replacement and withdrawal of
  normalized contributions, zero after expiration/Close, and immutable zero-allocation reads.
- Runtime recovery failure and repeated shutdown detach both listeners.
- SDK queries observed by Coordinator, rates returning to zero after two minutes
  without traffic, renewed access, and release/reload/drop behavior.

Actual commands, builds, coverage, lint, and E2E outcomes belong to the workspace's
`docs/plans` evidence. This verification list is not a claim that a particular
revision has passed those checks.
