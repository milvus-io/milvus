# StreamingNode replica discovery

ViewQueryService exposes a VChannel-scoped replica watch. QueryViewClient can
obtain candidate replicas and their maximum locally Up View versions without
querying Coordinator for every request.
The WAL-owned SNQueryViewHandler is authoritative for local View state; the RPC
service owns only connection/subscription lifetimes. QN does not implement Watch.

## Extraction boundary

This branch includes the directory, local/RPC watch, shared subscription manager,
and injectable query-client routing. Production Proxy ownership and the QN
session observer/transport are parent integration points. See the explicit
[client transport contract](query_client.md#component-and-integration-boundary).
Discovery does not activate production Proxy QueryView routing on its own.

## Directory and publication

A replica belongs to the directory if at least one local View version is Up.
UpRecovering is excluded until recovery completes. Multiple Up versions produce
one entry containing the maximum composite QueryViewVersion. Pending Down remains advertised while actually Up; Watch never acquires a
query reference or renews serving leases. Publication follows local persistence,
including asynchronous recovery/failure and lease-expiry transitions. Dropping
and WAL shutdown remove eligibility. No extra persistence is introduced.

`VChannelReplicas` carries the VChannel once and a repeated `Replica` containing
`replica_id` and `max_up_view_version`. VChannel plus ReplicaID identifies the
ShardID without repeating the VChannel string. It does not replicate the full
View, worker topology, Segment lists, Schema or LoadInfo.

Notify when a replica appears/disappears or its maximum Up version changes.
Thus a handoff from V10 to V11 is observable even if ReplicaIDs stay unchanged.
Maintain the maximum per shard and an advertised-Up bit per entry. A newly Up
version updates the maximum only when greater; retiring a nonmaximum version
changes no directory entry. Only retiring the maximum scans that shard's retained
versions to find the remaining maximum. Versions use the existing lexicographic
(StreamingVersion, CompactVersion, QueryVersion) ordering. Normal transitions
are O(1), plus notifications; maximum retirement is O(retained shard versions),
not O(all collections or segments). Publication and fallback use successfully
persisted advertised state, before entry cleanup. Recovery may complete out of
order and must not replace a greater maximum with a smaller newly recovered View.
If the maximum actually leaves Up while an older version remains Up, advertise
the remaining maximum; snapshots describe current eligibility, not a monotonic
watermark. An empty set removes the shard's eligibility.
The lock order is shard -> directory; directory never acquires shard/handler
locks, calls consumer code, or sends RPCs.

## Transport and ownership

One bidirectional WatchVChannelReplicas stream is scoped to one PChannel owner
and term. It multiplexes VChannel subscriptions keyed only by VChannel. Stream
creation validates the owner and acknowledges readiness before any subscriptions.
Subscribe returns a full snapshot immediately, including an authoritative empty
list. Repeated Subscribe is idempotent. Subsequent eligibility or maximum-version changes send full
replacement snapshots. Unsubscribe removes the listener and pending update before
sending its acknowledgement. The single server sender orders all snapshots and
acknowledgements, so none for that subscription follows its unsubscribe ACK.

Registration and initial snapshot capture share the directory lock. A stream
stores only a pending VChannel set and reads the latest snapshot when sending;
slow consumers coalesce changes rather than accumulating an event log. No
per-VChannel goroutine, polling, replay cursor, ACKed data position, or resource
pin is required. WAL unavailability immediately terminates associated RPCs, before resource
draining. Handoff also closes the directory. A blocked gRPC Send must not delay
handler return: transport cancellation then releases blocked Send/Recv workers.
Errors use native streaming gRPC status, not response error fields.

## Client and LocalWAL

`HandlerClient.QueryViewClient().SubscribeReplicas(vchannel, handler)` returns a
Subscription. A client-owned Manager resolves PChannel and shares exactly one
logical watcher per PChannel. Callers do not create Watchers or wait in a
per-VChannel goroutine. Multiple handlers for the same VChannel share state and
one physical subscription. Subscription.Snapshot() is an optional synchronous
cache read (Replicas and Valid); notifications use callbacks, not change channels.
Each delivered snapshot owns its replica slice and value-type versions; neither
callbacks nor cache readers can mutate another snapshot or directory metadata. Invalid distinguishes disconnected
or uninitialized state from an authoritative empty set.

A PChannel has a recovery loop and a callback dispatcher, plus one receiver while
connected. VChannels and handler registrations have no goroutines. Idle watchers
park their two control loops until reuse; physical streams are canceled when the
last subscriber leaves. The Manager retains at most one watcher per encountered
PChannel until HandlerClient.Close. Thus control-loop cost scales with PChannels,
not collections, VChannels or subscribers.

Within each attempt, the receive loop directly applies snapshots and unsubscribe
acknowledgements while the recovery loop sends subscription changes. Both share
subscription state under a short lock; neither holds it during Send or Recv.
Transitions are registered before sending, so responses arriving before Send
returns are safe. A flow-controlled Send cannot prevent response consumption.
Failure in either direction closes the attempt to unblock the other direction.

The callback dispatcher coalesces pending updates per handler and executes
serially outside all state/transport locks. Handlers only update local routing
state; they must not block on I/O or close the owning Manager/HandlerClient.
Subscription.Close is safe inside a callback: it removes pending notifications;
an already selected callback may finish after Close. Manager.Close cancels
recovery and joins dispatchers and callbacks. HandlerClient closes the Manager
before waiting for its own in-flight operations, so owner discovery cannot keep
shutdown blocked. No callback goroutine is created per update or subscription.

Each attempt uses a small Stream interface (Send, Recv, Close), not the generated
gRPC client interface. After resolving the current assignment, HandlerClient
first calls registry.GetLocalAvailableWAL with the exact owner term. A local WAL
is unwrapped to its replica-directory Provider and bridged directly using
NewLocalStream. Only existing shouldUseRemoteWAL fallback conditions choose RPC;
local internal failures are not silently converted to a remote route. The local
adapter preserves snapshot/unsubscribe ordering without gRPC, goroutines, WAL
messages, or serialization. It observes both WAL Unavailable and directory Close.
The remote adapter retains ViewQueryService and the validated Ready handshake.

The logical watcher invalidates cached availability on failure, rediscovers the
assignment, backs off and recreates desired subscriptions. Local-to-remote and
remote-to-local changes use the same resume loop; no separate recovery state
machine exists. Attempt cancellation/Close unblocks Send and Recv and the old
receiver is joined before a replacement starts. Re-subscription waits for the
old unsubscribe acknowledgement, preventing stale snapshots from becoming the
new initial result. No wire subscription ID is introduced.

This follows TransformLog's lifetime/dispatch patterns with an independent
implementation. Latest-state snapshots need no cursor, replay or SyncUp.

## Query routing boundary

The directory is a candidate hint, not an acquired QueryView, schema readiness
certificate, or downstream QN liveness proof. GetQueryPlan still validates the
requested schema and acquires an actual Up View. The advertised maximum does not
pin that query to the maximum: an old-schema query may select an older retained
Up View. A newer advertised version does not prove that a failed QN has been
removed, and a temporary QN reconnect need not change the version. Connection
readiness and session liveness remain separate inputs to query retry.
Replica selection and execution
failure-driven reselection are implemented by [QueryViewClient](query_client.md).
The discovery service supplies candidates; it does not pin query versions or
implement query retry.
Current assignment resolves one WAL owner. Future WAL replicas require explicit
WAL-replica assignments and separate directories; the PChannel term is an owner
fence, not a permanent WAL-replica identity.

## Verification

Cover initial populated/empty snapshots, concurrent registration/publication,
multiple Up versions, composite-version ordering, same-replica version updates,
out-of-order recovery, maximum retirement/fallback, snapshot ownership,
recovery/failure/lease retirement, no lease renewal,
unsubscribe/re-subscribe ordering, shared local handles, stream failure and
handoff/reconnect, slow subscribers, cancellation, and bounded pending state.

Key packages: `internal/views/replicadiscovery`,
`internal/streamingnode/server/wal/snview`,
`internal/streamingnode/server/wal/adaptor`,
`internal/streamingnode/client/handler`.
