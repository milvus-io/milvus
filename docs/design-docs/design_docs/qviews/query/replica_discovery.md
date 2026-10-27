# StreamingNode replica discovery

StreamingNodeHandlerService exposes VChannel-scoped replica subscriptions.
The streaming QueryViewClient can
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

The local directory also tracks whether any version is Preparing, Ready or
UpRecovering. These states never become query candidates. When a subscription
has not received its first snapshot, no Up replica plus at least one such pending
version defers that snapshot until a replica becomes Up or all pending versions
fail/retire. With neither Up nor pending versions, it immediately publishes an
empty snapshot. Any available Up replicas take precedence over other pending
versions. Recovery registers all pending entries before starting preparation.
This is an event-driven delivery gate for each physical registration, not a
blocking Subscribe call or a per-VChannel goroutine. Existing subscriptions
still receive removal of their Up replicas immediately, including when replacement
versions are preparing; they must not retain retired routing hints.

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

`StreamingNodeHandlerService.SubscribeReplicas` follows TransformLog's stream /
subscription separation. Creation metadata carries the assigned PChannel and
term. The server validates WAL ownership, acquires a domain stream, and returns
`CreateReplicaWatchStreamResponse` before accepting subscriptions. QN does not
implement this RPC. The former unreleased ViewQueryService Watch RPC is removed.

`ReplicaWatchRequest` and `ReplicaWatchResponse` use extensible oneofs. Creation,
snapshot, subscription error, close-subscription and close-stream have distinct
messages. A client-allocated positive subscription ID identifies each physical
registration; the VChannel belongs to the stream's PChannel. IDs may arrive out
of allocation order with concurrent callers. The client never reuses an ID in
an attempt. Multiple registrations for one VChannel are distinct subscriptions.

The server sends the create acknowledgement before that subscription's initial
full snapshot, subject to the initialization gate above. Creation acknowledgement
does not certify an initialized replica list. Subsequent changes send
full replacement snapshots. Closing a subscription first detaches its domain
listener and removes pending snapshots, then acknowledges the close. A single
sender orders snapshots and acknowledgements, so no snapshot for that ID follows
its close acknowledgement. A replacement registration uses a new ID.
Subscription failures carry StreamingError; stream admission and transport errors
use native streaming gRPC status, as with TransformLog.

Registration and initial capture are synchronized by the directory. Domain
updates coalesce per VChannel; the RPC adapter coalesces per subscription ID.
Neither retains an event log. WAL unavailability terminates the stream before
resource draining, independently of eventual directory Close. The RPC handler
returns even if Send is flow-controlled, allowing gRPC cancellation to release
its sender and receiver. No polling, replay cursor, data-position ACK or resource
pin is needed for these latest-state routing hints.

## Client and LocalWAL

The domain `wal.ReplicaWatchStream` exposes `Subscribe`, `Done`, `Error` and
`Close`; `wal.ReplicaWatchSubscription` exposes `Snapshot` and `Close`. Physical handlers consume
owned snapshots, must not block and must not synchronously close their stream
or subscription. The upper client isolates application callbacks separately.

The contracts live in `wal/replica_watch.go`, beside TransformLog's contracts.
`qviews.ReplicaSnapshot` and `qviews.Replica` are shared value types. The directory
and direct local stream are private implementations inside `wal/snview`; clients
depend on the WAL contracts and qviews values, not the directory implementation.
There is no separate `internal/views/replicadiscovery` package.

`HandlerClient.AcquireReplicaWatchStream` resolves the assignment and creates
one physical attempt. It owns no sharing, reconnect loop or logical subscription
manager. It first looks up a local available WAL with the exact owner term. The
local WAL Provider returns the domain stream directly: there is no local RPC,
Send/Recv emulation, control-message queue, or wire acknowledgement. Only the
existing `shouldUseRemoteWAL` conditions select remote fallback; local internal
errors are not silently redirected to RPC.

The remote adapter owns the serialized Send path and an independent Recv loop.
Recv directly applies snapshots and lifecycle acknowledgements to physical
subscriptions. It never forwards responses to a goroutine that might be waiting
for Send. Failure cancels the RPC without acquiring the send mutex. Close joins
the receiver; the close handshake is bounded even under flow control.

`streaming.WAL().QueryViewClient().SubscribeReplicas(vchannel, handler)` uses the
upper streaming `replicadiscovery.NewResumingClient`. It shares one resumable
stream per PChannel and one physical subscription per desired VChannel. Multiple
logical handlers share the cached state. A PChannel has a recovery loop and a
callback dispatcher, plus its physical delivery loop while connected; there are
no goroutines per VChannel or handler registration.

Removing the last logical subscription cancels an in-progress acquisition.
An established physical stream remains reusable for one idle minute, following
TransformLog's idle lifecycle, then closes. Normal idle retirement does not
consume failure backoff. A failed attempt invalidates cached availability,
rediscovers the owner, backs off, and recreates desired subscriptions. Local to
remote and remote to local migration use this same loop. A replacement physical
attempt starts only after the old one closes. Old callbacks are checked against
the logical registration and attempt context before changing current state.

Each snapshot owns its replica slice and value-type versions. Invalid means
uninitialized/disconnected, distinct from an authoritative empty set. The upper
callback dispatcher coalesces pending notifications and invokes handlers outside
state and transport locks. Logical Subscription.Close is safe inside a callback;
an already selected callback may finish after Close. Application handlers must
not block on I/O or close the owning client. Closing the resuming client joins
its recovery loops and callbacks. WALAccesser owns it and closes it before
waiting for HandlerClient operations or closing HandlerClient.

Latest-state discovery shares TransformLog's layering and extensible protocol
shape, but has no cursor, replay or SyncUp semantics.

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

Key packages: `internal/views/qviews`,
`internal/streamingnode/server/wal`,
`internal/streamingnode/server/wal/snview`,
`internal/streamingnode/server/wal/adaptor`,
`internal/streamingnode/client/handler/replicadiscovery`,
`internal/distributed/streaming/internal/replicadiscovery`,
`internal/streamingnode/server/service/handler/replicadiscovery`.
