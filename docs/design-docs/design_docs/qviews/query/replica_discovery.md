# StreamingNode replica selection

## Ownership

Proxy resolves a collection to VChannels and sends GetQueryPlan with
`ShardID{ReplicaID: UnknownReplicaID, VChannel: ...}` to the current WAL owner.
That owner already manages the VChannel's QueryView replicas. It selects a
replica and the newest locally Up View matching the exact requested schema.
Statistics uses the newest Up View without a read-schema selector.
An explicit ReplicaID remains supported and never silently falls back.

The returned QueryPlan carries the actual ShardID, full QueryViewVersion, paired
MVCC and work nodes. Proxy uses these identities for Phase 2, reduction and
retained requery. Moving selection does not change schema, load/partition,
DataVersion or MVCC semantics. It does not implement gradual handoff or allow
arbitrary fallback to historical data/load generations.

## Local index and selection

SNQueryViewHandler keeps an index from VChannel to its shard handlers, alongside
the existing full ShardID map. Recovery and new shard creation register membership;
final removal unregisters it only if the callback still refers to the same shard
incarnation. Closing the handler clears both indexes. The index contains pointers,
not duplicated View metadata or segment topology.

UnknownReplicaID requests rotate the starting replica within this VChannel,
then inspect candidates before waiting. A recovering replica must not hide a
servable replica. When a matching UpRecovering entry requires waiting, state
notifications from every candidate are observed, including a preparing replica
that becomes Up later. Membership notifications also wake requests when another
replica joins or leaves. Waits use the caller context. Selection never scans unrelated collections
and never acquires a shard mutex while holding the handler mutex.

The rotation is shared across concurrent requests; it is not a per-request
promise to visit every replica before the client's retry budget expires.
Node/View exclusion and half-open probe constraints are follow-up work.

## Retry and load intent

Without a usable View, planning reports ViewNotFound. Proxy checks the current
Coordinator load configuration before concluding that the collection is unloaded.
A loaded collection continues planning retries under the caller deadline, with
bounded backoff and shared load checks. No discovery watch is needed to notice
preparation completion or a concurrent release.

Actual Phase 2 failures still cancel and join the whole failed shard attempt
before resetting its partial results. Explicit executor MVCC and BarrierMVCC
survive replanning. Retained requery does not acquire a different plan.

## Lease and retirement

This change removes discovery publication, not the serving-lease protocol.
Pending Down and explicit release control retain their existing behavior.
Already admitted planning and explicit-version execution keep their existing
references and renewal rules; this change adds no hard lifetime limit.
See [QueryView lease](../query_view_lease.md).

## Protocol removal and compatibility

SubscribeReplicas, its creation metadata, stream/subscription messages,
VChannelReplicas, domain watches, local/RPC adapters and resumable clients are
removed. There is no replacement candidate-list protocol. Coordinator replica
management and ViewFeedback remain independent and unchanged.

A new client can use the existing UnknownReplicaID planning path on an older SN.
An old client requiring SubscribeReplicas cannot use the removed RPC on a new
SN. Deploy clients that no longer subscribe before removing server support;
this change does not claim transparent compatibility in the reverse order.

## Key packages and verification

- `internal/views/queryclient`: owner-routed planning, retries and load checks.
- `internal/streamingnode/server/wal/snview`: replica index and exact-schema selection.
- `internal/streamingnode/server/wal/adaptor/query_plan.go`: actual plan identity and MVCC.
- `internal/distributed/streaming/query_view.go`: shared owner-routed client access.

Tests cover replica rotation, VChannel/schema isolation, explicit-replica
compatibility, recovery waits, membership changes, cancellation, shard-incarnation
cleanup, actual-plan dispatch, missing-node suppression, session updates and
load-info rechecks. Live failure/recovery and mixed-version validation must be
reported separately from unit/static validation.
