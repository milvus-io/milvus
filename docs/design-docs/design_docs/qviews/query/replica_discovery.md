# StreamingNode replica selection

## Ownership

Proxy resolves a collection to VChannels and sends GetQueryPlan with
`vchannel` and request-local routing constraints to the current WAL owner.
That owner already manages the VChannel's QueryView replicas. It selects a
replica and an eligible locally Up View matching the exact requested schema.
Statistics chooses the latest schema. The per-replica handoff policy then selects
within the highest already-Up LoadInfoVersion.
The planning RPC no longer accepts an explicit ReplicaID. Internal explicit-View
execution continues to use its actual ShardID.

The returned QueryPlan carries the actual ShardID, full QueryViewVersion, paired
MVCC and work nodes. Proxy uses these identities for Phase 2, reduction and
retained requery. Moving selection does not change schema, load/partition,
DataVersion or MVCC semantics. [Handoff planning](query_client.md#handoff-planning-policy)
may sample an active source/target pair; it never permits arbitrary fallback to
historical data/load generations.

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
Node/View exclusions are applied before acquiring a serving lease. Each replica
contributes one plan from its newest eligible target or its active handoff source;
exclusions filter both endpoints before sampling and never authorize a fallback
to an older data/load generation. The bypass_handoff retry hint selects only the
newest eligible target, still subject to all exclusions. A candidate touching
an excluded QN is skipped as a whole, never returned with missing workers.
See [client circuit breaker](query_view_circuit_breaker.md).

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

This feature is unpublished. GetQueryPlanRequest field 2 changes directly from
ShardID to string vchannel; all callers must use the updated protocol. No old
client/server compatibility path is provided.

## Key packages and verification

- `internal/views/queryclient`: owner-routed planning, retries and load checks.
- `internal/streamingnode/server/wal/snview`: replica index and exact-schema selection.
- `internal/streamingnode/server/wal/adaptor/query_plan.go`: actual plan identity and MVCC.
- `internal/distributed/streaming/query_view.go`: shared owner-routed client access.

Tests cover replica rotation, VChannel/schema isolation, explicit-replica
compatibility, recovery waits, membership changes, cancellation, shard-incarnation
cleanup, actual-plan dispatch, missing-node suppression, session updates and
load-info rechecks. Live failure/recovery validation must be
reported separately from unit/static validation.
