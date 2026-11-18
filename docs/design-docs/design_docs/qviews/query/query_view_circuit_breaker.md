# QueryView client circuit breaker

## Ownership and protocol

`queryclient.QueryViewCircuitBreaker` owns bounded cross-request observations.
It may be injected through `ViewQueryClientConfig.CircuitBreaker` to share state
across multiple clients. StreamingNode retains replica/View selection and stores
no client health state. A planning attempt carries a fresh immutable
`QueryRoutingConstraints`: excluded QN IDs and excluded `(replica_id, version)`
identities scoped to the request's vchannel. Versions include DataVersion.

This unpublished protocol replaces GetQueryPlanRequest.shard_id with string
vchannel at field 2. QueryPlan and execution RPCs retain actual ShardID.

SN checks the selected exact-schema Up View against the excluded View identities.
When node exclusions are present, a call reference pins the View while SN builds
the plan. After partition resolution and optimization, SN checks its actual QN
workers before inline statistics or timed lease renewal. A rejected plan releases
its reference and selection continues with another replica. It does
not fall back to arbitrary historical versions or omit failed workers. Filtering
all serving candidates yields ALL_CANDIDATES_EXCLUDED, distinct from absent or
not-yet-ready Views. Its gRPC status is Aborted: neither transparent gRPC retries
nor the WAL readiness loop may resend the same exclusions. Proxy owns the bounded
wait and rebuilds constraints on each attempt. The public Milvus error remains
retriable ServiceUnavailable. No schema/data/load/MVCC or serving-lease rule is relaxed.

## Failure classes

| Class | Evidence | Removal |
| --- | --- | --- |
| QN connection | Three consecutive connection failures within ten seconds | One probe per key per client; successful RPC restores connection eligibility |
| QN session gone | Previously observed node disappears from session discovery and the RPC picker cannot find it | Five-minute TTL; no probe |
| View unavailable | A selected QueryNode returns ViewNotFound or ViewInvalidated | One-minute TTL; no probe; a new version is independent |

Picker absence alone is a discovery/connection observation, not authoritative
session deletion. The existing session observer retains at most 4096 recent
deletion records; it does not create a watch or perform etcd IO per query.

Input errors, caller deadlines/cancellation, derived fanout cancellation, local
client shutdown and unclassified/shared failures do not trip a local circuit.
Node failure does not also poison the View. View errors prove node reachability
but do not make the failed View healthy. Successful planning alone is not worker
success. Raw RPC observation precedes reduction and includes filter-first and
retained execution; retained execution never replans.

Phase 1 errors never create Proxy Node/View exclusions and carry no failed View
identity. SN owns selection and local resource lifetime; ordinary handoff cannot
release a View while planning holds its call reference. Planning failures keep
their existing retry/load-intent handling. Only Phase 2 QueryNode responses create
View exclusions, using the actual execution request's full identity; this also
applies to filter-first and retained execution. Inline growing statistics remains
part of Phase 1 and does not contribute to View exclusions. All StreamingNode
RPC responses bypass breaker observation, including Phase 2 and retained calls:
WAL owner handoff can recover the same View identity, so its transient errors
must not create a View tombstone. SN success also cannot clear QN failure evidence.

## Recovery, concurrency and bounds

Connection cooldown begins at one second and doubles to a 30-second base, with
20% jitter. Planning only snapshots exclusions; a cooled node with no probe
owner may be selected by SN, without reserving it during GetQueryPlan. After
the plan arrives, Proxy rechecks the selected View and QNs against current shared
state and atomically reserves probes only for selected cooled QNs. If any selected
node is blocked or already has a probe owner, the whole plan is rejected without
partial reservations and replanned within the admission wait budget. Healthy QNs
need no probe, and SN workers are not probed.

Remaining permits are released after execution joins. Cancellation/unused permits
do not grow failure backoff; release wakes admission waiters. An attempt retains
the change channel from its last exclusion check so a release between that check
and entering the wait cannot be missed.
Entry identity and generation fence late results. Recovered node entries retain
a bounded generation fence until their idle TTL; View and gone-session tombstones
cannot be cleared by a late success.

The client allocates entries only after failures. Defaults bound the Node table
to 1024 entries and the View table to 16384. Views are indexed by VChannel and
maintained in expiration order; constraint construction never scans unrelated
View entries. Reads do not refresh expiration. Cleanup is opportunistic during
activity and memory remains bounded while idle; there is no per-entry goroutine.
Capacity pressure evicts the oldest View or declines a new Node entry, with a
metric. Request-local exclusions preserve immediate retry protection independently
of the shared cache. TTL/capacity eviction forgets evidence; it is not proof of
health, and a subsequent failure may reinsert an identity.

All-excluded admission waits poll at most once per second to notice newly
published Views, and wake early on breaker changes. A five-second per-call total
wait budget and the caller context bound these waits; they do not consume the
normal execution-attempt budget. No load-info request is issued for this error.

Each retry constructs fresh constraints by merging unexpired request-local
failures with shared Node exclusions (connection cooldown, active probe, or gone
session) and shared exact View exclusions for this vchannel. ALL_CANDIDATES_EXCLUDED
does not itself add failure evidence. Cooldown/TTL expiry or another request's
probe result may change the next snapshot; otherwise retries remain bounded and
return ServiceUnavailable when the budget expires. No discovery topology is
needed to build this snapshot.

Configuration is immutable on the breaker. The metric
`milvus_proxy_query_view_circuit_breaker_events_total{scope,reason}` uses only
fixed labels for transitions, expiration and capacity pressure.

## Key packages and validation

- `internal/views/queryclient/circuit_breaker*.go`: state, snapshots and RPC observations.
- `internal/querynodev2/client/handler/session_changes.go`: confirmed session removal.
- `internal/streamingnode/server/wal/snview`: pre-lease candidate filtering.
- `internal/streamingnode/server/wal/adaptor/query_plan.go`: selected plan identity and inline boundary.
- `internal/views/viewerror`: all-candidates-excluded planning status.

Tests cover TTL and exact identity, cross-shard node sharing, exclusive probes,
late callbacks, non-attributed planning failures, bounds, retained execution, wire status,
and SN exclusion before lease/inline execution. Unit, integration, race, coverage
and live failure/recovery runs must be reported separately; source tests alone do
not establish runtime correctness or performance.
