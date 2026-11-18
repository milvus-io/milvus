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
| QN connection | Known QN has no ready SubConn in this client’s picker | The next ready picker makes the node eligible immediately |
| QN session gone | Previously observed node disappears from session discovery and the RPC picker cannot find it | Five-minute TTL; no probe |
| View unavailable | A selected QueryNode returns ViewNotFound or ViewInvalidated | One-minute TTL; no probe; a new version is independent |

Picker absence alone is a discovery/connection observation, not authoritative
session deletion. The existing session observer retains at most 4096 recent
deletion records; it does not create a watch or perform etcd IO per query.

Input errors, caller deadlines/cancellation, derived fanout cancellation, local
client shutdown and unclassified/shared failures do not trip a local circuit.
Node failure does not also poison the View. View errors exclude the exact View identity. Successful planning alone is not worker
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
must not create a View tombstone.

## Recovery, concurrency and bounds

The QN balancer publishes an immutable list of known unavailable node IDs with
a revision channel after installing its picker. Membership or readiness changes
wake waiters, including reconnects at an unchanged address. Each Proxy uses the
snapshot for its own gRPC connection. Generic RPC failures such as Unavailable
and EOF leave connectivity decisions to the picker.

A missing target in the picker is avoided by the affected shard request until the
captured picker revision changes. The separate session discovery watch supplies
positive removal evidence for a shared gone-session exclusion. The View and gone
session tables hold at most 16384 and 1024 records respectively. Capacity pressure
evicts the oldest View or declines a new session record. Request-local exclusions
continue to protect the request in either case. Reads do not renew TTLs. Late
RPC observations beyond their exclusion lifetime are discarded.

All-excluded admission waits poll at most once per second to notice newly
published Views, and wake early on picker or breaker changes. A five-second per-call total
wait budget and the caller context bound these waits; they do not consume the
normal execution-attempt budget. No load-info request is issued for this error.

Each retry merges the picker's unavailable nodes, unexpired session and exact
View exclusions, and the request's local observations. Proxy rechecks a returned
plan if the picker or exclusions changed during planning. All-excluded responses
do not add failure evidence. When the routing wait budget expires the request
returns ServiceUnavailable.

Configuration is immutable on the breaker. The metric
`milvus_proxy_query_view_circuit_breaker_events_total{scope,reason}` uses only
fixed labels for transitions, expiration and capacity pressure.

## Key packages and validation

- `internal/views/queryclient/circuit_breaker*.go`: state, snapshots and RPC observations.
- `internal/querynodev2/client/handler/session_changes.go`: confirmed session removal.
- `internal/streamingnode/server/wal/snview`: exact View selection.
- `internal/streamingnode/server/wal/adaptor/query_plan.go`: selected plan identity and inline boundary.
- `internal/views/viewerror`: all-candidates-excluded planning status.

Tests cover TTL and exact identity, picker-driven connectivity,
late callbacks, non-attributed planning failures, bounds, retained execution, wire status,
and SN exclusion before lease/inline execution. Unit, integration, race, coverage
and live failure/recovery runs must be reported separately; source tests alone do
not establish runtime correctness or performance.
