# Conditional Upsert by Business Version

- **Feature DRI:** @weiliu1031
- **Primary Approver:** @liliu-z
- **Independent Approver:** @congqixia
- **Design Review:** Pending meeting with the DRI and both approvers
- **Status:** Draft; local implementation available, design review and runtime validation pending
- **Issue:** [54532](https://github.com/milvus-io/milvus/issues/54532)

## Summary

Add an optional `write_condition` to Upsert. Proxy compares an incoming business
version against the stored version and writes only qualifying rows. A stale
event is a successful no-op, rather than a request-level version conflict.
Existing partial-update Strong reads, merge and CAS provide the execution path.
WAL contains ordinary Delete/Insert transactions; internal CAS metadata and
StreamingNode protocols do not change.

The implementation covers milvus-proto, Milvus Proxy, RESTful v2, and synchronous
and asynchronous PyMilvus MilvusClient. Other SDKs and PyMilvus ORM are outside
this delivery. Transaction internals are unchanged and reviewed separately.

This delivery also changes ordinary partial-update retry and result aggregation,
even when no write condition is present.
The shared retry changes and conditional filtering are reviewed together in this
iteration; there is no separate prerequisite retry PR.

## Motivation and guarantees

An event with business version 12 can arrive before version 11. Unconditional
upsert would overwrite newer state with older state. Client-side read-then-write
has a race, and Milvus arrival timestamps do not describe business event order.
Comparison and commit admission must therefore cooperate on the server.

With CAS enabled, a written row's comparison and merge are protected against
concurrent writes to that PK between the Strong read and commit. Skipped rows
are evaluated at their channel's read snapshot and are never written. Different
vchannels can commit independently; neither request-level atomicity nor a common
cross-channel snapshot is promised.

Operational exception: `streaming.partialUpdate.enableCAS=false` retains the
existing unchecked append behavior. Conditional upsert adds no extra restriction
or forced-CAS protocol. Proxy still compares versions, but concurrent writes may
invalidate that comparison and overwrite newer data. The option defaults to true
and requires StreamingNode restart. Operators must understand that disabling it
also disables conditional-write concurrency protection.

## Scope and alternatives

The condition is a request-level parameter because it selects whole rows; a field
operation transforms one column and cannot suppress the rest of a stale row.
A separate RPC would duplicate the existing partial-update read/merge/CAS path.
Proxy evaluates the comparison because a query-side version filter would make a
stale existing PK look missing, and each incoming row can carry a different value.

Stale rows are skipped instead of poisoning a mixed event batch. Existing IDs and
request indexes are sufficient to identify successful rows, so this version does
not add a per-row outcome enum. With skip policy, the response intentionally does
not distinguish a missing PK from an obsolete version. A subsequent query shows
current state, not a reliable reconstruction of the earlier skip reason.

Non-goals are arbitrary expressions, floating/string/multi-field versions, full
row replacement semantics, deleted-version tombstones, cross-RPC deduplication,
constraints on unconditional writers, cross-channel atomicity, and changes to
transaction internals or import/restore/backfill fencing. Relative operations
remain non-retriable after CAS conflict in this delivery.

## Public contract

```python
result = client.upsert(
    collection_name="products",
    data=[{"id": 42, "version": 12, "status": "completed"}],
    write_condition={"field": "version", "op": "gt", "on_missing": "insert"},
)
# {"upsert_count": 1, "ids": [42], "succ_index": [0], ...}
```

`field` names an ordinary Int8/Int16/Int32/Int64 scalar column, present in the
request. PK, partition key, clustering key, dynamic fields and struct subfields
are rejected. `op` is explicitly `gt` or `gte`; it has no default.
`on_missing` is `insert` (default), `skip`, or `error`. Text enum values in SDK
and RESTful are case-insensitive; unknown numeric proto enums are rejected.

Presence of the condition implies `partial_update=true`, even if false was
provided. Omitted columns keep their old values. Full-row replacement is not
introduced. Each batch must have unique PKs.

| Stored state | Policy/comparison | Outcome |
| --- | --- | --- |
| Missing | insert | Insert using existing required-field/default rules |
| Missing | skip | Successful no-op |
| Missing | error | Fail preparation, before this attempt appends anything |
| Version NULL | Any supported incoming version | Update |
| Present | gt: incoming > stored | Update |
| Present | gte: incoming >= stored | Update |
| Present | Otherwise | Successful no-op |

The compared incoming value is the value that will be written, after existing
default filling. An incoming NULL with a default uses that default. Without a
default it is invalid, even if the row would otherwise be skipped. An absent
version column is invalid. Compare integers directly, without subtraction.

The version field allows only REPLACE. Other fields may use existing operations
with gt. GTE rejects every non-REPLACE operation, including PATH_REPLACE, because
an equal-version replay could apply an operation twice. Missing rows skipped by
policy do not enter merge, AutoID allocation, or PATH_REPLACE existence checks.
Missing rows selected for insert still obey PATH_REPLACE's all-existing rule.

### Results

On success, `ids` contains only written destination PKs. `succ_index` contains
their original request indexes, in increasing order and aligned with `ids`.
`upsert_count == len(ids) == len(succ_index)`. For example, writing request rows
0 and 2 returns `succ_index=[0,2]`; skipped rows are absent. All-skipped requests
return success with empty arrays and zero count, without WAL append.

Counts include each successful channel exactly once across all attempts.
`timestamp` is the maximum of successful commit timestamps and channel read
timestamps observed across attempts. Insert/delete counters describe actual mutations.
Failure returns an error, not a successful partial result; it can follow partial
commit. Error text must state this possibility once submission has begun.

Unconditional upsert keeps its existing response shape. RESTful v2 adds
`succIndex` only for conditional requests; PyMilvus MilvusClient exposes
`succ_index` only for these requests. RESTful v1 and ORM do not support this API.

### AutoID and scope

Missing AutoID rows receive fresh destination PKs. Skipped rows allocate no IDs.
Within a single RPC, allocated IDs remain stable across CAS rebuilds. A fresh
client RPC using the old lookup PK can allocate another ID and insert again;
there is no cross-RPC deduplication. An unknown result must not be automatically
replayed. When the first response was lost, the client may not know the assigned
PK, so querying the previous returned ID is not a general recovery mechanism.

Without a partition key, lookup/delete scope remains the requested partition
(default if omitted). Same-PK rows in other partitions are not protected. With
partition keys, existing all-partitions lookup/delete rules apply. Namespace
routing remains unchanged.

## Protocol

```protobuf
message WriteCondition {
  enum Op { OP_UNSPECIFIED = 0; GT = 1; GTE = 2; }
  enum MissingRowPolicy {
    INSERT_IF_MISSING = 0; // Default when omitted.
    SKIP_IF_MISSING = 1;
    ERROR_IF_MISSING = 2;
  }
  string field = 1;
  Op op = 2;
  MissingRowPolicy on_missing = 3;
}
// Add to UpsertRequest, after verifying the current upstream allocation:
// WriteCondition write_condition = 14;
```

MutationResult reuses `IDs`, `succ_index` and `upsert_cnt`. Generated bindings
must be regenerated using protoc. No new persistent format or internal message
field is needed. RESTful uses `writeCondition: {field, op, onMissing}`.

## Execution design

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Proxy
    participant A as Channel assignments
    participant Q as QueryNode
    participant S as StreamingNode
    C->>P: Upsert with write_condition
    P->>P: Validate and retain original row identities
    P->>A: Resolve candidate channel terms
    P->>Q: Strong PK read, including version column
    Q-->>P: Rows and actual readTS per channel
    P->>P: Fill defaults, compare, filter, merge
    P->>S: Selected Delete/Insert transactions with existing CAS proof
    S-->>P: Per-channel success / CAS rejection / unknown failure
    P->>P: Freeze successes; rebuild only rejected rows within budget
    P-->>C: Written IDs and original indexes, or terminal error
```

### Preparation and filtering

When `write_condition` is present, validate condition parameters next to field-op
resolution before retrieval and select conditional execution. Capture original
fields and row identities before merge. Resolve channel terms before the Strong
query and bind each channel's actual readTS using the existing CAS path.

After request default normalization and PK classification, compare in Proxy.
Do not filter the old-row query by version: that would conflate stale rows with
missing rows and could incorrectly insert them. Preserve existing upsert read
scope and authorization; never use the condition to widen row visibility.

Remove skipped rows from both insert/update selections and delete PKs before
merge and allocation. Recompute CAS groups from selected destination PKs using
only proofs captured before this attempt's read. Empty selections bypass CAS
packing and append. Missing query output columns must not be treated as missing
rows unless the query result is genuinely empty; malformed results fail closed.

### Channel retry and result ownership

Implement per-channel retry for ordinary partial update and conditional filtering
in the same delivery. Review both the ordinary-request behavior changes below
and the conditional-specific filtering and error projections.
Each original request row has stable identity and one state: pending, written,
or skipped. Written/skipped are terminal. Keep original row index, destination
PK, and routing identity separate from temporary merged row offsets.

| Channel outcome | Next action |
| --- | --- |
| Success | Freeze written rows, counts and commit timestamp |
| Deterministic CAS rejection | Rebuild only its pending rows |
| Non-CAS failure or unknown result | Record successes, stop; return execution error |
| Retry rebuild selects zero rows | Freeze skipped rows; complete without append |

Rebuild from the original partial payload, retaining allocated destination IDs;
run a new Strong read, compare again, merge and construct fresh messages.
Never replay successful channels. For AutoID, a previously existing row becoming
missing can acquire a new destination/channel; the new attempt uses proofs
captured for all candidate AutoID destinations, as today. Freeze successes by
row identity, rather than assuming the original lookup PK fixes routing forever.

At most five append attempts per pending row, including the first. Backoff starts
at 10 ms and caps at 40 ms; cancellation stops waiting. Any non-REPLACE field op
keeps the existing no-CAS-retry rule. Conditional results from different rounds are sorted by original request index.
Ordinary non-AutoID partial requests retain the first prepared attempt's
update-before-insert order; AutoID partial results use original request order.
The first-attempt ordering can differ from the old final-attempt ordering after
mixed channel success and CAS rejection, as detailed below.

`on_missing=error` has two distinct boundaries: a missing row in initial
preparation guarantees no submission; a missing row during rebuild may follow
another channel's commit and must not claim whole-request rollback.

### Ordinary partial-update compatibility

These shared changes apply to every partial update, including requests without
`write_condition` and requests that implicitly select partial update through
field operations. Conditional filtering applies only when `write_condition` is
present. Full upsert remains on its existing execution path.

| Behavior | Previous implementation | This delivery |
| --- | --- | --- |
| Mixed successful and CAS-rejected channels | Rebuild the entire request, including successful channels | Freeze successful rows and rebuild only CAS-rejected rows |
| Non-AutoID result IDs | Update-before-insert order of the final attempt | Update-before-insert order of the first prepared attempt, including frozen successes |
| AutoID result IDs | Original request order | Original request order, with destination IDs retained across attempts |
| InsertCnt / DeleteCnt | Counts from the final prepared attempt | Accumulated mutations from successful channels, each counted once; DeleteCnt counts successful rows with a corresponding delete PK |
| Timestamp | Maximum commit timestamp of the last successful append attempt | Maximum observed channel read timestamp and successful commit timestamp across attempts |
| Cancellation while waiting to retry | The retry helper can return its last CAS error, projected to ServiceUnavailable 2 | Return Canceled 10000 or Timeout 10001 for ordinary requests; conditional requests return 112 after submission |
| CAS rejection with a non-REPLACE operation | Return non-retriable PartialUpdateConflict 111 without rebuilding | Same no-CAS-retry rule and code 111, using the shared accounting path |
| New append-result accounting failure | No equivalent shared accounting step | Ordinary requests return non-retriable ServiceInternal 5; conditional requests project that error to 112 at the execution boundary |

For example, take a non-AutoID request ordered `[A, B]`, where A is initially
missing and B exists on another channel. The first merge orders updates before
inserts, producing `[B, A]`. If A commits and B rejects CAS, the previous whole
request retry reads A as existing too, and may return `[A, B]` with DeleteCnt 2.
The new retry freezes A's insertion and retries only B, returning `[B, A]` with
DeleteCnt 1. These intentional result changes follow directly from freezing A's successful
insertion instead of replaying it.

Ordinary response fields and single-attempt ID ordering remain compatible.
Ordinary CAS-budget exhaustion continues to use ServiceUnavailable 2. Existing
WAL error mappings remain compatible, including ordinary append schema mismatch
109. The new append-result bookkeeping deliberately classifies its own failures
as non-retriable SystemError ServiceInternal 5, including dependency failures that
would normally be transient. The append was already attempted, so those errors
cannot authorize replay of the write. Their original causes remain available for
diagnosis. Ordinary requests do not acquire conditional execution code 112 from
this shared step. Both ordinary and conditional paths require review and
regression coverage in this combined delivery.

The execution layer marks `appendAttempted` immediately before calling WAL, not
inside result accounting. This flag persists across attempts and records a call,
not a successful commit. It does not affect success counts. The accounting layer
returns generic errors; the outer partial-update execution layer projects
non-CAS failures to 112 only for conditional requests after an append attempt.
Initial preparation or packing errors retain their existing classifications.

## Error and retry decision table

| Origin | Public result | Proxy retry | PyMilvus replay |
| --- | --- | --- | --- |
| Invalid condition, duplicate PK, invalid incoming NULL | ParameterInvalid 1100 | No | No |
| Initial missing row with error policy | ParameterInvalid 1100, names missing PK | No | No |
| Pre-submission schema mismatch | Existing SchemaMismatch 109 | No | One schema refresh allowed |
| Request admission rate limit | Existing RateLimit 8 | No | Existing bounded rate retry |
| Condition false / missing with skip | Success, omit row | N/A | N/A |
| Deterministic CAS reject, REPLACE | Internal CAS retry signal | Only rejected rows | Not exposed during budget |
| CAS reject, relative operation | PartialUpdateConflict 111, non-retriable | No | No |
| Ordinary partial CAS budget exhausted | Existing ServiceUnavailable 2 | Budget exhausted | Existing SDK handling |
| Ordinary partial cancellation during retry wait | Canceled 10000 / Timeout 10001 | No | Existing SDK handling |
| Append-result accounting failure | ServiceInternal 5 for ordinary requests; ConditionalUpsertExecution 112 for conditional requests | No | No |
| Conditional CAS budget exhausted | PartialUpdateConflict 111, non-retriable | No | No |
| Conditional append schema mismatch / non-CAS error | ConditionalUpsertExecution 112, non-retriable SystemError | No | No |
| Conditional rebuild error after earlier submission | ConditionalUpsertExecution 112, non-retriable SystemError | No | No |
| Conditional write RPC transport error | PyMilvus local ConditionalUpsertExecution 112, outcome unknown | No extra replay | No automatic replay |

Code 112 is a new collection-family execution error, not a business-version
conflict (version failures are successful skips). It deliberately hides inner
retry classifications on the wire while retaining diagnostic causes. Never
recover CAS retryability from causes below a public execution error. Do not mark
system failures as InputError to suppress retries. Ordinary partial-update
compatibility changes are listed above; code 112 is reserved for conditional
requests and must not be introduced solely by shared bookkeeping.

PyMilvus must distinguish the write RPC from preparation: metadata reads retain
their existing retries; once the conditional write RPC is sent, transport errors
are terminal. Explicit server admission codes 109/8 still allow safe refresh or
rate retry. Append-phase schema mismatch and throttling must be projected to 112
before returning to SDK, so admission retry paths cannot replay partial commits.
Terminal codes 5/111/112 must be checked before connection recovery callbacks:
those callbacks currently recognize replication redirects by message text, which
can also appear inside a projected execution error. Diagnostic text must never
authorize whole-request replay. Both synchronous and asynchronous connection
recovery handlers reject these classifications even if called directly. Normal
replication redirects and pre-write schema/rate retries retain their existing
paths. Code 5 is an existing server code; this adds no new protocol error number.
No guarantee is made about other SDKs until their implementations are audited.

## Compatibility and operations

Upgrade all serving Proxies before sending conditional writes. Old Proxy versions
silently ignore unknown proto/JSON fields; a version check is an advisory check,
not mixed-version negotiation. During rolling upgrade, withhold conditional
client traffic until no old Proxy accepts traffic. Before downgrade, stop
conditional client traffic and keep it stopped while any old Proxy serves
requests. Conditional writes require no additional configuration on supported
Proxies.

No StreamingNode changes, forced-CAS marker or CDC format changes are introduced.
CDC replays committed standard transactions. Local conditional writes use the
existing primary-only CAS rules; replication does not reevaluate the condition.
CAS disabled behavior is exactly the operational exception stated above.

Ordinary writes can lower business versions. Deletes/TTL discard version history
and late events can recreate rows; use skip policy or logical deletion where
needed. Concurrent import, restore and backfill remain outside CAS guarantees.

## Observability and validation

Count Proxy-confirmed row-level outcomes once per RPC using
`milvus_proxy_upsert_write_condition_rows_total{node_id,db_name,collection_name,outcome}`:
`written`, `condition_not_met`, `missing_skipped`. This is not a count of rows in
successful RPC responses: confirmed outcomes remain counted when later
preparation or another channel fails. Skipped rows are terminal at their read
snapshot and leave the pending set. Accumulate skip counts locally during one
selection and publish them only when that selection completes successfully; an
incomplete selection publishes no provisional skip counts. A later failure does
not revoke an earlier completed selection's decisions.

Count `written` when Proxy confirms and accounts for a successful channel commit,
without waiting for PostExecute. Failed CAS attempts and unknown commit outcomes
are excluded. The sum need not equal the input row count for a failed RPC.
Internal attempts do not count the same terminal row again; a separate RPC counts
its own decisions, without cross-RPC deduplication. Metric names and labels remain
unchanged. Add feature usage keys `upsert_write_condition=gt|gte` and
`upsert_on_missing=insert|skip|error` to the existing mechanism.

Required tests cover every validation rule; signed integer boundaries; old NULL;
new NULL/defaults; all-missing/all-skipped and mixed batches; duplicate PKs;
relative operations and PATH_REPLACE; AutoID allocation and retained IDs;
namespace/partition routing; multiple channels and retry rounds; mixed
success/CAS/unknown outcomes; missing rows during rebuild; timestamp/result
aggregation; append schema mismatch versus admission mismatch; sync/async SDK
transport/rate/schema retries; REST response compatibility; and CAS-disabled
pass-through behavior. Trace error origin through wire projection and SDK.

Measure p99 against unconditional partial update across batch sizes and conflict
rates; target <=5% added latency without claiming this before measurement.
Native-library or service prerequisites that prevent validation are blockers,
not passing checks. Design approval and runtime validation remain pending.

### Implementation validation snapshot

After rebasing onto master and pinning the published proto, local validation
passed the complete Proxy DML, RESTful HTTP, feature-usage, merr and metrics
packages, plus 1,953 related PyMilvus tests (one skipped), including connection
recovery and async timeout/cancellation. These are unit/source checks, not
deployed runtime or performance evidence.

Full `make test-go` previously stopped in an unchanged macOS C++ test using
unavailable `std::jthread` and `std::stop_token`; this is not waived as a passing
result. Linux build and incremental Go tests are being run separately.
Multi-channel runtime fault injection, mixed-version rollout and p99
measurements remain pending.

The 3.0 integration uses the public fork revision from the 3.0 proto PR
through a versioned Go module replacement, with no local filesystem paths.
After that PR merges, pin its upstream revision and remove the fork replacement
before merging the Milvus backport. PyMilvus's master bindings retain compatible
conditional field/enum numbers. No 3.0 release floor is asserted before release.

### 3.0 backport

The backport retains the existing 3.0 nullable-field validity representation and
metric cleanup helpers. Conditional outcome metrics are registered and cleaned
on collection/database deletion. The master feature-usage subsystem is absent
from 3.0, so its conditional-upsert usage keys are not backported. The public
protocol and row-selection/retry/error contracts are otherwise unchanged.
