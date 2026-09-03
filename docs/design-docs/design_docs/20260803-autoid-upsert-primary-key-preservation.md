# MEP: Preserve Primary Keys in Full AutoID Upsert

- **Created:** 2026-08-03
- **Last Updated:** 2026-09-03
- **Author(s):** @weiliu1031
- **Feature DRI:** @weiliu1031
- **Primary Approver:** @xiaofan-luan
- **Independent Approver:** @congqixia
- **Design Review:** 2026-09-03

This MEP requires named Primary and Independent Approvers, a completed Design
Review, and both Approver stamps on the Design Doc PR before merge.

## 1. Overview

This MEP changes only non-partial, or Full, Upsert into a collection whose
primary key has `autoID=true`. Throughout this document, Full and Partial refer
to the effective mode after existing `field_ops` validation and normalization,
not only to the wire-level `partial_update` Boolean.

Milvus currently replaces the business primary key for every row in Full AutoID
Upsert. For an existing request primary key `P`, Proxy emits:

```text
Delete(P)
Insert(G, request fields)
```

`G` is allocator-generated. Because `P` and `G` can hash to different
VChannels, the two messages may commit in different WAL transactions. A legal
Snapshot cut can then contain `Delete(P)` without `Insert(G)`, and restoring
that Snapshot loses the logical entity.

The new Full Upsert path first retrieves the request PK at one fixed snapshot.
It preserves `P` when the row exists and applies a Proxy Boolean configuration
when the row is not found:

```text
proxy.autoIDUpsertInsertOnNotFound = true | false
default = true
```

| Full AutoID Upsert row | Configuration value | Result |
|---|---|---|
| PK omitted | Any | Reject; use Insert to create an entity without a lookup key |
| PK exists | Any | `Delete(P) + Insert(P)` and return `P` |
| PK is not found | `true` | Generate allocator-owned `G`, emit only `Insert(G)`, and return `G` |
| PK is not found | `false` | Reject the entire request before RowID allocation or WAL append |

Partial AutoID Upsert is unchanged. It continues to use its existing
read/merge/CAS path and requires every request PK to exist, regardless of this
configuration. Proxy reads the configuration only after normalization selects
effective Full AutoID Upsert.

The required invariants are:

1. For an existing Full Upsert row, request PK, Delete PK, Insert PK, and
   returned PK are identical.
2. A missing request PK is a lookup key only. It is never copied directly into
   the business PK. The final PK is allocator-derived, although its numeric or
   string value may coincidentally equal the lookup key when the allocator
   independently assigns that value.
3. A generated `G` comes from the same global allocator used by ordinary
   Insert, so concurrent and future Inserts cannot generate the same allocated
   ID.
4. Existence and TTL visibility are evaluated at one explicit classification
   timestamp across every queried VChannel.
5. Returned IDs remain aligned with Full Upsert request order.
6. Partial Upsert semantics, CAS messages, read timestamps, and retries are
   unchanged.

Non-AutoID Upsert, Partial Upsert, ordinary Insert, Import, WAL formats, and
stored row formats are unchanged.

### Goals

- Add one PK-only existence Retrieve before Full AutoID Upsert writes.
- Preserve the identity of every existing AutoID entity updated by Full Upsert.
- Keep an existing entity's Delete and Insert in one VChannel transaction.
- Support allocator-generated insert-on-not-found and snapshot-checked
  update-only behavior for Full Upsert.
- Support Int64 and VarChar AutoID, partition-key mode, and namespace modes.
- Fail before RowID allocation and WAL append when the standard Query used for
  classification returns an error or deterministic validation fails.
- Define the client, load-state, upgrade, and rollback boundaries introduced by
  the Full Upsert behavior change.

### Non-goals

- Changing Partial Upsert query, merge, CAS, retry, or not-found behavior.
- Treating PK-omitted AutoID Upsert as Insert.
- Retry-idempotent generated insert-on-not-found.
- A durable request-PK-to-generated-PK mapping or a new idempotency key.
- Caller-selected IDs in the AutoID allocator domain.
- Automatic collection or partition loading.
- Request-level atomicity across VChannels or distributed DML transactions.
- Commit-time compare-and-swap, row locks, or a guarantee that a target cannot
  change after Full Upsert classification.
- A linearizable, cluster-wide switch between configuration values.
- A per-collection or per-request insert-on-not-found option.
- Zero-downtime rolling upgrade between incompatible Full Upsert semantics.
- A new public Upsert protobuf, Upsert WAL message type, DML header, or
  persisted row/segment format.
- Repairing identities changed by historical Upserts or existing Snapshots.

## 2. Identity Model and Classification

Full AutoID Upsert must distinguish an update from creation before constructing
its business primary key:

- Existing `P` is an update. `Delete(P) + Insert(P)` uses one routing key and
  one VChannel transaction.
- A NotFound result does not establish allocator ownership of the caller-supplied
  `P`, so Proxy must never copy it directly as a business PK. Depending on the
  Proxy configuration, Full Upsert either uses an independently
  allocator-derived `G` or rejects the complete request.

When generation is enabled, Milvus derives `G` from the existing allocation for
the new row version:

```text
InternalRowID = common.AllocAutoID(globalAllocator)
G             = EncodeAutoID(InternalRowID)
```

Ordinary Insert uses the same global allocator, whose ranges do not overlap
across Proxies. An allocated ID is not returned to a free pool, even when the
write later fails. The caller never selects `G`; Milvus allocates and returns it
in `MutationResult.IDs`. Allocator ownership, rather than a `G != P`
comparison, is the safety invariant. If the allocator independently returns a
value equal to `P`, that value is safe because this allocation reserves it from
ordinary and future allocator users.

The lookup key remains `P` and is not an idempotency key. After an ambiguous
outcome, a retry may allocate another identity while `P` is still NotFound, or
update `P` if it has become visible; it cannot recover the first generated ID.
Applications that require update-only behavior set
`proxy.autoIDUpsertInsertOnNotFound=false`.

Partial AutoID Upsert already performs a Retrieve and uses an attempt-scoped
read timestamp plus Streaming CAS. It remains update-only: a missing request PK
is rejected. This MEP does not reuse, modify, or extend that CAS protocol for
Full Upsert.

## 3. Proposed Design

### 3.1 Full Upsert Configuration and Request Boundary

The system configuration is:

```text
proxy.autoIDUpsertInsertOnNotFound = true | false
default = true
```

Proxy determines the effective Upsert mode before reading this Full-only
configuration. It first validates every `field_ops` directive using the
existing rules and then normalizes the request:

```text
nonReplaceSeen = validateFieldPartialUpdateOps(request, schema)
effectivePartialUpdate = request.partial_update || nonReplaceSeen
```

An invalid field operation fails through the existing validation path. A valid
non-`REPLACE` operation selects the existing Partial Upsert path even when the
wire request says `partial_update=false`. The implementation must not read or
apply this configuration before that normalization.

- It applies only when `autoID=true` and `effectivePartialUpdate=false`.
- Its default is `true`, preserving the historical Full Upsert
  create-on-not-found outcome.
- It is defined by `ProxyCfg.AutoIDUpsertInsertOnNotFound` in
  `pkg/util/paramtable/component_param.go` and uses the existing refreshable
  configuration mechanism.
- It is independent of the collection property `allow_insert_auto_id`. That
  property remains part of ordinary Insert and Import handling; Full Upsert
  always requires a lookup PK and never reads that property to select its
  NotFound behavior.
- Effective Partial Upsert ignores the configuration and retains its existing
  update-only contract, including when a non-`REPLACE` field operation promoted
  the request from `partial_update=false`.

After mode normalization selects Full AutoID Upsert, Proxy reads the value once
and stores it on the task. A refresh affects only later requests. This is a
Proxy configuration: it adds no collection metadata, RootCoord path, policy
revision, WAL marker, or readiness protocol.

Once Full mode is selected and before classification, Proxy validates and
retains all Full Upsert fields and request PKs. It rejects omitted, null,
wrong-type, row-misaligned, or duplicate PKs and malformed field data. PK
omission is malformed Upsert input; it is not the same as a supplied PK that is
not visible at the classification snapshot.

### 3.2 One PK-Only Retrieve for Full Upsert

Each Full AutoID Upsert attempt selects one classification timestamp:

```text
classificationTs           = Upsert.BeginTs()
capturedCollectionUpdateTs = collectionInfo.updateTimestamp
queryCollectionUpdateTs    = collectionInfo.updateTimestamp observed by Query

ConsistencyLevel           = Customized
GuaranteeTimestamp         = max(
    classificationTs,
    capturedCollectionUpdateTs,
    queryCollectionUpdateTs)
MvccTimestamp              = classificationTs
EntityTtlPhysicalTime      = PhysicalTimeMicros(classificationTs)
CollectionTtlTimestamps    = ComposeTS(
    PhysicalTime(classificationTs) - collectionTTL)
```

`GuaranteeTimestamp` waits until both the captured schema and any newer
collection metadata observed by the nested Query are serviceable. The nested
Query may raise this wait fence, but it never changes the admitted request's
captured configuration value.
`MvccTimestamp` fixes row visibility at `classificationTs`, even when the
guarantee is later. Entity and collection TTL evaluation also use
`classificationTs`, so queueing delay cannot change an existence decision.

Proxy performs one logical internal Retrieve before allocating RowIDs or
constructing DML messages:

- The Retrieve requests only the primary-key field. The existing helper must
  honor its `outputFields` argument instead of requesting `*`.
- It uses one standard Query task and the existing collection-shard fan-out,
  load balancing, retry, cancellation, and namespace-sharding fast path. This
  MEP adds no PK-derived VChannel grouping or targeted Query scheduling.
- Every executed channel request carries the same non-zero guarantee, MVCC
  timestamp, and TTL cutoffs.
- Result order is not trusted. Proxy builds a typed PK set and classifies rows
  in original request order.

Existence uses the Full Upsert's logical partition scope:

| Mode | Physical partition scope | Query execution scope |
|---|---|---|
| Ordinary partition | Requested or default partition | Existing collection-shard Query fan-out |
| Partition key | All physical partitions | Existing collection-shard Query fan-out |
| Namespace as partition | Resolved namespace partition | Existing collection-shard Query fan-out |
| Namespace sharding | Namespace-derived partition | Existing namespace single-channel fast path when applicable |

Classification uses the same partition scope and load-state behavior as a
standard Query. For an exact partition scope, Proxy passes the resolved
partition ID. For the collection-wide partition-key scope, Proxy uses the
existing all-partitions query scope. Proxy does not enumerate expected
VChannels or add a feature-specific completeness protocol.

Any load-state, concurrent-release, RPC, timeout, cancellation, schema,
malformed-result, or decode error returned by the standard Query aborts the
request with that typed error. No RowID is allocated and no WAL message is
appended after such an error. Under the standard Query contract, a successful
response represents the requested scope and is used to classify each row as
Existing or NotFound. A successful response must contain the requested,
correctly typed PK field even when it contains zero rows. An absent or malformed
PK field is `merr.ErrDataIntegrity`, not an all-NotFound result.

If at least one row is NotFound while `insertOnNotFound=false`, Proxy rejects
the complete request before RowID allocation and WAL append with the dedicated
non-retriable InputError `merr.ErrAutoIDUpsertTargetNotFound`:

```text
autoID full upsert target not found because
proxy.autoIDUpsertInsertOnNotFound=false; not_found_count=N
```

The public error omits PK values.

### 3.3 Full Upsert Row Plan

After complete classification and deterministic validation, Proxy stores a
request-indexed classification plan containing the request IDs, one existence
bit per request row, and the Existing-row Delete IDs. Internal RowIDs remain in
the Upsert task's existing allocation state. After allocation, Proxy derives a
separate final Insert-ID sequence in request order.

Proxy allocates one internal RowID for every inserted row version. The final
business PK is:

```text
FinalPK[i] = RequestPK[i]                   if row i exists
FinalPK[i] = EncodeAutoID(InternalRowID[i]) if row i is NotFound
                                               and insertOnNotFound
```

The same `InternalRowID` allocation supplies `G`; no second allocator request is
made. Request IDs remain lookup state, result IDs remain response state, and
Insert/Delete IDs are derived from the request-indexed classification state for
message packing. `MutationResult.IDs` must not be reused as mutable routing
state. A NotFound row's final PK is accepted based on its allocator provenance.
Unit coverage must include a controlled `G == P` value. A black-box regression
may use lookup values outside the allocator domain and assert `G != P` only to
prove that Proxy did not copy the lookup value.

For `[P1, P2, P3]`, where `P1` and `P3` exist and `P2` is NotFound:

```text
Request IDs = [P1, P2, P3]
Result IDs  = [P1, G2, P3]
Insert IDs  = [P1, G2, P3]
Delete IDs  = [P1, P3]
```

For `E` existing rows and `M` generated rows, a successful result is:

```text
returned IDs[i] = RequestPK[i] or GeneratedPK[i]
InsertCnt       = E + M
UpsertCnt       = E + M
DeleteCnt       = E
```

When `insertOnNotFound=false`, a successful request always has `M = 0`.

### 3.4 Routing and WAL Boundary

Insert packing uses the final Insert IDs. Delete packing uses only the Existing
request IDs. Both use the existing namespace routing rule when applicable:

| Row kind | Insert route | Delete route |
|---|---|---|
| Existing `P` | `route(P, namespace)` | `route(P, namespace)` |
| Generated `G` | `route(G, namespace)` | None |

The write path continues to pass packed Insert and Delete messages to
`AppendMessages`. It adds no WAL message, DML header, or Streaming CAS marker.

`AppendMessages` groups messages by VChannel. The Delete and Insert for one
Existing `P` therefore enter one VChannel transaction, so a legal Snapshot
checkpoint cannot fall between them. A generated row has one Insert and no
Delete.

A Full Upsert request spanning VChannels can still partially commit across
those channels. This MEP guarantees entity-level identity consistency, not
request-level distributed atomicity.

### 3.5 Retry and Concurrency Boundaries

- Retrying an Existing row is identity-stable because its business PK remains
  the request PK.
- Retrying a NotFound row with `insertOnNotFound=true` is not identity-stable.
  A lost response or cross-VChannel partial success can produce another
  allocator-owned identity.
- Existence is decided at each attempt's `classificationTs`. Another writer
  may change the next attempt from generation to an update of `P`.
- Concurrent writes after classification follow existing MVCC ordering. This
  design adds no row lock, CAS, lookup-key reservation, or durable mapping.

The `false` configuration guarantees only that the target exists at the
classification snapshot. It is not a commit-time existence guarantee.
Applications intentionally creating AutoID entities should use Insert.

## 4. API, Compatibility, and Operations

### 4.1 API and SDK Behavior

No public request or response protobuf changes. Full AutoID Upsert already has
request PK fields and `MutationResult.IDs`; the Proxy system configuration
controls not-found behavior.

| Entry point | Required Full AutoID Upsert behavior |
|---|---|
| Raw gRPC / column request | Send the lookup PK and consume returned final IDs |
| REST v2 | Preserve the PK in conversion and return final IDs in request order |
| REST v1 | Continue rejecting AutoID Upsert |
| Go SDK row-based | Retain the AutoID PK in the Upsert request and return final IDs in `UpsertResult.IDs` |
| Go SDK column-based | Continue sending the PK and consume returned final IDs |

Insert conversion is unchanged. Returned IDs are authoritative because a mixed
batch can contain preserved and generated IDs. SDKs do not mutate caller-owned
row objects after the Server commits; applications consume `UpsertResult.IDs`.
If an RPC finishes with an unknown outcome, the SDK and application must not
infer a final ID from the lookup PK or treat a retry with that PK as recovery of
the first attempt. Generated-on-not-found has no idempotent retry contract.

Partial Upsert request construction and result handling remain under the
existing Partial Upsert contract. The new configuration does not change a
Partial request's missing-target result.

Compatibility depends on whether the client sends the AutoID PK:

| Client behavior | Old Server | Updated Server |
|---|---|---|
| Sends the AutoID PK | Historical identity replacement | Apply this MEP and return final IDs |
| Omits the AutoID PK | Reject malformed Upsert | Reject malformed Upsert; upgrade the client or use Insert |

Clients must not rely on identity preservation until every serving Proxy runs
the updated behavior. No runtime version negotiation is added.

### 4.2 Load-State Compatibility

Full AutoID Upsert currently succeeds against an unloaded collection because it
is a blind write. The new path must Retrieve before writing:

| Workflow | Old Server | Updated Server |
|---|---|---|
| Full AutoID Upsert before required scope is loaded | Blind write and replace identity | Typed load-state error; no allocation or WAL append |
| Full AutoID Upsert after required scope is fully loaded | Blind write and replace identity | Preserve Existing PKs; NotFound rows follow the Proxy configuration |
| Partial Upsert | Existing Query/load/CAS behavior | Unchanged |

This is an intentional Full Upsert workflow change. Applications using Full
AutoID Upsert as their first ingestion operation must use Insert for creation or
load the complete query scope before calling Full Upsert.

### 4.3 Error Boundary

| Condition | Error behavior |
|---|---|
| Malformed Full Upsert PK or fields | Existing typed input error, non-retriable |
| Full Upsert target not found while the configuration is `false` | `merr.ErrAutoIDUpsertTargetNotFound` (code 112), InputError, non-retriable |
| Incomplete load scope or concurrent release | Preserve the typed load/system error |
| Retrieve, schema, routing, packing, or WAL failure | Preserve or originate a typed system error |

All classification and deterministic construction failures occur before the
first WAL append. An error returned by the standard Query is system blame and
never a not-found input error.

`ErrAutoIDUpsertTargetNotFound` reserves code 112 in the collection error
family. It has `WithErrorType(InputError)` and `retriable=false`.
`oldCode(112)` maps to deprecated `commonpb.ErrorCode_IllegalArgument` for old
clients. The implementation rechecks code occupancy after rebasing and tests
the status, InputError flag, retriable bit, legacy projection, and `errors.Is`
round trip.

### 4.4 Configuration Change, Upgrade, and Rollback

Old and new Proxies interpret the same Full AutoID Upsert differently, and a
refreshable configuration change is not atomic across Proxies. Upgrade,
rollback, or a strict configuration transition therefore pauses Full AutoID
Upsert, makes every serving Proxy homogeneous in version and configuration, and
then resumes traffic. PK-retaining row clients are deployed only after the
Server transition. Partial Upsert is unaffected.

### 4.5 Existing Data and Replication

No data migration is required. Existing rows keep their stored PKs. Historical
identity replacements and Snapshots containing historical split-message states
are not repaired.

Replication replays committed source WAL semantics. It does not reclassify
existence or apply the target Proxy's configuration. No new replication
header or target-side validation is required.

## 5. Implementation Scope

The implementation is limited to:

- one refreshable Proxy Boolean and one typed not-found error;
- Full AutoID Upsert classification, final-ID construction, and message routing;
- fixed-snapshot and PK-only support in the existing internal Query path;
- Go row-SDK PK retention plus regression coverage for existing REST behavior.

Streaming reuses the existing same-VChannel transaction. There is no RootCoord,
CreateCollection, AlterCollection, public protobuf, WAL format, replication, or
Partial Upsert CAS change.

## 6. Validation and Acceptance

| Contract | Required automated evidence |
|---|---|
| Full semantics | Existing, NotFound, and mixed batches under `true`; complete mixed-batch rejection under `false`; Int64 and VarChar final IDs |
| Mode and configuration | Default and explicit values, one captured value per task, and effective Partial mode ignoring the configuration after `field_ops` normalization |
| Input and Query failures | Invalid PK payloads, malformed Query results, and typed Query errors all fail before allocation |
| Identity and routing | Allocator-owned `G`, controlled `G == P`, no Delete for NotFound, ordered result IDs, correct counts, and same-channel Existing-row routing |
| Snapshot | Query `PreExecute` preserves one MVCC and TTL classification point while allowing its metadata guarantee to advance |
| Error contract | Code 112 is InputError, non-retriable, legacy-compatible, and round-trips through `merr.Status` |
| Entry points | MiniCluster covers `false`; Go SDK covers row Insert versus Upsert PK handling; REST v1/v2 retain their existing contracts |
| Regressions | Relevant Partial Upsert, non-AutoID Upsert, Insert, Import, Query, and replication tests pass unchanged |

A real `queryTask.PreExecute` regression, rather than helper-only assertions,
uses a newer collection-update fence observed by the nested Query and asserts:

```text
GuaranteeTimestamp == max(
    classificationTs,
    capturedCollectionUpdateTs,
    queryCollectionUpdateTs)
MvccTimestamp         == classificationTs
MvccTimestamp         != 0
EntityTtlPhysicalTime == PhysicalTimeMicros(classificationTs)
CollectionTtlTimestamps ==
    ComposeTS(PhysicalTime(classificationTs) - collectionTTL)
```

The implementation is ready when this table passes and both required Design
Review approvals are recorded. The additional PK-only Query is an explicit
Full Upsert cost; performance measurement belongs to release validation and
does not change the semantic acceptance criteria.

## 7. Alternatives Considered

| Alternative | Why it is not selected |
|---|---|
| Insert a missing request PK `P` | Bypasses allocator ownership and can collide with generated AutoIDs |
| Always generate or always reject NotFound | Cannot provide both backward-compatible creation and snapshot-checked update-only behavior |
| Treat PK omission as Insert | Conflates identity lookup with creation; Insert already serves this use case |
| Add a durable `P -> G` mapping | Requires a new identity namespace, persistence, replication, and lifecycle design |
| Add a per-request or per-collection option | Expands the public API or RootCoord metadata without improving identity correctness; one Proxy configuration is sufficient |
| Add Full Upsert CAS, locks, or cross-VChannel transactions | Solves stronger concurrency or request-atomicity problems outside this identity fix |
