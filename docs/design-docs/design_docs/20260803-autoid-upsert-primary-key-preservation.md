# MEP: Preserve Primary Keys in Full AutoID Upsert

- **Created:** 2026-08-03
- **Last Updated:** 2026-09-15
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

The new Full Upsert path first retrieves the request PK through a standard Query.
It preserves `P` when the row exists and generates an allocator-owned ID when
the row is not found. No new configuration is introduced.

| Full AutoID Upsert row | Result |
|---|---|
| PK omitted | Reject; use Insert to create an entity without a lookup key |
| PK exists | `Delete(P) + Insert(P)` and return `P` |
| PK is not found | Generate allocator-owned `G`, emit only `Insert(G)`, and return `G` |

Partial AutoID Upsert retains master's read/merge/CAS path: existing PKs are
preserved, while missing rows receive generated IDs when insert fields are
complete. Incomplete insert fields are rejected.

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
4. Existence and TTL visibility follow standard Query semantics; Full Upsert
   does not pin a separate classification snapshot.
5. Returned IDs remain aligned with Full Upsert request order.
6. Partial Upsert semantics, CAS messages, read timestamps, and retries are
   unchanged.

Non-AutoID Upsert, Partial Upsert, ordinary Insert, Import, WAL formats, and
stored row formats are unchanged.

### Goals

- Add one PK-only existence Retrieve before Full AutoID Upsert writes.
- Preserve the identity of every existing AutoID entity updated by Full Upsert.
- Keep an existing entity's Delete and Insert in one VChannel transaction.
- Preserve allocator-generated insert-on-not-found for Full AutoID Upsert.
- Support Int64 and VarChar AutoID, partition-key mode, and namespace modes.
- Fail before WAL append when the standard Query used for
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
- An insert-on-not-found configuration or update-only mode for Full Upsert.
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
  `P`, so Proxy must never copy it directly as a business PK. Full Upsert uses
  an independently allocator-derived `G`.

For missing rows, Milvus allocates a batch of business PKs during preparation,
independently of the later internal RowID allocation:

```text
AllocatedPKs = common.AllocAutoID(globalAllocator, len(missingRows))
G            = EncodeAutoID(AllocatedPKs[missingRowOffset])
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

Partial AutoID Upsert already performs a Strong Retrieve and binds the actual
per-channel read snapshots to Streaming CAS. Missing rows with complete insert
fields receive generated IDs, retained across internal CAS retries. This MEP
does not reuse, modify, or extend that CAS protocol for Full Upsert.

## 3. Proposed Design

### 3.1 Full Upsert Request Boundary

Proxy first validates every `field_ops` directive using the existing rules and
normalizes the request:

```text
nonReplaceSeen = validateFieldPartialUpdateOps(request, schema)
effectivePartialUpdate = request.partial_update || nonReplaceSeen
```

An invalid field operation fails through the existing validation path. A valid
non-`REPLACE` operation selects the existing Partial Upsert path even when the
wire request says `partial_update=false`. Only `autoID=true` with
`effectivePartialUpdate=false` enters the new Full AutoID classification.
Partial behavior is unchanged. No configuration, collection metadata, RootCoord
path, WAL marker, or readiness protocol is added.

Before classification, Proxy validates the lookup PKs and rejects omitted, null,
wrong-type, row-misaligned, or duplicate PKs. PK omission is malformed Upsert
input; it is not the same as a supplied PK that is not visible to the Query.
Complete-payload validation and nullable/default field completion remain in
`insertPreExecute`, after preparation. A request with invalid non-PK data can
therefore query and allocate IDs before validation rejects it; a Query failure
can take precedence over that later validation error. No invalid payload is
appended to WAL.

### 3.2 One PK-Only Retrieve for Full Upsert

Full AutoID and Partial Upsert both use standard Strong Query preprocessing
and visibility:

```text
ConsistencyLevel                  = Strong
Initial GuaranteeTimestamp        = 0
Explicit MvccTimestamp             = unset
Full-specific snapshot / TTL logic = none
```

Each Query obtains its own timestamp through normal Strong Query scheduling;
it does not reuse `Upsert.BeginTs()`. Query preprocessing raises the guarantee
for newer collection metadata and derives TTL clocks through the existing path.
QueryNode can use the local WAL MVCC optimization for Strong reads and selects
the actual readable snapshot after waiting. The guarantee is not a fixed
snapshot. Full Upsert does not promise one shared MVCC timestamp across VChannels
or an existence decision frozen at request admission; concurrent writes and
expiry before the Query executes may affect its result.

Only Partial Upsert collects the executed per-channel snapshots for its CAS
proof. Neither path pins an explicit MVCC timestamp. Full Upsert adds no fields
or special branches to `queryTask`.

Proxy performs one logical internal Retrieve before finalizing business PKs and
packing DML messages:

- The Retrieve requests only the primary-key field. The existing helper must
  honor its `outputFields` argument instead of requesting `*`.
- It uses one standard Query task and the existing collection-shard fan-out,
  load balancing, retry, cancellation, and namespace-sharding fast path. This
  MEP adds no PK-derived VChannel grouping or targeted Query scheduling.
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
request with that typed error, before Full allocates business PKs or internal
RowIDs. No WAL message is appended. Under the standard Query contract, a successful
response represents the requested scope and is used to classify each row as
Existing or NotFound. A successful response must contain the requested,
correctly typed PK field even when it contains zero rows. Both modes rely on the
standard Query contract for PK type and lookup scope; Full does not add separate
type, requested-subset, or duplicate-result checks. PK classification uses set
membership and is unaffected by repeated result IDs. Errors from PK extraction,
parsing, and membership helpers pass through unchanged, as in Partial Upsert,
rather than being treated as NotFound.

### 3.3 Shared Query and Write Preparation

All Upserts enter `prepareUpsert` for function generation. Non-AutoID Full Upsert
then returns without querying. Full AutoID and Partial share `queryPreExecute`
for retrieval, classification, and Existing-row Delete preparation:

- Full requests only PKs, retains its working insert fields in request order,
  and returns the NotFound row offsets without merging old fields.
- Partial restores original fields and previously allocated IDs and prepares
  CAS terms before reading. After shared classification, it normalizes and
  validates patch fields, allocates missing AutoIDs, selects destination CAS
  proofs, and merges old fields. Omitted fields on existing rows retain their
  old values; Full does not inherit them.

Both modes use `allocateMissingAutoIDs` to batch-allocate business PKs for missing
rows and apply them with `checkUpsertPrimaryFieldData`. Full does this immediately
after classification returns to `prepareUpsert`; Partial does it before merging.
Only Partial saves allocated IDs and request-order results for CAS retries.
Full adds no retry state and never queries its freshly generated IDs again.
The PK helper rejects duplicate final PKs before replacing the working column;
both modes preserve its errors without Full-specific reclassification.

`prepareUpsert` fills the Insert fields and explicit Delete subset together.
`insertPreExecute` then retains its original internal RowID, timestamp, and
success-index initialization block, validates the complete payload, and reads
the finalized PK column without generating another business PK. Its validation
still fills omitted nullable/default fields for Full. The final business PK is:

```text
FinalPK[i] = RequestPK[i]             if row i exists
FinalPK[i] = AllocatedBusinessPK[i]   if row i is NotFound
```

For `N` request rows and `M` missing rows, Full allocates `M` business PKs during
preparation and `N` internal RowIDs later. When `M=0`, it skips the business-PK
allocation. Compared with reusing RowIDs, this costs one additional allocator
call and `M` IDs when rows are missing, but keeps PK preparation in one stage.
The allocator caches ID ranges, so an extra call need not issue a remote RPC.
Failed preparation or validation does not return allocated IDs to the pool.

Original lookup PKs determine the Delete subset before any replacement.
`MutationResult.IDs` supplies both the response IDs and the Insert routing IDs;
its values and order must match the
Insert payload's PK field and must not be changed during message packing. Delete
IDs contain only the request IDs classified as Existing. A NotFound row's final
PK is accepted based on its allocator provenance.
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
- Retrying a NotFound row is not identity-stable.
  A lost response or cross-VChannel partial success can produce another
  allocator-owned identity.
- Existence is decided from each attempt's Query result. Another writer may
  change the next attempt from generation to an update of `P`.
- Concurrent writes after classification follow existing MVCC ordering. This
  design adds no row lock, CAS, lookup-key reservation, or durable mapping.

Applications intentionally creating AutoID entities should use Insert.

## 4. API, Compatibility, and Operations

### 4.1 API and SDK Behavior

No public request or response protobuf changes. Full AutoID Upsert already has
request PK fields and `MutationResult.IDs`. Missing targets receive generated IDs.

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
existing Partial Upsert contract, including its missing-target behavior.

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
| Full AutoID Upsert before required scope is loaded | Blind write and replace identity | Typed load-state error; no WAL append |
| Full AutoID Upsert after required scope is fully loaded | Blind write and replace identity | Preserve Existing PKs; generate IDs for NotFound rows |
| Partial Upsert | Existing Query/load/CAS behavior | Unchanged |

This is an intentional Full Upsert workflow change. Applications using Full
AutoID Upsert as their first ingestion operation must use Insert for creation or
load the complete query scope before calling Full Upsert.

### 4.3 Error Boundary

| Condition | Error behavior |
|---|---|
| Malformed Full Upsert PK or fields | Existing typed input error, non-retriable |
| Incomplete load scope or concurrent release | Preserve the typed load/system error |
| Retrieve, schema, routing, packing, or WAL failure | Preserve or originate a typed system error |

All classification and deterministic construction failures occur before the
first WAL append. An error returned by the standard Query is system blame and
never a not-found input error.

No new error code or legacy error-code mapping is added.

### 4.4 Upgrade and Rollback

Old and new Proxies interpret existing-row Full AutoID Upsert differently.
Upgrade or rollback therefore pauses Full AutoID Upsert, makes every serving
Proxy homogeneous in version, and then resumes traffic. PK-retaining row clients
are deployed only after the Server transition. Partial Upsert is unaffected.

### 4.5 Existing Data and Replication

No data migration is required. Existing rows keep their stored PKs. Historical
identity replacements and Snapshots containing historical split-message states
are not repaired.

Replication replays committed source WAL semantics. It does not reclassify
existence. No new replication
header or target-side validation is required.

## 5. Implementation Scope

The implementation is limited to:

- Full AutoID Upsert classification, final-ID construction, and message routing;
- PK-only retrieval through the existing internal Query path;
- Go row-SDK PK retention plus regression coverage for existing REST behavior.

Streaming reuses the existing same-VChannel transaction. There is no RootCoord,
CreateCollection, AlterCollection, public protobuf, WAL format, replication, or
Partial Upsert CAS change.

## 6. Validation and Acceptance

| Contract | Required automated evidence |
|---|---|
| Full semantics | Existing PKs are preserved; NotFound rows are inserted with generated IDs; mixed batches and Int64/VarChar final IDs |
| Mode | `field_ops` normalization selects the existing Partial path; only effective Full AutoID enters classification |
| Input and Query failures | Invalid PK payloads and Query failures stop before Full allocation; subsequent allocation/field-validation failures never append to WAL |
| Allocation stages | Full uses one missing-PK batch followed by one internal RowID batch; all-existing skips PK allocation; Partial retry IDs remain stable |
| Identity and routing | Allocator-owned `G`, controlled `G == P`, no Delete for NotFound, ordered result IDs, correct counts, and same-channel Existing-row routing |
| Query visibility | Both modes use Strong reads with unpinned MVCC and standard Query metadata waits and TTL handling; only Partial collects actual per-channel CAS snapshots |
| Entry points | MiniCluster covers generated insertion and identity-preserving updates; Go SDK covers row Insert versus Upsert PK handling; REST v1/v2 retain their existing contracts |
| Regressions | Relevant Partial Upsert, non-AutoID Upsert, Insert, Import, Query, and replication tests pass unchanged |

A real `queryTask.PreExecute` regression verifies that the guarantee follows
the Strong Query's own timestamp or a newer collection update, MVCC remains
unset for QueryNode to select, and TTL handling follows standard Query semantics.
Request-construction tests verify that both modes use Strong without reusing
the Upsert timestamp, while only Partial binds executed per-channel snapshots
for CAS.

The implementation is ready when this table passes and both required Design
Review approvals are recorded. The PK-only Query and extra missing-PK allocation
are explicit Full Upsert costs; performance measurement belongs to release
validation and does not change the semantic acceptance criteria.

## 7. Alternatives Considered

| Alternative | Why it is not selected |
|---|---|
| Insert a missing request PK `P` | Bypasses allocator ownership and can collide with generated AutoIDs |
| Always reject NotFound | Breaks the historical Full AutoID insert-on-not-found behavior |
| Treat PK omission as Insert | Conflates identity lookup with creation; Insert already serves this use case |
| Add a durable `P -> G` mapping | Requires a new identity namespace, persistence, replication, and lifecycle design |
| Add an insert-on-not-found option | Not needed to preserve existing identities; retaining historical creation behavior keeps the change smaller |
| Add Full Upsert CAS, locks, or cross-VChannel transactions | Solves stronger concurrency or request-atomicity problems outside this identity fix |
