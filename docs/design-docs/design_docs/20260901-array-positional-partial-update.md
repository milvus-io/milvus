# Positional Partial Update for Array and Array of Struct

- **Feature DRI:** @weiliu1031
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD
- **Status:** Draft
- **Issue:** [milvus-io/milvus issue 53016](https://github.com/milvus-io/milvus/issues/53016)

## Summary

This design adds positional `REPLACE` semantics to partial upsert for:

- one existing element of a scalar `Array` field;
- one or more children of one existing `Array<Struct>` element; and
- a complete existing `Array<Struct>` element.

The public operation is `PATH_REPLACE`. Its path is relative to the parent
field and has one of two forms:

```text
[index]
[index][child]
```

REST exposure is limited to REST v2. REST v1 rejects `PATH_REPLACE` as an
unsupported operation.

The operation remains a Proxy-side read-modify-write. Proxy retrieves the old
row, applies the positional replacement in memory, materializes complete
top-level `FieldData`, and then uses the existing partial-upsert delete/insert
and WAL path. Internal WAL, storage, DataNode, StreamingNode, and CDC protocols
do not receive mutation paths.

The design deliberately keeps one path per parent field per request. Updating
different indexes or base paths of the same parent field requires separate
requests. This restriction removes the need for a general overlap resolver in
the current scope while still allowing a Struct element update to replace
multiple children under the same `[index]` base path.

## Motivation

Partial upsert currently replaces a complete top-level field. It also supports
whole-array `ARRAY_APPEND` and `ARRAY_REMOVE`, but it cannot replace an array
element in place. An application that wants to change only `scores[1]` or only
the `age` child of `profile[1]` must read the entity, merge it client-side, and
write the whole array back.

For example, given:

```json
{
  "id": 1,
  "profile": [
    {"age": 17, "city": "Beijing", "score": 0.8},
    {"age": 20, "city": "Shanghai", "score": 0.9}
  ]
}
```

the following operation should only replace one leaf value:

```text
field_name = profile
op         = PATH_REPLACE
path       = [1][age]
value      = 18
```

The result is:

```json
{
  "id": 1,
  "profile": [
    {"age": 17, "city": "Beijing", "score": 0.8},
    {"age": 18, "city": "Shanghai", "score": 0.9}
  ]
}
```

The server already retrieves existing rows for partial upsert. The minimal
implementation therefore extends the Proxy merge step instead of introducing
a nested mutation language into downstream storage protocols.

## Goals

- Support `Array[index]` replacement for every element type already supported
  by the corresponding `Array` representation.
- Support `Array<Struct>[index][child]` replacement for scalar and vector
  Struct children.
- Support replacing a non-empty subset, or all, of the children under one
  `Array<Struct>[index]` base path.
- Preserve the array length, element order, all non-target elements, and all
  non-target Struct children.
- Preserve the existing Milvus nullability boundary: target parent rows and
  replacement values must be non-null.
- Use one path grammar and equivalent failure semantics across SDKs, REST v2,
  and the server.
- Preserve compatibility with old clients and fail safely when a new client
  reaches an old Proxy.
- Reuse the existing partial-upsert materialization and write path after Proxy
  resolves the mutation.

## Non-goals

- Inserting, deleting, moving, sorting, or otherwise reordering an array
  position.
- Nested `ARRAY_APPEND` or `ARRAY_REMOVE`.
- Multiple base paths for the same parent field in one request.
- Wildcards, predicates, slices, negative indexes, or column-wide child paths
  such as `[age]`.
- Paths deeper than `[index][child]`.
- Updating an entity that does not already exist.
- Updating the same primary key more than once in one request.
- Adding element-level nullability to `Array`, `ArrayOfVector`, or
  `Array<Struct>` children.
- Replacing an Array element, a Struct child, or a complete Struct element with
  `null`.
- Adding a generic JSONPath or field-expression language.
- Changing function execution semantics for Struct fields or Array fields.
- Adding a request-level atomicity or CAS guarantee across VChannels. That work
  is intentionally handled separately.

## Terminology

- **Parent field**: the top-level `Array` or `Array<Struct>` named by
  `FieldPartialUpdateOp.field_name`.
- **Base path**: the request-wide `[index]` selected for one parent field.
- **Leaf target**: the concrete value replaced after schema resolution. A
  scalar Array base path has one leaf target. A Struct base path may expand to
  multiple child leaf targets.
- **Operand**: the replacement value carried by the matching `FieldData`.
- **Materialized row**: the complete row reconstructed by Proxy after applying
  the operand to the retrieved old row.

## Current Architecture and Invariants

### Partial-upsert data flow

The current Proxy path retrieves the existing entities by primary key, maps
the query result back to request order, merges updated and old fields, and then
runs the normal insert validation and flattening path. Existing entities are
written as delete plus insert. Missing entities currently follow partial-upsert
insert semantics.

`PATH_REPLACE` is resolved during this merge phase:

```text
SDK / REST v2
    |
    | UpsertRequest(field_ops, dense FieldData)
    v
Proxy static validation and path resolution
    |
    | retrieve complete old rows by PK
    v
Proxy positional merge in request-row order
    |
    | complete top-level FieldData
    v
existing StructArray flattening and insert validation
    |
    | existing delete + insert DML
    v
StreamingClient / legacy msgstream -> WAL -> downstream consumers
```

The path and operand are consumed before the request is converted to internal
DML. No path field is added to internal protobuf messages.

### Array order

An Array is an ordered sequence. `FieldData` represents each row as an ordered
list, and storage serializes that list without sorting or deduplication.

An `Array<Struct>` is physically represented by one aligned array column per
child. For a logical row, every child array has the same element count, and
element `i` of every child column belongs to the same Struct element. The
flattening path validates this lock-step invariant before writing.

Consequently, `PATH_REPLACE` has the following order contract:

- index zero refers to the first logical array element supplied by the user;
- replacement never changes array length or any offset;
- elements before and after the target retain their relative and absolute
  positions; and
- a Struct child replacement changes only the value at the same aligned child
  offset.

This guarantee concerns the logical order inside one field value. It does not
promise physical segment row order. A separate user mutation, such as whole
field replacement or `ARRAY_REMOVE`, may change later positional meaning.
Concurrent changes to positional meaning are part of the separate CAS design.

## Public API

### Protobuf

Extend the existing public `FieldPartialUpdateOp` in `milvus-proto`:

```protobuf
message FieldPartialUpdateOp {
  enum OpType {
    REPLACE = 0;
    ARRAY_APPEND = 1;
    ARRAY_REMOVE = 2;
    PATH_REPLACE = 3;
  }

  string field_name = 1;
  OpType op = 2;
  string path = 3;
}
```

`path` is meaningful only when `op == PATH_REPLACE`.

As with the existing non-`REPLACE` operations, the presence of
`PATH_REPLACE` implicitly enables partial-update processing. SDKs may still set
`partial_update = true` explicitly, but the server must not require both
signals.

The operation must use a new enum value instead of interpreting
`REPLACE + non-empty path` as positional replacement. During a rolling upgrade,
an old server may ignore the unknown `path` field. If the op remained
`REPLACE`, that server could silently perform a whole-field replacement. With
the new numeric enum value, the existing validation switch reaches its unknown
operation branch and rejects the request instead.

No op is embedded in `FieldData`. `FieldData` remains a reusable data carrier
for insert, query, search, and internal messages.

### Operation-to-payload matching

`field_name` matches one top-level `FieldData.field_name`. For each parent field
in one request:

- at most one `FieldPartialUpdateOp` is allowed;
- exactly one matching `FieldData` is required for `PATH_REPLACE`; and
- duplicate top-level `FieldData` entries are rejected.

The `path` is request-wide. Every entity row in the matching `FieldData` uses
the same path. For example, `path = "[1]"` updates index 1 for every primary key
in the request.

Different parent fields may each have one independent path in the same
request:

```text
scores  -> [1]
profile -> [2][city]
```

### Path grammar

The supported path grammar is:

```ebnf
path        = index-segment, [ child-segment ] ;
index-segment = "[", index, "]" ;
child-segment = "[", child-name, "]" ;
index       = "0" | nonzero-digit, { digit } ;
child-name  = schema-field-name ;
```

Rules:

- The path is relative to `field_name`; `[1]` is valid and `profile[1]` is not.
- The index is a zero-based, non-negative decimal integer.
- The canonical decimal form has no leading zeros except `0` itself.
- Whitespace is not accepted or normalized.
- `[index]` is valid for scalar `Array` and `Array<Struct>`.
- `[index][child]` is valid only for `Array<Struct>`.
- `child` must exactly match a direct child in the collection schema.
- Escaping, quoted child names, wildcards, predicates, and deeper nesting are
  not supported by this design.

Proxy resolves the string once against the collection schema and converts it
to an internal typed form:

```go
type resolvedPathReplace struct {
    parentFieldID int64
    index         int
    childFieldID  *int64
}
```

The raw string must not be reparsed after this validation boundary.

### FieldData encoding

The request continues to use column-based, dense `FieldData`. It does not add a
second value carrier inside `FieldPartialUpdateOp`.

For `N` entities, the matching parent `FieldData` has `N` outer rows. Each
outer row contains exactly one replacement element. A naked scalar or naked
Struct object is not accepted as shorthand.

The protobuf-level shapes are:

| Target | Matching parent `FieldData` | Per-entity operand |
|---|---|---|
| Scalar `Array[index]` | `type = Array`, one `ArrayArray.data` row per entity | One immediate scalar element |
| Struct scalar child | `ArrayOfStruct` with one scalar child | Singleton scalar Array row |
| Struct vector child | `ArrayOfStruct` with one vector child | Singleton ArrayOfVector row |
| Struct subset `[index]` | `ArrayOfStruct` with selected children | One element in each selected child row |

Every immediate replacement element must carry one concrete, non-null typed
value. `PATH_REPLACE` does not introduce or interpret element-level
`valid_data`; an operand carrying immediate-element validity metadata is
rejected. Field-level validity remains governed by the existing parent-field
contract, and every operand parent row must be present and non-null.

#### Scalar Array element

Logical Python example:

```python
data = [
    {"id": 1, "scores": [100]},
    {"id": 2, "scores": [200]},
]
field_ops = {
    "scores": FieldOp.path_replace("[1]"),
}
```

`scores: [100]` is the one-element operand container for entity 1. It does not
mean that the complete stored `scores` field becomes `[100]`.

#### One Struct child

```python
data = [
    {"id": 1, "profile": [{"age": 18}]},
    {"id": 2, "profile": [{"age": 21}]},
]
field_ops = {
    "profile": FieldOp.path_replace("[1][age]"),
}
```

The Struct operand must contain exactly the named child for
`[index][child]`.

#### Struct child subset

```python
data = [
    {"id": 1, "profile": [{"age": 18, "city": "Hangzhou"}]},
    {"id": 2, "profile": [{"age": 21, "city": "Ningbo"}]},
]
field_ops = {
    "profile": FieldOp.path_replace("[1]"),
}
```

If the Struct schema contains `age`, `city`, and `score`, this request replaces
`age` and `city` at index 1 and preserves `score`. If all schema children are
present, the operation is a complete Struct-element replacement.

For `[index]`, the set of present child columns is a non-empty, request-wide
field mask:

- every entity uses the same child set;
- an omitted child is preserved from the old element;
- every present child carries one concrete, non-null replacement value; and
- an empty child set is rejected.

This is a deliberate refinement of the initial issue text, which required a
complete Struct for `[index]`. It allows multiple children sharing one base
path without permitting multiple paths for the parent field.

The single-child forms `[index]` with `{child: value}` and
`[index][child]` with `{child: value}` have the same merge result. SDK
documentation should recommend `[index][child]` when only one child is updated,
because it expresses intent more precisely.

### SDK and REST v2 shape

All SDKs expose the same relative path string. SDK-specific typed builders may
be added later, but they must serialize to the canonical string grammar.

Python:

```python
field_ops = {
    "scores": FieldOp.path_replace("[1]"),
    "profile": FieldOp.path_replace("[1][age]"),
}
```

Go:

```go
option.WithPathReplace("scores", "[1]")
option.WithPathReplace("profile", "[1][age]")
```

REST v2:

```json
{
  "collectionName": "users",
  "partialUpdate": true,
  "data": [
    {"id": 1, "profile": [{"age": 18}]}
  ],
  "fieldOps": [
    {
      "fieldName": "profile",
      "op": "PATH_REPLACE",
      "path": "[1][age]"
    }
  ]
}
```

SDK requirements:

- preserve the user's path string exactly;
- preserve input entity order and array element order;
- build one outer operand row per entity;
- reject row-oriented input whose Struct child mask differs across entities;
- do not silently split one call into multiple requests; and
- leave grammar and schema validation authoritative on the server so all SDKs
  have identical acceptance rules.

The Python helper does not accept a bare string alias such as
`"path_replace"`, because the required path would be missing.

## Semantics

### Scalar Array `[index]`

For every request row:

1. Locate the existing entity by primary key.
2. Read the non-null parent Array.
3. Validate `index < len(oldArray)`.
4. Decode exactly one operand element.
5. Validate its type and require a concrete, non-null value.
6. Clone the old Array and replace only `oldArray[index]`.

Formally:

```text
result.length = old.length
result[index] = operand[0]
result[j] = old[j], for every j != index
```

### StructArray `[index][child]`

For every request row:

1. Locate the old Struct element at `index` across all aligned child columns.
2. Require exactly the named child in the operand mask.
3. Replace that child's value at `index`.
4. Preserve every other child at `index` and every other Struct element.

### StructArray `[index]`

The present operand children are expanded into a set of canonical leaf
targets:

```text
[index] + {age, city}
    -> (parentFieldID, index, ageFieldID)
    -> (parentFieldID, index, cityFieldID)
```

Each leaf is replaced independently, but the merge is validated and
materialized as one parent-field operation. Omitted children are copied from
the old aligned child arrays. The resulting StructArray still contains every
schema child before the existing flattening step runs.

### Existing-row requirement

Every primary key in a request containing any `PATH_REPLACE` must already
exist. This differs from ordinary partial upsert, which can promote a missing
primary key to an insert.

The singleton operand is not a complete Array value and cannot define the
initial value of a missing row. Proxy therefore rejects the entire request if
the old-row query does not return every requested primary key.

The query result may arrive in a different order from the request. Proxy must
continue using the existing primary-key-to-result-index mapping and apply each
operand to the old row with the same primary key, never by raw result position.

### Bounds and resizing

Bounds are evaluated independently for every old row because Array lengths may
differ across entities. The index must satisfy:

```text
0 <= index < oldArrayLength
```

An empty Array therefore rejects every positional path. Proxy does not append,
pad, create an element, or resize the Array when the index is out of range.

### Nullability boundary

This feature does not add or expand any Milvus nullability capability.
`StructArrayFieldSchema.nullable` or top-level `FieldSchema.nullable` continues
to control the complete parent row, but a positional replacement can only
target an existing non-null parent. The replacement operand must also contain
a concrete, non-null immediate value. `PATH_REPLACE` does not use
`element_nullable` to relax these rules.

The complete nullability matrix is:

| Case | Result |
|---|---|
| Existing parent Array is null | Reject |
| Operand parent row is missing or null | Reject |
| Scalar `Array[index] = null` | Reject |
| Struct child `[index][child] = null` | Reject |
| Immediate scalar/vector operand carries element `valid_data` | Reject |
| Omitted Struct child | Preserve the old value |
| Complete Struct element replacement with null | Reject |
| Any present Struct child is null | Reject |

These rules intentionally preserve the existing typed-Array storage and query
contract. Element-level nullability, including a shared Struct-element
validity representation, is a separate feature and is not a dependency of
positional replacement.

### Dynamic JSON keys

Only `FieldPartialUpdateOp.path` is parsed as a mutation path, and only when
`op == PATH_REPLACE`.

A row-data key named literally `"profile[1][age]"` remains an ordinary dynamic
JSON key. It is never interpreted as a path. The following two updates can
coexist because they target different top-level fields:

```json
{
  "id": 1,
  "profile": [{"age": 18}],
  "profile[1][age]": "literal dynamic value"
}
```

The static update is selected by `field_name = "profile"` plus
`path = "[1][age]"`; the bracketed row key is stored in the dynamic JSON field.

## Overlap and Duplicate Rules

This design allows one op and one base path per parent field. This produces the
complete request-level decision matrix below.

| Combination in one request | Result | Reason |
|---|---|---|
| `profile` whole-field `REPLACE` + `profile[index]` | Reject | Two semantics for one parent |
| `profile[index]` + `profile[index][age]` | Reject | Two ops/paths for one parent |
| `profile[index][age]` twice | Reject | Duplicate op for one parent |
| `profile[index][age]` + `profile[index][city]` | Reject | Use one `[index]` operand with both children |
| `profile[index]` + `profile[otherIndex]` | Reject | Multiple base paths for one parent; use separate requests |
| `profile[index]` + `profile[index]` | Reject | Duplicate op for one parent |
| `profile ARRAY_APPEND` + `profile[index]` | Reject | Multiple operations for one parent |
| `profile ARRAY_REMOVE` + `profile[index]` | Reject | Multiple operations for one parent |
| `[index]` operand contains `age` and `city` | Accept | One base path expands to two leaf targets |
| `[index]` operand contains all Struct children | Accept | Complete element replacement |
| `[index][age]` operand contains only `age` | Accept | Exact leaf target |
| `[index][age]` operand also contains `city` | Reject | Payload exceeds the declared leaf target |
| `profile[index]` + `scores[index]` | Accept | Different parent fields |
| Static `profile[index]` + dynamic literal key `"profile[index]"` | Accept | Different carriers and top-level fields |

Because duplicate operations and duplicate parent `FieldData` are rejected
before expansion, this design does not need a generic path trie or pairwise
leaf-overlap algorithm. Cross-request overlap is not detected here.

## Validation and Error Classification

All failures caused by request content use existing `merr` input-error
factories, normally `merr.WrapErrParameterInvalidMsg` or the corresponding
missing-parameter factory. Failures showing that internally retrieved rows or
schema metadata violate an invariant use a system-error factory such as
`merr.WrapErrServiceInternalMsg`.

Validation is split into three stages.

### Stage 1: request and schema validation

Before querying old rows, Proxy validates:

- `field_name` and `path` are present;
- `op` is supported;
- `path` is empty for operations other than `PATH_REPLACE`;
- `path` matches the supported path grammar;
- the parent field exists, is not the primary key, and has a supported type;
- the child exists when a child segment is present;
- one op and one top-level `FieldData` exist per parent;
- the operand has the correct parent `FieldData` type;
- the operand has one outer element per request row and one inner element per
  row;
- request primary keys are unique, so two operands cannot overlap on the same
  entity and parent path;
- the Struct child mask is non-empty and uniform across rows;
- `[index][child]` has exactly the named child; and
- value types and declared element types match the schema.

### Stage 2: old-row validation

After the query and before write dispatch, Proxy validates:

- every requested primary key was returned exactly once;
- every target parent row is non-null;
- every target index is in range;
- aligned Struct child arrays have consistent element counts; and
- every immediate replacement value is concrete and non-null.

### Stage 3: post-merge validation

The materialized request passes through existing validation:

- normal field-data row-count and type checks;
- max-capacity checks;
- StructArray full-child and aligned-length checks;
- existing field-level nullable checks; and
- existing insert preprocessing and function behavior.

The path-specific implementation must not bypass these validators.

Representative failures:

| Failure | Classification |
|---|---|
| Invalid grammar, negative index, whitespace, unknown child | InputError |
| Parent is not Array/StructArray | InputError |
| Missing or duplicate op/FieldData | InputError |
| Duplicate primary key in the request | InputError |
| Missing primary key | InputError |
| Null parent or out-of-range index | InputError |
| Wrong operand type/count/mask | InputError |
| Explicit null or immediate-element `valid_data` | InputError |
| Query returns duplicate PKs or malformed aligned Struct data | SystemError |
| Internal schema lookup fails after successful earlier resolution | SystemError |

All deterministic validation for the full request must complete before Proxy
starts dispatching its materialized DML. This avoids partial writes caused by a
known-bad path in a later row, but it does not add a new cross-VChannel
atomicity guarantee.

## Proxy Implementation

### Validation representation

Extend the partial-op validator to return resolved operations rather than only
a `fieldName -> enum` map. A suitable request-local representation is:

```go
type pathReplacePlan struct {
    arrayParent    *schemapb.FieldSchema
    structParent   *schemapb.StructArrayFieldSchema
    index          int
    explicitChild  *schemapb.FieldSchema
    operandChildren []*schemapb.FieldSchema
}
```

Exactly one of `arrayParent` and `structParent` is non-nil. The concrete type
should follow existing schema helper conventions. The important invariants are
that IDs, schema pointers, the parsed index, and the request-wide child mask are
fixed before the old-row query.

### Merge algorithm

For each target parent field:

1. Decode the operand rows without mutating the protobuf request.
2. For each request primary key, use the query-result PK map to locate the old
   row.
3. Clone only the affected per-row Array data.
4. Replace the target element or child in the clone.
5. Append the merged row to a complete parent `FieldData` in request order.
6. Replace the operand `FieldData` with the materialized complete field.
7. Remove `PATH_REPLACE` semantics from the downstream view; the result now has
   ordinary whole-field replacement semantics.

For a Struct subset, Proxy reconstructs every physical child column. Targeted
children use the operand value at `index`; omitted children use the old value.
All child columns preserve the same outer row count, inner element count, and
offsets.

Helpers must return new protobuf values instead of modifying query results or
request operands in place. This avoids aliasing across rows, retries, and test
fixtures.

### Interaction with existing Struct flattening

The incoming `PATH_REPLACE` operand may contain only a subset of Struct child
columns. That subset must be consumed before `checkAndFlattenStructFieldData`,
whose normal insert contract requires every child to be present.

After merge, the complete StructArray is passed through the unchanged
flattening path. Therefore no partial Struct representation reaches storage.

### Interaction with ARRAY_APPEND and ARRAY_REMOVE

Existing operations retain their current semantics. A parent field can carry
only one operation, so `PATH_REPLACE` cannot compose with append or remove in
one request. Applications that need both issue explicit sequential requests and
accept the normal concurrency boundary between them.

### Function fields

This feature does not make Struct fields eligible as function inputs or
outputs and does not introduce partial recomputation rules. Existing schema and
function validation remains authoritative. The materialized row continues
through the existing partial-upsert function pipeline.

## Compatibility and Upgrade

| Client / Proxy combination | Behavior |
|---|---|
| Old client -> old or new Proxy | Unchanged |
| New client, no `PATH_REPLACE` -> old or new Proxy | Unchanged |
| New client with `PATH_REPLACE` -> new Proxy | Supported |
| New client with `PATH_REPLACE` -> old Proxy | Rejected as unknown op; no silent whole-field replace |

The feature is considered available only after every request-serving Proxy in
a cluster supports enum value 3. SDK release notes must call out the minimum
server version.

REST v1 is outside the feature's compatibility surface and rejects
`PATH_REPLACE` before converting request data.

Downstream components can be upgraded independently because they receive only
the existing materialized DML representation. CDC observes the resulting full
delete/insert mutation, not the original path intent.

Proto changes must be made in `milvus-proto` and generated normally. Generated
files must not be hand-edited. Milvus then updates its proto dependency before
using the new enum and `path`. No element-nullability schema or wire changes
are required by this feature.

## Consistency and Atomicity Boundary

Positional replacement uses the same read snapshot, delete/insert conversion,
VChannel routing, retry behavior, and partial-success boundary as the existing
partial-upsert path.

This design does not claim that index `i` still identifies the same logical
element after a concurrent writer changes the Array. It also does not add
request-level atomicity across VChannels. Those concerns require the separate
CAS/VChannel design and its own tests. The only requirement here is that
path-specific deterministic validation completes before dispatch and that this
feature does not weaken the existing boundary.

SDKs must not hide that boundary by splitting one API call automatically.
Explicit multiple calls make ordering, retry, and partial success visible to
the application.

## Performance and Resource Impact

The feature has the same fundamental amplification as existing partial upsert:
it reads the complete old row and writes a materialized complete row even when
one element changes.

Additional CPU work is linear in the size of each targeted Array because the
implementation clones the row-local container. For StructArray `[index]`, it
also copies every aligned child array so the existing full-field write format
is preserved.

No new storage format, index update mechanism, WAL record, or downstream
memory state is introduced. Request limits, field max capacity, and existing
message size limits continue to bound resource use.

Potential future optimizations, such as storage-native leaf patches or
copy-on-write array buffers, are outside this design and must preserve the same
public semantics.

## Observability

Add low-cardinality Proxy metrics for positional partial update:

- request count by parent category: `array` or `struct_array`;
- result count by stable category: `success`, `invalid_path`, `missing_pk`,
  `null_parent`, `out_of_range`, `invalid_operand`, or `internal_error`; and
- merge latency and retrieved-row count using existing partial-upsert metrics
  where possible.

Do not put raw paths, child names, primary keys, or user values in metric
labels. Error messages may include the field name, canonical path, entity row
offset, expected type, and actual type, but must not log full field contents.

Use the request context for all logging. Debug logs may record the resolved
parent field ID, index, and child field IDs without recording replacement
values.

## Alternatives Considered

### Put the replacement value inside `FieldPartialUpdateOp`

Rejected. Milvus already represents row-aligned values in column-based
`FieldData`. A second untyped or nested value carrier would duplicate type,
nullability, vector, and row-alignment rules and require SDK-specific
conversion logic.

### Interpret `REPLACE + path` as positional replacement

Rejected because an old server can ignore the unknown `path` field and execute
whole-field `REPLACE`. A distinct enum value fails closed.

### Put the full path in `field_name`

Rejected. It conflicts with literal dynamic JSON keys, breaks schema field
lookup, and makes operation-to-column alignment ambiguous.

### Use one path per entity

Not supported by this design. `FieldData` is column-oriented, while per-entity
paths would create a second independently aligned row vector and substantially
increase validation, SDK, and overlap complexity.

### Allow multiple paths per parent field

Deferred. It requires a repeated mutation/value pairing format and complete
duplicate, ancestor/descendant, and sibling overlap semantics. This design
represents a same-index multi-child update as one `[index]` operation with a
Struct child mask. Other cases use explicit requests.

### Require all Struct children for `[index]`

Rejected after design discussion. Requiring a complete Struct forces callers
to resend known but unchanged values and makes the one-path restriction
unnecessarily limiting. A non-empty, request-wide child mask has unambiguous
column alignment and preserves omitted children.

### Let SDKs automatically split unsupported combinations

Rejected. Splitting changes atomicity, retry, ordering, and partial-success
behavior. The application must make that choice explicitly.

### Encode a null Struct as all-null children

Rejected. Those states are semantically different without a shared
Struct-element validity bitmap.

## Test Plan

### Proto and compatibility

- Verify `PATH_REPLACE = 3` and `path = 3` are generated for all supported
  languages.
- Verify existing op values and field numbers are unchanged.
- Send a serialized `PATH_REPLACE` request to an old validation implementation
  and verify it rejects the unknown enum instead of replacing the whole field.
- Verify unknown `path` alone cannot activate positional behavior.

### Proxy unit tests

- Parse every valid canonical path and reject every grammar extension excluded
  from this design.
- Resolve scalar Array and StructArray paths against schema IDs.
- Reject unknown fields, unknown children, wrong parent types, primary keys,
  duplicate ops, duplicate `FieldData`, and missing operands.
- Verify request-wide path and Struct child-mask alignment for multiple rows.
- Replace first, middle, and last elements for all supported scalar Array
  element types.
- Replace scalar and vector Struct children.
- Replace one child, multiple children, and every child through `[index]`.
- Verify `[index][child]` rejects extra or missing children.
- Verify non-target elements and children are byte-for-byte unchanged where
  their protobuf representation permits that assertion.
- Verify parent length and Struct child offsets never change.
- Reject nullable parent rows, null operand parent rows, explicit null scalar
  and vector operands, and immediate-element `valid_data`.
- Reject missing PKs, including a mixed request where some PKs exist and some
  do not.
- Map shuffled query results back to original request order by primary key.
- Treat malformed retrieved Struct alignment as a system error.
- Ensure no write dispatch occurs after any pre-dispatch validation failure.
- Exercise both legacy msgstream and StreamingNode-enabled routing after
  materialization.

### REST v2 tests

- Accept `PATH_REPLACE` plus `path` and preserve the relative string.
- Reject missing path, path on another op, unsupported op, wrong payload shape,
  and heterogeneous Struct child masks.
- Verify a literal dynamic key containing brackets remains dynamic data.

### SDK tests

For every SDK that exposes partial-update field operations:

- serialize the same relative path grammar;
- preserve one singleton operand container per entity;
- preserve array element order;
- reject or surface heterogeneous Struct masks without silently reshaping
  values;
- avoid automatic request splitting; and
- propagate server parameter errors consistently.

Python integration tests cover the row-oriented examples in this document. Go
integration tests cover the column-oriented builder.

### End-to-end tests

- Scalar Array replacement on rows with different Array lengths.
- StructArray scalar-child, vector-child, subset, and full-element replacement.
- Multiple parent fields with independent paths in one request.
- Dynamic JSON literal key coexistence.
- Query after compaction still returns the same logical element order and
  replacement result.
- CDC receives the materialized full mutation with no path-specific protocol
  dependency.

CAS-conflict and multi-VChannel partial-success tests belong to the separate
concurrency design, not this test plan.

## Implementation Plan

1. **Public proto**
   - Add `PATH_REPLACE` and `path` in `milvus-proto/proto/schema.proto`.
   - Regenerate all language bindings.
   - Release and consume the new proto revision.
2. **Proxy validation and planning**
   - Extend `internal/proxy/task_upsert_partial_op.go` with strict path parsing,
     schema resolution, duplicate `FieldData` detection, and operand-shape
     validation.
   - Return resolved request-local plans instead of only enum values.
3. **Proxy merge**
   - Extend `internal/proxy/task_upsert.go` and focused helpers to reject missing
     PKs, apply row-aligned positional replacements, and reconstruct complete
     parent `FieldData`.
   - Keep Struct subset operands ahead of normal Struct flattening.
4. **REST v2**
   - Add `path` and `PATH_REPLACE` handling in
     `internal/distributed/proxy/httpserver/request_v2.go`.
5. **Go SDK**
   - Add `WithPathReplace(fieldName, path)` while preserving the existing
     `WithFieldPartialOp` API.
6. **Python and other SDKs**
   - Add `FieldOp.path_replace(path)` or the idiomatic equivalent, all emitting
     the same proto representation.
7. **Documentation and examples**
   - Update partial-upsert and StructArray user documentation.
   - Update the linked issue to record the approved Struct subset refinement.
8. **Validation**
   - Run generated-proto checks, targeted Proxy/REST v2/SDK tests, full
     affected package tests, and integration tests from the designated
     worktree.

## Rollout

1. Merge and release the `milvus-proto` change.
2. Upgrade Milvus and regenerate against the new proto revision.
3. Deploy all Proxies before advertising SDK support.
4. Release SDK helpers with a documented minimum server version.
5. Monitor stable error categories and merge latency during initial rollout.

No feature flag is required for correctness because an old Proxy fails closed
on the unknown enum. A deployment may still gate SDK exposure until all Proxies
are upgraded.

## Acceptance Criteria

- `PATH_REPLACE [index]` replaces one existing scalar Array element without
  changing order or length.
- `PATH_REPLACE [index][child]` replaces exactly one existing Struct child.
- `PATH_REPLACE [index]` replaces a non-empty request-wide subset or all Struct
  children and preserves omitted children.
- The same parent field has at most one op/path in a request; different parent
  fields may be updated together.
- Every targeted primary key exists, every parent is non-null, and every index
  is in range before write dispatch begins.
- Every replacement value is concrete and non-null; this feature does not add
  element-level nullability.
- Literal bracketed dynamic JSON keys are never parsed as paths.
- SDK, REST v2, and protobuf use the same relative path string and dense
  singleton operand convention.
- Old Proxies reject the new enum and cannot silently perform whole-field
  replacement.
- Proxy materializes complete `FieldData`; downstream DML and storage protocols
  remain unchanged.
- All tests listed for the Milvus implementation and participating SDKs pass.

## Remaining Review Actions

- Select a Primary Approver and an Independent Approver from the repository
  maintainer list.
- Hold the required Design Review meeting and record its date.
- Update the issue text after approval so its Struct subset semantics match
  this document.
- Keep the CAS/VChannel design and ownership separate; link it here only after
  that design has a stable artifact.
