# Design Document: Scalar & Struct Array Element Match (`MATCH_*` / `element_filter`)

**Branch**: `feat/array-element-match`
**Date**: June–July 2026
**Scope**: Expression grammar + planparserv2 (Go), segcore (C++), and the scalar-index
build/load pipeline (datacoord, index worker, index meta, snapshot manifest, C++
`IndexFactory`). Adds a `MatchColumnInfo` locator to `plan.proto` on a new tag; the legacy
`MatchExpr.struct_name` stays on tag 1 `[deprecated]` with proxy dual-write and querynode
fallback for rolling-upgrade wire compatibility (see §7). Introduces a **nested
(element-level) layout for plain-ARRAY scalar indexes** behind a per-segment
`is_nested_index` marker (see §5). No RPC/SDK changes.

Extends the quantified element-match capability (`MATCH_ANY/ALL/LEAST/MOST/EXACT`
+ `element_filter`) uniformly across the two physical array-shaped containers —
scalar arrays and struct arrays. Sections 1–4 describe the query-side design;
§5 describes the nested array index and its per-segment marker; §6 covers
testing; §7 compatibility and upgrade/rollback; §8 performance trade-offs.

---

## 1. Overview

### 1.1 Motivation

Scalar `ARRAY` fields today support only **equality / membership** filtering:
`array_contains`, `array_contains_all`, `array_contains_any`, plus `array_length`
and positional access `arr[i]`. There is no way to express a **quantified
comparison over the elements** of a scalar array — e.g. "any element `> 90`",
"all elements `>= 60`", or "at least 2 elements `== 100`".

Struct array fields (array-of-struct) already support exactly this via
`MATCH_ANY/MATCH_ALL/MATCH_LEAST/MATCH_MOST/MATCH_EXACT` and `element_filter`,
using `$[subField]` to reference a struct element's sub-field. This feature
brings the **same quantified element-filtering capability to plain scalar
arrays**, closing the functional gap between scalar arrays and struct arrays.

Example:

```text
MATCH_ANY(scores, $ > 90)                       # some element > 90
MATCH_ALL(scores, $ >= 60)                       # every element >= 60
MATCH_LEAST(scores, $ == 100, threshold=2)       # at least 2 elements == 100
MATCH_MOST(scores, $ > 90, threshold=1)          # at most 1 element > 90
MATCH_EXACT(scores, $ == 100, threshold=3)       # exactly 3 elements == 100
MATCH_ANY(tags, $ == "x")                         # VarChar element arrays too
MATCH_ANY(scores, $ > 60 && $ < 90)              # compound element predicate
```

### 1.2 Key Requirements

1. **Quantified element filtering on scalar arrays** — the five `MATCH_*`
   operators with the same semantics they have for struct arrays.
2. **New element-self token `$`** — references the array element itself
   (scalar arrays have no sub-fields, so `$[x]` is rejected for them).
3. **All scalar element types** — Bool / Int8 / Int16 / Int32 / Int64 / Float /
   Double / VarChar.
4. **Both segment types** — sealed (chunked) and growing.
5. **Index acceleration** — plain-ARRAY scalar indexes (BITMAP / INVERTED / the
   sort family, incl. HYBRID's internal resolution) are **built as nested
   (element-level) indexes** once the cluster's scalar index engine version
   resolves to ≥ 5, so an index on the array field accelerates the element
   predicate through the same element-level lookup struct-array sub-field
   indexes already use (§5). On a legacy row-level array index, `MATCH_*`
   falls back to brute force (§4.6).
6. **No regression** — struct-array `MATCH_*` behavior and `array_contains*` /
   `array_length` are unchanged (one deliberate NULL-semantics fix aside, §2.3).
   `plan.proto` gains a `MatchColumnInfo` locator on a new tag (`struct_name`
   kept deprecated on tag 1 for wire compat, §7); no RPC/SDK changes.

### 1.3 Design Principle

**Treat a scalar array as a struct array that has exactly one implicit
sub-field — the element value itself.** Under this lens, `$` is the scalar
analogue of `$[subField]`, and the entire existing struct-array execution
pipeline (row↔element offset mapping, element-predicate evaluation, quantifier
counting, nested-index lookup) is reused unchanged. The query-side feature is
therefore additive plumbing — a new token, an element-self column resolution,
and registration of element offsets for scalar array fields — not a new
execution engine. The **index build side, by contrast, does change**: plain
scalar-ARRAY indexes switch to a nested element-level layout, guarded by an
explicit per-segment marker so legacy and nested indexes coexist safely (§5).

---

## 2. Architecture Overview

### 2.1 Data flow

```
expr string ──► planparserv2 (Go)
                 │  MATCH_*/element_filter(arrayField, <predicate over $>)
                 │  └─ $  ──► element-level ColumnInfo
                 │            { field_id = arrayField, data_type = ElementType,
                 │              is_element_level = true }
                 ▼
            planpb.MatchExpr { column = MatchColumnInfo{field_id, field_name},
                               predicate, match_type, count }
                 ▼  (CGO / proto)
            segcore (C++)
                 │  PhyMatchFilterExpr::Eval
                 │   ├─ Schema::ResolveArrayElementField(field_name)
                 │   │     scalar ARRAY ─► that field; struct ─► first array sub-field
                 │   ├─ segment->GetArrayOffsets(field_id)   # IArrayOffsets
                 │   ├─ rows → flattened element offsets
                 │   ├─ child predicate Eval (element-level read of $,
                 │   │   or nested-index lookup when one is loaded, §5)
                 │   └─ quantify per row: count matches vs MatchType/threshold
                 ▼
            row bitset (NULL rows masked off, §3.2/§4.3)
```

### 2.2 Component responsibilities

| Layer | Responsibility | Change |
|-------|----------------|--------|
| Grammar (`Plan.g4`) | Lex `$` as `ElementSelf`; accept it as an expr and in range forms; `threshold` becomes a soft keyword | add token + alt, drop `THRESHOLD` token |
| Parser (`parser_visitor.go`) | Resolve `$` → element-level `ColumnInfo`; route `MATCH_*`/`element_filter` to the scalar-array vs struct-array path; validate predicate shape | add resolution + branch + `validateMatchPredicateShape` |
| Plan proto | Locate the array target | **new** `MatchColumnInfo` locator on tag 5; `struct_name` kept deprecated on tag 1, dual-written (wire compat, §7) |
| Segcore — offsets | Build/maintain `IArrayOffsets` for scalar `ARRAY` fields (sealed + growing), incl. per-row validity | extend registration + row-valid bitmap |
| Segcore — resolution | Resolve a `MATCH_*` target that is a scalar array OR a struct array | `Schema::ResolveArrayElementField` |
| Segcore — execution | Read element values, evaluate predicate, quantify | **reuse** existing `MatchExpr.cpp` / `ProcessElementLevelByOffsets` |
| Datacoord — build decision | Decide the per-segment nested marker for plain-ARRAY index builds | `prepareJobRequest` → `typeutil.IsNestedArrayIndex` (§5.3) |
| Index worker (datanode) | Re-derive the marker post-clamp, build nested or legacy, echo the marker | `normalizeNestedIndexMarker` (§5.3) |
| Index meta / snapshot | Persist, distribute and restore the marker | `SegmentIndex.IsNestedIndex`, `querypb.FieldIndexInfo`, Avro manifest v5 (§5.3–5.4) |
| Segcore — index load | Route nested vs legacy loader **purely by the marker** | `IndexFactory::CreateScalarIndex` (§5.3) |

### 2.3 What this feature does NOT change

- Struct-array `MATCH_*` / `element_filter` quantifier semantics and code paths —
  with **one deliberate three-valued-logic fix**: rows that predate a field
  (schema-evolution backfill) used to surface as all-zero offsets, i.e. as empty
  arrays; they are now recorded as **NULL rows** (all-NULL offsets), so
  `MATCH_ALL` no longer vacuously matches them and `not array_contains(...)` no
  longer includes them (§4.3, pinned by `MatchTreatsBackfilledRowsAsNull`).
- `array_contains` / `array_contains_all` / `array_contains_any` /
  `array_length` semantics. Their execution over a **nested** index changes
  internally (element→row reduction inside the index func, §5.1), but results
  are identical.
- RPC or SDK wire format — the client sends expression strings, not plan protos;
  only internal protos change (`plan.proto` `MatchExpr` locator; the
  `is_nested_index` marker on `workerpb`/`indexpb`/`querypb`/`indexcgopb`
  messages — none are client-facing).
- The element-value read path (`ProcessElementLevelByOffsets`) — already generic.

---

## 3. Syntax Design

### 3.1 The `$` element-self token

`$` denotes the array element currently under evaluation inside a
`MATCH_*` / `element_filter` predicate. It is the scalar-array counterpart of the
struct-array `$[subField]`:

- `$` — the element value (scalar arrays).
- `$[subField]` — a sub-field of a struct element (struct arrays).

Mutual exclusivity is enforced by the parser:

- `$[subField]` inside a scalar-array `MATCH_*` → error
  *"scalar array element has no sub-fields; use `$` instead of `$[subField]`"*.
- bare `$` outside any `MATCH_*`/`element_filter` → error
  *"$ can only be used inside MATCH_*/element_filter on a scalar array field"*.

`$` is also accepted in range expressions: `MATCH_ANY(scores, 60 < $ < 90)`.

### 3.2 Operators (identical semantics to struct arrays)

| Operator | Matches a row iff |
|----------|-------------------|
| `MATCH_ANY(arr, pred)` | ≥ 1 element satisfies `pred` |
| `MATCH_ALL(arr, pred)` | all elements satisfy `pred` (**empty array = vacuous true**) |
| `MATCH_LEAST(arr, pred, threshold=N)` | matching-element count ≥ N |
| `MATCH_MOST(arr, pred, threshold=N)` | matching-element count ≤ N |
| `MATCH_EXACT(arr, pred, threshold=N)` | matching-element count == N |

**NULL vs empty**: a NULL row (the array field itself is NULL — distinct from a
real empty array `[]`) is UNKNOWN under **all five** operators and therefore
excluded, following SQL three-valued logic. In particular `MATCH_ALL` treats
`[]` as vacuously true but a NULL row as a non-match. Execution enforces this by
masking NULL rows off the result bitmap (`MaskNullRows` in `MatchExpr.cpp`,
backed by the per-row validity tracked in `IArrayOffsets`, §4.3).

`pred` is any element-level predicate over `$` (comparison, range, `&&`/`||`,
`like`/regex for VarChar elements), mirroring what struct-array predicates allow
over `$[subField]`. The predicate must actually reference the element — see the
shape validation in §4.2.

`element_filter(arr, pred)` produces an **element-level** bitset and is reserved
for element-level vector search; it is **not** a standalone row-level filter
(row-level scalar-array filtering uses `MATCH_*`). This matches the existing
struct-array contract.

**Container divergences (intentional, pinned by tests)**:

- `element_filter` accepts only physical array columns (scalar/struct array) —
  it works through `IArrayOffsets`.
- `array_contains_all` on a struct sub-field desugars at parse time into
  `MATCH_ANY(...) && MATCH_ANY(...)` conjuncts, so it cannot accept a template
  placeholder list (template values are filled after parsing, while the struct
  path must expand the list during parsing). An empty list has **IS NOT NULL
  semantics** on both containers: vacuously true for every REAL array
  (including an empty `[]`), UNKNOWN → excluded for a NULL row (pg: `@>` is
  strict, `NULL @> '{}'` yields NULL, not true). The struct desugar emits the
  same `NullExpr IsNotNull` plan as `struct_array is not null`; the scalar
  runtime empty-list path evaluates per-row instead of returning blanket true.
  `contains_any(x, [])` stays definite FALSE for every row (zero comparisons,
  pg: `NULL = ANY('{}')` is false).

---

## 4. Component Design

### 4.1 Grammar (`internal/parser/planparserv2/Plan.g4`)

- New lexer token `ElementSelf: '$';`. ANTLR longest-match keeps `$meta`
  (`Meta`) and `$[ident]` (`StructSubFieldIdentifier`) intact; a bare `$`
  matches `ElementSelf`.
- New expr alternative `| ElementSelf  # ElementSelf`.
- `ElementSelf` added to the `Range` / `ReverseRange` identifier set so
  `lo < $ < hi` parses.
- The dedicated `THRESHOLD` lexer token is **removed**; `threshold` is now a
  **soft keyword**. The grammar accepts `kw=Identifier ASSIGN IntegerConstant`
  and the visitor checks the identifier text case-insensitively
  (`VisitMatchThreshold`: *"expected 'threshold', got '%s'"*). A collection
  field literally named `threshold` therefore keeps working as an ordinary
  identifier everywhere else in an expression (the old token would have
  shadowed it).
- Generated parser is regenerated via `internal/parser/planparserv2/generate.sh`
  (ANTLR 4.13.2).

### 4.2 Parser (`internal/parser/planparserv2/parser_visitor.go`)

- `getColumnInfoFromElementSelf()` builds an element-level `planpb.ColumnInfo`
  for `$`: `field_id` = the scalar array field, `data_type` = `element_type` =
  the array's element type, `is_element_level = true` (plus pk/partition/
  clustering/nullable flags for symmetry with the struct sub-field path).
- `VisitElementSelf` wraps it as a column expression (mirrors
  `VisitStructSubField`).
- `parseMatchExpr` and `VisitElementFilter` resolve the named field:
  - if it is a scalar `DataType_Array` field → set `currentElementArrayField`
    (so `$` resolves) for the duration of predicate parsing;
  - otherwise → the **existing struct-array path** (`currentStructArrayField`),
    with field-type validation deferred to `$[subField]` resolution (preserving
    pre-existing behavior — no upfront rejection).
- The nesting guard (no `MATCH_*` inside a `MATCH_*`) covers both contexts.
- **Predicate-shape validation** (`validateMatchPredicateShape`) runs for BOTH
  `MATCH_*` and `element_filter`. Segcore evaluates the predicate with
  **element** offsets (`PhyMatchFilterExpr::EvalWithOffsets`), so a row-level
  column reference inside the predicate would be indexed by element offsets —
  garbage results or out-of-bounds reads. The validator therefore rejects at
  parse time: (a) any column reference that is not element-level (`$` /
  `$[subField]` of the MATCH target) — *"predicate can only reference array
  elements ($ or $[subField]); row-level field references are not supported"*;
  (b) constant predicates (`AlwaysTrueExpr`, or no element reference at all) —
  *"predicate must not be constant; it has to test the array element via $ or
  $[subField]"*.
- Wire format: `planpb.MatchExpr.column` (a `MatchColumnInfo`) carries the
  target field id and name. Segcore dispatches by **name only** —
  `Schema::ResolveArrayElementField(field_name)` — for both scalar and struct
  arrays; `field_id` is carried in the locator for logging/diagnostics only.
  The old `struct_name` string stays on tag 1 (deprecated, dual-written) so old
  querynodes still resolve struct-array targets and old-proxy plans still
  parse — see §7.

### 4.3 Segcore — element offsets for scalar arrays

`MATCH_*` execution needs an `IArrayOffsets` (row → flattened-element-range
mapping) for the target field. This already exists for struct array fields;
the feature registers it for scalar `DataType::ARRAY` fields too, and extends
the abstraction with **per-row validity** so NULL rows stay distinct from empty
arrays (§3.2).

- **Sealed** (`ChunkedSegmentSealedImpl`): in the array-offsets registration
  path, when a field is a scalar `DataType::ARRAY` (no struct name), build
  `ArrayOffsetsSealed` keyed by `field_id`. `ArrayOffsetsSealed::BuildFromSegment`
  iterates chunks via the `ArrayView` path (`array_views[i].length()`) as
  before, and **additionally now tracks row validity** (a `row_valid_` bitmap
  populated from the field's valid data) — it no longer works "without change".
  Rows backfilled by schema evolution (the field did not exist when the row was
  written) get `ArrayOffsetsSealed::BuildAllNulls(row_count)` — all-NULL
  offsets — instead of the previous all-zeros shape, so nested-index consumers
  do not mistake historical NULLs for `[]`.
- **Growing** (`SegmentGrowingImpl`): lazily create an `ArrayOffsetsGrowing` for
  scalar array fields (at init, and on `Reopen` for fields added by schema
  evolution), tracked in `struct_representative_fields_`, and fed per-row
  element lengths at insert time (`ExtractArrayLengths` / `…FromFieldData`).
  NULL rows are fed a **length `-1` sentinel** (enforced for nullable fields in
  `SegmentGrowingImpl`) and recorded invalid (`ArrayOffsetsGrowing::Insert`:
  `valid = array_len >= 0`); an empty array is length 0 and stays valid. Rows
  backfilled by schema evolution are recorded via
  `ArrayOffsetsGrowing::InsertNulls`.
- **Row-validity API**: validity is exposed through the thread-safe
  `IArrayOffsets::AndRowValidBitmap(view, start, count)` — it ANDs invalid rows
  off a caller-owned bitmap (taking the growing segment's shared lock) and
  **replaced** the earlier raw-pointer `GetRowValidBitmap` accessor, which was
  unsafe against concurrent growth. `MATCH_*` uses it (via `MaskNullRows` in
  `MatchExpr.cpp`) to exclude NULL rows from every quantifier.

### 4.4 Segcore — field resolution

`Schema::ResolveArrayElementField(name)` resolves a `MATCH_*` /
`element_filter` target **by name**:

- if a field named `name` exists and is `DataType::ARRAY` → return it
  (scalar array);
- if a field named `name` exists with any other type — **including top-level
  `VECTOR_ARRAY`** — → `ThrowInfo(ErrorCode::ExprInvalid, "field '{}' (data
  type {}) does not support MATCH_*/element_filter; expected a scalar array or
  struct array field")`. Element-level vector search does not go through this
  resolution, so vector-array fields are rejected here rather than accepted;
- otherwise → `GetFirstArrayFieldInStruct(name)` (struct array, existing
  behavior).

The three execution sites — `PhyMatchFilterExpr::Eval` (`MatchExpr.cpp`),
`PhyElementFilterBitsNode` and `PhyIterativeElementFilterNode` — switch from
`GetFirstArrayFieldInStruct` to `ResolveArrayElementField`. Everything
downstream (element-offset conversion, child-predicate evaluation via
`ProcessElementLevelByOffsets`, quantifier counting) is unchanged.

### 4.5 Element value read

The child predicate's `ColumnInfo` is element-level (`is_element_level = true`,
`data_type` = element type). `ProcessElementLevelByOffsets` (already present)
maps each flattened element offset → `(row, idx)` via `IArrayOffsets` and reads
the scalar value from the `ArrayView` (sealed) / `Array` (growing) — handling
null elements, VarChar, and empty arrays. No new accessor is required.

### 4.6 Index usage by the element predicate

Because the child predicate is an ordinary `SegmentExpr`, it transparently uses
the scalar index pinned on the array field via the same `CanUseNestedIndex()`
path used by struct arrays — **provided that index is nested (element-level)**.
This PR makes plain-ARRAY indexes build nested behind a per-segment marker;
the layout, the marker and its full build→load plumbing are specified in §5.

When the loaded index is a **legacy row-level** array index (marker
missing/false), an element-level predicate cannot use it: its postings map
values to *row* ids, while the element predicate consumes *element* offsets.
Exec-path guards in `UnaryExpr.cpp`, `TermExpr.cpp`, `BinaryRangeExpr.cpp` and
`BinaryArithOpEvalRangeExpr.h` detect this
(`element_level_ && exec_path_ == ScalarIndex && !PinnedIndexIsNested()`) and
fall back to brute force (`ExprExecPath::RawData`), so a rolling upgrade stays
correct. The mirror-image guard — row-level whole-array ops over a *nested*
index — is described in §5.1/§8.

There is still no quantifier- or selectivity-aware path selection (a cost
heuristic is deferred to a follow-up).

---

## 5. Nested array index & the per-segment marker

This section covers the half of the feature that changes the storage side:
plain-ARRAY scalar indexes switch from a row-level to a nested (element-level)
layout, and a per-segment marker — not version inference — decides which loader
opens which files.

### 5.1 Layout: what "nested" means

A **legacy** array scalar index maps `value → row ids` (a row matches if any
element equals the value — the only question `array_contains` needs). A
**nested** index maps `value → element ids` (positions in the flattened
element stream), which is what `MATCH_*` / `element_filter` predicates consume.

Build routing (`IndexFactory::CreateNestedIndex`):

| Index type | Nested build |
|------------|--------------|
| INVERTED | Tantivy, **one document per element** (`InvertedIndexTantivy` with `is_nested_index = true`; each element is added under a running element offset) |
| BITMAP | `BitmapIndex` keyed by element id (`value → element-id bitmap`) |
| everything else (sort family, incl. HYBRID/AUTOINDEX internal resolution) | `CreateNestedIndexScalarIndexSort` — one `(value, element offset)` entry per element |

Consuming a nested index:

- **Element-level predicates** (`MATCH_*` / `element_filter` children) use the
  element bitset directly; the expression framework slices it per row via
  `IArrayOffsets` (`need_element_slicing` in `Expr.h`).
- **Row-level contains-style ops** (`array_contains*`) still use the index:
  the index func reduces element matches to row hits internally
  (`ProcessIndexChunksWithRowLevel`, `JsonContainsExpr.cpp`) and recovers
  per-row NULL validity from `IArrayOffsets::AndRowValidBitmap` — a nested
  index only exposes element-level validity, and a NULL row has zero elements,
  so `not array_contains(nullable, x)` would otherwise wrongly include NULL
  rows (`Expr.h`, `ProcessIndexChunksImpl`).
- **Row-level whole-array equality** (`arr == [1,2]`, `!=`) is served
  EXACTLY from the nested index via a positional AND
  (`ExecArrayEqualForNestedIndex`): row length == k AND the element bitset of
  `v_i` has bit `s + i` set for every position `i` (element ranges from
  `IArrayOffsets`, NULL rows recovered via `AndRowValidBitmap`). Whole-array
  ops other than Equal/NotEqual, float-element equality, type-mismatched
  targets, and index-only-loaded segments keep the brute-force fallback
  (`CanServeNestedArrayEquality`, decided once in `DetermineExecPath`) — see
  §8.

Legacy and nested indexes **coexist** in a cluster indefinitely (old segments
keep their old files). They are distinguished ONLY by the explicit per-segment
marker below — never by inferring from the index engine version at load time.

### 5.2 Version gate: why 5

`pkg/common/common.go` defines the gate:

```go
CurrentScalarIndexEngineVersion          = int32(5)
MinScalarIndexVersionForNestedArrayIndex = int32(5)
```

> Plain-ARRAY scalar indexes build as NESTED (element-level) indexes so
> MATCH_*/element_filter can consume them directly. The version gate MUST be 5
> (not 3/4): versions 3 and 4 were already reported and persisted by releases
> without nested-array support, so keying nested builds or nested load-routing
> off <=4 would (a) let a mixed cluster hand element-level postings to an old
> querynode and (b) route legacy array indexes persisted with version 3/4 into
> the nested loader.

The **resolved** scalar index engine version used for the build decision is
`ResolveScalarIndexVersion()` (`internal/datacoord/index_engine_version_manager.go`):
the **minimum current version across all registered querynodes**, clamped to
the coordinator's own supported range. Nested builds therefore never start
while any pre-v5 querynode is registered.

### 5.3 The `is_nested_index` marker and its plumbing

**Single source of truth**: `typeutil.IsNestedArrayIndex(field, scalarIndexVersion)`
(`pkg/util/typeutil/schema.go`):

```go
return field.GetDataType() == schemapb.DataType_Array &&
    !IsStructSubField(field.GetName()) &&
    scalarIndexVersion >= common.MinScalarIndexVersionForNestedArrayIndex
```

i.e. *plain ARRAY field + not a struct sub-field + resolved version ≥ 5*. Every
site that decides or persists the marker applies this one rule, enforcing the
invariant **"scalar index version ≥ 5 on a plain ARRAY field ⟺ nested"** even
under version-skewed peers:

```
datacoord  prepareJobRequest (internal/datacoord/task_index.go)
    │   marker := IsNestedArrayIndex(field, ResolveScalarIndexVersion())
    │   CreateJobRequest{ is_nested_index, current_scalar_index_version }
    ▼
index worker  PreExecute → normalizeNestedIndexMarker
    │   (internal/datanode/index/task_index.go)
    │   RE-DERIVES the marker from the field schema + POST-CLAMP version,
    │   overriding the request bit; echoes it in workerpb.IndexTaskInfo
    ▼
datacoord  FinishTask (internal/datacoord/index_meta.go)
    │   model.SegmentIndex.IsNestedIndex  ──►  persisted to etcd
    ▼
distribution  querypb.FieldIndexInfo.is_nested_index
    │   (filled from segment index meta in internal/datacoord/handler.go)
    ▼
querynode load  indexcgopb CreateIndexInfo.is_nested_index
    │   (arrives in C++ as index param "nested_index" /
    │    CreateIndexInfo.nested_array_index)
    ▼
C++  IndexFactory::CreateScalarIndex (internal/core/src/index/IndexFactory.cpp)
        marker true  → CreateNestedIndex (element-level loaders)
        missing/false → CreateCompositeScalarIndex (legacy loader)
```

**Why the worker re-derives** (`normalizeNestedIndexMarker`): the marker drives
the cgo build path, is echoed into the task result, and is persisted with the
segment index meta — so it must be correct in **both skew directions**:

- an **old datacoord** has no `is_nested_index` field in its request proto, but
  can still resolve scalar version 5 from upgraded querynodes. Trusting the
  absent bit would build a row-level index *stamped v5*; any later marker
  re-derivation (the snapshot-restore copy path, §5.4) would then classify it
  nested and route it through the nested loader — silently wrong results;
- a worker **clamped below v5** builds a legacy row-level index and must report
  the marker false regardless of what the request said.

**Load routing is by the marker only.** Missing/false means the legacy
composite loader — including HYBRID/AUTOINDEX, whose file layout only
`HybridScalarIndex` can open — so every old index deterministically loads via
the path it was built with, and element-level `MATCH_*` predicates on such a
segment fall back to brute force (§4.6 guards).

**Marker semantics — the struct sub-field carve-out.** The marker means
*"loading this index requires nested-array-capable (v5) code"*, NOT *"the
physical layout is element-level"*. Struct sub-field indexes (`parent[sub]`,
ARRAY-typed) are physically element-level too, but **every released binary**
routes them to the nested loader **by name** — the `IsStructSubField(name)`
check precedes the marker check in `IndexFactory::CreateScalarIndex` — so they
load correctly everywhere, and their marker MUST stay false. If it were set,
the snapshot-restore version guard (§5.4) would wrongly refuse to place them on
a pre-v5 pool that can in fact load them.

**Reserved index param.** `nested_index` (`common.NestedIndexKey`) is an
internal build/load config key; index params are copied verbatim into that
config, so a user-supplied value would bypass the version gate and
desynchronize the persisted marker from the physical layout.
`indexparamcheck.ValidateIndexParams` rejects it across `IndexParams`,
`UserIndexParams` and `TypeParams`, and is enforced at datacoord `CreateIndex`
and `AlterIndex` (`internal/datacoord/index_service.go`), snapshot restore
(`internal/datacoord/snapshot_manager.go`), and the rootcoord schema-alter DDL
callback (`internal/rootcoord/ddl_callbacks_alter_collection_schema.go`).

### 5.4 Snapshot, copy & restore

- **Manifest**: Avro snapshot manifest format version 5
  (`SnapshotFormatVersion = 5`, `internal/snapshotio/snapshot.go`) adds
  `is_nested_index` (Avro default `false`) to index file entries
  (`AvroSchemaV5`). Manifests written at version ≤ 4 decode with the marker
  false. Losing the marker on the round-trip would make a nested ARRAY index
  load as a legacy composite index — HYBRID/AUTOINDEX then fails hard on the
  missing `INDEX_TYPE` entry.
- **Restore re-derivation**: `syncVectorScalarIndexes`
  (`internal/datacoord/copy_segment_task.go`) re-derives the marker
  **authoritatively** with the same `IsNestedArrayIndex(field, echoed version)`
  rule against the target collection schema, instead of trusting the bit echoed
  by the copy worker — a pre-nested (old) copy worker rebuilds the index info
  field-by-field and silently drops the new field while still echoing scalar
  version 5. It falls back to the worker echo only when the field cannot be
  resolved in the target schema (schema drift).
- **Restore version guard**: `checkNestedArrayIndexVersion`
  (`internal/datacoord/snapshot_manager.go`, called from `RestoreData`) refuses
  to restore a snapshot containing any nested index onto a pool whose resolved
  scalar index engine version is < 5, with an explicit *"please complete the
  rolling upgrade first"* error.

### 5.5 Memory estimation & caches

- **Load estimates** (`ScalarIndexLoadResource`, `IndexFactory.cpp`): a nested
  index has element-count cardinality, so row-count-based formulas undershoot.
  For ARRAY fields with the `nested_index` param set, the mmap **resident**
  estimate of BITMAP and of the sort family is floored to the index file size
  (`resident_bytes = std::max(resident_bytes, index_size_in_bytes)`); legacy
  row-level array indexes keep their row-sized estimates (`BitsetBytes(num_rows)`
  / `SortLegacyAuxBytes(num_rows)`). HYBRID first resolves to its internal index
  type (`ResolveHybridInternalIndexType`) and recurses.
- **Per-element offset cache**: `BitmapIndex` skips `BuildOffsetCache()` for
  nested indexes (`!is_nested_index_` guard, `BitmapIndex.cpp`): the cache is
  sized by the ELEMENT count, heap-resident, absent from the serialized file
  and unaccounted by the load estimator; `Reverse_Lookup` falls back to the
  bitmap-map scan.
- **Expr-result disk cache**: element-level result bitmaps (size = element
  count, produced by `MATCH_*`/`element_filter` child predicates over nested
  indexes) skip the disk slot (`ExprResCacheManager::Put` rejects
  `result->size() != active_count`, `ExprCache.cpp`): the per-segment disk slot
  size is fixed by the first put, and a size mismatch is treated as a growing
  segment — deleting the file and permanently disabling disk cache for the
  segment. Element-level results stay memory-cached only.

### 5.6 Load-time self-identification (defense in depth)

Independent of the marker, the index files themselves constrain misrouting:

- **BitmapIndex** persists and reads an `is_nested` flag in its file meta
  (`BITMAP_INDEX_IS_NESTED_META`) — it self-identifies on load.
- **Tantivy inverted** reads an `is_nested` meta key — it also self-identifies.
- **The sort family cannot self-identify** — but it fails **loudly**: loading a
  nested-built sort index through the legacy path trips an `AssertInfo` on the
  `idx_to_offsets` blob size (expected `total_num_rows_ * sizeof(int32_t)`, got
  an element-count-sized blob) rather than returning silently wrong results.

This matters for the old-datacoord upgrade window analyzed in §7.2.

---

## 6. Testing

### 6.1 Parser (`plan_parser_v2_test.go`)

- `TestScalarArrayMatchAny`: asserts the produced plan (`MatchExpr.column`'s
  `MatchColumnInfo.field_name`, `match_type`, and the predicate's element-level
  `ColumnInfo` with the array's element `data_type`).
- `TestScalarArrayMatchVariants`: all five `MATCH_*` + `element_filter`, compound
  predicates, range form, Int64 and VarChar element arrays, and negatives
  (`$[sub]` on a scalar array; bare `$` outside a match; non-array field;
  nesting).
- `TestMatchPredicateShapeRejected`: row-level column references and constant
  predicates inside `MATCH_*`/`element_filter` are rejected (§4.2).
- `TestExpr_ThresholdSoftKeyword`: `threshold` works as a match argument AND as
  an ordinary field name (§4.1).
- `TestMatchExprStructNameDualWrite` / `TestMatchColumnInfoArrayOnlyShape`:
  locator dual-write and shape (§7.1).
- `TestMatchTemplatePlaceholders` / `TestElementFilterTemplatePlaceholders` /
  `TestElementFilterRejectsJSON` / `TestExpr_StructArrayContainsDesugar`:
  template values and the container divergences of §3.2.

### 6.2 Segcore

- `test_chunk_segment.ScalarArrayOffsetsBuiltForArrayField`: scalar array
  `IArrayOffsets` build (row count, total element count, per-row range).
- `ScalarArrayMatchExprTest` (parameterized over **sealed** and **growing**),
  Int64 / VarChar / Bool / Float / Double element arrays, covering:
  - all five operators (`MATCH_ANY/ALL/LEAST/MOST/EXACT`);
  - compound element predicates (`$ > 60 && $ < 90`, `$ >= 40 && $ <= 100`,
    `$ == "x" || $ == "y"`), the range form (`60 < $ < 90`), `like` patterns,
    `in` / `not in` element predicates;
  - the **empty-array vacuous-truth** edge (empty row included by `MATCH_ALL`,
    excluded by `MATCH_ANY`) and **NULL-row three-valued** cases
    (`NullableArrayScalarStructAligned`, `NullableNotMatchThreeValued`,
    `AllNullRowsMatchNothing`).
  - Each case asserts the **exact** matched-row set via a full-recall Retrieve
    (no false positives *and* no false negatives).
- **NULL-vs-empty backfill**: `MatchTreatsBackfilledRowsAsNull`
  (`MatchExprTest.cpp`) pins §2.3 — after schema evolution, backfilled rows are
  excluded by `MATCH_ALL` and by `not array_contains(...)`;
  `ScalarArrayMatchNullableIngest.BinlogLoadAndInsertAgree` pins that binlog
  load and live insert agree on NULL-row bookkeeping.
- **Nested index build/load/consistency** (`ScalarArrayMatchIndex*` in
  `MatchExprTest.cpp`): nested INVERTED accelerates `MATCH_*`
  (`NestedInvertedIndexAccelerates`, `NestedInvertedIndexNullable`), nested
  BITMAP and sort-family nullable cases (`NestedBitmapIndexNullable`,
  `NestedScalarSortIndexNullable`), index-only segments
  (`IndexOnlyWithoutRawHasNoOffsets`), and indexed-vs-brute-force equality for
  Int64/VarChar over inverted and sort indexes (`*IndexedEqualsBruteForce`).
  `NonNestedIndexFallsBackToBruteForce` pins the legacy-index fallback of §4.6.
  `NestedArrayIndexRawData.NestedIndexesReportNoRawData`
  (`test_element_filter.cpp`) pins raw-data reporting.
- The pre-existing struct-array `MatchExprTest` is unchanged in behavior; its
  growing-segment tests were additionally strengthened with full-recall
  verification.

### 6.3 Marker plumbing (Go) & end-to-end

- `Test_isNestedArrayIndex` (`internal/datacoord/task_index_test.go`): the
  build-decision rule of §5.3.
- `TestSegmentIndex_MarshalUnmarshal_IsNestedIndex`
  (`internal/metastore/model`): etcd meta round-trip.
- `TestSnapshotManager_CheckNestedArrayIndexVersion`
  (`internal/datacoord/snapshot_manager_test.go`): restore version guard (§5.4).
- `TestValidateIndexParamsReservedNestedIndexKey`
  (`internal/util/indexparamcheck`): reserved-param rejection (§5.3).
- Integration: `tests/integration/scalararraymatch` (`TestScalarArrayMatch`) —
  end-to-end MATCH over scalar arrays.
- Python client: `test_milvus_client_struct_array_match.py` (MATCH family over
  struct arrays incl. nullable rows, empty arrays, `not MATCH_*`, desugar) and
  updated `test_milvus_client_struct_array_nullable.py` (NULL rows excluded
  from every MATCH operator; real `[]` still vacuous).

### 6.4 Coverage summary

| Dimension | Covered |
|-----------|---------|
| Operators | ANY, ALL, LEAST, MOST, EXACT (+ element_filter parse) |
| Element types | Int64, VarChar (+ Bool/Float/Double in segcore; parser also exercises element-type plumbing) |
| Segment types | sealed, growing, index-only (no raw data) |
| Predicate forms | simple comparison, compound `&&`/`||`, ternary range, `like`, `in`/`not in` |
| Edge cases | empty array (vacuous truth), NULL row (three-valued: result is NULL/excluded, not a vacuous match), backfilled rows = NULL (not `[]`), threshold boundaries (0, N) |
| Negatives | `$[sub]` on scalar array, bare `$`, non-array field, nesting, row-level/constant predicates, reserved `nested_index` param |
| Index | nested INVERTED/BITMAP/sort accelerate `MATCH_*` with results asserted equal to brute force; legacy (non-nested) index → brute-force fallback; marker plumbing, snapshot round-trip and restore guard unit-tested |

All tests are **correctness** tests; this PR includes no performance
benchmarks (§8).

---

## 7. Compatibility & Risk

### 7.1 Plan wire format

- **Wire change (no released impact)**: `MatchExpr` gains a
  `MatchColumnInfo` locator carrying the target field ID and name. It is added as
  a **new field** (`MatchColumnInfo column = 5`) while `string struct_name = 1`
  is kept `[deprecated]` on its original tag: `struct_name = 1` already shipped
  on master (struct-array MATCH, #46518), and reusing tag 1 with a different
  type would silently misdecode plans between mixed proxy/querynode builds
  (string and message share wire type 2 — the decoder does not error, it
  misparses). Proxies **dual-write** both locators; the C++ reader
  (`PlanProto.cpp`) uses `column` when present and falls back to `struct_name`
  for plans produced by an old proxy (struct arrays were the only pre-`column`
  target). The client SDK is untouched (it sends expression strings, not plan
  protos). No RPC/SDK changes.
- **Rolling upgrade (syntax)**: the `MATCH_*` scalar-array syntax is understood
  only by upgraded QueryNodes. A new proxy sending such a plan to a
  not-yet-upgraded querynode fails cleanly at execution (the old node resolves
  the dual-written `struct_name` and reports "No array field found in struct")
  — a transient window closed by completing the querynode upgrade.
- **Dual-write sunset**: the `struct_name` dual-write can stop **one minor
  release after every supported rolling-upgrade path guarantees querynodes
  parse `MatchColumnInfo`** (i.e. once the oldest supported upgrade source
  already contains this feature); until then proxies keep writing both
  locators, and the C++ fallback stays.

### 7.2 Nested-index upgrade / rollback matrix

| Scenario | Behavior |
|----------|----------|
| **Forward rolling upgrade** (any order) | Safe. The build gate is the **min-of-current** engine version across registered querynodes (§5.2), so no nested index is ever built while any pre-v5 querynode is registered. Pre-existing etcd meta has no marker → decodes false → legacy loader; existing indexes behave exactly as before. |
| **Old datacoord + upgraded querynodes/workers** (window) | The datacoord resolves v5 from the upgraded querynodes but its proto has no marker field. The worker-side re-derivation (§5.3) guarantees the **build is still nested** and the echo is correct; however the old datacoord's meta model drops the echoed bit, so the **persisted marker is false** and stays false after the datacoord upgrade. On load, the nested files hit the legacy path: BITMAP and INVERTED **self-identify from file meta** and still load correctly; the sort family **fails loudly** (§5.6) — an explicit error, not silent corruption. The copy/restore re-derivation (§5.4) self-heals the marker in meta. |
| **Rollback / old-image scale-up after nested indexes exist** | **NOT detected — operational rule required.** A pre-v5 querynode re-added to the cluster drops the unknown `is_nested_index` proto field and loads nested files through the row-level path; nested INVERTED then returns **silently wrong results** (element ids read as row ids). **Do not downgrade querynodes below the release that ships engine v5 once any nested index has been built.** To roll back anyway: drop/rebuild the array-field indexes first, or restore from a pre-upgrade snapshot. |

### 7.3 Behavior compatibility

- **Struct arrays**: same parser path (default branch) and same execution;
  regression-guarded by the existing `MatchExpr` test suite. One deliberate
  semantic fix ships with this PR (§2.3): schema-evolution-backfilled rows are
  now NULL rows instead of empty arrays, so `MATCH_ALL` / negated
  `array_contains` no longer match them — the SQL-correct behavior.
- **Schema evolution**: scalar array fields added via `AlterCollectionSchema`
  get their `IArrayOffsets` registered on growing-segment `Reopen`; backfilled
  rows are recorded as NULL (§4.3).

---

## 8. Performance trade-offs

- **Whole-array op regression (narrowed to non-equality ops)**: the legacy
  `ExecArrayEqualForIndex` needs `value → row id` postings, and a nested index
  stores `value → element id`, so the two offset spaces disagree. Whole-array
  **Equal/NotEqual** is now served exactly from the nested index by the
  **positional-AND path** (`ExecArrayEqualForNestedIndex`): row length == k
  AND element at position *i* matches *v_i*, computed from per-value element
  bitsets (`In()`, one query per distinct target value, cached per segment)
  plus `IArrayOffsets` row ranges; NULL rows are excluded via
  `AndRowValidBitmap` exactly like brute force. The `UnaryExpr` exec-path
  guard (`!element_level_ && data_type == ARRAY && PinnedIndexIsNested() &&
  !CanServeNestedArrayEquality()` → `RawData`) still forces a **brute-force
  scan** for the remaining unservable shapes: whole-array ops other than
  Equal/NotEqual, float/double element equality (inexact via index),
  type-mismatched targets, and index-only-loaded segments (no
  `IArrayOffsets`). `array_contains*` is NOT affected: it keeps using the
  nested index via the element→row reduction (§5.1).
- **Build cost scales with element fan-out**: a nested build indexes one entry
  per **element** (tantivy: one document per element), so build time and index
  size grow with average array length relative to the legacy row-level layout.
- **Memory admission**: nested BITMAP / sort-family resident estimates are
  floored to index file size (§5.5) — deliberately more conservative than the
  legacy row-sized estimates for the same data.
- **No benchmarks in this PR**: all included tests are correctness tests. The
  regression window above is established from code inspection of the exec-path
  guards, not from measurements; benchmarking the nested build and the
  brute-force fallback is follow-up work.
