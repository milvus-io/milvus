# Design Document: Scalar Array Element Match (`MATCH_*` / `element_filter`)

**Branch**: `feat/scalar-array-match`
**Date**: June 2026
**Scope**: Expression grammar + planparserv2 (Go), segcore (C++). No proto/RPC changes.

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
5. **Index acceleration** — reuse the existing nested scalar-index path so an
   inverted index on the array field accelerates the element predicate.
6. **No regression** — struct-array `MATCH_*` behavior and `array_contains*` /
   `array_length` are unchanged; no proto/RPC/SDK wire changes.

### 1.3 Design Principle

**Treat a scalar array as a struct array that has exactly one implicit
sub-field — the element value itself.** Under this lens, `$` is the scalar
analogue of `$[subField]`, and the entire existing struct-array execution
pipeline (row↔element offset mapping, element-predicate evaluation, quantifier
counting, nested-index lookup) is reused unchanged. The feature is therefore
additive plumbing — a new token, an element-self column resolution, and
registration of element offsets for scalar array fields — not a new execution
engine.

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
            planpb.MatchExpr { struct_name = arrayField,   # reused field
                               predicate, match_type, count }
                 ▼  (CGO / proto)
            segcore (C++)
                 │  PhyMatchFilterExpr::Eval
                 │   ├─ Schema::ResolveArrayElementField(struct_name)
                 │   │     scalar ARRAY ─► that field; struct ─► first array sub-field
                 │   ├─ segment->GetArrayOffsets(field_id)   # IArrayOffsets
                 │   ├─ rows → flattened element offsets
                 │   ├─ child predicate Eval (element-level read of $)
                 │   └─ quantify per row: count matches vs MatchType/threshold
                 ▼
            row bitset
```

### 2.2 Component responsibilities

| Layer | Responsibility | Change |
|-------|----------------|--------|
| Grammar (`Plan.g4`) | Lex `$` as `ElementSelf`; accept it as an expr and in range forms | add token + alt |
| Parser (`parser_visitor.go`) | Resolve `$` → element-level `ColumnInfo`; route `MATCH_*`/`element_filter` to the scalar-array vs struct-array path | add resolution + branch |
| Plan proto | Carry the array field name | **reuse** `MatchExpr.struct_name` (no proto change) |
| Segcore — offsets | Build/maintain `IArrayOffsets` for scalar `ARRAY` fields (sealed + growing) | extend registration |
| Segcore — resolution | Resolve a `MATCH_*` target that is a scalar array OR a struct array | `Schema::ResolveArrayElementField` |
| Segcore — execution | Read element values, evaluate predicate, quantify | **reuse** existing `MatchExpr.cpp` / `ProcessElementLevelByOffsets` |

### 2.3 What this feature does NOT change

- Struct-array `MATCH_*` / `element_filter` semantics and code paths.
- `array_contains` / `array_contains_all` / `array_contains_any` / `array_length`.
- Plan proto, RPC, or SDK wire format (`MatchExpr.struct_name` is reused as-is).
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

`pred` is any element-level predicate over `$` (comparison, range, `&&`/`||`,
`like`/regex for VarChar elements), mirroring what struct-array predicates allow
over `$[subField]`.

`element_filter(arr, pred)` produces an **element-level** bitset and is reserved
for element-level vector search; it is **not** a standalone row-level filter
(row-level scalar-array filtering uses `MATCH_*`). This matches the existing
struct-array contract.

---

## 4. Component Design

### 4.1 Grammar (`internal/parser/planparserv2/Plan.g4`)

- New lexer token `ElementSelf: '$';`. ANTLR longest-match keeps `$meta`
  (`Meta`) and `$[ident]` (`StructSubFieldIdentifier`) intact; a bare `$`
  matches `ElementSelf`.
- New expr alternative `| ElementSelf  # ElementSelf`.
- `ElementSelf` added to the `Range` / `ReverseRange` identifier set so
  `lo < $ < hi` parses.
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
- Wire format: `planpb.MatchExpr.struct_name` carries the scalar array field
  name; the scalar-vs-struct distinction is re-derived at execution time. **No
  proto change.**

### 4.3 Segcore — element offsets for scalar arrays

`MATCH_*` execution needs an `IArrayOffsets` (row → flattened-element-range
mapping) for the target field. This already exists for struct array fields;
the feature registers it for scalar `DataType::ARRAY` fields too.

- **Sealed** (`ChunkedSegmentSealedImpl`): in the array-offsets registration
  path, when a field is a scalar `DataType::ARRAY` (no struct name), build
  `ArrayOffsetsSealed` keyed by `field_id`. `ArrayOffsetsSealed::BuildFromSegment`
  already iterates chunks via the `ArrayView` path (`array_views[i].length()`),
  so it works for scalar arrays without change.
- **Growing** (`SegmentGrowingImpl`): lazily create an `ArrayOffsetsGrowing` for
  scalar array fields (at init, and on `Reopen` for fields added by schema
  evolution), tracked in `struct_representative_fields_`, and fed per-row
  element lengths at insert time (`ExtractArrayLengths` / `…FromFieldData`,
  with explicit null-row → length-0 handling).

### 4.4 Segcore — field resolution

`Schema::ResolveArrayElementField(name)` resolves a `MATCH_*` target:

- if a field named `name` exists and is `DataType::ARRAY` / `VECTOR_ARRAY` →
  return it (scalar / vector array);
- else → `GetFirstArrayFieldInStruct(name)` (struct array, existing behavior);
- if `name` exists but is a non-array type → a clear error.

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

### 4.6 Index

Because the child predicate is an ordinary `SegmentExpr`, it transparently uses
the **nested scalar index** (Tantivy inverted / `ScalarIndexSort`) on the array
field when one exists and the cost heuristic selects it — the same
`CanUseNestedIndex()` path used by struct arrays. No index code changes; an
inverted index on a scalar array field accelerates `MATCH_*` predicates.

---

## 5. Testing

### 5.1 Parser (`plan_parser_v2_test.go`)

- `TestScalarArrayMatchAny`: asserts the produced plan (`MatchExpr.struct_name`,
  `match_type`, and the predicate's element-level `ColumnInfo` with the array's
  element `data_type`).
- `TestScalarArrayMatchVariants`: all five `MATCH_*` + `element_filter`, compound
  predicates, range form, Int64 and VarChar element arrays, and negatives
  (`$[sub]` on a scalar array; bare `$` outside a match; non-array field;
  nesting).

### 5.2 Segcore

- `test_chunk_segment.ScalarArrayOffsetsBuiltForArrayField`: scalar array
  `IArrayOffsets` build (row count, total element count, per-row range).
- `ScalarArrayMatchExprTest` (parameterized over **sealed** and **growing**),
  Int64 and VarChar element arrays, covering:
  - all five operators (`MATCH_ANY/ALL/LEAST/MOST/EXACT`);
  - compound element predicates (`$ > 60 && $ < 90`, `$ >= 40 && $ <= 100`,
    `$ == "x" || $ == "y"`) and the range form (`60 < $ < 90`);
  - the **empty-array vacuous-truth** edge (empty row included by `MATCH_ALL`,
    excluded by `MATCH_ANY`).
  - Each case asserts the **exact** matched-row set via a full-recall Retrieve
    (no false positives *and* no false negatives).
- The pre-existing struct-array `MatchExprTest` is unchanged in behavior; its
  growing-segment tests were additionally strengthened with full-recall
  verification.

### 5.3 Coverage summary

| Dimension | Covered |
|-----------|---------|
| Operators | ANY, ALL, LEAST, MOST, EXACT (+ element_filter parse) |
| Element types | Int64, VarChar (parser also exercises element-type plumbing) |
| Segment types | sealed, growing |
| Predicate forms | simple comparison, compound `&&`/`||`, ternary range |
| Edge cases | empty array (vacuous truth), threshold boundaries (0, N) |
| Negatives | `$[sub]` on scalar array, bare `$`, non-array field, nesting |
| Index | reuses struct-array nested-index path (no new code) |

---

## 6. Compatibility & Risk

- **Wire compatibility**: no proto/RPC/SDK changes; `MatchExpr.struct_name` is
  reused to carry the array field name, so the message format is unchanged.
- **Rolling upgrade**: the *new scalar-array syntax* is NOT understood by old
  QueryNodes. An old QueryNode resolves `MatchExpr.struct_name` via
  `GetFirstArrayFieldInStruct` (struct-array only), so a scalar array name such
  as `MATCH_ANY(scores, $ > 90)` fails to resolve there. The format is
  wire-compatible but the semantics are not — the new syntax must only be used
  after **all** QueryNodes are upgraded. Struct-array expressions are unaffected.
  No version gating is added: this is a new query syntax users opt into post-upgrade.
- **Struct arrays**: untouched — same parser path (default branch) and same
  execution; regression-guarded by the existing `MatchExpr` test suite.
- **Schema evolution**: scalar array fields added via `AlterCollectionSchema`
  get their `IArrayOffsets` registered on growing-segment `Reopen`.
- **Build note (unrelated to this feature)**: native macOS/clang-19 builds
  require the upstream `rapidjson/cci.20230929` pin (PR #50664); this feature
  carries no build-system change of its own.
