# Design Document: Scalar & Struct Array Element Match (`MATCH_*` / `element_filter`)

**Branch**: `feat/array-element-match`
**Date**: June–July 2026
**Scope**: Expression grammar + planparserv2 (Go) and segcore (C++), brute-force
execution only. Adds a `MatchColumnInfo` locator to `plan.proto` on a new tag; the legacy
`MatchExpr.struct_name` stays on tag 1 `[deprecated]` with proxy dual-write and querynode
fallback for rolling-upgrade wire compatibility (see §7). The **nested
(element-level) layout for plain-ARRAY scalar indexes** (index build/load
pipeline and its per-segment marker) lands in a follow-up PR (§5). No RPC/SDK
changes.

Extends the quantified element-match capability (`MATCH_ANY/ALL/LEAST/MOST/EXACT`
+ `element_filter`) uniformly across the two physical array-shaped containers —
scalar arrays and struct arrays. Sections 1–4 describe the query-side design;
§5 is a placeholder for the follow-up nested array index; §6 covers
testing; §7 compatibility; §8 performance trade-offs.

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
5. **Correct coexistence with array indexes** — this PR executes the element
   predicate by brute force over raw data. A **legacy row-level** array index
   pinned on the field must never serve an element-level predicate: exec-path
   guards detect the mismatch and fall back to brute force (§4.6). Building
   plain-ARRAY scalar indexes as nested (element-level) indexes — so an index
   on the array field accelerates the element predicate through the same
   element-level lookup struct-array sub-field indexes already use — lands in
   a follow-up PR (§5).
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
execution engine. The **index build side is untouched by this PR**: switching
plain scalar-ARRAY indexes to a nested element-level layout (guarded by an
explicit per-segment marker so legacy and nested indexes coexist safely) lands
in a follow-up PR (§5).

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
                 │   │   or nested-index lookup when one is loaded)
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

The index build/load pipeline (datacoord build decision, index worker, index
meta/snapshot, `IndexFactory` load routing) is unchanged in this PR — it moves
to the nested-array-index follow-up (§5).

### 2.3 What this feature does NOT change

- Struct-array `MATCH_*` / `element_filter` quantifier semantics and code paths —
  with **one deliberate three-valued-logic fix**: rows that predate a field
  (schema-evolution backfill) used to surface as all-zero offsets, i.e. as empty
  arrays; they are now recorded as **NULL rows** (all-NULL offsets), so
  `MATCH_ALL` no longer vacuously matches them and `not array_contains(...)` no
  longer includes them (§4.3, pinned by `MatchTreatsBackfilledRowsAsNull`).
- `array_contains` / `array_contains_all` / `array_contains_any` /
  `array_length` semantics.
- RPC or SDK wire format — the client sends expression strings, not plan protos;
  the only internal proto change is the `plan.proto` `MatchExpr` locator (not
  client-facing).
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
Building plain-ARRAY indexes nested (behind a per-segment marker, with its full
build→load plumbing) lands in a follow-up PR (§5); in this PR the element
predicate over a scalar array therefore executes by brute force in practice.

When the loaded index is a **legacy row-level** array index, an element-level
predicate cannot use it: its postings map
values to *row* ids, while the element predicate consumes *element* offsets.
Exec-path guards in `UnaryExpr.cpp`, `TermExpr.cpp`, `BinaryRangeExpr.cpp` and
`BinaryArithOpEvalRangeExpr.h` detect this
(`element_level_ && exec_path_ == ScalarIndex && !PinnedIndexIsNested()`) and
fall back to brute force (`ExprExecPath::RawData`), so today's row-level array
indexes keep answering correctly. The mirror-image guard — row-level
whole-array ops over a *nested* index — ships with the nested-index follow-up.

There is still no quantifier- or selectivity-aware path selection (a cost
heuristic is deferred to a follow-up).

---

## 5. Nested array index & the per-segment marker (follow-up PR)

Nested-index acceleration for scalar-array element predicates — the nested
(element-level) build layout for plain-ARRAY scalar indexes, the per-segment
nested-index marker and its build→load plumbing, and the exact whole-array
equality served from a nested index — lands in a follow-up PR, which will
fill in this section.

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
- **Legacy-index fallback**:
  `ScalarArrayMatchIndexConsistency.NonNestedIndexFallsBackToBruteForce` pins
  the exec-path guard of §4.6 — a legacy row-level array index must produce
  exactly the brute-force row sets. The nested-index build/load/consistency
  suites (`ScalarArrayMatchIndex*`) ship with the nested-index follow-up PR.
- The pre-existing struct-array `MatchExprTest` is unchanged in behavior; its
  growing-segment tests were additionally strengthened with full-recall
  verification.

### 6.3 End-to-end

The marker-plumbing Go tests (datacoord build decision, meta round-trip,
snapshot restore guard, reserved-param rejection) ship with the nested-index
follow-up PR.

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
| Segment types | sealed, growing |
| Predicate forms | simple comparison, compound `&&`/`||`, ternary range, `like`, `in`/`not in` |
| Edge cases | empty array (vacuous truth), NULL row (three-valued: result is NULL/excluded, not a vacuous match), backfilled rows = NULL (not `[]`), threshold boundaries (0, N) |
| Negatives | `$[sub]` on scalar array, bare `$`, non-array field, nesting, row-level/constant predicates |
| Index | legacy (non-nested) index → brute-force fallback asserted equal to brute force; nested-index acceleration coverage ships with the follow-up PR |

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

### 7.2 Nested-index upgrade / rollback matrix (follow-up PR)

This PR builds no nested indexes for plain-ARRAY fields, so there is no
nested-index upgrade or rollback surface yet; the full upgrade/rollback matrix
ships with the nested-index follow-up PR (§5).

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

- **Brute force only**: in this PR the element predicate over a scalar array
  always executes by brute force over raw data — cost linear in the total
  element count of the addressed rows. The run-based element resolution in
  `ProcessElementLevelByOffsets` (one row resolution per consecutive element
  run) keeps the constant factor low, but no index acceleration exists yet.
- **Legacy-index fallback is correctness-over-speed**: when a row-level array
  index is pinned on the field, the element-level guards (§4.6) release the
  pin and scan raw data instead — deliberately slower than a (wrong) index
  answer. Nested-index acceleration, its build-cost trade-offs (one indexed
  entry per element) and the whole-array-op interactions move to the follow-up
  PR (§5).
- **No benchmarks in this PR**: all included tests are correctness tests;
  benchmarking the brute-force path and the future nested build is follow-up
  work.
