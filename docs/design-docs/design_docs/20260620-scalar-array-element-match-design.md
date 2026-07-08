# Design Document: Array & JSON Element Match (`MATCH_*` / `element_filter`)

**Branch**: `feat/array-json-element-match`
**Date**: June–July 2026
**Scope**: Expression grammar + planparserv2 (Go), segcore (C++). Adds a `MatchColumnInfo` locator to `plan.proto` on a new tag; the legacy `MatchExpr.struct_name` stays on tag 1 `[deprecated]` with proxy dual-write and querynode fallback for rolling-upgrade wire compatibility (see §7). No RPC/SDK changes.

Extends the quantified element-match capability (`MATCH_ANY/ALL/LEAST/MOST/EXACT`
+ `element_filter`) uniformly across the three array-shaped containers — scalar
arrays, struct arrays, and **JSON arrays** — and lets scalar/JSON arrays build a
**nested tantivy index** as the element carrier. Sections 1–5 describe the
scalar-array core; §6 describes the JSON-array and nested-index extension.

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
   `array_length` are unchanged. `plan.proto` gains a `MatchColumnInfo` locator
   on a new tag (`struct_name` kept deprecated on tag 1 for wire compat, §7);
   no RPC/SDK changes.

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
            planpb.MatchExpr { column = MatchColumnInfo{field, data_type, nested_path},
                               predicate, match_type, count }
                 ▼  (CGO / proto)
            segcore (C++)
                 │  PhyMatchFilterExpr::Eval
                 │   ├─ Schema::ResolveArrayElementField(column)
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
| Plan proto | Locate the array/JSON target | **new** `MatchColumnInfo` locator on tag 5; `struct_name` kept deprecated on tag 1, dual-written (wire compat, §7) |
| Segcore — offsets | Build/maintain `IArrayOffsets` for scalar `ARRAY` fields (sealed + growing) | extend registration |
| Segcore — resolution | Resolve a `MATCH_*` target that is a scalar array OR a struct array | `Schema::ResolveArrayElementField` |
| Segcore — execution | Read element values, evaluate predicate, quantify | **reuse** existing `MatchExpr.cpp` / `ProcessElementLevelByOffsets` |

### 2.3 What this feature does NOT change

- Struct-array `MATCH_*` / `element_filter` semantics and code paths.
- `array_contains` / `array_contains_all` / `array_contains_any` / `array_length`.
- RPC or SDK wire format — the client sends expression strings, not plan protos; only the internal `plan.proto` `MatchExpr` changes (adds the `MatchColumnInfo` locator on a new tag; `struct_name` stays for old-proxy plans).
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

**Container divergences (intentional, pinned by tests)**:

- `element_filter` accepts only physical array columns (scalar/struct array) —
  it works through `IArrayOffsets`, which JSON arrays (parsed per document at
  evaluation time) do not have. JSON users express the same semantics with
  `MATCH_ANY(json["path"], pred)`; the parser rejects `element_filter` on a
  JSON field.
- `array_contains_all` on a struct sub-field desugars at parse time into
  `MATCH_ANY(...) && MATCH_ANY(...)` conjuncts, so it cannot accept a template
  placeholder list (template values are filled after parsing; scalar/JSON
  `contains_all` take the runtime `JSONContainsExpr` path, which has no struct
  implementation in segcore). An empty list has **IS NOT NULL
  semantics** on all three containers: vacuously true for every REAL array
  (including an empty `[]`), UNKNOWN → excluded for a NULL row (pg: `@>` is
  strict, `NULL @> '{}'` yields NULL, not true) and for a JSON path that does
  not resolve to a real array. The struct desugar emits the same `NullExpr
  IsNotNull` plan as `struct_array is not null`; the scalar/JSON runtime
  empty-list path evaluates per-row instead of returning blanket true.
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
- Wire format: `planpb.MatchExpr.column` (a `MatchColumnInfo`) carries the target
  field id/name, `data_type` (ArrayOfStruct / Array / JSON), element type, and JSON
  `nested_path`; scalar-vs-struct-vs-JSON dispatch reads `data_type`. The old
  `struct_name` string stays on tag 1 (deprecated, dual-written) so old
  querynodes still resolve struct-array targets and old-proxy plans still
  parse — see §7.

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
field when one exists — there is no quantifier- or selectivity-aware
path selection yet (a cost heuristic is deferred to a follow-up) — the same
`CanUseNestedIndex()` path used by struct arrays. Array scalar indexes are now
built as nested (element-keyed) — see §6.2 for the index-layer changes; an
inverted index on a scalar array field accelerates `MATCH_*` predicates.

**HYBRID/AUTOINDEX substitution (intentional)**: `CreateNestedIndex` special-
cases INVERTED and BITMAP; every other requested type — including HYBRID, the
AUTOINDEX default for numeric/varchar arrays — falls through to the nested
sort family (`ScalarIndexSort`/`StringIndexSort`). Build and load route
identically off the persisted marker, so this is internally consistent;
`DESCRIBE INDEX` keeps reporting the requested type (HYBRID/AUTOINDEX) while
the physical structure is a nested sort index (which the engine evaluation in
§6 showed wins scalar queries anyway). HYBRID's `bitmap_cardinality_limit`
param is accepted but physically inert for plain arrays.

---

## 5. Testing

### 5.1 Parser (`plan_parser_v2_test.go`)

- `TestScalarArrayMatchAny`: asserts the produced plan (`MatchExpr.column`'s
  `MatchColumnInfo.field_name`, `match_type`, and the predicate's element-level
  `ColumnInfo` with the array's element `data_type`).
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
| Edge cases | empty array (vacuous truth), NULL row (three-valued: result is NULL/excluded, not a vacuous match), threshold boundaries (0, N) |
| Negatives | `$[sub]` on scalar array, bare `$`, non-array field, nesting |
| Index | array scalar indexes built as nested element carriers (§6.2); legacy non-nested indexes still load and fall back to brute force |

---

## 6. JSON Array & Nested-Index Extension

The scalar-array design (§1–5) generalizes to two further axes with no new syntax
surface beyond the target locator: **JSON arrays** as a fourth `MATCH_*` target
(brute-force evaluation in this PR), and a **nested index** as the element
carrier for scalar arrays (a JSON element-level index is a later step, see §6.2).

### 6.1 JSON array targets

`MATCH_*` accepts a JSON-array target addressed by a JSON path, with the same `$`
element-self token:

```text
MATCH_ANY(meta["scores"], $ > 90)
MATCH_ALL(meta["tags"],   $ == "vip")
MATCH_LEAST(meta["k"],    $ == 100, threshold=2)
```

- **Parse** (`parser_visitor.go`): a `JSONIdentifier` target resolves to the JSON
  field + path; `MatchColumnInfo` carries `(field_id, nested_path)` instead of a
  struct sub-field locator. Template placeholders inside the `element_filter` are
  filled the same way as scalar arrays.
- **Element read** (segcore): each row's JSON value at the path is opened with
  simdjson and iterated as an array; each element is fed to the same
  `element_filter` sub-expression evaluated with the `$`-bound value.
- **Missing/non-array paths are three-valued** (same rule as the NULL-row entry
  in §5.3): a row whose JSON path does not resolve to a real array — field
  NULL, JSON `null`, missing key, or a non-array value — is **UNKNOWN**, not
  false. `MaskJsonNonArrayRows` clears both the match bit *and* the valid bit
  for such rows, so they are excluded from the result — including under `NOT`
  (`not MATCH_ANY(...)` does not resurrect them). A genuine empty array `[]` is
  different: it is a *valid* zero-element input evaluated vacuously
  (`MATCH_ALL`/`MATCH_MOST` match it, `MATCH_ANY`/`MATCH_LEAST` do not).
- **Numeric precision**: element reads go through `at_numeric()`-aligned typing —
  integral elements compare **exactly as int64** (no 2^53 double truncation),
  uint64/double as double. This mirrors the row-level `*JSONCompare` path so that
  `meta["id"] == 9007199254740993` behaves identically whether evaluated at row
  level or element level (fix in this PR; guarded by
  `JsonArrayMatchExprTest.Int64PrecisionAtPath` and
  `ScalarArrayIndexRegressionTest.JsonElementTermHugeElementNoUB`).

### 6.2 Nested index as element carrier

Scalar arrays can build a **nested** index: one indexed document per array
element, the element's owning row-id carried as the user doc-id, so an
element-level term/range predicate resolves to a row bitmap by walking the
matched element ids and mapping each back to its owning row via `IArrayOffsets`
(a per-element loop; a word-wise bitset reduction is a possible later
optimization, not part of this branch).

- **Scope — scalar arrays only in this PR**: the nested (element-keyed) build
  ships for plain scalar `ARRAY` fields, adding nested build paths and an
  `is_nested` persistence flag to `BitmapIndex`, `ScalarIndexSort` /
  `StringIndexSort`, and `InvertedIndexTantivy`. JSON `MATCH_*` always
  evaluates brute force: `MatchExpr.cpp` hard-dispatches JSON targets to the
  per-row path, and `IndexFactory` still routes JSON fields through
  `CreateJsonIndex` (row-level). A JSON element-level index fast-path is an
  explicit later step.
- **Build**: scalar array indexes build nested; nullable compact-buffer reads
  are fixed for nested builds. Row-level array queries (`array_contains`,
  `array_length`) keep working on a nested index — the expression framework
  converts the element-level result back to row level via `IArrayOffsets`.
- **Legacy compatibility (self-describing marker, not version inference)**:
  a plain-`ARRAY` field can carry two index shapes — the legacy row-level
  index and the new nested (element-level) index — and both can exist under
  the same engine version, so the two are distinguished by an **explicit
  per-segment `is_nested_index` marker** persisted in the segment's index
  meta, never by interpreting a version number. Struct-array sub-field
  indexes are born nested and need no disambiguation.
  - **Build**: datacoord decides per build task — a plain-`ARRAY` field gets
    a nested build (and the marker set) only when the negotiated scalar
    index engine version is >= 5, the version this feature ships with
    (negotiated as the *minimum* across querynodes, so during a rolling
    upgrade no old node is ever handed an element-level index). This is the
    only place the version is consulted; it is a capability signal for NEW
    builds, not a property of existing indexes.
  - **Load**: routing reads the persisted marker. Missing or false — every
    index built before this feature, at any engine version — routes to the
    legacy composite loader it was built with (including HYBRID/AUTOINDEX
    layouts, which only `HybridScalarIndex` can open) and keeps serving
    row-level queries; `MATCH_*` falls back to brute force on it. No rebuild
    is forced; compaction/re-index naturally produces marked nested indexes.
  - Known limitation: the snapshot/restore Avro manifest does not yet carry
    the marker (schema-versioned; follow-up), so a restored HYBRID-type
    nested array index would mis-route — tracked separately.
- **Empty-input edge**: an array field whose rows are all NULL or all empty
  builds a valid **empty** nested index instead of failing with `DataIsEmpty`;
  in the V3 packed layout, zero-byte `idx_to_offsets` entries skip the mmap
  (`mmap(len=0)` is EINVAL).
- **AUTOINDEX**: numeric arrays resolve to HYBRID and fall through to
  `ScalarIndexSort` (now also built nested), so sort remains the array default;
  nested tantivy is used only when the field is explicitly `INVERTED`. (The
  sort-vs-inverted trade-off for the nested element index is tracked separately
  in #51055; the single-segment build optimization in #51054.)
- **Compatibility**: the nested index is a new physical layout selected at build
  time; query resolution reads whichever layout the segment carries, so mixed
  row-level/nested segments coexist during upgrade.

## 7. Compatibility & Risk

- **Wire change (no released impact)**: `MatchExpr` field 1 changes from
  `string struct_name` to a richer `MatchColumnInfo` locator, because a
  JSON-array target (`MATCH_ANY(meta["scores"], $ > 90)`) must carry a
  `nested_path` that a bare field name cannot express. The locator is added as
  a **new field** (`MatchColumnInfo column = 5`) while `string struct_name = 1`
  is kept `[deprecated]` on its original tag: `struct_name = 1` already shipped
  on master (struct-array MATCH, #46518), and reusing tag 1 with a different
  type would silently misdecode plans between mixed proxy/querynode builds
  (string and message share wire type 2 — the decoder does not error, it
  misparses). Proxies **dual-write** both locators; the C++ reader uses
  `column` when present and falls back to `struct_name` for plans produced by
  an old proxy (struct arrays were the only pre-`column` target). The client
  SDK is untouched (it sends expression strings, not plan protos). No RPC/SDK
  changes.
- **Rolling upgrade**: the `MATCH_*` scalar-array/JSON syntax is understood
  only by upgraded QueryNodes. A new proxy sending such a plan to a
  not-yet-upgraded querynode fails cleanly at execution (the old node resolves
  the dual-written `struct_name` and reports "No array field found in struct")
  — a transient window closed by completing the querynode upgrade. The
  nested-index *build* is separately gated by the negotiated scalar index
  engine version and the per-segment nested marker, see §6.2. The internal
  `nested_index` build/load config key is reserved (CreateIndex rejects it as
  a user index param) so the version gate cannot be bypassed.
- **Downgrade / mixed-pool placement**: the build gate protects NEW builds
  only. Nested array indexes already built must not be served by a pre-v5
  querynode (HYBRID/AUTOINDEX fails to load on the missing INDEX_TYPE entry;
  INVERTED would silently read element ordinals as row offsets). Guarded
  entry point: snapshot restore rejects manifests carrying
  `is_nested_index=true` segment indexes while the resolved pool version is
  below the gate (mirroring the JSON path-index version check). Residual
  windows — rolling a querynode back below v5, or scaling out with an old
  image, after nested indexes exist — are not guarded by a load-time check
  (the old binary predates the marker); the supported procedure is to drop or
  rebuild plain-ARRAY indexes before downgrading querynodes. The marker's
  semantics is deliberately "requires nested-capable (>= v5) code to load",
  not "physically element-level": struct sub-field indexes are element-level
  too but are routed nested by NAME on every released binary, so they stay
  marker=false and are exempt from these guards.
- **Struct arrays**: untouched — same parser path (default branch) and same
  execution; regression-guarded by the existing `MatchExpr` test suite.
- **Schema evolution**: scalar array fields added via `AlterCollectionSchema`
  get their `IArrayOffsets` registered on growing-segment `Reopen`.
- **Build note (unrelated to this feature)**: native macOS/clang-19 builds
  require the upstream `rapidjson/cci.20230929` pin (PR #50664); this feature
  carries no build-system change of its own.
