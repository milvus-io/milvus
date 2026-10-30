# Query Return Distance (`calc_distance`)

- **Created:** 2026-10-30
- **Author(s):** @yhmo
- **Status:** Draft
- **Component:** Proxy / QueryNode
- **Related Issues:** milvus-io/milvus#43274
- **Replaces:** `CalcDistance` RPC (deprecated)

## Summary

`query()` gains the ability to return a distance/similarity column alongside
regular output fields. The caller supplies a reference vector and a metric via a
new `query_params` key, and the QueryNode computes `distance(ref_vector,
row.vector)` for every row matching the filter. The result is emitted as a
synthetic `float32` column (default name `distance`) appended to
`QueryResults.fields_data`, reusing the existing per-row output structure.

## Motivation

The `CalcDistance` RPC is deprecated — the proxy returns
`"CalcDistance deprecated"` directly (`internal/proxy/impl.go:4011`) and the
segcore implementation was removed long ago. Users still need the capability,
and the natural home for it is `query()`: filter by expression, and for every
matching row compute its distance to a chosen reference vector. The result is a
single scalar per row, which fits `fields_data` (row-aligned) without any new
result structure.

## Scope

- **1-to-N semantics.** One reference vector vs. every row matching the query
  filter.
- **Any metric within the vector family.** The metric type is taken from
  `calc_distance.metric_type` and validated against the **supported metric set
  of the target vector type** (see §1); it is **not** validated against the
  index metric (see "Why no index metric check"). Computation is always
  brute-force over raw stored vectors.
- **All vector field types** (v1): `FloatVector`, `Int8Vector`,
  `Float16Vector`, `BFloat16Vector`, `BinaryVector`, `SparseFloatVector`.
- **Two reference forms.** `vector` may be a direct vector (JSON array/object,
  shape depends on the field type) or a JSON string (a single-vector expression
  resolved server-side).

## Non-goals

- **N×M pairwise distance matrix** (the old `CalcDistance` `op_left ×
  op_right` semantics). Not row-aligned and out of scope.
- **Multiple reference vectors per request.** Would produce multiple distance
  values per row, which `fields_data` cannot express.
- **Array-of-vector fields.** The distance column is one scalar per row, so
  `Array<vector>` targets are out of scope for v1.
- **Asymmetric binary metrics** (`SUBSTRUCTURE`, `SUPERSTRUCTURE`) and the
  minhash-specific `MHJACCARD`.
- **BM25 relevance scoring** for sparse text vectors.
- **round_decimal / custom precision.** Distance uses plain `float32`, matching
  search's `scores`/`distances` (`schema.proto:439`, `:445`).
- **Index-assisted computation.** When the metric matches the index metric,
  pushing distance down to segcore's knowhere brute-force kernels is a
  documented follow-up, not part of v1.

## Cross-links

- `design_docs/20261005-arrow-retrieve-no-fielddata.md` — the Arrow retrieve
  path this feature must exclude (the reduce point and Arrow-selection
  mechanics are shared).
- `design_docs/20260708-vector-anchored-join.md` — related reference-row
  semantics.

## Design Details

### 1. API & Wire Contract

No proto change for the wire request. A single new `query_params` key is added
to the existing `QueryRequest.query_params` (`milvus.proto:1492`, `repeated
KeyValuePair`). `metric_type` and `field` are always required; `vector`'s shape
depends on the **target field's data type** (the server resolves the field from
the collection schema):

| Field type | `vector` input form | Metric set |
|---|---|---|
| `FloatVector` | `[1.1, 2.1, 3.1, ...]` (numbers, len = dim) | L2 / IP / COSINE |
| `Int8Vector` | `[1, -2, 3, ...]` (integers, len = dim) | L2 / IP / COSINE |
| `Float16Vector` | `[1.1, 2.1, ...]` (numbers, len = dim, rounded to fp16) | L2 / IP / COSINE |
| `BFloat16Vector` | `[1.1, 2.1, ...]` (numbers, len = dim, rounded to bf16) | L2 / IP / COSINE |
| `BinaryVector` | `[0, 1, 255, ...]` (byte values 0-255, len = dim/8) | HAMMING / JACCARD |
| `SparseFloatVector` | `{"indices":[1,2,3],"values":[0.1,0.2,0.3]}`, `{"1":0.1,"2":0.2}`, or `[{"index":1,"value":0.1}, ...]` | IP / COSINE |

```json
"calc_distance": {
  "metric_type": "COSINE",
  "field": "embedding",
  "vector": [1.1, 2.1, 3.1, ...]
}
```

- **`metric_type`** (required): the distance algorithm. Required because the
  algorithm must be known to compute anything; validated against the target
  field type's metric set above.
- **`field`** (required): the target vector field name. Explicit binding avoids
  ambiguity on collections with multiple vector fields.
- **`vector`** (required): two mutually exclusive forms, distinguished by JSON
  native type (`json.RawMessage` first byte `[`/`{` vs `"`):
  - **direct vector** — an array for dense types, an object or array-of-pairs
    for sparse (see table), single row only; interpretation is deferred until
    the field's data type is known and each type carries its own dimension /
    byte-length validation;
  - **string** — a single-vector expression (e.g. `"id == 2"`), resolved
    server-side; must match exactly one row.

The presence of `calc_distance` implies "compute and return distance"; the user
does **not** list `distance` in `output_fields`.

#### Wire encoding of the reference vector

The resolved reference vector is transported in the plan as opaque bytes whose
meaning is fixed by the `ref_vector_type` field — the same type-carrying
`bytes` + `enum` pattern the search placeholder group uses for query vectors:

| `ref_vector_type` | `ref_vectors[0]` bytes |
|---|---|
| `FloatVector` | float32 little-endian (4 × dim) |
| `Int8Vector` | signed int8 (dim) |
| `Float16Vector` | float16 little-endian (2 × dim) |
| `BFloat16Vector` | bfloat16 little-endian (2 × dim) |
| `BinaryVector` | packed bits (dim/8) |
| `SparseFloatVector` | compact sparse row: `(index uint32, value float32)` × nnz, indices sorted |

The compact sparse row is the same format `typeutil.SparseFloatArray.Contents`
rows are stored in (`typeutil.CreateSparseFloatRow` / `SparseFloatBytesToMap`),
so the QueryNode decodes the reference and each row uniformly.

#### SDK value contract

The wire value of `calc_distance` is always a **JSON string**; the SDK is
responsible for serialization. The server validates with `gjson.Valid` and fails
fast with `WrapErrParameterInvalid` on malformed JSON.

| SDK | Param type | Value to wire | calc_distance support |
|---|---|---|---|
| **Java** | `Map<String, Object>` (VectorUtils.java:149) | `String.valueOf(value)` (:154) — dicts get mangled, strings pass through | JSON string ✓ zero change |
| **C++** | `unordered_map<string,string>` (`extra_params_`) | string set verbatim (DqlUtils.cpp:1627) | JSON string ✓ zero change |
| **Rust** | `HashMap<String, String>` (`extra_params`, dql.rs:80) | string verbatim | JSON string ✓ zero change |
| **C#** | `IDictionary<string, string>` (`ExtraParameters`, QueryParameters.cs:98) | string verbatim | JSON string ✓ zero change |
| **Node** | `{[key: string]: any}` (Data.ts:1301; `parseToKeyValue` default no-stringify, Format.ts:46) | `any` passed through — complex objects may mis-serialize | JSON string ✓ (caller stringifies) |
| **Go** | `map[string]string` (read_options.go:787) | string verbatim | JSON string ✓ zero change |
| **pymilvus** | `**kwargs`, keys picked manually (prepare.py:2438) | **no generic passthrough, unknown keys dropped** | **one line added** (serialize dict, append) |

pymilvus additionally supports the native dict form
`query_params={"calc_distance": {...}}` and serializes it internally
(`utils.dumps` → `orjson.dumps` for dicts, `pymilvus/client/utils.py:308`).

### 2. Reference vector resolution (proxy, expression form only)

When `vector` is a string (expression), the proxy resolves it in `PreExecute`
using an internal `QueryTask` (`MarkInternal`, `task_query.go:149`), executing
`expr=<vector>` with `output_fields=[field]`:

- **0 rows** → `WrapErrParameterInvalid` "reference vector not found";
- **>1 rows** → `WrapErrParameterInvalid` "expression must match exactly one
  vector";
- **exactly 1 row** → extract the row's `field` vector in its canonical byte
  encoding (§1 table) and **inject it as the direct-vector form**. The
  downstream path is then identical to the direct-vector case (the plan carries
  the resolved vector; the expression is a proxy-side detail). Extraction is
  per field type: `Float32ArrayToBytes` for `FloatVector`; the raw bytes are
  already canonical for `BinaryVector` / `Float16` / `BFloat16` / `Int8` /
  `SparseFloatVector`.

Notes:

- A small MVCC TOCTOU exists between resolution and the main query's execution
  (the reference row could be modified/deleted in between). Accepted for v1.
- The expression form supports every vector type; there is no dense-only
  restriction (the extraction is type-aware).

### 3. Output column naming & conflict resolution

Resolved in the proxy and carried in the plan:

- Default column name: `distance`.
- If that name collides with the query's **final output column set** (schema
  fields + user-requested fields), prepend `_` until free:
  `distance` → `_distance` → `__distance` → ...
- The resolved name (`distance_output_name`) must be identical in the
  `FieldData` column generated by the QueryNode and in the name appended to
  `QueryResults.output_fields` by the proxy.

`"distance"` is not a legal output field name on its own: without
`calc_distance` it already fails in `translateOutputFields` ("field not exist",
`internal/proxy/dql/util_dql.go:296`), and with `calc_distance` it is only
meaningful via the conflict-resolution above.

### 4. Proxy changes (`internal/proxy/dql/`)

- `parseQueryParams` (`task_query.go:442`) parses `calc_distance` (JSON string)
  into `metric_type` / `field` / `vector`; the direct-vector JSON is held raw
  and interpreted in `resolveCalcDistance` once the field's data type is known.
- **`translateOutputFields` untouched.**
- **`output_fields` and `calc_distance` are fully decoupled:** output_fields
  returns exactly what it names; `calc_distance.field` is only the source of
  the raw vectors read internally. It need not appear in `output_fields`.
- **Validation (all proxy-side, fail early):**
  - `gjson.Valid` on the value, then presence of `metric_type` / `field` /
    `vector` → `WrapErrParameterInvalid`;
  - `field` is an existing **vector field** of a supported type (§1 table);
  - direct form: single row, type-specific shape and length (dim for dense,
    dim/8 bytes for binary, sparse JSON for sparse) → `WrapErrParameterInvalid`;
  - expression form: exactly one matching row (see §2), extracted vector
    length matches the field's encoding;
  - `metric_type` within the field type's supported set (§1 table);
  - no index-metric consistency check (see "Why no index metric check");
  - disallowed with aggregation / GROUP BY (results are not raw rows, nothing
    to score).
- **Internal injection:** even when `calc_distance.field` is not in the user's
  `output_fields`, add it to `plan.OutputFieldIds` (internal marker) so segcore
  fetches its raw data; the user-visible `output_fields` is unchanged.
- **Plan carries the distance spec** in `planpb.QueryPlanNode`
  (`pkg/proto/plan.proto:377`):
  ```protobuf
  message QueryPlanNode {
    ...
    optional CalcDistanceSpec calc_distance = 8;  // present iff calc_distance requested
  }

  message CalcDistanceSpec {
    repeated bytes ref_vectors = 1;       // length 1, canonical encoding per ref_vector_type
    string metric_type = 2;               // from calc_distance.metric_type
    int64 vector_field_id = 3;            // from calc_distance.field
    bool return_distance = 4;             // true iff calc_distance present
    string distance_output_name = 5;      // resolved column name
    schema.DataType ref_vector_type = 6;  // target field type, fixes ref_vectors encoding
  }
  ```
  (Alternatively carried on `RetrieveRequest`, `internal.proto:233`; the plan is
  preferred, matching search's `VectorANNS.query_info.metric_type` and because
  the plan is already built in the proxy.)
- **Response assembly:** after `reduceRetrieveResults` (`task_query.go:1295`),
  append `distance_output_name` to `QueryResults.output_fields` so SDKs rebuild
  the column.

### 5. QueryNode changes — distance computed at QN reduce

Computed in the QueryNode, not segcore and not the proxy:

- **Location:** `internal/querynodev2/tasks/query_task.go`, after
  `RunQNQueryPipeline` (`:183`) and `MaterializeArrowSelection` (`:202`), before
  assembling `t.result`. At this point `reducedResult.FieldsData` already holds
  the per-row vector column (thanks to the §4 internal injection).
- **Pure Go, no C++ changes:** read `ref_vector` / `ref_vector_type` /
  `metric_type` / `vector_field_id` / `distance_output_name` from the plan, and
  dispatch the per-row computation on `ref_vector_type`
  (`internal/querynodev2/tasks/calc_distance.go`):
  - `FloatVector` / `Int8Vector` / `Float16Vector` / `BFloat16Vector` →
    decode the reference and the flattened rows to `[]float32` (the
    fp16/bf16/int8 decoders are `typeutil.Float16BytesToFloat32Vector` /
    `BFloat16BytesToFloat32Vector` / an int8 loop) and reuse the existing
    SIMD `distance.CalcFloatDistance`;
  - `BinaryVector` → `distance.CalcBinaryDistance` (new, pure Go: HAMMING
    popcount / JACCARD over packed bits);
  - `SparseFloatVector` → `distance.CalcSparseDistance` (new, pure Go: IP dot
    product / COSINE over the compact rows via `typeutil.SparseFloatBytesToMap`).
  - The synthetic column is always `float32`, no round_decimal.
  - **Metric-agnostic:** single path regardless of index metric.
  - Index acceleration (knowhere kernels, `SearchBruteForce.cpp`) is a
    follow-up; it would need C++ changes and a second path, so v1 keeps one Go
    path.
- **Drop the raw vector after computing:** if `vector_field_id` is not in the
  user's `output_fields`, remove that column from `FieldsData` after distance
  computation (it is internal input only); keep it when requested.
- **Synthetic column requirements** (so the proxy's global reduce matches it by
  FieldId, `AppendFieldData` `pkg/util/typeutil/schema.go:1418`):
  - the same reserved `FieldId` across all QN results (a negative or extreme
    constant never colliding with schema fields);
  - `FieldName = distance_output_name`;
  - appended last = the position of `distance_output_name` in `output_fields`.
- **Exclude the Arrow transport:** add `calc_distance` to the exclusion
  condition in `shouldUseArrowTransport` (`internal/querynodev2/segments/
  retrieve.go:194`). Arrow records carry only user output columns
  (`retrieve.go:43`), so an internally-injected vector field is not available on
  the Arrow path. Forcing the protobuf path (same exclusion as aggregation /
  ORDER BY / ignore_non_pk, `retrieve.go:202-216`) guarantees the vector data is
  in `segcorepb.RetrieveResults.FieldsData`. The flag is read from
  `RetrievePlan.GetReturnDistance()`, set once by the QueryNode from its
  already-parsed plan — never by re-unmarshalling the serialized expr plan.
- **Cost:** QN reduce truncates to `limit`, so at most `#QN × limit` rows are
  scored per QN (vs. `#segment × limit` if computed per segment).

### 6. Orthogonality with iterator / ORDER BY / limit

- **limit:** the result set is finalized first; distance is computed only on
  the limited rows.
- **iterator:** pagination walks by PK cursor
  (`ExecPlanNodeVisitor.cpp:342`); distance is a pure function of each page's
  rows and does not participate in ordering or the cursor.
- **ORDER BY:** supported. `order_by=distance` still fails at
  `translateOrderByFields` ("field not exist",
  `internal/util/reduce/orderby/types.go:163-166`), but `order_by` on any
  schema field combined with `calc_distance` works: the ORDER BY pipeline's
  `FieldIDRemapOperator` rebuilds `FieldsData` from the user's output-field id
  list, so the proxy appends the synthetic column's reserved FieldId
  (`calcDistanceReservedFieldID`, `-10000`) to that remap target when
  `calc_distance` is present. Appending it last keeps the column position
  aligned with the `output_fields` order (the synthetic column is always
  appended last in PostExecute).
- The sole restriction stays on **aggregation / GROUP BY** (non-raw results,
  nothing to score).
- **COSINE zero-norm:** a dense (float/int8/fp16/bf16) all-zero reference
  vector with `metric_type=COSINE` is rejected up front with
  `WrapErrParameterInvalid`, because `CosineImplPure` would otherwise emit NaN
  for every row. Sparse already returns 0 for zero norms; binary is restricted
  to HAMMING/JACCARD, so no check is needed there.
- **Expression partition scope:** the expression-form reference vector is
  resolved in `PreExecute` *after* `GetPartitionIDs`, so the internal
  reference-vector query reuses the caller's resolved `PartitionIDs` instead
  of scanning all partitions.

### 7. C++/segcore components: untouched (superseded C++ implementation)

An early plan placed the distance computation in segcore's C++ distance
kernels. That implementation approach is **superseded** — the distance is
computed in the QueryNode's Go reduce (§5), so the C++ side does nothing
new for `calc_distance`.

This does **not** mean the C++ components below are obsolete or scheduled for
removal: they remain fully in use by the normal query path and are listed here
only to record that `calc_distance` does not touch them.

- `query::RetrievePlan`, `CreateRetrievePlanByExpr` — the plan is parsed as
  before; the new `calc_distance` plan fields are proto3-unknown to the C++
  side and dropped by `ParsePartialFromCodedStream` (`Plan.cpp:251`);
- segcore distance kernels — not invoked; computation is pure Go;
- `RetrieveArrow` — unchanged (the Go side merely excludes the Arrow transport
  for `calc_distance`, §5).

C++ passes the plan binary through unchanged; there is no C++ change in this
feature.

## Why no index metric check

In search, the metric passed must equal the loaded index's metric
(`SearchOnSealed.cpp:105`), because search *uses the index*. `calc_distance`
does not: it brute-forces against raw stored vectors fetched on the query path
(`FillTargetEntry`), so any supported metric computes fine regardless of the
index metric. Enforcing a match would also diverge from the deprecated
`CalcDistance` (which accepted arbitrary metrics) and from the no-index flat
case (no stored metric to compare against). Note for users: the `distance`
column is comparable to a search score only when the metrics agree.

## Test Plan

- **Go unit tests — distance kernels** (`pkg/util/distance/calc_distance_test.go`):
  hand-verified HAMMING / JACCARD over packed bits and sparse IP / COSINE
  against reference implementations.
- **Go unit tests — proxy** (`internal/proxy/dql/task_query_test.go`):
  `calc_distance` JSON parsing (direct-vector forms per type: float array,
  int8 array, fp16 array, bf16 array, binary byte array, sparse object /
  array-of-pairs; expression forms), `gjson.Valid` malformed-JSON error,
  `distance` not a legal output field, per-type metric/dimension validation,
  expression 0-row/multi-row errors, agg combination error, name conflict
  resolution, internal injection of `calc_distance.field` into
  `OutputFieldIds` without appearing in user `output_fields`.
- **Go unit tests — QueryNode** (`internal/querynodev2/tasks/query_task_test.go`):
  distance column correctness after QN reduce per type (float vs.
  hand-computed L2/IP/COSINE; int8/fp16/bf16 vs. float32-decoded reference;
  binary HAMMING/JACCARD; sparse IP/COSINE), column alignment, empty results,
  growing + sealed segments, combinations with ORDER BY / iterator / limit,
  and dropping the vector column when the user did not request it.
- **Go unit tests — proxy reduce:** distance column merged by FieldId across
  multiple QN results (`AppendFieldData` path).
- **Regression — normal query unchanged:** with no `calc_distance`, the result
  (fields, order, limit/offset, iterator, ORDER BY) must be byte-identical to
  the pre-feature behavior, and the Arrow transport must still be used. Covers
  the empty-plan-fields path end to end (proxy parse → plan marshal → C++
  `ParsePartialFromCodedStream` → QN reduce → response assembly).
- **Integration:** standalone Milvus + pymilvus; `calc_distance` (native dict)
  returns distances matching search for the same vector for float and sparse
  fields; binary HAMMING/JACCARD matches search scores; both
  `output_fields`-with/without-the-vector-field cases; expression form
  (exactly one hit) consistent with the direct-vector form.
- **Error codes:** new errors all use `WrapErrParameterInvalid`; no new error
  codes, no merr guard changes.

## Upgrade / rollout note

All new logic is gated on `calc_distance` being present, so existing queries are
unaffected, and `QueryPlanNode` additions are proto3-backward-compatible (C++
parses plans with `ParsePartialFromCodedStream`, `Plan.cpp:251`; unknown fields
are ignored). During a rolling upgrade, a new proxy against an old QueryNode
silently drops the new plan fields: normal queries stay correct, but
`calc_distance` returns without the distance column (no error). Either upgrade
the whole cluster before advertising the feature, or document that behavior.
