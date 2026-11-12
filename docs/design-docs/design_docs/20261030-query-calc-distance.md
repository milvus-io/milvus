# Query Return Distance (`calc_distance`)

- **Created:** 2026-10-30
- **Author(s):** @yhmo
- **Status:** draft
- **Component:** Proxy / QueryNode
- **Related Issues:** milvus-io/milvus#43274
- **Replaces:** `CalcDistance` RPC (deprecated)

## Summary

`query()` gains the ability to return a distance/similarity column alongside
regular output fields. The caller supplies a reference vector and a metric via a
new `query_params` key, and the QueryNode computes `distance(ref_vector,
row.vector)` for every row matching the filter. The result is emitted as a
synthetic `float32` column (named by `output_name`, required) appended to
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
- **Vector sub-fields of Struct fields** (e.g. `calc_distance.field =
  "items[embedding]"`). A struct array field holds a per-row *list* of structs,
  each with its own vector, so one row maps to N vectors (N variable, possibly
  0) — a 1-to-many relation that a single per-row distance scalar cannot
  express, and the sub-field data is element-level (nested under
  `StructArrays`, aligned by `element_indices`) rather than a flat
  row-oriented `FieldData_Vectors` column. Supporting it requires defining a
  per-row reduction over the row's elements (min/avg/max distance) or an
  element-aligned distance output — both out of scope for v1. The sub-field
  *can* be retrieved via `output_fields=["items[embedding]"]`; it just cannot
  be a calc_distance target.
- **Asymmetric binary metrics** (`SUBSTRUCTURE`, `SUPERSTRUCTURE`) and the
  minhash-specific `MHJACCARD`.
- **BM25 relevance scoring** for sparse text vectors.
- **Ordering by the distance column (`order_by=distance`).** The distance
  column is a synthetic field, not a schema field, so `translateOrderByFields`
  already rejects `order_by=distance` with "field not exist". Ordering rows by
  their distance to a reference vector is exactly what search does — same
  reference vector, same metric, plus filter and output fields — and search is
  both the intended API for it and faster (index-assisted). So calc_distance
  deliberately does not plan to support sorting by its own column; it exists to
  attach a distance column to a query result without reordering it.
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
| `SparseFloatVector` | `{"indices":[1,2,3],"values":[0.1,0.2,0.3]}`, `{"1":0.1,"2":0.2}`, or `[{"index":1,"value":0.1}, ...]` | IP |

#### `vector` input forms per type

The `vector` value travels as a JSON string on the wire (SDK value contract
below); the *shape* of its parsed content depends on the target field's data
type, resolved from the collection schema. Concrete forms:

| Field type | Parsed JSON shape | Example `vector` | Length rule |
|---|---|---|---|
| `FloatVector` | array of numbers | `[0.1, 0.2, 0.3, 0.4]` | exactly `dim` floats |
| `Float16Vector` | array of numbers | `[0.1, 0.2, 0.3, 0.4]` | exactly `dim` floats, rounded to fp16 |
| `BFloat16Vector` | array of numbers | `[0.1, 0.2, 0.3, 0.4]` | exactly `dim` floats, rounded to bf16 |
| `Int8Vector` | array of integers | `[1, -2, 3, 4]` | exactly `dim` integers |
| `BinaryVector` | array of byte values | `[0, 255, 1, 128]` | exactly `dim/8` elements, each 0-255 |
| `SparseFloatVector` | object or pair array | see below | variable (nnz), no fixed dim |

Sparse accepts any of these three equivalent forms (all encode the same
`{index: value}` data):

```json
{"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]}
{"1": 0.1, "2": 0.2}
[{"index": 1, "value": 0.1}, {"index": 2, "value": 0.2}]
```

Notes:

- The float family (FloatVector / Float16 / BFloat16) is **always input as a
  float32 array**; half-precision rounding happens server-side
  (`Float32ArrayToFloat16Bytes` / `Float32ArrayToBFloat16Bytes`), so the caller
  never provides fp16/bf16 bits.
- Binary elements are byte values 0-255; the server packs them into `dim/8`
  packed bytes (`encodeCalcDistanceVector`, one element = one byte), matching
  the on-disk binary vector encoding.
- Sparse has no fixed dimension, so it cannot be a fixed-length array; the
  three object/pair forms are canonicalized into a sorted compact sparse row
  (`CreateSparseFloatRow`).
- Dimension / byte-length validation per type happens at resolve time (§4); a
  mismatch is `WrapErrParameterInvalid` ("dimension does not match field").

```json
"calc_distance": {
  "metric_type": "COSINE",
  "field": "embedding",
  "vector": [1.1, 2.1, 3.1, ...],
  "output_name": "distance"
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
- **`output_name`** (required): the name of the synthetic distance column in
  the result. It is the user's choice, so no auto-renaming happens; the proxy
  rejects it when it collides with any name in the query's final output column
  set (see §3).

**Nullable target fields** are supported: a null row has no vector to score,
so its distance is a zero placeholder with `ValidData=false` on the synthetic
column (see §5) — never a NaN value, which JSON cannot represent.

The presence of `calc_distance` implies "compute and return distance"; the user
does **not** need to list the distance column in `output_fields` — its name
(`output_name`) is appended to the result's output columns by the proxy.

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
| **Java** | `Map<String, Object>` | `String.valueOf(value)` — dicts get mangled, strings pass through | JSON string ✓ zero change |
| **C++** | `unordered_map<string,string>` (`extra_params_`) | string set verbatim | JSON string ✓ zero change |
| **Rust** | `HashMap<String, String>` (`extra_params`) | string verbatim | JSON string ✓ zero change |
| **C#** | `IDictionary<string, string>` (`ExtraParameters`) | string verbatim | JSON string ✓ zero change |
| **Node** | `{[key: string]: any}` (`queryParams`, `parseToKeyValue` default no-stringify) | `any` passed through — complex objects may mis-serialize | JSON string ✓ (caller stringifies) |
| **Go** | `map[string]string` (`QueryOption.queryParams`) | string verbatim | JSON string ✓ zero change |
| **pymilvus** | `**kwargs`, keys picked manually | **no generic passthrough, unknown keys dropped** | **one line added** (serialize dict, append) |

pymilvus additionally supports the native dict form
`query_params={"calc_distance": {...}}` and serializes it internally
(`utils.dumps` → `orjson.dumps` for dicts, `pymilvus/client/utils.py:308`).

### 2. Reference vector resolution (proxy, expression form only)

Instead of a direct vector array, `vector` may be a JSON string carrying a
single-vector expression that selects exactly one row; the server resolves it
to the reference vector before building the plan:

```json
"calc_distance": {
  "metric_type": "L2",
  "field": "embedding",
  "vector": "id == 2",
  "output_name": "distance"
}
```

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

The column name is **user-chosen** via `calc_distance.output_name` and carried
verbatim in the plan:

- No auto-renaming (`_`-prefixed fallbacks) exists: the name the user writes is
  the name the QueryNode emits and the proxy appends to `output_fields`.
- The proxy **rejects** the request when `output_name` collides with the query's
  **final output column set** (schema fields + user-requested fields +
  requested dynamic-field keys + the primary key field, which is always present
  even when not requested) — two same-named columns would be ambiguous for
  every SDK, so it fails fast with `WrapErrParameterInvalid` instead. This
  covers every *schema-known* name, including names surfaced by
  `output_fields=["*"]`, which expands to the actual schema fields.
- **Dynamic JSON keys are the one exception: the collision is resolved by the
  SDK, not rejected.** `output_fields=["*"]` (or `$meta`) returns a collection's
  dynamic keys, which are data-dependent and unknowable at plan time, so the
  proxy cannot enumerate them into the collision set — and it deliberately does
  not reject `*`/`$meta` outright, because the common case has no colliding key
  and a blanket rejection would block legitimate use. The convention, which SDKs
  implement in row assembly (they unpack the `$meta` JSON column and merge its
  keys into the result row): a **real output column shadows a dynamic key of the
  same name** — the calc_distance column is a real `FieldData` column
  (`common.CalcDistanceFieldID`), so it wins over a colliding dynamic key. The
  shadowed dynamic value is not surfaced under that name; SDKs should make the
  shadowing observable (warning or renamed key) rather than silently dropping
  it.
- The chosen name (`distance_output_name`) must be identical in the
  `FieldData` column generated by the QueryNode and in the name appended to
  `QueryResults.output_fields` by the proxy.

`"distance"` is not a legal output field name on its own: without
`calc_distance` it already fails in `translateOutputFields` ("field not exist",
`internal/proxy/dql/util_dql.go:296`), and with `calc_distance` it is only
meaningful as the `output_name` value.

### 4. Proxy changes (`internal/proxy/dql/`)

- `parseQueryParams` (`task_query.go:442`) parses `calc_distance` (JSON string)
  into `metric_type` / `field` / `vector` / `output_name`; the direct-vector
  JSON is held raw and interpreted in `resolveCalcDistance` once the field's
  data type is known.
- **`translateOutputFields` untouched.**
- **`output_fields` and `calc_distance` are fully decoupled:** output_fields
  returns exactly what it names; `calc_distance.field` is only the source of
  the raw vectors read internally. It need not appear in `output_fields`.
- **Validation (all proxy-side, fail early):**
  - `gjson.Valid` on the value, then presence of `metric_type` / `field` /
    `vector` / `output_name` → `WrapErrParameterInvalid`;
  - `field` is an existing **vector field** of a supported type (§1 table);
  - `output_name` does not collide with the query's final output column set
    (schema/user-requested fields, requested dynamic-field keys, the primary
    key field) → `WrapErrParameterInvalid` (§3);
  - direct form: single row, type-specific shape and length (dim for dense,
    dim/8 bytes for binary, sparse JSON for sparse) → `WrapErrParameterInvalid`;
  - expression form: exactly one matching row (see §2), extracted vector
    length matches the field's encoding;
  - `metric_type` within the field type's supported set (§1 table);
  - no index-metric consistency check (see "Why no index metric check");
  - the reference vector contains no NaN/Inf (decoded for
    Float/Float16/BFloat16; a large finite float32 narrows to Inf in half
    precision) and, for COSINE, is not all-zero — `WrapErrParameterInvalid`
    (see §6);
  - disallowed with aggregation / GROUP BY (results are not raw rows, nothing
    to score);
  - disallowed with query function chains (QueryScan): the QueryScan path
    materializes only the RetrieveRequest output fields, so the injected
    vector column is absent (`prepareQueryFunctionChains` rejects the
    combination, §6);
  - excluded from the Materialize reply path: it skips the QN reduce that
    computes the distance column (`materializeInScope` returns false, §6).
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
    string distance_output_name = 5;      // user-chosen column name (§3)
    schema.DataType ref_vector_type = 6;  // target field type, fixes ref_vectors encoding
  }
  ```
  (Alternatively carried on `RetrieveRequest`, `internal.proto:233`; the plan is
  preferred, matching search's `VectorANNS.query_info.metric_type` and because
  the plan is already built in the proxy.)
- **Response assembly:** after `reduceRetrieveResults` (`task_query.go:1295`),
  the `distance_output_name` is appended to `QueryResults.output_fields` **only
  when the reserved-FieldId column actually survived the reduce**, and the
  empty-fill/rewrite pass (below) is folded into the same scan. For
  **zero-row results**, the empty-fill (`FillRetrieveResultIfEmpty`)
  classifies the reserved FieldId as a system field and fabricates a nameless
  Int64 column; PostExecute rewrites it into the Float32 distance column with
  the resolved name, preserving the column's `ValidData` (idempotent — on
  non-empty results the QueryNode already emitted the correct column). An old
  QueryNode (all-old cluster) drops the calc_distance plan field and produces
  no reserved-FieldId column, so no name is appended and the response cleanly
  carries no distance column; a **mixed** old/new cluster errors instead
  (§Upgrade note).
- **Reserved FieldId:** the synthetic column's FieldId is the single shared
  constant `common.CalcDistanceFieldID` (`-10000`), used by both the QueryNode
  (column producer) and the proxy (ORDER BY remap target), so the two sides
  cannot drift.

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
  - `FloatVector` → reuse the SIMD `distance.CalcFloatDistance` (the data is
    already `[]float32`);
  - `Int8Vector` / `Float16Vector` / `BFloat16Vector` → **fused
    decode-and-score kernels** `distance.CalcInt8FloatDistance` /
    `CalcFloat16FloatDistance` / `CalcBFloat16FloatDistance` (new, pure Go):
    the bytes are decoded inline inside the scoring loop, so no intermediate
    `[]float32` array is materialized for the rows. Only the single reference
    vector is decoded once (negligible, one row). A decode-then-score pass
    measured 7-18× slower than the distance kernel itself and allocated a
    32 MiB-per-block intermediate, which is why the fused form exists.
  - `BinaryVector` → `distance.CalcBinaryDistance` (new, pure Go: HAMMING
    popcount / JACCARD over packed bits);
  - `SparseFloatVector` → `distance.CalcSparseDistance` (new, pure Go: IP dot
    product over the compact rows via `typeutil.SparseFloatBytesToMap`).
  - **Fused-decoder SIMD path:** the three fused kernels dispatch through hook
    variables (same pattern as `L2Impl`). On amd64, `init` probes the CPU and
    swaps in SIMD decoders: **F16C** (`VCVTPH2PS`) for float16 and **AVX2**
    sign-extension/shift for int8 and bfloat16 (`pkg/util/distance/asm`), each
    with a scalar tail for non-multiples of the vector width. The SIMD path
    decodes a row into a reused dim-sized buffer and scores it with the
    existing SIMD `asm.IP`/`asm.L2` (FMA3), so **all three gates require
    `HasAVX2`** (F16C alone, as on Ivy Bridge, would call the FMA3 kernels
    and SIGILL), and no per-block intermediate array is materialized. Machines
    without F16C/AVX2 keep the pure-Go fallback —
    Milvus's minimum CPU requirement (SSE4.2) does not guarantee F16C, so the
    CPUID detection (`asm.HasF16C`, CPUID.1:ECX[20]) is mandatory. The
    decoder and SIMD kernels are verified against the scalar decoders on
    boundary values and against the pure-Go fused kernels on random data. Unit tests
    pin the SIMD kernels bit-for-bit against the scalar decoders on
    subnormal/zero/normal boundaries and within tolerance against the pure-Go
    fused kernels on random data.
  - The synthetic column is always `float32`, no round_decimal.
  - **Metric-agnostic:** single path regardless of index metric.
  - Index acceleration (knowhere kernels, `SearchBruteForce.cpp`) is a
    follow-up; it would need C++ changes and a second path, so v1 keeps one Go
    path.
- **Drop the raw vector after computing:** if `vector_field_id` is not in the
  user's `output_fields`, remove that column from `FieldsData` after distance
  computation (it is internal input only); keep it when requested.
- **Nullable vector fields:** the reduce output carries compact data (valid
  rows only) plus a `ValidData` bitmap. calc_distance scores the valid rows and
  expands the distance column to one entry per row, keeping it row-aligned
  with the ids. Nullness is mirrored onto the synthetic column's `ValidData`
  (null rows get a zero placeholder value) — **never NaN** — because NaN cannot
  be represented in JSON and would break the RESTful output. The cross-shard
  merge (`buildMergedFieldDataRange`) preserves `ValidData` for any field that
  carries a bitmap even when it is not in the collection schema (the synthetic
  column's id is not a schema field), so nullness survives the reduce.
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
- **Bounded parallel scoring.** `CalcFloatDistance`
  parallelizes over the *left* vectors (`pkg/util/distance/calc_distance.go`),
  but calc_distance always has exactly one reference vector, so that path
  degenerates to a single goroutine and scores every row serially.
  `calcDistances` therefore cuts the reduced rows into fixed-size blocks and
  scores them with a **bounded worker pool** (`runCalcDistanceBlocks`): each
  worker takes the next block, decodes and scores it in its own goroutine, and
  writes a disjoint range of the distance column. Two properties are bounded by
  construction:
  - **Memory is no longer the sizing constraint.** The fused kernels decode a
    row inline (or into a reused dim-sized `rowBuf` on the SIMD path), so a
    block's transient working set is just its output distances array
    (`blockRowLimit × 4` bytes) plus one `rowBuf` per worker — not
    `blockRowLimit × dim × 4`. An earlier draft sized blocks from a 32 MiB
    budget (`defaultMaxBlockMemoryBytes`) to bound a per-block decoded
    `[]float32` that the fused kernels removed; the budget and its dim
    dependency are gone. Peak transient memory is `totalRows × 4` (the result
    column itself) plus `GOMAXPROCS` dim-sized decode buffers, independent of
    block size.
  - **Block count tracks the CPU count.** `blockRowLimit = max(256,
    ceil(totalRows / (2 × GOMAXPROCS)))` for dense fields and
    `max(256, ceil(totalRows / (4 × GOMAXPROCS)))` for sparse, so the result
    splits into roughly 2× (dense) or 4× (sparse) blocks per worker — enough
    granularity for the pool to rebalance stragglers (sparse rows have variable
    nnz cost) — with a 256-row floor to keep per-block overhead negligible.
    More cores → more, smaller blocks. The worker count never exceeds
    `GOMAXPROCS`; surplus blocks queue and are consumed in order, so the pool
    stays busy without spawning one goroutine per block.
  - **Measured.** Block-parallel scoring vs the single-block serial path on a
    12-core x86-64 box (production build, 16384 rows): ~1.5-2× on 2 cores and
    ~1.8× (`FloatVector`, memory-bandwidth-bound IP/COSINE SIMD) to ~4.5-4.9×
    (`Float16` fused, `SparseFloatVector`) on 12 cores; the gain saturates once
    blocks reach ~2×GOMAXPROCS, which is exactly what this sizing targets.
- **Cost:** QN reduce truncates to `limit + offset` (the proxy merges the
  caller's offset into the limit it hands down), so each QN scores at most
  `limit + offset` rows — `#QN × (limit + offset)` cluster-wide. Scoring in
  the worker also means the raw vector column never leaves the QueryNode
  unless the user requested it (§5 drop rule).

#### Why score in the worker, not the proxy

Distance is computed in the worker (per QN reduce) rather than in the proxy
(final reduce) because scoring is cheap but shipping the vectors is expensive:

- The per-row scoring cost is ~250-380 ns for a 768-dim vector; transferring
  one row's vector is `dim × 4` bytes. Moving the computation to the proxy
  would force every candidate row's vector column from every QN over the
  gRPC boundary (`#QN × (limit+offset) × dim × 4` bytes) to score only the
  final `limit` rows — trading cheap compute for expensive transport.
- The worker already holds the raw vectors locally (fetched via the internal
  `OutputFieldIds` injection), so scoring there costs zero extra transport.

A corollary: an "offset down-sinking" scheme — having each worker skip its
first `offset` rows because the proxy will discard them — is **not** correct
across shards. `offset` is a global concept: the final `[offset, offset+limit)`
rows of the merged, globally-sorted result can land in the middle of *any*
worker's locally-sorted output, so a worker cannot know which of its own rows
survive without seeing the other workers' data. The remaining cost of scoring
discarded rows under a large `offset` (or under ORDER BY, whose final order is
decided in the proxy) is inherent to the offset/ORDER BY semantics; the
block-parallel scoring above keeps that cost latency-bound rather than
serial.

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
  (`common.CalcDistanceFieldID`, `-10000`, shared with the QueryNode's column
  producer) to that remap target when
  `calc_distance` is present. Appending it last keeps the column position
  aligned with the `output_fields` order (the synthetic column is always
  appended last in PostExecute).
- The restrictions stay on **aggregation / GROUP BY** (non-raw results,
  nothing to score), **query function chains / QueryScan** (that path
  materializes only the RetrieveRequest output fields, so the injected vector
  column is absent — `prepareQueryFunctionChains` rejects the combination),
  and the **Materialize reply path** (it skips the QN reduce that computes the
  distance column — `materializeInScope` excludes calc_distance).
- **COSINE zero-norm:** a dense (float/int8/fp16/bf16) all-zero reference
  vector with `metric_type=COSINE` is rejected up front with
  `WrapErrParameterInvalid`, because cosine would otherwise be undefined for
  every row. A zero-norm **data row** scores **0** (not NaN) across all dense
  types: `CosineImpl`/`CosineImplPure` return 0 for zero norms, matching
  knowhere's normalize-then-IP search semantics (`NormalizeVec` keeps zero
  vectors zero, norm 1.0) and the fused int8/fp16/bf16 kernels. This guard is
  a **shared change** to `CosineImpl`/`CosineImplPure` and therefore also
  applies to the existing COSINE callers of `CalcFloatDistance`
  (`internal/util/clustering`, used by segment pruning / pruning optimizer),
  where a zero-norm vector now scores a deterministic 0 instead of an
  undefined NaN — a safe direction for the sort/compare those paths perform.
  Binary is restricted to HAMMING/JACCARD and sparse to IP, so no COSINE
  check applies there.
- **Reference vector NaN/Inf:** a reference vector containing NaN or Inf is
  rejected up front with `WrapErrParameterInvalid`, for every metric — it
  would otherwise poison the whole distance column. Float16/BFloat16 are
  decoded for the check (a large finite float32 narrows to Inf in half
  precision); Int8 has no NaN representation, and sparse/binary carry no NaN
  concept. Stored data rows are not scanned: Milvus does not validate NaN/Inf
  on dense-vector insert, so a dirty row can still yield NaN distances, which
  matches search's behavior on such data.
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
column is comparable to a search score only when the metrics agree. For
`COSINE` the value is cosine *similarity* (1 = identical, range [-1, 1]),
exactly what Milvus search reports for the COSINE metric — the faiss-family
backends normalize then run IP (`knowhere` `metric.h` maps COSINE to
`METRIC_INNER_PRODUCT`; `flat.cc` normalizes first) and HNSW negates its
internal `-IP` back at the search boundary (`hnsw.cc` `transform ? -dist :
dist`), so the two are directly comparable despite the "distance" name. This
is also the official definition: the metric docs
(`milvus.io/docs/metric.md?tab=floating`, "Cosine similarity") define Milvus's
`COSINE` as `cos(A,B) = Σ(aᵢ·bᵢ)/(|A|·|B|)` with value range [-1, 1] and
"a greater value indicates a greater similarity"; it explicitly notes that
`1 - similarity` is the derived *cosine distance*, which is not what the
`COSINE` metric returns.

## Test Plan

- **Go unit tests — distance kernels** (`pkg/util/distance/calc_distance_test.go`):
  hand-verified HAMMING / JACCARD over packed bits, sparse IP against
  reference implementations, hand-computed truth values for the fused
  int8/fp16/bf16 kernels, and zero-norm COSINE returning 0 on both the pure
  and SIMD `CosineImpl` paths.
- **Go unit tests — fused decoders** (`calc_distance_fused_amd64_test.go`):
  F16C/AVX2 decoders bit-for-bit against the scalar decoders on
  subnormal/zero/normal boundaries and on uniform random bit patterns (inf/NaN
  lanes compared with `IsNaN`); SIMD fused kernels vs. pure-Go within
  tolerance on random data.
- **Go unit tests — proxy** (`internal/proxy/dql/task_query_test.go`):
  `calc_distance` JSON parsing (direct-vector forms per type: float array,
  int8 array, fp16 array, bf16 array, binary byte array, sparse object /
  array-of-pairs; expression forms), `gjson.Valid` malformed-JSON error,
  missing `output_name`, `distance` not a legal output field, per-type
  metric/dimension validation, reference-vector NaN/Inf rejection, expression
  0-row/multi-row errors and per-type canonical byte extraction (a stubbed
  `QueryRunner`), agg combination error, `output_name` collision rejection
  (including the requested dynamic-field key and the primary-key field, both
  named `distance`), the shared `common.CalcDistanceFieldID`, internal injection
  of `calc_distance.field` into `OutputFieldIds` without appearing in user
  `output_fields`, and the zero-row rewrite of the reserved synthetic column
  into a Float32 distance column.
- **Go unit tests — QueryNode** (`internal/querynodev2/tasks/query_task_test.go`):
  distance column correctness after QN reduce per type (float vs.
  hand-computed L2/IP/COSINE; int8/fp16/bf16 vs. float32-decoded reference;
  binary HAMMING/JACCARD; sparse IP), column alignment, empty results,
  nullable target fields (zero placeholder + `ValidData`, no NaN, plus a
  valid-row-count guard), block sizing (`blockRowLimit`, core-scaled target
  blocks with a 256-row floor),
  large-N parallel scoring order,
  growing + sealed segments, combinations with ORDER BY / iterator / limit,
  and dropping the vector column when the user did not request it.
- **Go unit tests — proxy reduce:** the synthetic distance column
  (`common.CalcDistanceFieldID`) survives the cross-QN reduce — the plain
  pipeline merges it by PK order and the ORDER BY pipeline's
  `FieldIDRemapOperator` keeps it (both with the reserved id in the output id
  list), staying row-aligned with the ids.
- **Regression — normal query unchanged:** with no `calc_distance`, the result
  (fields, order, limit/offset, iterator, ORDER BY) must be byte-identical to
  the pre-feature behavior, and the Arrow transport must still be used. Covers
  the empty-plan-fields path end to end (proxy parse → plan marshal → C++
  `ParsePartialFromCodedStream` → QN reduce → response assembly).
- **Integration:** standalone Milvus + pymilvus; `calc_distance` (native dict)
  returns distances matching search for the same vector for float and sparse
  fields; binary HAMMING/JACCARD matches search scores; both
  `output_fields`-with/without-the-vector-field cases; expression form
  (exactly one hit) consistent with the direct-vector form; nullable field
  with null rows returns `null` distances (ValidData) rather than NaN.
- **Error codes:** new errors all use `WrapErrParameterInvalid`; no new error
  codes, no merr guard changes.

## Upgrade / rollout note

All new logic is gated on `calc_distance` being present, so existing queries are
unaffected, and `QueryPlanNode` additions are proto3-backward-compatible (C++
parses plans with `ParsePartialFromCodedStream`, `Plan.cpp:251`; unknown fields
are ignored). During a rolling upgrade, `calc_distance` is only safe after the
**whole cluster** has been upgraded:

- **All QueryNodes old** — a new proxy injects the target vector field id into
  `plan.OutputFieldIds`; the old QueryNode fetches it as a normal column but
  drops the unknown `calc_distance` plan field, so the response carries no
  distance column. `output_fields` then has no distance name appended (the
  append is conditional on the reserved-FieldId column surviving), so this case
  degrades cleanly: the query succeeds without a distance column.
- **Mixed old and new QueryNodes** — **the query errors.** Old QueryNodes return
  the injected vector column while new QueryNodes return the synthetic
  `-10000` distance column in its place (the vector column is dropped when the
  user did not request it), and the proxy's cross-QN reduce merges
  positionally (`buildMergedFieldDataRange`, helpers.go:120-133), rejecting the
  mismatched column count/order with "FieldsData count mismatch / order
  mismatch".

So `calc_distance` must not be advertised until every QueryNode runs the new
binary; during the upgrade window a `calc_distance` query may fail rather than
degrade.

## Appendix: Benchmark — Go vs SIMD fused decoders

Measured on a 3.2 GHz / 12-core x86-64 CPU (F16C + AVX2 present), production
build (no `-N -l`). Payload: 10 000 rows × 768 dim, i.e. 14.6 MiB of fp16/bf16
bytes. Numbers are per-call, single goroutine; the QueryNode's bounded worker
pool (§5) parallelizes across blocks, so wall-clock scales with worker count.

### Conversion only (bytes → `[]float32`)

| Type | Pure Go | SIMD (asm) | Speedup |
|---|---|---|---|
| fp16 | ~47 ms / 328 MB/s | ~6.7 ms / 2330 MB/s | ~7× |
| bf16 | ~16 ms / 944 MB/s | ~6.5 ms / 2330 MB/s | ~2.5× |

### Fused kernel (decode + IP score)

| Type | Pure Go | SIMD (asm) | Speedup |
|---|---|---|---|
| fp16 | ~46 ms | ~3.5 ms | ~13× |
| bf16 | ~23 ms | ~3.5 ms | ~6.5× |

Observations:

- **fp16 benefits far more than bf16.** Pure-Go fp16 decoding is slow (~328
  MB/s, per-bit sign/exponent/mantissa extraction), while bf16 decoding is
  shift-and-or (~944 MB/s, already compiler-friendly). Under SIMD both converge
  to the ~2.3 GB/s ceiling.
- **The fused kernels amplify the gap** (fp16 ~13×) because the decode and the
  `asm.IP`/`asm.L2` scoring are vectorized together.
- **Allocation:** the pure-Go decode allocates a ~30 MiB intermediate
  `[]float32` per call; the SIMD decode reuses a caller buffer (0 allocs). The
  pure-Go fused kernels decode inline per row and allocate only the distance
  array (~40 KB).
- These numbers were produced with the AGENTS.md-required
  `-gcflags="all=-N -l"` disabled to reflect production; with the flag, pure Go
  runs 3-4× slower and the gap looks larger.

## Appendix: SDK usage examples

The wire value of `calc_distance` is always a **JSON string** (see §1). Every
example below passes a pre-serialized JSON string as the value of the
`calc_distance` key; the four required fields are `metric_type`, `field`,
`vector` and `output_name`. The `vector` array shape must match the target
field's data type (`[0.1, 0.2, ...]` for `FloatVector`, `[1, -2, 3]` for
`Int8Vector`, byte values for `BinaryVector`, a sparse object or
array-of-pairs for `SparseFloatVector`), or be a JSON string expression (e.g.
`"id == 2"`).

The reference vector below is a 4-dim `FloatVector` named `embedding` with
metric `L2` and output column name `distance`.

```json
{
  "metric_type": "L2",
  "field": "embedding",
  "vector": [0.1, 0.2, 0.3, 0.4],
  "output_name": "distance"
}
```

### pymilvus

pymilvus additionally supports the **native dict form**: pass `query_params`
directly and the SDK serializes it to JSON internally
(`utils.dumps` → `orjson.dumps`). This requires the one-line `calc_distance`
pass-through described in §1 (unknown `query_params` keys are otherwise
dropped by `prepare.py`).

```python
from pymilvus import Collection

col = Collection("test")
res = col.query(
    expr="id > 0",
    output_fields=["id"],
    query_params={
        "calc_distance": {
            "metric_type": "L2",
            "field": "embedding",
            "vector": [0.1, 0.2, 0.3, 0.4],
            "output_name": "distance",
        }
    },
)
# res[0] contains the extra "distance" column
for row in res:
    print(row["id"], row["distance"])
```

### Java (MilvusClientV2)

`QueryReq.queryParams` is a `Map<String, Object>` and the SDK stringifies every
value with `String.valueOf`. A Java `Map` value
would be mangled into `{metric_type=L2, ...}` — not valid JSON — so pass a
**pre-serialized JSON string**. Gson or any JSON library works.

```java
import com.google.gson.Gson;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

Map<String, Object> calcDistance = new HashMap<>();
calcDistance.put("metric_type", "L2");
calcDistance.put("field", "embedding");
calcDistance.put("vector", List.of(0.1f, 0.2f, 0.3f, 0.4f));
calcDistance.put("output_name", "distance");

Map<String, Object> queryParams = new HashMap<>();
queryParams.put("calc_distance", new Gson().toJson(calcDistance)); // must be a JSON string

QueryResp resp = client.query(QueryReq.builder()
        .collectionName("test")
        .filter("id > 0")
        .outputFields(List.of("id"))
        .queryParams(queryParams)
        .build());
// resp.getQueryResults() rows each carry a "distance" field
```

Do not put `NaN`/`Inf` into `vector`: Gson would serialize them as `NaN`, which
is invalid JSON and the server rejects it.

### C++ (MilvusClientV2)

`QueryArguments` carries an `unordered_map<string,string>` (`extra_params_`);
`AddExtraParam` stores the string verbatim.

```cpp
#include <milvus/MilvusClientV2.h>

milvus::QueryArguments args;
args.SetCollectionName("test");
args.SetFilter("id > 0");
args.SetOutputFields({"id"});
args.AddExtraParam("calc_distance",
    R"({"metric_type":"L2","field":"embedding","vector":[0.1,0.2,0.3,0.4],"output_name":"distance"})");

milvus::QueryResult result;
milvus::Status status = client.Query(args, &result);
```

### Go (milvusclient)

`QueryOption.queryParams` is a `map[string]string` and is written to
`QueryRequest.QueryParams` verbatim. Pass the
JSON string through the SDK's queryParams map. A `WithQueryParams(map[string]string)`
pass-through setter on `queryOption` is the one-line addition this needs (same
as the pymilvus one in §1); until it lands, build the
`milvuspb.QueryRequest` directly and set `QueryParams`:

```go
import (
    "github.com/milvus-io/milvus/client/v3/milvusclient"
)

const calcDistance = `{"metric_type":"L2","field":"embedding","vector":[0.1,0.2,0.3,0.4],"output_name":"distance"}`

option := milvusclient.NewQueryOption("test").
    WithFilter("id > 0").
    WithOutputFields("id").
    WithQueryParams(map[string]string{"calc_distance": calcDistance})

resultSet, err := client.Query(ctx, option)
// resultSet rows include the "distance" column
```

### Node (milvus-sdk-node)

`client.query` spreads `params` into `queryParams` and calls
`parseToKeyValue` **without** `valueToString`, so a JS
object is not auto-serialized — the caller must `JSON.stringify` the value:

```typescript
import { MilvusClient } from '@zilliz/milvus2-sdk-node';

const client = new MilvusClient({ address: 'localhost:19530' });
const res = await client.query({
  collection_name: 'test',
  filter: 'id > 0',
  output_fields: ['id'],
  params: {
    calc_distance: JSON.stringify({
      metric_type: 'L2',
      field: 'embedding',
      vector: [0.1, 0.2, 0.3, 0.4],
      output_name: 'distance',
    }),
  },
});
// res.data[i] carries the "distance" field
```

### Rust (milvus-sdk-rust)

`QueryRequest.extra_params` is a `HashMap<String, String>` and is written to the
proto `query_params` verbatim; pass the JSON string:

```rust
use std::collections::HashMap;
use milvus::v2::client::ClientV2;
use milvus::v2::request::dql::QueryRequest;

let mut params = HashMap::new();
params.insert(
    "calc_distance".to_string(),
    r#"{"metric_type":"L2","field":"embedding","vector":[0.1,0.2,0.3,0.4],"output_name":"distance"}"#.to_string(),
);

let req = QueryRequest::builder()
    .collection_name("test")
    .filter("id > 0")
    .output_fields(vec!["id".to_string()])
    .extra_params(params)
    .build()?;
let resp = client.query(req).await?;
// resp.results().rows()? each carry the "distance" field
```

### C# (MilvusClient)

`QueryParameters.ExtraParameters` is an `IDictionary<string, string>`; assign
the JSON string directly:

```csharp
using Milvus.Client;

var parameters = new QueryParameters
{
    CollectionName = "test",
    Filter = "id > 0",
    OutputFields = new[] { "id" },
};
parameters.ExtraParameters["calc_distance"] =
    "{\"metric_type\":\"L2\",\"field\":\"embedding\",\"vector\":[0.1,0.2,0.3,0.4],\"output_name\":\"distance\"}";

MilvusQueryResult result = await client.QueryAsync(parameters);
// result[0] carries the "distance" field
```

### Shared notes

- **Expression form.** Replace the array with a JSON string of a single-row
  expression, e.g. `"vector": "id == 2"` (the JSON value itself must be a
  quoted string). The server resolves it with an internal query that must match
  exactly one row.
- **`output_name` is not required in `output_fields`.** The proxy appends the
  distance column to the result automatically; it must not collide with an
  existing output column name (including the primary key).
- **The distance column is `float32`.** Rows whose target field is null carry a
  zero placeholder and `ValidData=false` (never `NaN`).
- **Metric per field type.** `L2`/`IP`/`COSINE` for the dense float family,
  `HAMMING`/`JACCARD` for `BinaryVector`, `IP` only for `SparseFloatVector`.

## Future work

### Per-group distance aggregation

`calc_distance` is currently mutually exclusive with `aggregation` / `group_by`
(§4). A natural extension is to aggregate the synthetic distance column within
groups — e.g. `query(output_fields=["avg(distance)"], group_by="tag")` returns,
per `tag` group, the mean of its member rows' distances. Semantically this is
plain SQL (`SELECT tag, AVG(distance) FROM ... GROUP BY tag`) with `distance` a
per-row derived column; the requirement is coherent and well-defined.

Why the current rejection exists — the two structural blockers:

- **Pipeline order.** The synthetic column is appended *after* the QN reduce
  (`applyCalcDistanceToResult` runs on the reduced result), while GROUP BY
  collapses rows *during* the reduce (`DeduplicateByGroups`); a grouped output
  has no per-row vector left to score.
- **Aggregates only reference schema fields by FieldId.** `avg(distance)` fails
  in `translateOutputFields` ("field not exist") because the synthetic column
  (`common.CalcDistanceFieldID`) is invisible to aggregate expressions.

Implementation path (design sketch):

1. **Lift the mutual exclusion, restricted to the distance column.** Allow the
   combination only when the aggregates' target field id is
   `common.CalcDistanceFieldID` (the calc_distance output column). Ordinary
   schema-field aggregates combined with calc_distance stay rejected — a
   grouped row has no raw vector to score, and there is no per-group semantic
   for them.
2. **Compute distance pre-group.** At the QN, inject the synthetic column into
   the row stream *before* `DeduplicateByGroups` (instead of after the reduce),
   so the group reducer can consume it. The proxy's `ReduceByGroups` consumes
   the same column for the cross-shard merge. This is the ordering change that
   makes the whole feature possible.
3. **Address the synthetic column from aggregates.** Allow
   `Aggregate.FieldId = common.CalcDistanceFieldID` as a valid input;
   `translateOutputFields` maps `avg/sum/min/max/count(distance)` to the
   synthetic column (the aggregate's `output_name` is the resolved column
   name). `min`/`max` per group (nearest/farthest member distance) are the most
   useful; `count(distance)` ≈ group size.
4. **Restrict the aggregate target to the distance column itself** — never the
   raw vector column.

Cost: per-group `avg(distance)` requires scoring *every* matching row of each
group — there is no `limit + offset` truncation to lean on, so the cost grows
linearly with the filtered set size and vector width. Document a cap or quota
(e.g. max matching rows per group-by calc_distance query) before advertising
the feature.

### base64 / hex string input for BinaryVector

The `vector` value travels as a JSON string, and JSON has no native bytes type,
so a `BinaryVector` reference is currently input as a JSON array of byte values
(`[0, 255, 1, ...]`, §1). An alternative input form is a base64 (or hex) string
— `"AP8B"` / `"00ff01"` — matching the raw-bytes feel of the search placeholder.

The blocker is disambiguation with the expression form: `vector` is a JSON
string for both a base64-encoded binary vector and a single-vector expression
(`"id == 2"`), and the server currently tells them apart by the string's
*content* (first byte `[`/`{` vs `"`). Two workable rules:

- **Per-field-type binding**: strings are base64 only for `BinaryVector`
  fields and expressions only for the dense/sparse types, so each field type's
  string meaning is fixed and needs no heuristic. But `"id == 2"` on a binary
  field then becomes unparseable as an expression — an acceptable narrowing,
  since binary expression references are rare.
- **Content probe**: try expression parsing first and fall back to base64, or
  vice versa; riskier because a base64 payload can collide with expression
  syntax.

The first rule is the clean choice if this extension is picked up; it needs a
`base64` decode step in `encodeCalcDistanceVector` and validation that the
decoded length equals `dim/8`.
