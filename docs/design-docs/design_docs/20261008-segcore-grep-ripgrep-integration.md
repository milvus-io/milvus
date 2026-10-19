# Grep: QueryScan Execution and ripgrep Integration

- **Created:** 2026-10-08
- **Updated:** 2026-10-27
- **Scope:** Current Grep implementation: matching engine, QueryScan execution,
  in-process Arrow transport and distributed computed results.
- **Engine dependency:** ripgrep `15.2.0`, commit `e89fff89ac9af12e8d4ce9d5fd07beb408ca730f`

## Summary

Grep searches independent VarChar/Text field values with a memory-only Rust
matcher. QueryScan starts with filter/MVCC candidate offsets, reads and matches
text in native batches, then reads PK/timestamp only for matching rows. The
worker merges those rows, retrieves ordinary output fields for winners, and
exports one protobuf result. In-process row data uses Arrow; RPC uses protobuf.

Proxy forwards raw arguments. QueryNode validates the single-Grep chain and
binds each segment; native code compiles one matcher and creates one session per
segment call. GrepOp returns row selection and JSON details. The pipeline reads
identities separately afterward. Each sealed native Grep call acquires its own
read lease and captures its own snapshot. Candidate, identity and output reads
do not share a request-owned snapshot object. The current runner reads candidate
text directly; there is no additional Grep index-bitmap stage.

`milvus-grep-core` is a local path dependency of `tantivy-binding`. The final Rust
artifact remains `libtantivy_binding.a`, linked into `milvus_core`. The C++ Grep
module wraps the memory-only API and adapts it to segment reads.

This document is the single Grep reference. The shared
[Function Chain row-selection contract](20261010-function-chain-row-selection.md)
remains separate because it also serves other operators.

- [Supported matching scope](#2-goals-and-scope)
- [Build integration](#3-selected-dependency-and-build-integration)
- [QueryScan pipeline, Arrow ownership and RPC results](#4-queryscan-execution-and-result-transport)
- [Argument grammar and C API](#5-parsing-and-c-api)
- [Engine ownership](#6-ownership-and-concurrency)
- [Budgets and failure semantics](#7-budgets-cancellation-and-failure-semantics)
- [Verification and historical measurements](#11-verification-and-recorded-results)
- [Planned element_filter support](#13-planned-support-element_filter-with-grep)

## 1. Motivation and Integration Context

Milvus already has RE2-based string matching in
[`RegexQuery.h`](../../../internal/core/src/common/RegexQuery.h). Its full and
partial matchers primarily return Boolean results and enable `dot_nl`.
That behavior is useful for existing filters. Grep instead returns matching
lines and content-relative spans. Existing filter semantics remain unchanged.

The repository also has an established Rust-to-C++ integration:

- [`tantivy-binding/Cargo.toml`](../../../internal/core/thirdparty/tantivy/tantivy-binding/Cargo.toml)
  declares Rust dependencies and produces `staticlib` and `rlib` artifacts.
- [`tantivy/CMakeLists.txt`](../../../internal/core/thirdparty/tantivy/CMakeLists.txt)
  invokes `cargo +1.89`, imports the resulting static library, and installs its
  headers.
- [`build.rs`](../../../internal/core/thirdparty/tantivy/tantivy-binding/build.rs)
  runs cbindgen. Its current
  [configuration](../../../internal/core/thirdparty/tantivy/tantivy-binding/cbindgen.toml)
  generates a C++ header.
- [`src/CMakeLists.txt`](../../../internal/core/src/CMakeLists.txt) links
  `tantivy_binding` into `milvus_core`. The
  [Segcore target](../../../internal/core/src/segcore/CMakeLists.txt) is an
  object library, not the final shared-library link target.

This design reuses that build path while keeping Grep logic separate from
Tantivy index objects.

## 2. Goals and Scope

Grep searches independent UTF-8 VarChar/Text values in memory and returns
matching lines with content-relative byte spans. Each non-NULL value is one
input; values are never concatenated across records. C++ skips database NULLs
and retains the mapping from batch positions to segment offsets.

The request supplies an rg-style `arguments` string and a separate field name.
Supported flags are repeated `-e`, `-F`, `-i`/`-s`, and `-w`/`-x`, including the
exact aliases in [the argument grammar](#51-argument-string-grammar-and-whitelist).
Parsing and compilation happen on the execution node. The matcher performs no
file traversal, external command execution, network access or terminal output.

Matching is LF line-oriented. Context, multiline, smart case, CRLF normalization,
fragment-only output, line numbers, field offsets, per-field match limits,
inversion, counts, PCRE2, transcoding and replacement are unsupported. The
parser rejects unlisted flags, including flags whose supplied value would make
them a no-op. There is no fallback to a different matcher.

Collection/partition selection, authorization, MVCC, primary keys, projection,
Query limit/offset and scheduling belong to the Query layer. Matching precedes
final record pagination. The memory-only C API has no storage or database
identity inputs and no resumable cursor, reserved feature flags or generic
extension dictionary.

The [QueryScan pipeline](#six-stage-grep-data-flow) defines the JSON result
representation; [matching-line rules](#53-matching-line-results) define line
boundaries, span enumeration and empty-input behavior.

## 3. Selected Dependency and Build Integration

### 3.1 Version and License

Use the **15.2.0 release commit**, not upstream `master`. The release declares
Rust 1.85 as its minimum version and is compatible with the selected Milvus
Rust 1.89 toolchain at the independent-build level.

| Crate | Version in the selected source commit | Use |
|---|---|---|
| `grep` | 0.4.1 | Facade used by `milvus-grep-core` |
| `grep-matcher` | 0.1.9 | Matching interface and occurrence enumeration |
| `grep-regex` | 0.1.14 | Pattern compilation and matching |
| `grep-searcher` | 0.1.17 | Retained facade dependency; unused by the current matching path |
| `grep-cli` | 0.1.12 | Retained default dependency |
| `grep-printer` | 0.3.1 | Retained default dependency |
| `grep-pcre2` | 0.1.10 | Optional; not enabled in version one |

These are the package versions in the release source, not the minimum version
ranges appearing in individual dependency declarations. Pinning only the
crates.io facade version would not pin all of its transitive dependencies.

ripgrep is dual-licensed `Unlicense OR MIT`. Select MIT, retain its copyright
and license text, and review notices for the final resolved dependency set.

### 3.2 Source Management and Layout

Use the public upstream `grep` crate through a Cargo Git dependency pinned
by full commit SHA. Cargo owns its source checkout/cache; Milvus does not
maintain a source snapshot, Git submodule, fork, or parser-export patch.
The [Cargo manifest](../../../internal/core/thirdparty/grep/Cargo.toml)
records the pinned dependency; the lockfiles described below preserve its resolved
dependency graph. Refer to the selected
[upstream MIT notice](https://github.com/BurntSushi/ripgrep/blob/e89fff89ac9af12e8d4ce9d5fd07beb408ca730f/LICENSE-MIT)
for project-wide third-party distribution notices; no local copy is maintained.

Parameter tokenization, whitelist enforcement, conflicts and normalization
belong in shared Go code called by the query execution entry. Rust owns engine validation, compilation and matching.
The core manifest records the upstream revision; both the standalone and binding
lockfiles pin the resolved source and transitive dependencies.

```text
internal/core/thirdparty/grep/
  Cargo.toml                # Pinned Git facade dependency, consumed by binding.
  Cargo.lock                # Reproducible standalone core tests.
  build.rs                  # Generate the independent C header with cbindgen.
  cbindgen.toml
  include/milvus_grep.h      # Generated; do not edit.
  src/lib.rs
  src/ffi.rs                # C ABI types, handles, validation, panic boundary.
  src/plan.rs              # Shared validation and matcher compilation.
  src/session.rs
  src/result.rs
  src/error.rs
  tests/

internal/core/thirdparty/tantivy/tantivy-binding/
  Cargo.toml                 # Local Grep path dependency.
  Cargo.lock                 # Locked integrated dependency graph.
  build.rs                   # Existing Tantivy header generation.
  src/lib.rs                 # Re-export the Grep FFI module for static aggregation.

internal/util/function/grep/
  options.go, lexer.go, parser.go  # Pure Go argument parsing and budgets.
  *_test.go                       # Grammar, limits and fuzz tests.

internal/core/src/grep/
  Grep.h/.cpp               # RAII wrapper and typed Segcore errors.
  GrepTest.cpp              # Real Rust integration tests, also in all_tests.
  CMakeLists.txt            # Object library, linked into milvus_core.
  GrepFaultTest.cpp          # Isolated test-only ABI failures, collected by all_tests.

# Query adapter:
# Filter/MVCC candidates, field pinning, batching, row mapping and reduction.
```

`milvus-grep-core` is an `rlib`. `tantivy-binding` declares:

```toml
milvus-grep-core = { path = "../../grep" }
```

`milvus-grep-core` pins the upstream Git dependency:

```toml
[dependencies]
grep = { git = "https://github.com/BurntSushi/ripgrep.git", rev = "e89fff89ac9af12e8d4ce9d5fd07beb408ca730f" }
```

Matching uses `grep::matcher` and `grep::regex`; the facade also retains the
Searcher dependency. The default facade features do not launch the CLI or
expose all of its behaviors.

The pinned upstream command-line parser lives in the executable's private
`crates/core/flags` module, not in the facade's public API. `grep::cli` is not
an argument parser for rg. Milvus does not export or depend on that parser.
The pure Go package `internal/util/function/grep` implements the argument grammar below: tokenize the
string, check each option occurrence against the whitelist, validate conflicts,
and produce typed patterns/options. The Query protocol carries the original
argument string. There is no shell, rg configuration loading,
process argv access, or upstream `HiArgs` construction on this path.

Rust reuses `RegexMatcherBuilder` for engine validation and compilation at the
execution node. Do not substitute Go's regexp compiler for the selected Rust
engine's syntax validation.

Use one pinned Git source for the Grep crates. Initial builds may fetch it;
CI can cache Cargo Git and registry sources. `--locked` enforces the committed
resolution but is not an offline flag. Fully offline builds require all
resolved sources to be cached or vendored through a separate workflow.

### 3.3 Build Integration

The [Tantivy CMake target](../../../internal/core/thirdparty/tantivy/CMakeLists.txt)
invokes `cargo +1.89 build --locked` from `tantivy-binding`, adding `--release`
for non-Debug builds. Cargo builds Grep through the local path dependency and
uses the binding lockfile. The target runs Cargo on each build; Cargo's own
fingerprinting decides whether Rust sources need rebuilding. Grep does not
produce a separate static library in this integration.

The Grep [build script](../../../internal/core/thirdparty/grep/build.rs) generates
C-compatible `milvus_grep.h` from `src/ffi.rs` with cbindgen. CMake records the
header as a build byproduct and installs it alongside `libtantivy_binding.a`
and the existing Tantivy headers. The imported `tantivy_binding` target depends
on installation, and the final native link remains `milvus_core`.

The standalone Grep crate has its own lockfile for isolated tests. Those tests
do not replace integrated binding tests. Generated headers and native libraries
must come from the same build. The [C++/Go boundary](#6-ownership-and-concurrency)
passes execution-local arguments synchronously; normalized options and native
handles do not enter the distributed Query protocol.

## 4. QueryScan Execution and Result Transport

### Execution model and supported scope

QueryScan runs Grep after filter/MVCC produces candidate offsets. Native code reads matching text in batches; the Go chain handles offsets and matching details. Only matching rows receive PK/timestamp lookup. The worker merges by PK/timestamp, reads ordinary output_fields for winners, then exports one protobuf result. Candidates, identities and details use in-process Arrow; RPC uses protobuf.

Query accepts one QueryScan chain containing exactly one independent Grep operator. It declares one schema VarChar/Text input and one detail output. The text field is a native read dependency; the runtime DataFrame contains only `$seg_offset`. Dynamic fields, expression children, multiple Grep operators and subsequent operators are unsupported. Count, aggregation, GROUP BY, ORDER BY, element-level Query and Query iterator are excluded.

`output_columns` controls detail export; a chain exporting columns must have a name. Computed names cannot collide with collection or system fields. The protobuf stage is `FunctionChainStageQueryScan`, and the Go stage is `query_scan`. Static preparation validates structure, fields and output schema. Each segment creates its own chain/GrepOp, parses arguments and binds the segment. Native code compiles a matcher and creates a session per segment call, reusing them across that call's text batches.

See [query_plan.go](../../../internal/util/function/chain/query_plan.go) and [operator_grep.go](../../../internal/util/function/chain/operator_grep.go).

### Request and row limits

This example illustrates FunctionChain fields, not SDK invocation syntax:

```json
{
  "expr": "status == 500",
  "output_fields": ["id", "title"],
  "limit": 20,
  "offset": 5,
  "function_chains": [{
    "name": "text_hits",
    "stage": "QueryScan",
    "ops": [{
      "op": "grep",
      "inputs": ["message"],
      "outputs": ["matches"],
      "params": {"arguments": "-i -e 'timeout'"}
    }],
    "output_columns": ["matches"]
  }]
}
```

Grep accepts only `arguments`. `QueryScanBuildContext.QueryLimit` supplies the worker Query limit, including the offset allowance prepared by Proxy, as the per-segment native match cap. The example uses a worker cap of 25; Proxy applies offset=5 and limit=20. Unlimited (-1) maps to MaxInt64. The cap counts matching documents, not lines or spans. Cross-segment deduplication and final paging can still return fewer documents than requested.

Candidate retrieval uses the candidate row cap and byte budget, without truncating at the Query limit. `function.grep.maxScanRows=0` leaves candidate row count uncapped; a positive value selects an ordered prefix. When more candidates may remain, HasMoreResult is retained and a warning is logged, so enabling this cap can make results incomplete. The flag is aggregated downstream; it does not represent an extra Grep probe of unread text.

Public computed output uses `QueryResults.fields_data`, identified by field name and aligned with ordinary result rows. Internal retrieve results keep computed columns in `computed_results`. Span endpoints use Int64.

### Six-stage Grep data flow

Assume Grep reads `message`, exports `matches`, and the ordinary output projection is `id, title`. N denotes candidates, M segment matches, and K worker merge winners. Each document can contain multiple matching lines, each with multiple spans.

| Stage | Representation | Row contents | Still deferred |
| --- | --- | --- | --- |
| Retrieve offsets | Arrow Record → DataFrame | `$seg_offset: Int64` | PK, timestamp, text, ordinary outputs |
| Grep output | OperatorResult → DataFrame | Selected `$seg_offset`, `matches` | PK, timestamp, ordinary outputs |
| With identity | DataFrame | `$seg_offset`, `matches`, `$id`, `$timestamp` | Ordinary outputs |
| Merge | DataFrame + Selections | All winning columns plus sidecar source locations | Ordinary outputs |
| Materialize | DataFrame | Previous columns + `id, title` | Response payload is available |
| To proto | segcorepb.RetrieveResults | Ids, FieldsData, ComputedResults | Offsets/source locations are omitted |

The entry point is [RunQueryScanPipeline](../../../internal/querynodev2/segments/query_scan.go), selected by [QueryTask](../../../internal/querynodev2/tasks/query_task.go) when a query chain exists.

**1. Retrieve offsets.** After filter/MVCC, candidate mode retains the existing `find_first_n` PK traversal order, without the ordinary Query limit. `maxScanRows` and the candidate byte budget still apply; offsets are not re-sorted by physical position.

```text
RetrieveSegmentResult
├─ Segment: source segment handle
├─ Result: scalar counts, HasMoreResult and scanned-byte statistics
└─ Record: non-null $seg_offset: Int64[N]
```

The chain receives one logical chunk containing only offsets. Segment identity lives in the execution binding, not in a per-row column. An offset is meaningful only within its segment. No PK, timestamp, text or output columns are exported yet, although candidate traversal can internally access PK metadata. Native code may construct protobuf repeated data internally, but the QueryScan C++/Go boundary exports row data through Arrow C Data and keeps only scalar statistics in the protobuf header. See [ExecPlanNodeVisitor.cpp](../../../internal/core/src/query/ExecPlanNodeVisitor.cpp), [segment_c.cpp](../../../internal/core/src/segcore/segment_c.cpp) and [retrieve_arrow_result.go](../../../internal/util/segcore/retrieve_arrow_result.go).

**2. Grep output.** The native runner reads text in batches using offsets; it does not build a complete Go `message` column. Its Arrow output is:

```text
positions: Int64[M]
details: Binary[M]  // milvus.data_type = JSON
```

Positions are strictly increasing indices into the immediate input offset array, not physical offsets. Each Binary details row encodes a JSON array of matching-line objects. Content is the matching line, not necessarily the whole document. Paired starts/ends describe `[start, end)` UTF-8 byte ranges relative to that line, not character indices.

```text
input offsets = [12, 37, 90]
positions     = [0, 2]
details       = [
  [{content: "foo bar foo", starts: [0, 8], ends: [3, 11]}],
  [{content: "foo",         starts: [0],    ends: [3]}]
]

After chain selection:
$seg_offset = [12, 90]
matches     = [first document's details, second document's details]
```

[GrepOp.Execute](../../../internal/util/function/chain/operator_grep.go) returns positions as RowSelection and retains details directly as the named JSON Binary output column. Positions do not remain a DataFrame column. If details are not declared as exported output, pruning can discard them while preserving selection. The native type is in [native/types.go](../../../internal/util/function/grep/native/types.go); construction and import validation are in [SegmentGrep.cpp](../../../internal/core/src/grep/SegmentGrep.cpp) and [grep.go](../../../internal/util/segcore/grep.go).

The native runner and GrepOp use a Binary column with `milvus.data_type` set to
the decimal Milvus JSON enum. Static planning and runtime output share this
field declaration. The builder's `SetComputedField` consumes logical type and
nullability, and empty output frames use the same declaration. Binary columns
can be selected/reordered without interpreting JSON. The native boundary checks
JSON match shape once; protobuf export only copies the bytes. Result budgeting
charges encoded JSON size (including escaping) plus 16 bytes per document for
position, offset and validity allowance.

**3. With identity.** `RetrieveIdentitiesByOffsets` reads only selected offsets, returning `$seg_offset: Int64`, `$id: Int64/String` and `$timestamp: Int64`. `withQueryIdentities` checks offset equality and order row by row, then attaches retained identity arrays without copying payload buffers. The frame becomes `$seg_offset | matches | $id | $timestamp`. PK provides logical identity, timestamp supports version selection, and offset supports later physical reads. Empty matches skip identity retrieval; a fully empty result receives a complete typed schema during final assembly. See [query_scan.go](../../../internal/querynodev2/segments/query_scan.go) and [retrieve_identities.go](../../../internal/util/segcore/retrieve_identities.go).

**4. Merge.** `mergeQueryScanResults` consumes existing PK order, chooses duplicate sources using timestamps, and applies the implementation's limit/reduceType branches. It returns:

```text
queryScanMergeResult
├─ Frame: $seg_offset, matches, $id, $timestamp (K rows)
└─ Selections: [{SegmentIndex, Offset}, ...] (K entries)
```

Selection i corresponds to frame row i. SegmentIndex indexes the valid source-segment slice; it is not the segment's actual ID. A separate source-frame row index gathers all columns from the same winning row. Duplicate replacement updates both the physical source and the detail-row reference. Column schemas are checked by name before gathering. Gather reads directly from the original frames by column name, accepts differing producer order, and uses the first frame's column order for output; no intermediate reordered frames are constructed. QueryScan inputs share one prepared chain and contain identities, offsets and computed details; ordinary collection fields are materialized after merge. Gather retains Arrow field compatibility checks and copies column metadata from the first frame.

| Address | Targets | Purpose |
| --- | --- | --- |
| Grep positions | Candidate frame rows | Select matching offsets |
| Merge source-row index | Matched frame rows | Gather identity and details |
| SegmentIndex + Offset | Physical segment rows | Read ordinary outputs |

Consecutive source rows become Arrow slices. One slice can share buffers; concatenating multiple slices copies selected data. See [query_scan_merge.go](../../../internal/querynodev2/segments/query_scan_merge.go).

Before materialization, QueryScan deducts computed columns' referenced Arrow
buffer sizes (`ArrayData.SizeInBytes()`) from `MaxOutputSize` and passes the
remaining budget into `FillRetrieveFieldsOrdered`. Slices may retain larger
buffers, making the computed estimate conservative. After grouping merged
winners by source segment, C++ sums each segment's selected row count times
its requested fields' average sizes, including the timestamp field in the
retrieve plan. It uses the same `get_field_avg_size` statistics as ordinary
Retrieve, with a locked accessor for growing statistics. The aggregate check
runs before any output field reads; ordinary late-materialization callers pass
the full output budget. Missing statistics may still estimate zero and averages
can underestimate individual rows, so the final ordinary-plus-computed protobuf
size check remains required.

**5. Materialize.** `materializeQueryFrame` converts Selections to `segIndices: Int32[K]` and `segOffsets: Int64[K]`. `fetchFieldsAsRecord` calls `FillRetrieveFieldsOrdered`, returning ordinary columns in final worker row order. Retained arrays are attached to produce `$seg_offset | $id | $timestamp | matches | id | title`. The internal `$id` and projected collection field `id` can coexist. Native Timestamp is skipped in favor of retained `$timestamp`. Projecting `message` reads it as an ordinary output field here; matching does not provide a reusable full-text output cache. These are worker-local winners, still subject to delegator/Proxy reduction and pagination. See [query_scan.go](../../../internal/querynodev2/segments/query_scan.go) and [ignore_non_pk_ops.go](../../../internal/querynodev2/segments/ignore_non_pk_ops.go).

**6. To proto.** [chain.ToRetrieveResults](../../../internal/util/function/chain/converter.go)
exports the final DataFrame using its saved column definitions, without taking
a CollectionSchema. Collection origin determines ordinary versus computed
output. Empty frames retain the same definitions. See
[column conversion](#column-metadata-and-conversion-details) for metadata,
chunk conversion and validity rules.

| Source | Destination |
| --- | --- |
| `$id` | RetrieveResults.Ids |
| `$timestamp` | Timestamp system Int64 FieldData in FieldsData |
| Saved collection field definition | FieldsData |
| Non-system column without a collection definition | ComputedResults |
| `$seg_offset`, other unexported system columns, sidecar Selections | Omitted |

Matches becomes a JSON FieldData named `matches`, without a chain-name wrapper
or collection field ID:

```text
ComputedResults[column named matches].Scalars.JsonData.Data
├─ document 0: [{"content":"foo bar foo","starts":[0,8],"ends":[3,11]}]
└─ document 1: [{"content":"foo","starts":[0],"ends":[3]}]
```

Each entry is one UTF-8 JSON byte string. Zero-row responses retain an empty
JSONArray. Exported bytes are Go-owned and outlive the native DataFrame.
Statistics remain separately assembled by the pipeline.

### Ordinary Query: three paths

Ordinary Query does not run the Grep DataFrame/with-identity pipeline. Routing is in [retrieve.go](../../../internal/querynodev2/segments/retrieve.go) and [query_pipeline.go](../../../internal/querynodev2/segments/query_pipeline.go).

| Path | Segment output | After merge | Ordinary field reads |
| --- | --- | --- | --- |
| Protobuf | Ids, Offset, FieldsData, statistics | Merged protobuf columns | Before merge |
| Arrow | Protobuf identity/statistics header + Arrow outputs | Result header + ArrowSelection | Before merge |
| IgnoreNonPk | Protobuf PK, offsets, timestamp, etc. | IDs + Selections, then field fetch | After merge |

**Protobuf:** filter/MVCC → offsets with segment limit → FillTargetEntry reads PK, timestamp and outputs → segcorepb.RetrieveResults → PK/timestamp merge, deduplication and row selection → merged FieldsData → internalpb.RetrieveResults. Ordinary values already exist when merge runs; no offset-based reread is needed.

**Arrow:** filter/MVCC → offsets → read outputs → protobuf header (Ids, Offset, system fields/statistics) plus Arrow ordinary columns → reduce produces a result header and ArrowSelection → MaterializeArrowSelection writes selected Arrow rows to protobuf FieldsData. ArrowSelection borrows source records and `(resultIdx, rowIdx)` references; records must remain alive through conversion. This materialization converts already-read values, unlike Grep's post-merge segment reads. Ordinary Arrow transport requires InterfaceZeroCopyEnabled and excludes aggregation, ORDER BY and IgnoreNonPk. Candidate mode always uses Arrow independently of this switch.

**IgnoreNonPk:** filter/MVCC → offsets → read PK/timestamp first → protobuf RetrieveResults → MergeByPKWithOffsets produces `IDs + [{SegmentIndex, Offset}]` → FetchFieldsData reads winning ordinary fields → protobuf RetrieveResults. Eligibility includes multiple segments, a finite limit, a permitting plan and no aggregation; ORDER BY is excluded by the plan. The second read uses Arrow or protobuf according to InterfaceZeroCopyEnabled. Unlike Grep, identity is already available after the first retrieve. See [ignore_non_pk_ops.go](../../../internal/querynodev2/segments/ignore_non_pk_ops.go) and [arrow_materialize.go](../../../internal/util/queryutil/arrow_materialize.go).

### RPC boundary and final pagination

```text
worker: internalpb.RetrieveResults {Ids, FieldsData, ComputedResults, statistics}
  → delegator: cross-worker merge, deduplication and limit
  → Proxy: cross-shard merge and final offset/limit page
  → milvuspb.QueryResults {FieldsData: ordinary columns + computed columns, ...}
```

The worker forwards segcorepb fields into internalpb. Arrow records, segment handles and candidate offsets do not cross this RPC boundary. Computed columns follow the same source-row selection as ordinary fields through version replacement and pagination; ordinary Query has no Grep computed columns. See [query_task.go](../../../internal/querynodev2/tasks/query_task.go), [queryutil/computed_results.go](../../../internal/util/queryutil/computed_results.go) and [Proxy query_pipeline.go](../../../internal/proxy/dql/query_pipeline.go).

### Scheduling and native reads

Segment chains execute independently with GOMAXPROCS-bounded concurrency per
request and shared request cancellation. Native Grep attempts run on the node's
shared SQ pool, so concurrent requests share its execution capacity. Read-gate
retry backoff occurs outside the pool. Pool submission can block while full;
the worker checks cancellation before starting Grep. The caller awaits native
completion and keeps segments pinned through reads
and conversion. Each sealed native Grep call acquires its own read lease and
captures a snapshot for that call; candidate, identity and output reads do not
share a request-owned snapshot. GrepOp retains no matcher/session across calls.
Text batches use pinned views or owned data and skip NULL values.

[Resource limits](#71-resource-limits) define candidate, matching and output
budgets. [Arrow ownership](#arrow-boundary-ownership) defines row-buffer lifetime;
[engine ownership](#6-ownership-and-concurrency) defines native handles.

### Arrow boundary ownership

Reuse `CRetrieveArrowResult`, `RetrieveArrowResult.GetResult`, and their existing
one-shot import and cleanup rules. Its header remains a small protobuf carrying
scalar statistics, with no offsets, IDs or field data in QueryScan mode.

The native retrieval implementation may still construct protobuf repeated
fields internally. The exporter moves their storage into a shared
`ProtoStorageOwner` before creating Arrow arrays. Fixed-width non-null arrays
alias that storage using the existing `FieldDataToArrow` conversion; strings
are copied into Arrow's string layout. Exported buffers outlive the native
future/result wrapper and are released through Arrow C Data callbacks.

Candidate records are owned by `RetrieveSegmentResult` and released through
`ReleaseRecords`, including partial segment-fanout failures. Identity records
and detail arrays remain owned by the QueryScan pipeline until final output
assembly. On validation failures the imported identity record is released.
Final string PKs are copied so the RPC response does not borrow native memory.

This is not an end-to-end zero-copy claim. Native candidate construction,
string conversion, Function Chain selection, final field materialization and
RPC serialization may still allocate/copy. It also does not eliminate final
PK/timestamp re-reads or cache full input text for output projection.

### Computed-result protocol and reduction

`computed_results` is a repeated FieldData, using the same data representation
as ordinary `fields_data`. Each column has one outer row per result ID. The
Grep payload layout is shown in [final export](#six-stage-grep-data-flow).

Segcore RetrieveResults carries the final materialized computed_results at field
10, aligned with its IDs and ordinary fields. QueryScan returns this single
result object; the task forwards the computed fields into the internal RPC
result without a deep copy. Per-segment details remain Arrow until final row
selection; this does not add computed protobuf handling to the generic segcore
reducer. Ordinary Query leaves this field unset.

Internal RetrieveResults carries computed_results at field 22. Proxy appends these
columns to public QueryResults.fields_data after pagination and empty-field
completion; no separate public computed-results field is needed. Public definitions
come from the milvus-proto Go module version pinned in go.mod and pkg/go.mod. This feature requires matching server and client
protocol definitions. It does not change persisted collection data formats.

The PK/timestamp/limit algorithms do not inspect computed contents. At the same
materialization points that handle IDs and ElementIndices, the reducer gathers
computed fields using exactly the same rowRef list. Timestamp replacement thus
selects both normal and computed data from the same version. Sorting applies
the same permutation; document paging applies the same range. These shared
reduction helpers also support element-level data, but the QueryScan validator
rejects element-level requests.

Payload validation runs once at each reduce ingress, before empty-shard
filtering and single-input shortcuts, so malformed discarded results are not
hidden by pagination. Subsequent row selection, sorting and paging only map
column names and check row references; they do not recursively revalidate the
nested payloads. Each RPC receiver still validates its own incoming results. Empty successes keep
the typed empty FieldData columns. Empty inputs without computed fields may
coexist with typed empty inputs. Ordinary queries carry nil and do not enter
typed-column conversion. Aggregate and group-by queries remain excluded by the
existing QueryScan request validator.

Computed field identity is its top-level name. Producer-local IDs and column
ordering do not participate in Query computed-result matching. Column headers
are checked for duplicate/missing names and incompatible types before gathering
rows. Collection fields retain their schema/field-ID validation and ordinary
merge path.

The computed-result merger supports scalar and JSON columns. It preserves
full-length validity when some inputs omit an all-valid mask. JSON documents
are opaque bytes; they are not parsed or re-encoded by the reducers. Output
containers own their slices; JSON byte payloads may be shared with Go-owned
inputs and must not be mutated after publication.

### Column metadata and conversion details

The shared [row-selection contract](20261010-function-chain-row-selection.md#assembly-and-metadata)
defines immutable column descriptors and copy-on-write setters. Query attaches
complete collection definitions during materialization; Search imports its
scalar definitions from FieldData or native Arrow metadata. Descriptors retain
IDs, logical types, nullability, element types and vector parameters through
selection and merge. Computed outputs receive fresh metadata. A numeric field
ID alone does not establish collection origin.

Search and Query use `exportFieldData` and `ArrowChunkedColumnToFieldData`.
The chunked converter validates the shared physical type and vector dimension
once per column; Arrow enforces matching physical types across its chunks.
Single-array conversion validates at its own entry point and uses the same
writer. The converter allocates the final protobuf payload and validity mask
once for the whole column, then fills them directly from each chunk without
intermediate Arrow concatenation or per-chunk protobuf objects and merging.
Narrow integers and Geometry String layouts are supported. Dense vectors and vector arrays validate byte
width and dimension, including for empty columns; dimensions can be inferred
only from an unambiguous physical width. Collection Array uses Binary protobuf
storage; ArrayOfVector uses its native List layout.

Zero-row output uses typed zero-chunk Arrow columns derived with
`NativeColumnType`; export creates typed empty protobuf payloads directly.
Computed scalars retain declared nullability. Search omits all-valid masks for
Boolean, numeric, string, timestamp and Geometry scalars at response assembly;
this omission does not apply to JSON/Array. Query retains the converter's
validity representation.
Search Merge in HybridSearch L2 rerank receives inputs prepared from a common
plan/schema. It preserves the first available definition for each column,
including nullability; it does not revalidate collection definitions across
those inputs.

Strings and JSON payloads are copied into Go-owned protobuf storage for the
selected output rows. Export does not decode or re-encode Grep JSON.

### Client decoding

Decode each `matches.scalars.json_data.data` entry with `json.loads`. The local
E2E script `tests/scripts/grep_query_e2e.py` compares the resulting line content
and UTF-8 byte spans against its oracle.

## 5. Parsing and C API

The memory-only C API uses three opaque handles. Its all-input operations are
listed below. The C++ runner bounds each matching batch by the read batch and
remaining Query match quota. Go owns parsing
and normalized configuration. Plan owns a compiled matcher, Session owns
execution state, and Result owns matching lines and spans.

| Function | Responsibility |
|---|---|
| `mg_grep_compile` | Validate/compile an execution-local typed spec; no CLI parsing |
| `mg_grep_plan_free` | Release the caller's Plan reference |
| `mg_grep_session_create` | Create task-local execution state retaining the Plan |
| `mg_grep_session_cancel` | Request sticky cancellation |
| `mg_grep_session_free` | Release a quiescent Session |
| `mg_grep_execute_batch` | Match an input batch synchronously |
| `mg_grep_result_view` | Borrow a read-only batch result |
| `mg_grep_result_free` | Release all result-owned storage |

### 5.1 Argument String Grammar and Whitelist

The query execution entry implements this grammar using the shared Go parser. It applies to the UTF-8 string after the
SDK/transport has decoded it once. It does not include host-language string delimiters or JSON escapes.
For example, the actual argument string is:

```text
-i -e "user_id=[0-9]{3,6}\b.*timeout"
```

The same string in a JSON request property would be:

```json
{"arguments":"-i -e \"user_id=[0-9]{3,6}\\b.*timeout\""}
```

The example shows the Grep operator arguments after transport decoding. Parsing
produces the following logical spec (field identity is resolved by GrepOp on QueryNode):

```json
{
  "patterns": ["user_id=[0-9]{3,6}\\b.*timeout"],
  "pattern_kind": "regex",
  "case_mode": "insensitive",
  "boundary": "none"
}
```

#### Lexical rules

- Unquoted ASCII space, tab, CR, and LF separate tokens. Other Unicode
  whitespace is literal. Quotes are removed and adjacent quoted/unquoted
  pieces concatenate into one token. Empty quotes create an empty token.
- Single quotes preserve every enclosed byte except the closing single quote;
  a backslash has no special meaning inside single quotes.
- In double quotes, backslash escapes only `"` and `\`. Before any other byte
  it is preserved, so `\b`, `\d`, and `\n` reach the regex parser unchanged.
- Outside quotes, backslash escapes ASCII token-separating whitespace, either
  quote, or another backslash. Before other bytes it is preserved. A dangling
  unquoted backslash or an unclosed quote is an error. Escaped whitespace is
  literal, not a shell line-continuation instruction.
- No shell is invoked. No variable, command, glob, tilde, or environment
  expansion occurs. Shell metacharacters inside a pattern are literal pattern
  bytes; outside a pattern, unexpected tokens fail. `#` does not start a comment.
- The argument string must be valid UTF-8 and contain no literal NUL. Bound its
  bytes before expensive setup. Never read process argv, stdin,
  rg configuration files, or pattern files.

#### Accepted options

| Short | Exact long alias | Normalized effect |
|---|---|---|
| `-e PATTERN` | `--regexp PATTERN` | Append one pattern, preserving order; at least one required |
| `-F` | `--fixed-strings` | All patterns are literal; default is regex |
| `-i` | `--ignore-case` | Case-insensitive |
| `-s` | `--case-sensitive` | Case-sensitive; also the default |
| `-w` | `--word-regexp` | Word boundary mode |
| `-x` | `--line-regexp` | Whole-line boundary mode |

Support attached `-ePATTERN` and `--regexp=PATTERN`. Short flags can be grouped
(e.g. `-Fi`); `e` consumes the remainder of its token or the next token.
The next token after a separated `-e` is always its value, even if it starts
with `-`: `-e -C` searches for the pattern `-C`, not context. A missing next token
is an error. Boolean flags take no values; `--ignore-case=false` is invalid.
Do not support abbreviated long names or unlisted negated aliases.

Repeated `-e` appends patterns, including empty ones; `-e ""` is valid. Patterns
are compiled as one ordered OR expression, not searched independently. Repeated
`-F`, `-w`, or `-x` is idempotent. For `-i`/`-s`, the last occurrence wins.
Combining `-w` and `-x` is an error, irrespective of order, rather than inheriting
rg's precedence. `--` ends options, but all following positional tokens are
rejected. A final bare `--` is allowed if a pattern was already supplied.
There is no positional pattern, filename, executable name, or implicit empty
pattern. Use `-e` for every pattern.

Unknown flags and rg flags outside this whitelist fail
at the execution node as typed request-input errors. The diagnostic names the offending
option and reason; do not parse error messages for control flow. These Go
parsing failures do not cross the Rust C API. There is no best-effort
acceptance, ignored option, or fallback to ordinary Query. An unsupported flag
fails even if its value would disable its effect, such as `-C 0`.

| Actual argument string | First-phase outcome |
|---|---|
| `-i -e "user_id=[0-9]{3,6}\b.*timeout"` | Accepted; preserves the regex word-boundary escape |
| `-F -e 'a.b' -e timeout` | Accepted; two literal patterns |
| `-i -s -e error` | Accepted; case-sensitive |
| `-e ""` | Accepted; explicit empty pattern |
| `-i -C 2 -e "user_id=[0-9]{3,6}\b.*timeout"` | Error: context is outside the initial scope |
| `-P -e error` / `-U -e error` / `-S -e error` | Error: unsupported option |
| `-w -x -e error` | Error: conflicting boundary modes |
| `-e` / `-e "unterminated` / `-e error app.log` | Error: missing value, unclosed quote, or positional token |
| `-e '['` | Error: invalid regex (`MG_GREP_INVALID_PATTERN`) |

#### Go parser API and error contract

`internal/util/function/grep` provides the pure-Go `grep.Parse` API. It performs
no shell execution, filesystem lookup, environment expansion, cgo calls or regex
compilation. GrepOp calls it during per-segment construction; the
[QueryScan execution model](#execution-model-and-supported-scope)
defines Proxy forwarding, operator construction and native plan lifetime.

```go
spec, err := grep.Parse(
    `-i -e "user_id=[0-9]{3,6}\b.*timeout"`,
    paramtable.Get().FunctionCfg.GrepMaxArgumentBytes.GetAsInt64())
```

The byte limit is positive and supplied by the factory from
`function.grep.maxArgumentBytes`; see [resource limits](#71-resource-limits)
for defaults. `Spec` contains ordered patterns and
matching enums, not field IDs, database metadata, native handles or a wire
representation. Its fields are private and only the parser populates them.
Read-only accessors do not expose the backing pattern slice; the zero value is invalid.

The parser is bounded by the arguments byte limit and does not
accept a context. It returns no partial Spec on failure. Malformed user arguments
produce existing merr input errors (`ParameterInvalid`, `ParameterMissing` or
`ParameterTooLarge`); invalid service budgets produce system errors. The caller
checks request cancellation before and after construction. The parser does not
translate C ABI statuses. Parsing success does not establish regex validity: `-e '['` produces a
Spec, but native compilation rejects it before matching.

### 5.2 Normalized Spec and Input Contract

The normalized matching options are `pattern_kind` (regex/literal), `case_mode`
(sensitive/insensitive), and `boundary` (none/word/line). Zero initialization
selects regex, case-sensitive, no boundary restriction. Reject unknown enum
values when constructing the Go specification and at the Rust compile boundary.
Word mode uses the pinned builder's
Unicode word matching semantics; it is not a tokenizer or Chinese segmentation.

The shared builder fixes Unicode on, LF line-oriented matching, and disables
cross-line search. Set the matcher line terminator to LF and configure line
anchors consistently. No literal LF is accepted in a normalized pattern;
regex expressions requiring LF are rejected by the line matcher builder.
Character classes follow that builder's line-terminator restriction. Literal
mode treats backslashes as ordinary pattern bytes after argument tokenization.
In regex mode, reject Unicode-disabling inline constructs with syntax-aware
validation; other inline flags follow the pinned builder and cannot enable
cross-line scanning. Engine validation and execution use the same compiled matcher.

`MgGrepBytes` is length-delimited, not a C string. Patterns and field values
must be valid UTF-8. Normalized patterns cannot contain literal NUL (consistent
with the argument grammar); a regex escape such as `\x00` can match NUL in
text. Field text may contain NUL and BOM bytes, which remain unchanged.

The C layer never resolves fields. C++ excludes database NULLs; `{NULL, 0}`
represents an empty non-NULL field. Reject a NULL data pointer with nonzero
length. Pattern count is at least one. An empty batch permits a NULL text-array
pointer and returns a Result with zero entries. Otherwise a successful Result
has exactly one entry per input, including inputs without matches.

Validate pointer/length combinations, counts, checked arithmetic (including
Rust slice length constraints) and result metadata budgets before allocating.
Validate all text UTF-8 before matching; validation checks cancellation. Actual non-NULL pointer validity and
memory extent are caller obligations, not something the ABI can safely probe.

### 5.3 Matching Line Results

The Rust/C API has one output shape:
`BatchView -> TextResult[] -> Line[] -> {content, Span[]}`.

- LF separates logical lines. Remove only that terminal LF from `content`;
  a preceding CR remains ordinary content. Do not trim whitespace.
  A final unterminated line is searchable. A trailing LF does not invent an
  additional line after EOF. CRLF normalization is unsupported.
- Empty input has no logical lines and produces no matches. `"\n"` has one
  empty-content line, which an empty pattern selects with span `[0, 0]`.
- Matching and occurrence enumeration use the same logical line boundaries.
  The matcher runs on the complete logical line, not a cropped matching
  fragment. Line anchors and absolute anchors are evaluated against that input.
- Enumerate the combined matcher in non-overlapping order, retaining upstream
  tie/alternative order and empty-after-nonempty suppression. Omit any empty
  occurrence inside a UTF-8 code point. Continue enumeration to subsequent
  boundaries without duplication or looping. Nonempty span endpoints must also
  be valid character boundaries. These are acceptance requirements, not an
  assumption that `grep-regex`'s byte-oriented empty iterator already does it.
- Each selected logical line appears once, with all eligible spans; retain
  identical content from different lines as distinct entries in line order.
  Every returned line has at least one span, including an empty span where
  appropriate. `line_count > 0` is the record selection decision.
- Spans satisfy `0 <= start <= end <= content.len` and address UTF-8 bytes of
  that line's returned content. They are not original-field offsets. Do not
  duplicate matched text or return line numbers, roles, or field identifiers.
- Success includes all matching lines and spans for every input in the batch.
  The batch API has no first-hit early return or silent truncation. The C++
  runner caps the number of inputs in each call by the remaining Query match
  quota, so it never needs to truncate a returned record. Resource failure,
  deadline and cancellation produce no Result.

Result views are immutable and owned by their Rust handle. SpecView is a
borrowed C representation assembled by the caller from execution-local normalized
configuration. Neither layout is a serialized wire representation.

## 6. Ownership and Concurrency

The [Go segcore adapter](../../../internal/util/segcore/grep.go) flattens parsed
patterns into a byte buffer and lengths array, then passes them with scalar
options to the synchronous `ScanGrepByOffsets` call. `runtime.KeepAlive` keeps
the borrowed Go inputs alive through that call. No Go struct containing nested
Go pointers is passed as a Rust ABI view.

The [segment adapter](../../../internal/core/src/grep/SegmentGrep.cpp) reconstructs
C++ string views. [Grep.cpp](../../../internal/core/src/grep/Grep.cpp) builds
`MgGrepBytes` descriptors and `MgGrepSpecView` for `mg_grep_compile`. Rust borrows
them only during compilation and retains no caller pointer.

Patterns and configuration are copied or owned by the Plan before compilation
returns. A Session retains a strong reference to its Plan, so freeing the
caller's Plan handle does not invalidate existing Sessions. Each Session clones
the matcher and owns its regex engine cache pool. No mutable Session state is
shared between segment calls.

One Session may execute multiple batches sequentially. It cannot execute two
batches concurrently. Cancellation is the only operation allowed concurrently
with execution on the same Session. Different Sessions may run in parallel
under Milvus scheduling.

Input text is borrowed only until `execute_batch` returns. A Result owns its
arrays and text storage. All nested view pointers remain stable until
`result_free`, independent of the input, Session, and Plan lifetimes. Empty
arrays have NULL pointers. Read-only Result views may be shared, provided
release is synchronized with all readers.

Only Rust release functions free Rust objects. C++ wraps handles in RAII;
it never calls `free`/`delete` on returned storage. NULL release is a no-op.
The caller must synchronize releases: Plan release must not race with Session
creation; all Session execution and cancellation calls must have returned
before Session release; Result readers must finish before their handle is
released. Release functions do not wait for concurrent users.

Example lifecycle:

```text
Proxy: request checks -> forward field name + unchanged argument string
QueryNode: resolve field ID -> Go parse -> flat bytes/lengths/options
C++: string views -> MgGrepSpecView -> mg_grep_compile -> plan
session_create(plan) -> session
plan_free(plan)                         # Session retains ownership.
execute_batch(session, texts, ...) -> result
session_free(session)                   # No execution/cancel call remains.
result_view(result) -> copy/convert     # Input/session may already be gone.
result_free(result)
```

The installed header and library are delivered together for the same build and
architecture. Use C POD, fixed-width integers, `size_t`, and pointers; no Rust
`String`/`Vec`, C++ containers, packed structures, or cross-language `bool`.
Rust uses `repr(C)` structures and integer aliases. Do not add unused
`abi_version`, `struct_size`, or reserved fields for hypothetical consumers.
An incompatible future ABI change requires an explicit versioning decision.

### C++ Wrapper Contract

[`grep/Grep.h`](../../../internal/core/src/grep/Grep.h) exposes move-only Plan,
Session and Result owners. The public header contains no generated C view
structures. Result accessors return borrowed `std::string_view` content and
Span values, avoiding another copy of the complete Rust output. Index checks
throw a typed Segcore error; borrowed text must not outlive the owning Result.

A copyable CancellationToken pins shared Session state through cancellation
callbacks, including callbacks after the C++ Session owner is gone. Callers
still synchronize move/destruction of a C++ object with its own method calls.
An execute call pins its Session state locally; ordinary errors leave it
reusable, while internal execution failures remain poisoned even after cancel.
The Rust FFI validates descriptor sizes and accounts for result metadata;
the C++ wrapper owns temporary descriptor vectors and adopts returned handles
before checking call status.

`milvus::grep::GrepError` derives from `SegcoreError` and assigns the Milvus
error code at construction from the private status and operation phase. It
retains the original status (including unknown future values), operation, and
a bounded owned UTF-8 diagnostic. The Query/cgo adapter adds configuration
context while preserving that code. The generic cgo exception handler can
therefore preserve classification even without a Grep-specific catch.
Construction may allocate, like other `SegcoreError` subclasses; allocation
failure propagates as `std::bad_alloc` to the existing cgo OOM handler.

## 7. Budgets, Cancellation, and Failure Semantics

### 7.1 Resource Limits

| Configuration/control | Current purpose | Default |
| --- | --- | --- |
| function.grep.maxArgumentBytes | Argument byte limit | 65536 |
| function.grep.maxRegexBytes | Native regex compilation budget | 10485760 |
| function.grep.maxDFACacheBytes | Native DFA cache budget | 8388608 |
| function.grep.batchSize | Candidate rows per text read batch | 128 |
| function.grep.maxScanRows | Candidate rows per segment; 0 is uncapped | 0 |
| QuotaConfig.MaxOutputSize | Native matching-result byte budget; separately used for combined ordinary and computed output checks | Shared configuration |
| Request context | Execution deadline and cancellation | Request-defined |

The function settings are refreshable and fall back to defaults for invalid or out-of-range values. Operators and candidate plans read settings at their respective preparation boundaries. Read batch size is independent of remaining match quota. Native matching stops at input exhaustion or its record cap. See [function_param.go](../../../pkg/util/paramtable/function_param.go), [operator_grep.go](../../../internal/util/function/chain/operator_grep.go) and [SegmentGrep.cpp](../../../internal/core/src/grep/SegmentGrep.cpp).

The final QueryNode output size check sums the protobuf sizes of both FieldsData
and ComputedResults against the same MaxOutputSize limit. Computed values also
remain subject to the native Grep result budget.

Rust `max_result_bytes` accounts for logical result structures, C view arrays,
owned text and unselected-row metadata. The C++ runner separately charges the
encoded JSON output, including escaping and per-document Arrow overhead.

Compile and result limits are strictly positive and supplied by the service,
not raw CLI switches. The matching API has no input-byte budget or independent
timeout. QueryScan bounds read batches by row count and propagates request
cancellation, including request deadline expiry, through the session token.
Pointer/length and result-size arithmetic remain checked; zero-length texts
still consume descriptor/result metadata. Test the metadata-only exhaustion
case rather than checking only text bytes.

These are not aggregate RSS guarantees. Engine cache limits may apply to
multiple caches; logical result bytes exclude allocator overhead and temporary
engine storage. Go bounds concurrent segment executions. C++ bounds text batches
and accumulated encoded output per segment call, passing its remaining output
budget to each Rust batch. These are not one shared cross-segment byte budget.

The batch API has **whole-batch success or failure**. The C++ runner stops
between calls when the Query match cap is reached; it never turns a budget or
execution failure into partial success. Resource exhaustion,
invalid input and cancellation discard that call's output and return
`*out_result=NULL`. No details are silently truncated to fit a budget. Success
returns all matching lines and spans for every input field in that batch. If a
later batch fails, the caller fails the request rather than publishing earlier batches as a
complete success. Soft detail truncation is unsupported.

### 7.2 Cancellation and Timeout

`session_cancel` sets Rust-owned atomic state. It is idempotent and sticky:
cancellation before execution cannot be lost, and future executions on that
Session also return CANCELLED. There is no reset operation; use a new Session
for a new task. C++ does not share an atomic memory layout with Rust.

Check cancellation at validation loops, text boundaries and Matcher call
boundaries. The final cancellation check before publishing a
result is the success linearization point; later cancellation does not revoke
an already completed result. A cancellation callback must retain the Session
until it returns. Freeing a handle is not cancellation.

Cancellation and deadlines are cooperative. They cannot immediately interrupt
every individual regex call. Pattern compilation has no Session cancellation
handle. The Go execution path checks request cancellation before native entry,
and the native runner checks before acquiring its lease and compiling. The
session callback is installed after compilation; batch execution observes the
request token cooperatively. A single compiler/builder call cannot be preempted.
Compilation is bounded by service compile limits. Hard preemptive timeout is
not promised.

A cancelling Matcher wrapper must check before/after matching calls, including
candidate searches and detail enumeration. Test cancellation on long fields and expensive
patterns before accepting a service latency objective.

### 7.3 Errors and Milvus Classification

The C API returns a `uint32_t` status and optionally fills a caller-owned
512-byte UTF-8 diagnostic buffer. Success clears the buffer; failure may
truncate at a character boundary and always terminates it with NUL. Control
flow uses the status code, never error-string parsing. No error allocator,
`error_free`, global `last_error`, or I/O error category is needed.

The status constants in the generated header are **private Grep statuses**, not Milvus
`ErrorCode` values or wire codes. The execution-side Go and C++
adapter must classify by origin before constructing merr or typed Segcore
errors respectively:

| Origin | Required treatment |
|---|---|
| Malformed argument string, unsupported option, or invalid option combination from the request | Request-input failure at the execution entry, propagated to Proxy |
| Malformed user pattern or unsupported user pattern syntax | Request-input failure |
| Invalid user pattern UTF-8 | Request-input failure |
| Invalid UTF-8 in a persisted field | Data-integrity/system failure; do not blame the current request |
| Invalid descriptor, enum, output pointer, or internally supplied configuration | Internal adapter/plan contract failure unless proven to originate from request input |
| Pattern/result policy limit or regex engine build rejection | Request-input failure (`InvalidParameter`); no cross-replica retry or delegator blacklisting |
| Request cancellation or deadline | Preserve cancellation/deadline semantics; do not translate to success or empty results |
| Recovered Rust panic | Internal failure, never invalid input |

The Rust core exposes typed local errors. Upstream
`grep-regex::Error::regex/generic` stringify some syntax and compiled-size
failures into the same `ErrorKind::Regex`. AST/HIR validation catches syntax
early; remaining `Error::EngineBuild` failures map to the distinct private
`MG_GREP_ENGINE_BUILD_ERROR` status (8), without message parsing. Actual
`try_reserve` failures map to `MG_GREP_ALLOCATION_ERROR` (9), separate from
policy limits. Grep maps both `ResourceLimit` and `EngineBuildError` to
`InvalidParameter`, including C++ checks on accumulated JSON result bytes.
This deliberately treats all engine build rejections as request errors even
when upstream does not expose a more specific cause. Allocation failures,
internal adapter failures and cancellation retain their separate classifications.
The original error source is retained inside Rust. The
execution adapter must preserve these private categories and classify by origin,
without guessing from diagnostic text. Malformed normalized specs and unexpected
descriptor failures remain system errors. Context cancellation/deadline causes
must be preserved through remote error propagation so callers can apply the
corresponding retry policy.

Use the repository's
[error handling guide](../../dev/error_handling_guide.md) and
[casebook](../../dev/error_handling_casebook.md). Do not invent numeric 20xx
codes or cast a Grep status into `CStatus.error_code`. Audit producers and the
actual consumer path before choosing the existing Milvus code mapping.

Existing typed boundary patterns are visible in
[`tantivy-error.h`](../../../internal/core/thirdparty/tantivy/tantivy-error.h),
[`CGoCatch.h`](../../../internal/core/src/common/CGoCatch.h), and
[`merr/segcore.go`](../../../pkg/util/merr/segcore.go). The Grep adapter
must preserve its chosen classification through executor wrapping and the
cgo boundary.

Every failed handle-producing function sets a valid output slot to NULL. A
recoverable ordinary error clears per-call state. An internally panicked
Session is poisoned and returns INTERNAL_ERROR on later executions until it
is freed. The C handle also retains core internal failures so cancellation
cannot mask a poisoned state. Unwinding panics are caught at the ABI boundary; C++ exceptions do
not cross it. Abort and allocation failure cannot all be claimed recoverable.

## 8. Memory-Only Execution and Upstream Dependencies

The current core implements the equivalent line adapter described in Section
5.3: scan LF boundaries in bounded chunks, match each complete logical line
through `grep::matcher::Matcher`, and collect owned line/span results. It
does not construct a Searcher, transcode input, inspect BOM/binary markers,
map files, print, or write files. The engine handle and buffer lifetimes are
defined in [ownership and concurrency](#6-ownership-and-concurrency).

The facade retains I/O-related source and dependencies. `encoding_rs`,
`encoding_rs_io`, and `memmap2` are non-optional Searcher dependencies in this
version; a downstream `default-features=false` does not remove them. Cargo
feature unification can also re-enable shared capabilities through other
consumers. Do not promise that all unused code disappears from the final
shared library or use linker elimination as a security boundary.

The library uses Rust `log`; trace/debug messages may contain patterns or
extracted literals. Integrate with the existing process logger, suppress these
module-specific payload logs, and do not register a second global logger or
disable all Milvus logging. At the Go boundary, follow the repository's
[`mlog` guidance](../../agent_guides/observability/logging.md) and carry the real
request context. No new logging callbacks or telemetry knobs belong in this C
API. Metrics use low-cardinality labels rather than patterns or record IDs.

The contract forbids active external I/O by the matcher. It does not promise
that the entire query avoids storage reads: Milvus loads fields, mapped input
can fault pages, and allocators may use anonymous mmap. Verify the isolated
search path separately from the whole query's I/O behavior.

## 9. Deployment and Data Formats

- Existing RE2 filters and their semantics remain unchanged.
- This component does not change persisted data, indexes, WAL, manifests, or
  storage paths. It introduces no data migration.
- Existing requests keep their behavior. Grep requires server and client
  definitions that support QueryScan chains and computed columns in `fields_data`.
- NULL rows are excluded by C++; empty fields and zero-length matches have the
  explicit rules above. One complete field value is one independent input;
  storage chunks must not accidentally become matching boundaries.
- MIT and transitive notices accompany distribution. Full dependency-license
  and security review is based on the integrated lockfile, not the facade alone.

The integrated path-dependency build resolves Grep under the binding
lockfile and root profile. Standalone core tests use their own lockfile and may
resolve a different graph, so binding
integration tests are mandatory even when the crate's own tests pass.

## 10. Implementation Map

| Component | Main implementation |
| --- | --- |
| Request forwarding | `internal/proxy/dql/query_function_chain.go` |
| Static chain preparation and segment-bound operator | `internal/util/function/chain/query_plan.go`, `operator_grep.go` |
| Argument grammar | `internal/util/function/grep/{lexer,parser,options}.go` |
| Candidate mode and late identity reads | `internal/core/src/segcore/SegmentInterface.cpp`, `segment_c.cpp` |
| Text batching and JSON result construction | `internal/core/src/grep/SegmentGrep.cpp` |
| Native handles and Rust matching | `internal/core/src/grep/Grep.cpp`, `internal/core/thirdparty/grep/src/` |
| Go native boundary and result validation | `internal/util/segcore/{grep,grep_result,retrieve_identities}.go` |
| Worker execution and merge | `internal/querynodev2/segments/{query_scan,query_scan_merge}.go` |
| Field export and distributed computed reduction | `internal/util/function/chain/converter.go`, `internal/util/queryutil/computed_results.go` |

## 11. Verification and Recorded Results

### Go parser verification

Run from the repository root:

```sh
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/grep/...
go test -race -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/grep/...
go test -tags dynamic,test -gcflags="all=-N -l" -run '^$' -fuzz '^FuzzParse$' -fuzztime=15s ./internal/util/function/grep
```

The parser package can also be tested with `CGO_ENABLED=0`, without native Milvus
libraries or pkg-config configuration. Parser tests verify grammar and bounded
normalization; native matching and Query integration require their own tests.

### C++ wrapper and FFI fault tests

`GrepTest.cpp`, `GrepFaultTest.cpp` and `SegmentGrepTest.cpp` are collected by
`all_tests` in the normal `BUILD_UNIT_TEST` build. Existing C++ UT and coverage
jobs run them without an additional executable or a standalone CMake project.
Fault tests compile the unchanged `Grep.cpp` in their own translation unit
with renamed C++ namespace, opaque handles and C ABI entry points. The test
implementations cannot replace real Rust symbols or affect other test cases.
Production wrapper code has no test hooks or extra dispatch/lifetime state.

```sh
internal/core/output/unittest/all_tests --gtest_filter='Grep.*:GrepFault.*:*GrepScanTest*'
```

### Acceptance Cases

The following matrix is for phase one. Context, multiline, fragments, counts,
and per-field match limits are not enabled by these tests and need separate
acceptance contracts when implemented.

| Area | Required cases |
|---|---|
| Argument parsing | Every short/long alias; grouped flags; attached/separated values; literal backslashes; single/double quotes; empty token; Unicode; repeated patterns; last-wins case mode; `--`; all rejected examples in Section 5.1 |
| Rejection | Unsupported flags including no-op values, positional paths, missing values, malformed quotes, conflicting boundaries, invalid UTF-8/NUL, invalid regex; no execution Plan published on compilation failure |
| Pattern semantics | Regex/literal, ordered OR, case modes, word/line boundaries, inline flags, forbidden Unicode disabling and literal LF, line/absolute anchors |
| Text/results | Empty field, blank line, trailing LF, retained CR, unterminated final line, NUL/BOM, invalid UTF-8, all occurrences, empty matches on UTF-8 boundaries, identical lines retained, exact content-relative spans |
| Limits | Arguments byte limit, configurable compile/cache limits, metadata-only batches, result bytes, checked arithmetic, cancellation at native entry and during execution |
| Lifecycle | Compile retains no caller pointers; Plan release before Session; inputs/Session released before Result view; independent Sessions; synchronized cancellation/free |
| Failure | Whole-batch failure on any invalid input, budget, cancel or deadline; sticky cancellation; panic poisoning; no unwind across ABI; no partial result publication |
| Query integration | Raw arguments survive transport unchanged; shared grammar with per-segment operator construction and native compilation; Filter polarity, snapshot and NULL handling; selection before limit; row/detail alignment through reduction; compatible server/client deployment |
| Error propagation | Remote parse failures retain input classification through Proxy; execution failures through C++/executor/cgo to Go; input/system origin and retry classification audited end to end |
| Build | Execution-adapter symbol retention/export; header coexistence and C/Rust layout; clean parallel generation; merged graph; Linux Release/LTO; existing Tantivy regression |

Use a standalone search probe to trace file/network/process activity after
initialization, with relevant detailed logs suppressed. Separate loader,
allocator, and input page-fault effects from active external operations.

Benchmark short/long records, low/high match density, multiple batch sizes,
and concurrent Sessions. Measure Proxy forwarding, execution-side parsing/compilation, and steady-state
line/span extraction separately. Compare
final `libmilvus_core` size, build time, peak memory, result-copy cost, and
throughput against the same toolchain/profile baseline. Do not infer runtime
improvement from a smaller dependency count.

Acceptance includes failure-path verification from the error producer to the
consumer; successful matching alone does not establish error or retry behavior.

### Query pipeline verification

- Compare Arrow merge with the protobuf reducer for Int64/string PKs, duplicate
  replacement, timestamp ties, unsorted offsets, empty inputs, reduction modes
  and bounded/unbounded limits.
- Exercise C++/Go candidate and identity export/import, capped/empty candidates,
  one-shot import, cancellation and record access after native wrapper release.
- Check final PK/detail alignment through worker export, RPC serialization,
  delegator merge and Proxy paging, including reordered computed columns.
- Reject malformed types, row counts, validity masks and duplicate/missing names
  before empty-input filtering and single-input shortcuts. Check typed empty
  results and nullable scalar merges.
- Run real QueryNode tasks with both ordinary transport settings, and ordinary
  Query regressions. Standalone E2E must cover growing/sealed VarChar/TEXT and
  final ordinary fields; it does not replace multi-process cluster testing.

### Historical validation records

The following records were retained from the Arrow transport and computed-result
design notes. They describe those earlier runs and were not rerun as part of
this documentation consolidation. Revision IDs and local artifact paths identify
historical measurements, not reproducible repository fixtures.

#### Arrow transport validation (2026-10-19)

The native library and Go server were rebuilt together. Validation passed:

- 1,200 randomized Arrow/protobuf reducer comparisons, plus malformed-column
  validation (nulls, duplicate names, wrong types and offset alignment).
- `internal/util/segcore/...`, `internal/querynodev2/tasks/...`, and
  `internal/util/function/chain/...` package tests with `dynamic,test` and
  `-gcflags="all=-N -l"`.
- Targeted segments tests for QueryScan merge, ordinary PK merge, field fetch,
  QueryNode pipeline and retrieval routing.
- Standalone extended smoke E2E: 124 correctness checks covering VarChar/TEXT,
  growing/sealed segments, complex regexes and final ordinary/detail outputs.
  Test-owned collections were removed successfully.

Cancellation tests respect the existing asynchronous completion race: a short
native read may finish successfully before cancellation is delivered, and that
result must still be released. No extra cancellation checkpoints were added.
OOM/allocator failures were not fault-injected; the existing Arrow status mapper
and native future error propagation are reused. This run validates correctness,
not a measured throughput or latency improvement.

#### Computed-result validation before the performance comparison

- Full queryutil, proxy/dql and querynodev2/tasks package tests passed.
- Targeted segments QueryScan tests passed, including released/sliced Arrow and
  uint64-to-int64 overflow rejection. Ordinary Query task coverage passed.
- Internal/public wire contract tests and full fastpb/resource suites passed.
  The existing RetrieveResults decoder folds field 22 through protoMerge;
  no production codec change is needed. Differential tests cover duplicate
  message occurrences and truncated input before updating the descriptor hash.
- Current generated Python client + standalone extended E2E: 132 checks passed,
  no collection cleanup errors. Production binary build succeeded.
- Multi-source reduction across a serialized worker response and Proxy paging
  is covered in tests. A multi-process cluster deployment was not exercised.
- Performance was not measured in that functional validation run. The later
  comparison is recorded below.

#### Historical workload replay (2026-10-26)

Replay revision `6474b1cbf1` was compared with the recorded 2026-10-20 run of
`c39c00788c`. The replay reused the same `short_10k` corpus, verified by SHA-256
`c862d5e954f14a90800e2c4273983ae16541ffdcc1b13e785d19cac00e084cd3`.
Both workloads used 10,000 sealed rows, two shards, Strong consistency,
Query limit 10,000, offset zero and GOMAXPROCS=48. Each concurrency group ran
for 30 seconds. Both runs used a per-segment match cap of 10,000.

Both Go builds used `dynamic,sonic,with_jemalloc,bytedance_tango` tags and normal
optimizations. The replay required `-ldflags=-checklinkname=0` and loaded the
current Grep native implementation compiled with `-O3`. Latency covers RPC and
protobuf decoding; full JSON correctness checks ran during warm-up, with row-ID
checks on each timed response. All eight groups passed: 172,313 requests, zero
errors. Test collections were removed and the test instance was stopped.

| Matching documents | Concurrency | Historical QPS | Current QPS | QPS change | Historical P95 (ms) | Current P95 (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| 10 | 4 | 317.4 | 329.0 | +3.6% | 17.95 | 17.34 |
| 10 | 8 | 823.6 | 824.3 | +0.1% | 12.97 | 12.85 |
| 10 | 16 | 1526.7 | 1554.7 | +1.8% | 12.87 | 12.55 |
| 10 | 32 | 1855.5 | 1886.1 | +1.6% | 24.83 | 24.31 |
| 1116 | 4 | 85.3 | 87.7 | +2.8% | 62.06 | 60.49 |
| 1116 | 8 | 194.5 | 202.6 | +4.2% | 57.25 | 52.24 |
| 1116 | 16 | 286.0 | 396.7 | +38.7% | 84.83 | 50.53 |
| 1116 | 32 | 253.8 | 459.4 | +81.0% | 200.90 | 96.65 |

The 10-hit query was `QSPARSE0010`; the 1,116-hit query was
`(?i:warning|error|failed)`. Low-hit throughput was approximately unchanged.
The high-hit workload scaled better at concurrency 16/32. At concurrency 32,
server CPU use also increased from about 17.9 to 36.4 cores. This is evidence
of higher throughput with more CPU utilization, not lower per-query resource
cost. These are single historical/replay runs, not repeated trials establishing
statistical significance. Other code and build differences exist between the
revisions, so the improvement cannot be attributed solely to JSON encoding.

Raw local artifacts for this run were
`/tmp/milvus-concurrency-perf-20261020/report.json` and
`/tmp/grep-json-history-replay-7dba39ni/report.json`; they are not repository
fixtures. The table above preserves the comparison independently of those
local files. No multi-process cluster performance claim is made.

## 12. Remaining Validation Questions

- The configured defaults and per-segment limits are implemented. Their
  suitability across workloads and aggregate peak-memory behavior still require
  workload measurements.
- Cancellation latency on long individual matcher calls must meet the service
  objective. Cooperative checks do not provide hard preemptive interruption.

The selected ripgrep revision, local path dependency, single Rust static artifact
and memory-only C boundary are fixed by this design.

## 13. Planned Support: element_filter with Grep

A future version will support combining `element_filter` with QueryScan Grep.
This is planned work; the current implementation rejects element-level Query.

The initial scope is document-level Grep on a schema VarChar/Text field alongside
element filtering of a struct array. For example, `element_filter` may select
`items` elements whose price exceeds 50, while Grep checks the document's
`message` for `timeout`. A matching document retains its selected element indices
and one document-level Grep detail value; a nonmatching document is removed
together with its element indices. Matching text inside individual struct-array
elements is a separate extension.

### DataFrame representation and selection

Keep one DataFrame row per document. Add the reserved system column
`$query_element_indices: List<Int32>` only for element-level QueryScan. Each
list contains the matching struct-array element indices for that document;
its outer rows align with `$seg_offset`. Keep the existing Search system column
`$element_indices: Int32` and its import, merge and export contracts unchanged.

```text
candidate frame:
  $seg_offset | $query_element_indices
  100         | [1, 3]
  200         | [0]
       ↓ Grep: one scan of the document-level input field per candidate document
  selected offsets | selected element-index lists | matches
       ↓ withQueryIdentities
  selected offsets | selected element-index lists | matches | $id | $timestamp
       ↓ mergeQueryScanResults / output-field materialization
  document winners with element-index lists and document-level details
       ↓ ToRetrieveResults
  IDs + FieldsData + ElementLevel + ElementIndices + ComputedResults
```

The new column is a companion column before merge. Grep consumes only offsets
and returns its existing RowSelection; the framework applies that selection to
all retained input columns. QueryScan already uses `KeepAllSystemColumns`, so
registering the new name as a system column preserves it through pruning.
`withQueryIdentities` and materialization retain input columns. No Grep-specific
index-list selection or separate offset-to-indices sidecar is needed.

System-column retention and Arrow row-selection support are separate concerns.
The current `dispatchPickByIndices` in `operator_base.go` supports scalar and
binary arrays, but not Arrow List arrays. Extend the common selection path for
`List<Int32>`, including empty selections and chunk boundaries, before enabling
this feature. Avoid relying on the pinned Arrow v17 generic Take path without
checking its list-copy/allocation cost. Grep's native document-field read and
matching interfaces remain unchanged.

### Candidate retrieval and limits

- Extend native candidate retrieval and Arrow export to carry the element-level
  mode and each candidate document's indices alongside offsets. The current
  candidate return precedes element-index serialization; preserve the metadata
  before that return and import it as the new system column.
- Use element-level candidate selection without applying the final Query limit
  before Grep. Earlier documents can fail Grep while later candidates match.
  Define candidate document/element count bounds explicitly and charge the
  index lists as well as offsets against the candidate byte budget.
- Keep the native Grep matching-document cap distinct from element pagination.
  Verify its interaction with deduplication and cross-segment limits before
  choosing the cap for element-level requests; passing an element limit does
  not make native Grep count elements.

### Merge, pagination and export

`mergeQueryScanResults` continues to choose document versions by PK/timestamp.
The selected row identifies both the physical document and all companion
columns; element lists need not be duplicated in `OffsetSelection`. Timestamp
replacement must select the new row's element list and Grep details together.
Equal-timestamp duplicates retain the existing tie behavior; this design does
not introduce unioning of element lists across duplicate document versions.

For ordinary QueryScan, each selected document contributes one to the limit.
For element-level QueryScan, it contributes the length of its index list.
Adjust that count when replacing a selected version. A worker merge may retain
the complete index list of the final document that reaches the limit, leaving
precise trimming to the existing element-level pagination path. Verify that
Delegator/Proxy reduction uses the same element-count semantics and that the
user offset is applied only at the intended final paging boundary.

For example, lists `[1, 2, 5]` and `[0, 3]` with final limit 4 and offset 0
become `[1, 2, 5]` and `[0]`. Both documents retain their document-level Grep
detail value. Computed columns remain aligned with document rows, not expanded
element rows.

`ToRetrieveResults` exports the system column through `ElementLevel=true` and
row-aligned `ElementIndices`; it must not export it as ordinary or computed
FieldData. Empty frames preserve the element-level mode and typed zero-row
columns. Reuse QueryTask forwarding and queryutil element pagination. Public
QueryResults continues to use `fields_data` for ordinary and computed columns
and `element_indices` for element positions; no new public proto field is
required.

PyMilvus expands each returned document into rows with distinct `offset`
values. Those rows share the document-level Grep detail value and the requested
full array fields. This extension does not add element-only field reads or
reduce the full-array response amplification.

### Enablement and validation

Remove the QueryScan element-level restriction only after candidate export,
List selection, merge accounting and result export are supported end to end.
Keep the restrictions on struct-array Grep inputs, ORDER BY, aggregation and
Query iterator. Update the current supported-scope section when implementation
lands; until then this section describes planned behavior only.

Cover:

- Multiple matching elements per document, with one document-level Grep scan.
- Grep rejection removing the document, element indices and details together;
  later matching documents must not be hidden by an early candidate limit.
- Full, sparse and empty List row selections, column pruning and chunk handling.
- Limits/offsets within a document, across documents and across shards.
- Version replacement with different element counts, preserving detail alignment.
- Empty results, candidate budget exhaustion and document-cap behavior.
- Unchanged document-level Grep and existing Search `$element_indices` behavior.
- Live PyMilvus decoding of element offsets, complete requested arrays and shared
  document-level Grep details, for both growing and sealed segments.

## References

- [ripgrep 15.2.0 release manifest](https://github.com/BurntSushi/ripgrep/blob/15.2.0/Cargo.toml)
- [Facade and default dependencies](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/grep/Cargo.toml)
- [Searcher implementation](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/searcher/src/searcher/mod.rs)
- [Private CLI parser boundary](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/core/flags/mod.rs)
- [Matcher builder](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/regex/src/matcher.rs)
- [CLI matcher/searcher configuration](https://github.com/BurntSushi/ripgrep/blob/15.2.0/crates/core/flags/hiargs.rs)
- [Dual-license selection](https://github.com/BurntSushi/ripgrep/blob/15.2.0/COPYING)
- [MIT license](https://github.com/BurntSushi/ripgrep/blob/15.2.0/LICENSE-MIT)
- [Cargo dependency and path rules](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html)
- [Rust foreign-code linking](https://doc.rust-lang.org/reference/linkage.html#mixed-rust-and-foreign-codebases)

## Appendix A. Generated C Header

The source of truth is `internal/core/thirdparty/grep/src/ffi.rs`; its build script
regenerates [milvus_grep.h](../../../internal/core/thirdparty/grep/include/milvus_grep.h).
Read the generated header together with the implementation and native tests.
The C API is a same-build in-process ABI, not a wire format or stable plugin ABI.
