# Arrow inside the QueryNode retrieve path: one FieldData build at the RPC boundary

**Status:** implemented and measured. Gated off by default
(`common.interface.zeroCopy`).
**Replaces:** `20260928-arrow-retrieve-transport-and-reduce.md` (deleted; its
verified findings are carried over below).

## Terminology

This document does **not** use "phase 1 / phase 2". That numbering collided with
two unrelated axes and caused a wrong scoping decision once already. It names
functions:

- **`AsyncRetrieve`** — the segcore retrieve call. Every retrieve goes through it.
- **`FillRetrieveFieldsOrdered`** — the late-materialization second fetch, used
  only when `ignore_non_pk` is on. Already Arrow since #52973.

## Goal

Inside the QueryNode, a retrieve result should exist as Arrow from the moment
segcore produces it until the response is assembled. `schemapb.FieldData` is
built **exactly once**, at the RPC boundary, from the rows the reduce selected.

Before this change the same payload was built into `FieldData` once per segment
(segcore serializes a `DataArray`, Go parses it back) and then copied again by
the cross-segment `AppendFieldData` merge.

## Non-goals

- **The wire format.** The RPC still carries `fields_data`. Carrying Arrow across
  the RPC needs a coordinated client and server change plus version negotiation,
  and is deliberately out of scope — the conversion back to `FieldData` happens
  before `proto.Marshal`. The latency harness keeps `marshal`/`unmarshal` in its
  totals, and prints each arm's wire size; it asserts nothing and is behind
  `//go:build arrowbench`, so it is a diagnostic a human reads, not a gate. The
  standing check that the wire is unchanged is the differential e2e test, which
  compares the assembled responses with `proto.Equal`.
- **Aggregation and GROUP BY.** Their columns carry `field_id = 0` and are
  identified positionally, so they cannot be matched by id. They keep the
  protobuf path.
- **ORDER BY.** Excluded by routing, not by capability — see "Known follow-ups".
- **`FillRetrieveFieldsOrdered` / `ignore_non_pk`.** Already Arrow; different
  workload (see below). Nothing here is aimed at it.
- **`milvuspb.QueryResults`.** Public client contract, has `FieldsData`.

## Why `AsyncRetrieve` is the target

### The two late-materialization scopes

Milvus defers fetching expensive fields at two different scales, and they are the
same technique applied to different boundaries:

| | scope | selection pass | reduce happens | fetch addresses rows by |
|---|---|---|---|---|
| **requery** | cluster: proxy ↔ nodes | the search itself (ids + distances) | proxy, global topK | `pk IN (...)` |
| **`ignore_non_pk`** | node: node ↔ segments | retrieve with non-PK fields withheld | within the node | `(segIndices, segOffsets)` |

A search *is* the selection pass of a cluster-scale late materialization, and the
requery is its fetch. Structurally identical to `ignore_non_pk`, one scale up.

They do not compose, and the reason is not arbitrary: inside a requery every row
in the `IN` list is already wanted, so the reduce discards nothing and deferring
saves nothing — it would only add a CGO round trip. `shouldEnableIgnoreNonPk`
expresses exactly that by requiring `req.Limit != Unlimited`.

The two also differ in cost per row, because of **what identifier survives the
boundary**: `SearchResultData` (`schema.proto:441-457`) carries `ids`, `scores`,
`distances`, `topks` — **no segment offsets**. The proxy only ever learns PKs, so
a requery must re-resolve PK → offset per row. `ignore_non_pk` keeps
`(segment, offset)` because it never left the node and the segments stay pinned.
Unifying them would mean shipping offsets to the proxy plus a snapshot-version
token and a PK fallback when offsets go stale (compaction, GC, segment reload) —
a distributed-consistency change, out of scope here.

### Consequence

**A requery has `Limit == Unlimited`**, so `ignore_non_pk` never engages and its
full payload — vectors included — flows through `AsyncRetrieve`. Trace:
`search_pipeline.go:1358` builds a `reQuery: true` `QueryTask` whose
`RetrieveRequest` sets no `Limit`; `PreExecute` sets
`t.Limit = queryParams.limit + queryParams.offset` (`task_query.go:877`); the
requery's `QueryParams` carries only `CollectionID`, no limit key, so
`limit = typeutil.Unlimited` (`:479`). `ShallowCopyRetrieveRequest` forwards
`Limit` verbatim (`shallowcopy.go:69`), so the QueryNode sees `Unlimited` and
`shouldEnableIgnoreNonPk` returns false.

`AsyncRetrieve` is also the broader lever: it serves requery's payload fetch
**and** `ignore_non_pk`'s selection pass. `FillRetrieveFieldsOrdered` serves only
the latter's fetch.

## The transport across `AsyncRetrieve`

`AsyncRetrieveAsArrow` returns a `CRetrieveArrowResult`: the protobuf
`RetrieveResults` with user columns removed, plus the user columns as an exported
Arrow RecordBatch. Details below took a measurement or a correction to get right
and are easy to break:

- **The split keeps system columns in the protobuf header.**
  `NewTimestampedRetrieveResult` looks the Timestamp column up in `fields_data`
  by id and hard-errors when absent.
- **Row count comes from the columns, not `results->offset_size()`.**
  `FillTargetEntryDirectly` (aggregation) leaves `offset` empty while its columns
  hold rows, and the Arrow builders only assert when the source is *shorter* than
  the requested count — so trusting `offset` would silently drop every row for
  any shape that slipped past routing. `RowCountOfDataArray` measures a column
  and asserts all columns agree.
- **`milvus.field_order` records the order as observed**, not derived from the
  plan: `FillTargetEntry` follows `plan->field_ids_` while `FillOrderByResult`
  follows `pipeline_field_ids_` and appends system fields last. Recording what is
  there is branch-agnostic and makes mixed-path merging safe.
- **`milvus.valid_data_fields`** records which columns' source `DataArray`
  carried a validity bitmap. Arrow cannot express the difference — an all-true
  bitmap and an absent one both arrive as `NullN() == 0` — and guessing from
  schema nullability diverges, because the ORDER BY pipeline allocates
  `valid_data` for every scalar regardless (`ExecPlanNodeVisitor.cpp:364-374`).
  That divergence is not cosmetic: `AppendFieldData` appends a validity bit only
  for sources that have one (`schema.go:1367-1371`), so mixing produces a
  `ValidData` shorter than the row count.
- **No per-field Arrow metadata.** `milvus.field_id` / `milvus.data_type` cost
  ~44 of ~126 extra allocations per call (decoded by `cdata.decodeCMetadata`,
  then cloned again by `arrow::StructOf`). Columns are matched positionally from
  `field_order` instead. `MilvusField` is untouched — the search export needs it.
- **An earlier revision of this doc claimed that a throw inside the retrieve
  future terminates the process, and three code comments rested design decisions
  on it. That claim has been WITHDRAWN.** It contradicts the code it cited:
  `Future.h`'s `registerConsumeCallback` installs a `thenError` arm for
  `milvus::SegcoreError` specifically, and `asyncProduce` fulfills the promise via
  `promise_->setWith(runner)`, which captures the exception rather than letting it
  escape. The observation behind the claim (a `libc++abi` abort) was never
  reproduced in a committed test, and nothing in the repo lets a reader re-run it,
  so per G3 it is not asserted. The three decisions it was used to justify stand
  on their own grounds instead, and none of them needs it:
  - no C++ guard on the routing precondition — Go owns the routing, so a C++ copy
    of the predicate could only go stale, and throwing on a caller-contract
    violation would turn a Go bug into a QueryNode crash;
  - no Go fallback in `RetrieveArrow` — every error it can return is either a bug
    or one the protobuf path fails on identically;
  - excluding a payload-less or type-less `DataArray` — `FieldDataToArrow` has no
    branch for it, so the column cannot be carried at all regardless of how the
    failure would surface.

  Whether such a throw is in fact delivered to Go is left open and deserves its
  own investigation; it is not load-bearing here either way.

## Design

### The reduce reports a selection; nothing gathers

The Arrow-routed reduce pipeline is a single operator
(`buildPlainReducePipeline`: `ReduceByPKTS → output`), and the reduce decides
which rows win from **PKs and timestamps alone** — both of which live in the
protobuf header. It never reads a user column.

So the reduce does not merge the user columns at all. It reports a
`queryutil.ArrowSelection`: the per-segment Arrow records plus the `RowRef`s it
chose. `MaterializeArrowSelection` then writes those rows straight into
`FieldData` at response assembly.

An earlier revision gathered the selection into one merged Arrow record first.
That record had no consumer — the pipeline ends at the reduce and the response
needs `FieldData` — so gathering only inserted a full copy of the payload
between two steps that can address the source rows directly. Counting payload
passes:

| path | passes |
|---|---|
| protobuf | `DataArray` build → serialize → `proto.Unmarshal` → `AppendFieldData` | **4** |
| Arrow, gathering | `DataArray` build → *(aliased, 0)* → gather → convert | **3** |
| **Arrow, lazy selection** | `DataArray` build → *(aliased, 0)* → write into `FieldData` | **2** |

Measured by hand on the merge-and-materialize span alone (1 segment, 1000
rows) while the gathering variant still existed: protobuf 240 µs, gathering
539 µs, lazy selection 219 µs. The gathering arm has since been deleted, so no
committed harness reproduces this row — it is recorded to explain why the
intermediate record was abandoned, not as a number to re-measure.

**Lifetime.** The records are owned by `QueryTask.Execute`, whose
`defer segments.ReleaseRecords(results)` already spans the response assembly, so
materializing there needs no new ownership plumbing. "At the RPC boundary" is
concretely "at the end of `Execute`, before that defer runs".

**Peak memory goes down, not up.** Gathering had the sources, the merged record
and the IPC buffer alive simultaneously; the lazy path has the sources and the
`FieldData`.

### Zero-copy aliasing instead of a second C++ copy

`FieldDataToArrow` was copying every column into a fresh Arrow buffer. For most
types that copy is pure waste: the protobuf payload already has Arrow's exact
value-buffer layout. `FieldDataToArrow` takes an optional `owner`
(`ProtoOwner = std::shared_ptr<void>`); when supplied, the Arrow array aliases
the protobuf memory through a `ProtoBackedBuffer` that keeps the owner alive.
`ExportRecordBatch`'s C release callback owns the RecordBatch → ArrayData →
buffer → owner chain, so the Go side releasing the record unwinds all of it.

`PartitionRetrieveResult` moves the user columns out of the result into that
owner **before** the export. The order is forced: the arrays point into those
buffers, so partitioning afterwards (as the copying version did) would free
memory Arrow still referenced.

Aliased when there are no nulls: all five dense vector types
(`fixed_size_binary(w)`, where `w` *is* the protobuf per-row size), INT32,
INT64, FLOAT, DOUBLE. Still copied: BOOL (protobuf stores a byte per element,
Arrow bit-packs), INT8/INT16 under `preserve_integer_width` (protobuf widens
them to int32), and every variable-length type. Nullable columns are never
aliased — `MergeDataArray` compacts them, so the physical layout diverges from
Arrow's.

Callers that cannot guarantee the lifetime pass `nullptr` and are bit-identical
to before: `FillRetrieveFieldsOrdered` and both search exports.

### Not every column belongs in the Arrow batch

`WorthCarryingAsArrow` leaves three kinds of column in `fields_data`, next to
the system columns, so the protobuf reduce merges them. It addresses rows by
the same `selectedRows`, so the result is identical either way.

**ARRAY** — its Arrow representation is a protobuf blob **serialized per row**
(`FieldDataToArrow`'s `array_data` branch calls `SerializeToString` into a
`BinaryBuilder`): segcore would pay that serialize and Go a matching `Unmarshal`
to carry bytes the protobuf path already had in final form. Excluding it was the
largest single contributor to the one shape where the transport still lost —
8-segment all-types went from 0.96x to 0.99x on this change alone, with
`materialize` dropping 2.3x.

**The vector-of-vector types** — excluded for a different reason, and NOT
because of serialization: `VECTOR_ARRAY` already exports as a native
`list(fixed_size_binary)` with no serialize step at all. It is excluded because
the Go side has no GATHER for a `LIST` column: the gathers accept only
STRING/BINARY/BOOL and `materializeColumn` has no `ArrayOfVector` case, so
carrying it would reach the "no gather handles" error. Note the converter is not
the gap — `arrowconv.arrowColumnToFieldData` already reads a `LIST` back via
`arrowListToVectorArray`, which the `ignore_non_pk` path uses. Lifting this means
adding a gather case, and unlike ARRAY there is no cost argument against it.

**Sparse float vectors** — a byte-identity reason, not a cost one. The protobuf
reduce sets `SparseFloatArray.Dim` to the max over each CONTRIBUTING SEGMENT's
declared dim, which covers every row that segment *retrieved*. An Arrow column
holds only the rows the reduce *kept*, so any selection that is a strict subset
— a limit truncation, a timestamp dedup — would report a smaller `Dim` and
break byte-identity. Leaving it behind makes the two identical by construction,
and costs nothing: a length-prefixed blob gains no copy from Arrow either way.

**Anything the exporter has no branch for** — the predicate is an ALLOW-list
keyed on the same payload oneof `FieldDataToArrow` dispatches on, not a deny-list
over `DataType`. There is no fallback behind it: a column that is carried but
cannot be exported reaches `NotImplemented` and fails the user's query. A
deny-list would hand `true` to every type added to the enum later — `Mol`,
`Date`, `Time`, `Decimal`, `UUID` and `Struct` are already in `schema.proto` with
no exporter branch — and to payload oneofs the exporter does not handle
(`bytes_data`, `geometry_wkt_data`, `mol_data`), since `fd.type()` and the
payload oneof are set independently. Fail closed; keep the two lists in step.

### One Arrow producer, deliberately

`FieldDataToArrow` is the only thing that builds a retrieve Arrow record, so
`arrowconv`'s type switch is its mirror image by construction and the gathers
can take a column's type from the first record that has one.

An earlier revision added a second producer: `ArrowColumnSink`, which handed the
sealed take path's storage-native `arrow::Table` over directly, skipping the
`ArrowToDataArray` → `FieldDataToArrow` round trip. It was removed, and removing
it is what makes the paragraph above true.

It was removed because it could not run. The sink required
`!is_external_collection`, while `useTakeForOutput` defaults true only FOR
external collections and false for internal — so take-on-by-default was exactly
the configuration the sink excluded, and nothing could reach it without a
separate config change. It also had no Go-level coverage, since every Go test
here uses growing segments.

Two producers would also have had to agree per column and per type, and did
not: storage keeps INT8/INT16 narrow where `FieldDataToArrow` widens them to
int32, and the sink had no equivalent of `WorthCarryingAsArrow`'s exclusions.
Because `TryTakeForRetrieve` returns false *per segment*, one request could mix
sink and fallback segments, and the Go gather takes a column's type from the
first record it sees — so the mismatch was an unchecked type assertion away
from a QueryNode panic, or a column-count mismatch. Reconciling that needed a
shared type rule, an `arrow::compute::Cast`, narrow-integer handling on the Go
side, and defensive checks in two gathers: a substantial amount of machinery
whose only purpose was to make two producers agree. All of it went with the
sink.

If the take path is made Arrow-native later, it is recoverable from history
(commit `44099fa169`) and should land with a Go-level test that sets
`InternalCollectionUseTakeForOutput=true`.

### Materialization is one pass per column

`materializeColumn` writes the selected rows of a column straight into the
`FieldData` oneof it belongs in — a flat typed slice for the scalars and dense
vectors, one slice per row for the bytes-valued ones. It declines a column only
when that is unsound, and the caller falls back to
`queryutil.GatherColumns` + the existing converter.

After the variable-length writers were added, **the only case still reaching the
gather is a compacted-nullable column**, checked once by making `GatherColumns`
fail at runtime and observing that only the nullable case broke. That probe is
not committed.

The gather's original fallback (`array.Concatenate` + `compute.TakeArray`) is a
trap worth naming: Concatenate copies **every row of every record**, not just the
selected ones. Retrieving 500 of 4000 rows copied 4000 and then 500. On a schema
whose JSON, array and sparse columns took that path, `materialize` was 3852 µs
and the whole transport was a **2.6× regression**. `gatherVarLen` (direct
per-row append) and then the one-pass writers removed every live caller, and the
branch itself is now **deleted**: a column with no gather returns an error
(`arrow_merge.go`), because by construction nothing should arrive there and a
reviewer should hear about it if the construction changes.

### `maxOutputSize` has to see the Arrow columns

The guard (`quotaAndLimits.maxOutputSize`, default 100 MB) accumulates a per-row
size during the reduce and refuses before materializing. `rowSize` summed
`FieldsData`, which on the Arrow path holds only the system columns — 8 bytes
against a real 536 for a 128-dim float vector schema, a **67× undercount** that
grows with vector width. The guard's purpose is to refuse *before* allocating, so
an undercount turns it into "materialize twice, then refuse one hop later".

`rowSizeCalculator.withArrowRecord` adds the Arrow columns: fixed widths summed
once per result, variable-length read from the offsets per row. The widths mirror
`calcFieldElementSizeWithCompactIndex` exactly, so the guard trips at the same
row count on both transports.

The per-segment guard in `SegmentInterface.cpp` (`Retrieve`'s
`output_data_size > limit_size` check) is path-independent
(it estimates from `plan->field_ids_` before any materialization) and was never
affected; so were the delegator and proxy guards, which see materialized
`FieldData`. The gap was exactly the QueryNode's cross-segment sum.

### Routing

`shouldUseArrowTransport` requires `common.interface.zeroCopy` and excludes
aggregation, ORDER BY and `ignore_non_pk`. There is **no row-count threshold**.

**Element-level (`ArrayOfVector`) queries are not excluded, and must not need to
be — but only because `vector_array` stays out of the Arrow batch.** The column
rides in the protobuf header next to `element_indices`, which the reduce reads
from the header and re-emits through `buildMergedElementIndices`; the Arrow
materializer has no `ArrayOfVector` case and no element-indices handling at all.
So this is the one shape where the C++ allow-list and the Go routing have to stay
in step jointly: admitting `vector_array` to the batch without also teaching the
materializer about element indices would drop them silently. The note is on
`WorthCarryingAsArrow` as well.

## Measurements

Method: the two arms alternate single calls, the order flips per sample, and the
verdict comes from a sign test on per-pair wins. Medians in µs.

**Do not measure this with `-gcflags="all=-N -l"`.** It disables optimization for
every package including protobuf, penalizing the protobuf arm far more than
Arrow's buffer work, and it reported Arrow winning 1.24×–2.58× where the
optimized build showed 0.96×–1.14×.

### Narrow schema (int64 PK, int8, float, 128-dim float vector)

Every output column takes the one-pass writer. `GOGC=off`, two runs:

| shape | ratio | verdict |
|---|---|---|
| 1 seg × 100 | 1.039 / 1.029 | ARROW |
| 1 seg × 1000 | 1.168 / 1.163 | ARROW (z +15.8 / +13.2) |
| 1 seg × 10000 | 1.326 / 1.319 | ARROW |
| 4 seg × 1000 | 1.095 / 1.068 | ARROW |
| 8 seg × 2000 | 1.061 / 1.020 | ARROW |

### All types (14 columns: 4 dense vectors, JSON, array, sparse, …)

`GOGC=400` (this schema OOMs at `GOGC=off`), two runs:

| shape | ratio | verdict |
|---|---|---|
| all-types 1 seg × 100 | 1.014 / 1.017 | ARROW-leaning |
| all-types 1 seg × 500 | 1.080 / 1.088 | ARROW |
| all-types 4 seg × 500 | 1.034 / 1.027 | ARROW |
| all-types 8 seg × 800 | 1.009 / 1.004 | break-even |

Getting the all-types shape here took three measured fixes, each of which the
narrow schema could not have exposed, because it has no variable-length output
column at all:

| | 8 seg × 800 |
|---|---|
| `array.Concatenate` in the gather fallback | 0.385 / 0.394 |
| + direct per-row variable-length gather | 0.941 / 0.973 |
| + one-pass writers for the variable-length types | 0.958 / 0.935 |
| + ARRAY kept out of the Arrow batch | 0.992 / 0.990 |
| + sparse kept out too (for correctness, see above) | **1.025 / 1.034** |

Those are the ratios measured as each fix landed, in one sitting, to show the
progression. They are not the final figure: the authoritative number for this
shape is the **1.009 / 1.004** in the Measurements table above, re-measured after
the remaining changes. Absolute timings on this machine drift enough between
runs that only within-run ratios are comparable, which is why the two differ.

The 8-segment shape is the weakest and the reason is structural, not
algorithmic: `doOnSegments` (`segment_do.go:25-37`) launches one `errgroup.Go`
per segment, so wall-clock retrieve is ≈ max over segments rather than the sum
and the per-segment saving does not accumulate. Measured directly: holding total
rows and hits constant and varying only the segmentation, the retrieve advantage
collapses from −159 µs at 1 segment to −9 µs at 8.

### Arrow's per-call cost is small

Single segment, sweeping hits 1 → 4000, fitting `saving = a + b·rows`:
`a = −24.7 µs`, `b = +0.213 µs/row`, break-even **115 rows per segment**. The
aliasing raised the marginal saving 34 % (0.159 → 0.213 µs/row, reproducible to
three significant figures) and doubled the fixed cost (−11.0 → −24.7 µs, from
the holder and the per-column buffer allocations), so it pays above ~250 rows
per segment.

### A measurement trap: GC attribution

An apparent per-segment Arrow penalty at 4 segments (retrieve 774 → 911 µs) was
**GC accounting**, not cost. With `GOGC=off` the same fixture gives 815 → 750.
The Arrow arm allocates more garbage per iteration, and in an alternating
harness the GC for iteration *k* lands inside iteration *k+1*'s first timed
phase. The extra allocation is real work; attributing it to `retrieve` was not.

## Verification

- **Differential, byte-identical, with one stated exception.** A result through
  the Arrow path must equal the same result through `fields_data` under
  `proto.Equal`. The single exception is the payload byte at a row whose
  validity bit is FALSE: segcore stores a value for a null scalar row and the
  protobuf path carries it through, while Arrow's builders call `AppendNull`,
  which writes a zero. `valid_data` itself, the column order, the lengths and
  every value at a valid row are identical, so the reason for the bar -- a
  positional delegator merge must not misalign -- holds exactly. Where that
  normalization is needed (`compareBothTransports`, the nullable schemas) it is
  applied to BOTH sides rather than skipping the column; the narrow differential
  test needs none and uses a plain `proto.Equal`, since its output set has no
  nullable column. Nullable
  VECTORS do not diverge at all (their payload is compacted on both paths). The bar is byte-identical because results from both transports
  can meet in one delegator reduce during a rolling config change, and the merge
  indexes columns positionally — a difference in order or in `valid_data` is
  silent corruption, not a visible error. Covered: the narrow schema
  (1/4/8 segments, zero hits), every type at once, a nullable vector, a
  nullable scalar (a DIFFERENT path -- scalars index logically where vectors
  index a compacted payload), and a VarChar PK.

  NOT covered, despite an earlier claim here: `count(*)`. The proxy compiles it
  to an aggregate (`dql/task_query.go:658`), so `hasAggregation` excludes it from
  this path entirely and a real `count(*)` never reaches the transport. The test
  that claimed it drives a no-output-fields retrieve, which exercises a
  zero-column batch and nothing else; it is named for that now.
- **Subset selection.** A selection that is a strict subset of the rows
  retrieved — a limit truncation or a timestamp dedup — is the only shape where
  "report a selection" and "merge the columns" can diverge, and so the only one
  that tests the design rather than its surroundings. It is also where the
  sparse `Dim` divergence above was found. Note that a finite request limit
  across MORE THAN ONE segment routes to `ignore_non_pk` instead
  (`shouldEnableIgnoreNonPk`), so truncation reaches this path only with a
  single segment; that routing fact has its own test rather than being assumed.
- **Non-vacuity, enforced in the suite rather than argued.** The assertion that
  does the work is the **Arrow column COUNT**, not "a selection exists". The
  export emits a `RecordBatch` even when it carries zero columns, and
  `MaterializeArrowSelection` degenerates to a header reorder in that case
  (0 columns == 0 user ids passes its cardinality guard), so a regression that
  stopped moving columns out of the protobuf header would leave every
  byte-identity comparison green **with the transport effectively off**. That was
  a real hole in an earlier revision of these tests: the one guard present
  (`!selection.Empty()`) could not see it. `expectedArrowCols` now derives the
  count from the schema and the output set, independently of the Arrow path, so
  the two cannot co-vary.

  Mechanism-level probes were also run by hand and are recorded for provenance,
  but are NOT committed, so treat them as one-time evidence rather than a gate:
  corrupting the aliased buffer (all four columns fail → all alias); corrupting
  the one-pass writer (all four fail → all take it); failing `GatherColumns`
  (only the nullable case fails → the fallback's scope is what the design
  claims); forcing every column through the gather (tests still pass → the two
  paths are equivalent). Two that ARE committed and mutation-checked:
  `withArrowRecord` (reverting it makes the guard test report the 67× undercount
  and `over_budget/arrow` fail while `over_budget/protobuf` still refuses), and
  `ReconcileValidData` (no-op, inverted, and missing-key mutants each fail its
  unit test).
- **Selection shape, measured not assumed.** A strict subset is necessary but not
  sufficient — it also has to span records. Instrumenting the selection showed
  the dedup fixtures touched **one** record (one timestamp per segment means the
  highest-numbered segment wins every PK), so they were testing the easiest
  possible walk. `perPKWinner` makes the winner vary by PK: measured 4 records
  touched with 299 record switches over 300 rows, and 8 with 239 over 240.

  A walk that goes BACKWARDS within a record turns out to be **unreachable**, not
  merely untested: segcore returns each segment's retrieve output in PK order
  (`SegmentInterface.cpp`: "the reduce phase depends on the ids to do
  merge-sort") and the reduce is a k-way merge over those, so `rowIdx` ascends
  within every record by construction. Numbering a segment's PKs against insert
  order was tried and changed nothing, because the record is PK-sorted either
  way. A materializer relying on that would be correct.
- **Oracle strength.** `zeroInvalidRows` normalizes the one documented
  byte-identity exception, so it was mutation-tested to confirm it does not
  normalize away real regressions: wrong row selected, wrong output column order,
  data under the wrong field id, wrong value at a valid row, wrong `valid_data`,
  and truncated column are each still caught. Separately, the ARRAY column as
  generated was byte-identical in every row (`GenerateInt32Array` ignores the row
  index), which made it unable to witness a wrong-row bug on the half of the
  all-types test that rides protobuf; the fixture now stamps it per row and
  self-checks that consecutive rows differ.
- **Fixture determinism.** `GenInsertMsg` numbers PKs `0..rows-1` in *every*
  segment, so identical PKs with equal timestamps make the reduce's dedup winner
  arbitrary and a differential comparison meaningless. `shiftPrimaryKeys` gives
  each segment a disjoint range; `TestFixtureIsDeterministic` is a permanent
  guard that caught this.
- **Benchmark methodology.** As above, plus: state build flags, and never
  compare absolute numbers across runs — the off arm alone drifted ±50 % between
  runs on this machine, so only within-run ratios are meaningful.

## Environment traps, all pre-existing

- `all_tests` does not link on macOS — `LoadAdmissionControllerTest.cpp:523` uses
  `std::jthread` / `std::stop_token`. Link a standalone binary from
  `ninja -t commands unittest/all_tests` with just the test object plus
  `init_gtest.cpp.o`.
- Enabling `BUILD_UNIT_TEST` makes `install` depend on `all_tests`, so
  `ninja install` **silently stops refreshing** `internal/core/output/lib`. A test
  run can load an hours-old library and appear to pass.
- `cp` over a dylib invalidates its signature and Apple Silicon SIGKILLs the
  loader with no output. Run `codesign -s - -f` on the copy.
- cgo compiles against the **installed** header, so `segment_c.h` changes need
  copying into `internal/core/output/include/` when `ninja install` is broken.
- Changing a struct used by both the library and a test object requires
  recompiling the test object; a stale layout shows up as plausible-looking test
  failures.

## Package layout

The Arrow→FieldData conversion is cgo-free and lives in
`internal/util/arrowconv`. Only producing the record needs cgo, and that stays
in `internal/util/segcore` (`RetrieveAsArrow`, `RetrieveArrowResult`).

That split is what lets the reduce in `internal/util/queryutil` materialize a
selection: `queryutil` stays cgo-free, so its tests -- which cover the materializer's
guards directly (duplicate field ids, column-count mismatch, out-of-range row
refs, `field_order` cardinality, missing metadata) -- run without a built
`libmilvus_core`. Those guards are what turn a routing or alignment bug into an
error instead of corruption, and the end-to-end tests only ever feed them
well-formed input. It also keeps `rowRef`
unexported, and it is the precondition for the delegator or proxy reusing the
conversion if the wire format ever carries Arrow, since neither can import
`querynodev2/segments`.

An earlier revision put the materializer in `querynodev2/segments` and exported
`rowRef` to reach it. That was unnecessary: the helpers it needed from
`segcore` were pure Go all along and only lived in a cgo package by accident.

## Defects found by review, and fixed

- **Declining a column is not the same as guarding it.** An earlier revision
  made `materializeColumn` return `ok=false` when a vector's Arrow byte width
  disagreed with its schema dim, with a comment claiming this avoided a
  self-inconsistent column. It did not: `ok=false` routes the column to
  `gatherColumns`, and that path does not compare width to dim either —
  `resolveVectorDim` takes the dim from the schema, then `compactFloatVector`
  allocates `numRows*dim` floats and copies `numRows*width` bytes, so Go's
  `copy` silently truncates or zero-fills; its nullable branch writes at stride
  `dim` while reading at stride `width`, misaligning rows. The same bad column,
  by a longer route. Now one `checkVectorWidth` runs BEFORE the fast/slow
  dispatch and errors, so both paths are covered by a single check instead of
  two that each assume the other does it. Pinned by
  `TestMaterializeRejectsVectorWidthDimMismatch`, whose `gather path` subtest is
  the case the old form left unguarded — removing the check makes it pass
  silently with a wrong column, which is how the gap was confirmed rather than
  argued.


Recorded because each was invisible to the tests that existed when it was
written, and the *reason* each hid is reusable.

- **`maxOutputSize` undercounted nullable fixed-width SCALARS.**
  `calcFieldElementSizeWithCompactIndex`'s validity test lives inside its
  `GetVectors()` branch; the scalar branch returns the width unconditionally. The
  Arrow calculator charged 0 for any null row of any fixed-width column, so the
  guard tripped at different row counts on the two transports — the exact failure
  class `withArrowRecord` exists to close. Nullable *vectors* matched by luck,
  because there protobuf does return 0. Now restricted to `FIXED_SIZE_BINARY`,
  pinned by `TestRowSizeChargesNullableScalarLikeProtobuf`, which fails against
  the old behavior (8 vs 16 bytes).
- **`WorthCarryingAsArrow` was fail-OPEN.** See above. Nothing reachable today,
  but there is no fallback behind the predicate, so a type added to the enum
  later would have failed queries rather than riding protobuf.
- **The keep-alive test proved nothing.** `ArrowAlias.OwnerOutlivesEveryOtherHandle`
  used `std::static_pointer_cast`, whose aliasing constructor SHARES the control
  block, so `array.reset()` dropped use_count 2→1 and the owner was never
  released. It failed in CI, and it is the only assertion in the change that
  shows the aliasing keep-alive actually keeps alive — so it failed for a reason
  unrelated to the property under test, making a real regression
  indistinguishable from the noise. The handle is now scoped, and an
  `ASSERT_EQ(array.use_count(), 1)` makes the mistake un-repeatable.
- **`ThrowInfo` with the status concatenated into the FORMAT string.**
  `ThrowInfo` expands to `fmt::format(fmt::runtime(info), ...)`, so a `{` in
  third-party text raises `fmt::format_error` — not a `SegcoreError`, so
  `Future.h`'s `std::exception` arm would flatten it to `UnexpectedError` and
  destroy the retriable `MemAllocateFailed` the call exists to carry. Now a `{}`
  placeholder.
- **Per-record metadata divergence was applied silently** — a guard, not a
  reachable bug, and the distinction cost a round to establish. The materializer
  reads `field_order` / `valid_data_fields` from the template (whichever record is
  first) and applies that one reading to every record. An earlier draft of this
  entry claimed segments really can disagree because the sealed take path emits no
  `valid_data` and falls back per segment; **both halves are false** —
  `ArrowToDataArray` populates it whenever `field_meta.is_nullable()`, and the
  take fallback is whole-result (`clear_fields_data` + `clear_ids` + `return
  false`). Both keys are schema-derived per request, so no divergence is
  reachable. The check stays as defense in depth over an assumption that is
  invisible at the call site and would fail silently, since column-count
  agreement does not imply metadata agreement. Covered by
  `TestMaterializeRejectsDivergentRecordMetadata`.
- **Emptying `fields_data` entirely would have silently dropped a segment's rows**
  (the reduce skips a result with no columns). The invariant held only via the
  proxy appending the timestamp field, three layers away in another process.
  `PartitionRetrieveResult` now keeps one column back rather than trusting that —
  degrading by one carried column is invisible in the output.
- **The `total_rows` cross-check was order-dependent**, because `0` meant both
  "no baseline yet" and "zero rows": `{0, 5}` passed, `{5, 0}` threw. A short
  column is what would let `materializeColumn` index past an aliased buffer, so
  the guard against it had a hole on one side.
- **The positional merge had no field-id check.** `buildMergedRetrieveResults`
  takes the field id from the template and copies values from every other
  result's column at the same index, so matching counts with differing ORDER
  wrote one column's values under another's id — silently. Added, because this
  change gives that merge a second producer.
- **No size guard on the aliased dense-vector buffer.** The alias branch handed
  Arrow a `Buffer` whose declared size was never checked against the payload; an
  over-read would surface in Go, far from the cause.

### Two over-claims caught in the fix round itself

Worth recording because the same reflex produced both: restating a reviewer's
mechanism as established fact without re-reading the code.

- **The `VECTOR_ARRAY` exclusion reason was wrong twice.** First "its Arrow form
  is a protobuf blob serialized per row" (true of ARRAY only); then "nothing on
  the Go side can consume a LIST column" — also false, since
  `arrowconv.arrowColumnToFieldData` already converts one via
  `arrowListToVectorArray`, which the `ignore_non_pk` path uses. The actual gap
  is narrow: `gatherVarLen` accepts only `STRING/BINARY/BOOL` and
  `materializeColumn` has no `ArrayOfVector` case.
- **The `merr.Wrapf` fix was described as rescuing a lost error code.** Measured
  afterwards: every error in `arrow_to_fielddata.go` is already
  `ServiceInternal`, so both forms yield code 5, and both satisfy
  `errors.Is(err, inner)` because the inner error unwraps to the same `merr`
  sentinel. The only real difference is a message reading `service internal
  error: service internal error`. The change is still correct per the
  error-handling rule, but it fixes no live defect — and no test pretends
  otherwise, because no non-vacuous assertion exists. Two candidate tests were
  written, both shown vacuous by mutation, and both deleted.

The habit that caught all three: mutate the fix and confirm the test fails. A
test that passes against the bug it names is worse than no test.

## Known follow-ups

- **`ArrowToDataArray` (segcore's take() output builder) is not covered by any
  test.** The sealed differential cases added here are served by
  `bulk_subscript`: a take reader is null without a storage-v2 manifest, so
  segcore logs `[TakeAPI] retrieve fallback to bulk_subscript` and the
  take-enabled case collapses onto the same producer as the others. External
  collections default `UseTakeForOutput` on, so this producer IS reachable in
  production. Covering it needs a storage-v2 sealed fixture (manifest + column
  groups), which no Go test helper builds today.


- **ORDER BY is excluded by routing.** Its pipeline is
  `DeduplicatePK → OrderByLimit`, and the second operator sorts by field values,
  so unlike the plain reduce it *does* read user columns. The lazy-selection
  design does not extend to it for free.
- **segcore still builds a `DataArray` and then converts it.** Aliasing makes
  the conversion free for the dominant types, so what remains is the
  `DataArray` build itself. Making segcore emit Arrow natively would remove a
  payload pass; the sealed take path is already Arrow-native and is the
  natural place to start (see "One Arrow producer").
- **8-segment shapes gain the least** (1.061 / 1.020 narrow, 1.009 / 1.004
  all-types), bounded by the concurrency
  effect above rather than by anything in the materialization: the per-segment
  retrieve saving does not accumulate, so there is little for the one remaining
  materialize pass to be paid out of.
- **`fetchFieldsArrow` populated `FieldData.FieldName` where its protobuf twin
  leaves it empty** (pre-existing, #52973). Fixed here for parity, since the
  flag selecting between them is per-node and refreshable so both can meet in
  one positional merge, but it is worth knowing that path had the same class of
  divergence and no test looking for it.
- **ARRAY and sparse are excluded from the Arrow batch for fixable reasons, not
  principled ones.** ARRAY's Arrow form is a per-row serialized protobuf blob
  because that is what the export chose; Arrow has native list types, and using
  one would remove the serialize/unmarshal round trip that justifies the
  exclusion. Sparse is excluded because `SparseFloatArray.Dim` is defined over
  each contributing segment's declared dim, which an Arrow column cannot carry
  -- a per-segment dim in the schema metadata would fix it. Both are worth
  revisiting before concluding Arrow is a poor fit for those types.
- **Flipping `common.interface.zeroCopy` to true** is deliberately a separate
  change. All numbers here come from one machine, growing segments, and two
  schemas; sealed segments, large dimensions and varchar-dominated payloads are
  unmeasured.
