# MEP: Local Format for Storage V3 Scalar Fields

- **Feature DRI:** TBD
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD
- **Created:** 2026-03-05
- **Author(s):** @zhicheng
- **Status:** Under Review
- **Component:** RootCoord | DataNode | QueryNode | Storage
- **Related Issue:** [milvus-io/milvus#50304](https://github.com/milvus-io/milvus/issues/50304)
- **Target Release:** TBD

## Summary

`local_format` selects how a Storage V3 sealed scalar field is represented and
accessed locally by QueryNode. It does not select the format written to object
storage.

The initial choices are:

- `raw` (default): materialize the existing Milvus Raw column representation.
- `vortex`: retain a Vortex physical column group and read its Cells on demand.

The choice is recorded as field schema intent. At segment load time, QueryNode
combines that intent with the physical format recorded in the Storage V3
manifest and resolves one effective local backend for the complete physical
column group. Raw and Vortex then expose the same column-level Scan and Take
contract to sealed-segment consumers.

This document is primarily the design of local-format selection, loading,
configuration, and behavior. The column Scan/Take API is included only where it
defines the common boundary needed to hide Raw and Vortex storage details.

## Problem

The existing Raw backend is simple and fast once resident, but loading it
requires materializing scalar data into Milvus-owned chunks. Large VARCHAR,
JSON, ARRAY, and other scalar fields can therefore consume memory and I/O even
when a query reads only part of a segment.

Storage V3 may persist the same logical fields in Vortex columnar files. Vortex
has its own file layout, metadata, and decoding model, so exposing it as Raw
chunks would discard its ability to prune and load data at its native Cell
granularity. Milvus needs an explicit local-format choice and a common read
boundary that does not expose either backend's physical representation.

## Goals and Non-goals

Goals:

- Keep Raw as the default and preserve its existing data-access behavior.
- Allow eligible Storage V3 sealed scalar fields to use Vortex locally.
- Resolve a physical column group to one unambiguous local backend.
- Share Vortex footer, planner, cache, and Cell state among all columns in the
  same physical column group.
- Let QueryNode cache and evict Vortex data at Cell granularity.
- Keep nullability, filtering, ordering, and ownership semantics identical from
  the caller's perspective.
- Make configuration, fallback, failure, rollout, and recovery behavior
  explicit.

Non-goals:

- Changing the physical writer format through `local_format`.
- Vortex local format for primary-key, vector, system, or growing-segment data.
- Changing the query expression language.
- Supporting every scalar predicate as Vortex pushdown.
- Replacing every legacy Chunk consumer in the first implementation phase.
- Changing WAL, streaming, replication, or CDC behavior.

## Terminology and Core Invariants

- **Schema intent** is the field's `local_format` type parameter.
- **Physical format** is the column-group format recorded in the Storage V3
  manifest. It is selected by the writer, not by `local_format`.
- **Effective local backend** is the Raw or Vortex representation selected by
  QueryNode when loading a sealed segment.
- **Column group** is a physical group of fields stored in the same set of
  files.
- **Cell** is the Vortex cache/loading unit. For Raw it corresponds to the
  existing chunk boundary used by the column planner.

The following invariants are mandatory:

1. Every physical column group has exactly one effective local backend in one
   loaded segment generation.
2. A mixed or ambiguous group never partially loads as Vortex.
3. All Vortex fields in one physical group share one `VortexColumnGroup` and
   therefore the same files, footer metadata, Cell geometry, and cache slots.
4. A Cell pin protects every borrowed byte or view returned from that Cell for
   the documented result lifetime; owned decoded output is independent of the
   pin.
5. Filtering may suppress data construction, but it never changes row
   alignment or stored validity.
6. Scan positions and Take offsets are absolute segment offsets. File- and
   Cell-local coordinates remain backend-private.
7. Corrupt or incompatible Vortex input fails segment loading or the operation;
   it does not silently switch an already selected Vortex group to Raw.

## User-visible Schema Setting

`local_format` is stored in field type parameters.

| Value | Meaning |
|---|---|
| absent or `raw` | Use the Raw local backend. `raw` is the initial server default. |
| `vortex` | Request Vortex local access when the Storage V3 physical group is also Vortex and the complete group is eligible. |

Example:

```python
schema.add_field(
    field_name="description",
    datatype=DataType.VARCHAR,
    max_length=65535,
    type_params={"local_format": "vortex"},
)
```

Validation occurs during schema creation and alteration:

- unknown values are rejected;
- `vortex` is rejected for primary-key and vector fields;
- omitted values parse as `raw`;
- non-default values are preserved when the schema is serialized.

SDKs may later expose a typed option, but the server contract remains the field
type parameter.

## End-to-end Local-format Resolution

### 1. Write-time column-group planning

The Storage V3 split policy partitions fields by the exact schema intent:
absent/default, explicit `raw`, and explicit `vortex` remain separate. Later
system, vector, text, size, and remanent split policies operate inside those
partitions.

Keeping absent and explicit `raw` separate allows a future default to change
without reinterpreting fields that explicitly selected Raw. The split policy
does not set the writer format; normal writer configuration determines the
physical format stored in the manifest.

### 2. Manifest persistence

The manifest remains the authority for physical files, their ordered segment
row ranges, columns, sizes, and format. `local_format` remains schema metadata.
It propagates through the existing collection schema and AlterCollection path;
no new WAL record, streaming message type, acknowledgement, or CDC contract is
introduced.

Vortex files in one group must form an ordered, gap-free partition of
`[0, segment_row_count)`. This is validated when the segment is loaded. Each
file is opened independently and may use a different Arrow representation as
long as the field can be normalized to the caller-selected target type.

### 3. QueryNode load-time decision

QueryNode resolves the backend for the complete physical group:

| Physical group | Schema intent for all mapped fields | Effective backend |
|---|---|---|
| non-Vortex | any supported intent | Raw materialization |
| Vortex | every mapped logical field requests `vortex` | Vortex |
| Vortex | mixed, missing, unknown mapping, or any non-Vortex intent | current group default, which is Raw |
| any group containing the primary key | any | Raw |

Multiple logical fields may map to one physical external column. That physical
column is eligible only when every mapping requests Vortex. This prevents one
field's preference from changing another field's representation.

The Vortex path additionally requires Storage V3, scalar non-system fields,
non-empty file metadata, compatible schemas, aligned Cells across fields, and a
row count matching the segment manifest.

### 4. Publication and replacement

Load constructs the complete backend off to the side and publishes it as one
segment generation. A Vortex group creates one shared `VortexColumnGroup` and
one field-level `VortexColumn` proxy per logical field. Reopen/add-field
replacement publishes a new generation; existing operations continue using
their captured generation and immutable planner/statistics state.

Creating or altering a schema uses the normal collection metadata lifecycle.
Renaming a collection does not change field intent or persisted files. Dropping
or unloading a collection destroys the loaded ColumnGroups and their ephemeral
local cache files; durable object-storage cleanup remains the existing segment
lifecycle's responsibility.

## Raw Backend Behavior

Raw remains the compatibility and default path:

- the generic Storage V3 reader materializes the requested fields into the
  existing Raw representation;
- fixed-width access returns values directly from pinned chunks;
- variable-width access returns views whose lifetime is protected by the
  corresponding pin;
- Raw Scan stops at a chunk boundary so it can return a zero-copy batch;
- Raw Take resolves input offsets lazily and reuses the current chunk pin while
  consecutive accesses remain in that chunk;
- statistics that require loaded Raw payload remain an execution-time filter.
  They may skip comparison/value construction, but are not used to avoid the
  preceding load or pin.

Introducing the common column contract must not add full-range pinning,
unnecessary value construction, or forced offset sorting to the Raw hot path.

## Vortex Backend Behavior

### Shared column-group state

`VortexColumnGroup` owns the state shared by all fields in one physical group.
For every file it owns:

- the validated absolute segment row range;
- a sparse local filesystem view;
- one footer reader;
- immutable footer-backed planners for the projected fields;
- one cache slot and Cell translator;
- metadata memory accounting.

Footer and optional zone-map bytes are loaded when the group is initialized,
not once per field or query. Field-level `VortexColumn` objects reuse this group
state and select only their projected logical column.

### Cells, planning, and pinning

For Vortex V2 a Cell is one complete row group and its physical segments. For
V1, which lacks stable row-group boundaries, a Cell is the complete flat
physical unit. Cell row ranges are ordered, non-overlapping, and complete;
fields in the same group must agree on them.

Before data access, the footer-backed planner maps the requested segment range
or offsets to Cells and may use loaded zone maps to identify data that a filter
cannot match. QueryNode then pins only the Cells required by the operation,
subject to nullability:

- a non-nullable skipped Cell need not be pinned for data;
- a nullable field still needs authoritative validity, so the Cell remains
  readable even when its value payload is skipped.

Skip state means “do not construct/evaluate this data,” not “remove these rows.”
A skipped nullable row still returns its actual validity. Data is unspecified
when either the row is null or data is skipped; validity distinguishes a true
NULL from a valid skipped value.

### Sparse local backing and lifecycle

Vortex presents a sparse local file to the reader. Footer and zone-map ranges
are materialized first; a Cell translator fills data ranges on cache load.
Missing ranges remain sparse holes until their Cell is loaded.

The production backing is memory or mmap, selected by the normal scalar mmap
settings. Mmap files are local ephemeral cache artifacts:

- created with owner-only permissions;
- truncated for a new column-group generation;
- removed when the group is destroyed;
- rebuilt from remote Storage V3 files after QueryNode restart;
- punched or zeroed when cache eviction releases a Cell range.

They are not durable segment state and are not part of backup, replication, or
CDC.

### Predicate pushdown

Vortex can return matching row ids for the currently supported unary and binary
STRING/VARCHAR predicate forms. Unsupported predicates use data Scan and are
evaluated by the normal expression implementation. Disabling pushdown changes
only the execution strategy, never the result.

## Supporting Column Access Contract

Raw and Vortex are hidden behind column-level Scan and Take. This is a support
contract for local format, not a new user-visible query API.

### Scan

`Scan(options)` creates one cursor for one expression leaf. Options fix the
initial absolute segment position, target value type, output/predicate form,
filter, prefetch choice, and pin policy. The execution window and whether a
nullable data batch needs values or validity only are supplied later through
cursor positioning and bounded `Next` calls.

The cursor exposes:

- `Position()`: next unread absolute segment offset;
- `Seek(position)`: move forward without returning intervening rows;
- `Next(max_length, read_mode)`: return one batch starting exactly at the
  current position and advance by the batch's actual row count.

`max_length` is an upper bound. Raw may stop at a chunk boundary and Vortex may
stop at a reader boundary. The expression node owns the cursor across execution
windows, seeks when its window start differs from `Position()`, and consumes
successive batches until the window is complete. A batch never crosses a
skipped range silently: it carries aligned `data_skipped` state and, for a
nullable field, authoritative validity.

Pin policy is selected once at `Scan` creation:

- `ResultOwned` (default) transfers the Cell pin to each returned batch. The
  batch remains valid until its owner is released; the cursor holds no pin.
- `CursorOwned` keeps the current physical Cell or planned Cell-set pin in the
  cursor, reuses an identical pin plan, and releases it before pinning a
  different plan. The batch is borrowed only until the next `Next` or `Seek`.

The caller never pins Cells directly. Optional prefetch submits the remaining
planned Scan Cells for parallel cache loading and immediately releases those
prefetch pins; normal batch reads still acquire their configured pin owner. It
is enabled only on paths that intentionally preserve prior prefetch behavior,
not implicitly for validity-only reads.

### Take

`Take(options)` accepts a finite list of absolute segment offsets and returns
one `TakeResult` with exactly one position per input offset. Input order and
duplicates are preserved. Filtered positions stay aligned and carry
`data_skipped`; nullable positions also retain authoritative validity.

`Get(i)` returns the value, validity, and skip state for one position.
`IsValid(i)` reads validity without constructing data. `GetOwn()` materializes
an ordered, contiguous result independent of backend pins.

Raw keeps at most the currently accessed Cell pinned and copies only when owned
output is requested. Vortex may sort, group, and deduplicate offsets internally
to reduce decode work, then restores input order in its already-owned result.
Neither backend exposes Cell ids, chunk ids, or file-local offsets to callers.

## Configuration

| Setting | Default | Scope / refresh | Effect |
|---|---:|---|---|
| field type parameter `local_format` | `raw` | schema metadata; applied when a sealed segment generation loads | Requests Raw or Vortex local representation. It does not select the writer format. |
| field/collection property `mmap.enabled` | unset | schema property; applied at load | Overrides the global scalar mmap setting for the affected physical group. If any field in a group explicitly enables mmap, the group uses mmap. |
| `queryNode.mmap.scalarField` | `false` | QueryNode configuration | Selects mmap rather than memory backing when no field/collection override exists. Disabling it does not disable Vortex; it selects memory backing. |
| `queryNode.mmap.populate` | `true` | QueryNode startup configuration | Controls `MAP_POPULATE` for the Vortex sparse mmap backing. No effect when mmap backing is not selected. |
| `queryNode.segcore.tieredStorage.warmup.scalarField` | `sync` | QueryNode/collection warmup policy | `sync`, `async`, or `disable` controls proactive Cell loading. `disable` keeps on-demand loading. |
| `queryNode.segcore.enableVortexScanPushdown` | `true` | refreshable QueryNode setting | When false, Vortex filters use data Scan and normal expression evaluation. |
| `queryNode.segcore.scanCursorOwnsPin` | `false` | non-refreshable QueryNode setting | When false use `ResultOwned`; when true use experimental `CursorOwned` Scan pin lifetime. |

Configuration changes do not rewrite existing segment files. Schema or mmap
changes take effect when the affected segment generation is loaded or replaced;
the pushdown setting is read dynamically by execution.

## Failure, Concurrency, and Recovery

Vortex initialization validates file ordering and coverage, manifest row count,
field projection, schema compatibility, Cell geometry, and cross-field Cell
alignment. Invalid storage data is reported as a data-format failure. File
creation, remote reads, cache loading, cancellation, and memory failures retain
their underlying error category. No catch-all conversion should turn a
transient system error into an input error.

Once load-time resolution selects Vortex, a footer, planner, sparse-file, or
reader failure fails the load or operation. Falling back to Raw at that point
could hide corruption and create unpredictable memory behavior, so it is not
allowed.

The shared ColumnGroup, file table, planners, and statistics snapshots are
immutable after publication. Cache slots provide the synchronization for Cell
load/eviction. Cursors and Take results are operation-local and are not shared
concurrently. Borrowed Raw views also require the operation context and their
documented pin owner to remain alive.

On restart, QueryNode re-reads the manifest and footer, reconstructs the
ColumnGroup, and refills sparse Cell ranges on demand or according to warmup
policy. No local Vortex cache file is recovered as authoritative state.

## Observability and Troubleshooting

Segment loading logs the segment id, physical column-group index, field count,
and file count when Vortex is selected. Sparse-file cleanup and range-eviction
failures are logged as warnings. Existing segment-load, tiered-cache, mmap, and
query latency telemetry continues to apply.

There are currently no dedicated local-format metrics or traces. Operators can
diagnose selection by checking schema `local_format`, manifest physical format,
the Vortex load log, mmap/warmup settings, and cache/load failures. Dedicated
backend-selection, Cell-pruning, decoded-byte, and pin-lifetime metrics are a
follow-up before treating the feature as independently observable at scale.

## Compatibility, Rollout, and Rollback

- Existing schemas default to Raw; non-Vortex physical groups remain on Raw.
- `local_format=vortex` affects only Storage V3 sealed scalar groups that pass
  the complete-group eligibility check.
- A binary that understands the physical Vortex reader but not Vortex local
  format may ignore the local preference and materialize the group through its
  generic Raw reader path.
- A binary without support for the manifest's physical Vortex format cannot
  load that group. Rolling upgrade and rollback must therefore keep all serving
  QueryNodes at a version that can read the physical files before such files are
  introduced.
- Disabling Vortex pushdown is a safe execution fallback. Changing a field back
  to Raw requires publishing/reloading the affected segment generation; it does
  not rewrite object-storage files.
- Growing segments and vector indexes are unchanged.

## Alternatives Considered

### Treat Vortex as another Chunk implementation

Rejected because Vortex has no stable Raw chunk object to expose. Synthesizing
chunks would force decoding and ownership conversions before the caller knows
which rows it needs.

### Let each field own its own footer, planner, and cache

Rejected because fields in one physical column group share files and physical
Cells. Independent state would duplicate metadata, loads, and pins and could
observe inconsistent eviction lifetimes.

### Let `local_format` select the physical writer

Rejected because schema intent and persisted encoding have different lifecycle
and compatibility constraints. The manifest must remain authoritative for the
physical format.

### Fall back to Raw after a selected Vortex reader fails

Rejected because it hides corruption or infrastructure failures and can turn a
bounded on-demand load into unexpected full materialization.

## Verification and Acceptance

Correctness coverage must include:

- schema create/alter validation and `FieldMeta` round-trip;
- column-group splitting for absent, Raw, Vortex, primary-key, and mixed fields;
- load-time decision-table cases for Raw and Vortex physical groups;
- multiple ordered Vortex files, mixed per-file Arrow representations, empty
  Cells, malformed ranges, row-count mismatch, and cross-field misalignment;
- nullable and all-valid data, validity-only access, skipped valid rows, skipped
  NULL rows, and NOT/candidate-mask expression behavior;
- sequential/seeked Scan and ordered, shuffled, duplicate-offset Take;
- retrieve, requery, offset-input expression, temporary text-index build, and
  virtual-primary-key callers;
- cancellation, remote read failure, corrupt footer/data, local sparse-file
  failure, cache eviction, segment replacement, and restart reconstruction;
- memory and mmap backings with sync, async, and disabled warmup.

Performance acceptance compares the new Raw Scan/Take path with the previous
Chunk path on the same segment and workload. Benchmarks cover fixed-width and
view types, single and multiple Cells, hot and cold cache, sequential Scan,
ordered and shuffled Take, first-window latency, total throughput, peak pinned
bytes, and pin counts. A repeatable Raw regression requires optimization or an
explicit design decision before rollout; reduced pin calls alone are not
sufficient evidence. Vortex benchmarks separately measure pruning, bytes read,
decode cost, and memory reduction.

Required repository builds, focused unit tests, integration tests, DCO, and CI
must pass before merge.

## Implementation Phases and Follow-ups

Phase 1 introduces local-format selection and the common Scan/Take operations on
`ChunkedColumnInterface`, then migrates sealed expression and retrieve/requery
main paths. Legacy Chunk access remains only for consumers not yet migrated.

Phase 2 removes external Chunk access from sealed columns, leaves Chunk-specific
APIs only behind concrete Raw/Growing implementations, and renames the common
abstraction to `ColumnInterface`.

Known follow-ups:

Unless explicitly reassigned, the Feature DRI and Primary Approver own these
follow-ups:

- complete exact target validation for recursively represented ARRAY values
  after the storage representation stabilizes;
- avoid decoding Vortex Take positions that a filter has already skipped;
- decide whether `CursorOwned` should become the default from representative
  benchmarks; `ResultOwned` remains the current default;
- integrate sparse Vortex writes with the unified Milvus file-I/O controller
  when that interface is available;
- optimize random long VARCHAR/JSON/ARRAY Take and owned conversion;
- add dedicated local-format selection, pruning, decode, and pin metrics;
- expand safe predicate pushdown beyond the initial VARCHAR operators.

## Design Review Status

The technical document records the intended final state, but it is not ready to
merge under the Milvus Feature Design Review process until the Feature DRI,
Primary Approver, Independent Approver, and review date are filled in; the
required meeting (including Liu Li) is held; its conclusions are reflected
here; and both named approvers explicitly approve the Design Doc PR.

## References

- [Milvus PR: scalar local-format data-scan foundation](https://github.com/milvus-io/milvus/pull/51504)
- [Milvus issue: Vortex local format](https://github.com/milvus-io/milvus/issues/50304)
- [milvus-storage](https://github.com/milvus-io/milvus-storage)
- [Vortex](https://github.com/vortex-data/vortex)
