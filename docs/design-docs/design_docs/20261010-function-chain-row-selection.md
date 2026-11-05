# Function Chain Row Selection and Result Assembly

- **Created:** 2026-10-10
- **Scope:** Go Function Chain execution in `internal/util/function/chain`.
- **Status:** Framework implementation complete; native chain regression and
  targeted caller tests verified. Distributed expansion semantics remain out of scope.
- **Related:** [Function Chain API](20260624-function-chain-api.md),
  [Hybrid Search integration](20260818-hybrid-search-function-chain.md).

## Execution model

Single-input operators return input positions and computed columns. The
framework validates the result, applies one selection to every retained input
column, and assembles the complete DataFrame. Positions are relative to each
operator's immediate input. Business algorithms remain owned by their operators.

## Positions, identity and chunk boundaries

For each logical input chunk:

```text
indices[output_position] = immediate_input_position
```

An input of length `m` has positions `0..m-1` without storing a column. After
selection the next operator addresses its own input in the same way:

```text
input IDs:               [10, 11, 12, 13]
first operator indices:  [3, 1]
next input IDs:          [13, 11]
second operator indices: [1]
final IDs:               [11]
```

These positions are not primary keys, segment offsets, or persistent row IDs.
Real identity and provenance remain in their existing columns. In particular,
`$l1_source_index` is a token into QueryNode's source map: it follows selection
unchanged and must never be reset to the new row position. An external source
map must be reconstructed using its selected tokens, as the existing L1 adapter
does. The framework cannot synchronize arbitrary external parallel slices.

A DataFrame chunk denotes a logical query group. Selection preserves chunk
count, order, and zero-length chunks and never moves rows across groups. A
physical scan batch or Arrow execution batch does not establish a new logical
query group. Existing per-chunk Sort/Limit semantics and distributed stage
boundaries are unchanged; this API does not promise global sort or pagination
across independently executed DataFrames.

## Operator interface

```go
type Operator interface {
    Name() string
    Execute(*types.FuncContext, *DataFrame) (*OperatorResult, error)
    Inputs() []string
    Outputs() []string
    String() string
}

type OperatorResult struct {
    Columns   []NamedColumn
    Selection *RowSelection
}

type NamedColumn struct {
    Field arrow.Field
    Data  *arrow.Chunked
}

type RowSelection struct {
    Chunks []ChunkSelection
}
```

`FuncChain` uses `ExecuteOperator` to run a single-input operator and assemble
its output. Direct callers that need a complete DataFrame use this helper too.
Raw `Operator.Execute` callers receive only the selection/computed columns and
must release the result themselves and supply a valid input layout. Direct
Merge callers use `ExecuteMulti`, including for one input.
The expression interface continues to return `[]*arrow.Chunked`.

`Selection == nil` means every input row survives in unchanged order. A non-nil
selection contains exactly one entry per input chunk. Entries are constructed
with:

- `AllRows()`: retain the current order and count.
- `RowRange(start, end)`: select the half-open interval `[start, end)`.
- `RowIndices(indices)`: gather in the specified order; empty means zero rows.

The zero value of `ChunkSelection` is invalid. Missing chunks cannot silently
become identity selections. Index lists can reorder and repeat input positions;
repeated positions copy all retained columns consistently. This internal
mechanism does not expose a new public expansion operator or redefine record
identity, deduplication, or pagination.

Computed columns are already in selected output order. Their chunk counts and
lengths must match the selection; the framework does not select them again.
Every declared output must appear exactly once. Empty names, duplicate names,
undeclared replacements, missing outputs, mismatched Arrow types and invalid
nullability are internal contract failures.

## Assembly and metadata

The framework executes the following steps:

1. Validate the input layout and check cancellation before invoking the operator.
2. Validate every selection index/range, output shape, name and type before
   publishing anything. Validate checked total row-count arithmetic.
3. Add all computed columns first, in operator-returned order, including
   replacements for existing names.
4. Append input columns whose names were not replaced, preserving their relative
   order and applying the same per-chunk selection. Identity selection retains
   references; ranges use zero-copy slices; index lists use the existing
   `dispatchPickByIndices` helper for Boolean, integer, floating-point, String
   and Binary arrays, including nullable values. Binary selection copies opaque
   bytes without interpreting JSON. Non-empty index selections of List, Struct
   and other unsupported types are rejected.
5. Publish the complete output after successful assembly.

`assembleOperatorResult` receives the complete `inputDF` retained after any
framework column pruning, not the subset named by `op.Inputs()`. Input layout
validation belongs to `ExecuteOperator` and is not repeated during assembly.
`selectionSizes` validates mappings and derives output chunk lengths;
`validateOperatorColumns` validates computed-column declarations;
`selectColumn` performs the actual per-column selection using the original Go
index slices.

The output column order is computed columns followed by non-replaced input
columns. It need not match the upstream schema order. Operators read by name;
expression argument order follows their declared inputs. `ToSearchResultData`
exports ordinary fields and group-by fields in DataFrame order, so consumers
must use field identity or explicitly establish a required projection order.

Selection preserves the exact retained Arrow fields (type, nullable flag and
field metadata), schema metadata, DataFrame metadata, and retained Milvus field
IDs/types. Empty results retain their columns and schema, including DataFrames
with zero query chunks. Column pruning preserves the Arrow schema metadata as
well and remains a separate framework operation guided by liveness. `DataFrameBuilder.Build` reconstructs Arrow fields, while `CopyAllMetadata`
copies the DataFrame metadata map.

Each DataFrame column has one immutable descriptor with explicit origin and
an owned FieldSchema or computed metadata.
Origin distinguishes `columnDeclared` from `columnComputed`. Collection fields
retain their schema definitions. Computed columns use an explicit logical type
from Arrow field metadata when present, otherwise an unambiguous Arrow mapping.
Type, ID, nullability, element types and vector parameters have one authority.
CopyFieldMetadata replaces the entire descriptor, including absent properties,
and shares immutable snapshots across selection, pruning and merge. Builder
setters use copy-on-write. HybridSearch L2 rerank supplies Merge inputs from a
common plan/schema. Merge copies the first available column definition, including
its nullability, to the output descriptor. `SetFieldType` returns an
internal error on a type conflict with a saved declaration and leaves metadata
unchanged. Replacing a declaration requires `SetFieldSchema`; changing a type
does not silently change column origin. Search and Query both export from
this metadata through `exportFieldData(df, name)`. Query result export receives
only the DataFrame and classifies columns by saved collection origin. Search's
all-valid scalar mask omission is a response-assembly policy, not a separate
conversion implementation.

Computed columns use their declared `arrow.Field`; they do not inherit stale
source-field metadata or a collection FieldID merely because they overwrite an
existing name. Explicit `milvus.data_type` metadata is consumed by `SetComputedField`; otherwise
the Milvus type is inferred where a mapping exists. Replacing a
column can change its type and nullability; consumers must use the output
schema. Map declares expression outputs nullable because the expression
interface does not expose an output-nullability contract. GroupBy's generated
score is non-nullable. `Nullable: true` permits NULL values; it does not make
non-null values NULL. Public write permissions remain enforced by the existing
request/stage validation; declaring an output is not new client authorization.

Index slices are reused for selection of all retained columns without building
Arrow index arrays. The pinned Arrow v17 fork's `TakeArray` has quadratic
allocation/copy costs for variable-length binary and list arrays. The framework
therefore uses explicit scalar/Binary dispatch. `TakeArray` can be reconsidered
after upgrading Arrow with the fixes in apache/arrow-go#557 and #573, released
in v18.5.0. A zero-copy range can retain a larger backing buffer than its visible
output; this change does not claim an RSS bound.

For single-input operators, the framework checks cancellation only in
`ExecuteOperator`, after input validation and before invoking the operator.
`FuncChain` relies on this check instead of polling separately. Once execution
starts, successful execution and assembly may complete despite cancellation;
the next operator is skipped, while the last operator may return a successful
result. Expressions still receive the request context, so context-aware I/O can
abort and its returned cancellation/timeout error is preserved. Selection,
assembly and operator loops do not add cancellation polling. Merge retains its
separate `ExecuteMulti` path.

## Operator changes

| Operator | Selection | Computed columns |
|---|---|---|
| Map | nil | Expression outputs |
| Filter | Per-chunk true/non-null mask positions | None |
| Sort | Per-chunk stable sorted indices | None |
| Limit | Per-chunk ranges | None |
| GroupBy | Existing selected/ordered candidate indices | `$group_score` |

Map preserves the score-normalization contract introduced by
[PyUDF Function Chain](20260722-pyudf-function-chain.md): after checking output
count, outputs mapped to `$score` are normalized to Float32 before constructing
`NamedColumn`. Supported signed integers and Float64 are converted; unsupported
score types are rejected. Intermediate NULL/NaN/infinity values retain the
upstream contract; final score validity is checked at the existing export/L0
boundaries. User-function output-count and score-normalization failures retain
`ErrFunctionFailed`. Invalid framework selections or assembled column layouts
remain system/internal errors. Conversion failure releases all owned outputs,
including any already-converted columns.

Filter validates the complete Boolean mask shape before reading positions. False
and NULL exclude rows. Its expression output is temporary, is never attached to
the DataFrame, and is released on success, error and cancellation. It shares the
same scalar-column selection path as Sort and GroupBy.

Sort retains its existing comparisons, null placement and tie-break behavior.
Limit computes `start + min(limit, size-start)` to avoid overflow for large
limits; GroupBy uses the same bounded arithmetic for its group interval.
GroupBy remains candidate selection with a group score, not a general
aggregate that synthesizes source-free rows.

Merge retains `ExecuteMulti` and its complete-DataFrame output because fusion
requires input identity as well as row position and may combine scores across
inputs. `FuncChain` handles it first through that entry, including one-input
merges, and validates the resulting layout before subsequent execution. Its
single-input `Execute` method rejects accidental use; a single-input
`RowSelection` cannot express Merge's ownership/field-source rules.

## Ownership and failure behavior

- Input DataFrames and their columns are borrowed and immutable.
- Each `NamedColumn.Data` transfers one owned Arrow reference to OperatorResult.
  An expression returning an alias of an input column must retain it first.
  Multiple returned aliases need one owned reference per entry.
- `ExecuteOperator` releases OperatorResult on every exit, including an error
  returned together with partial columns. Assembly borrows that result and
  acquires independent references for successful output columns. Map handles an
  expression error first, releases any partial outputs, then checks the output
  count and normalizes `$score` before constructing named columns. Filter always releases its temporary
  mask, including when an expression returns both a mask and an error.
- Result release is idempotent. Selection slices remain immutable until assembly
  finishes; they have no Arrow reference-counted resources.
- Failed assembly releases all selected arrays and retained references. No partial
  DataFrame is returned. Input and successful output can be released in either
  order without invalidating each other.
- Invalid mappings, masks and internal layouts originate `ErrServiceInternal`,
  not user input errors. Existing expression errors are wrapped with `merr.Wrap`
  to preserve their codes and classification. The scalar selection helper's
  unsupported-type errors retain their existing system/internal code. The
  execution-entry check and expression-returned cancellation errors preserve the
  context error. No post-execution context check replaces successful output or
  an operator error with a concurrent cancellation error.

## Verification

The regression suite and new selection tests cover sequential operators,
per-query chunks, nullable scalar arrays, unsupported selection types, hidden source tokens, precomputed
columns, repeated positions, empty selection versus identity, zero query chunks,
metadata/pruning, range overflow, malformed selections/outputs/masks, error
preservation, cancellation and reference cleanup with Arrow's checked allocator.

### Validation boundaries

- Native chain regression and targeted QueryNode/Proxy tests cover selection,
  pruning, source alignment and metadata propagation. They do not replace a
  complete `make test-go` or deployed multi-node test.
- Expansion is tested at the per-chunk framework level. There is no public
  expansion operator; distributed identity, deduplication and pagination for
  such an operator are outside this contract.
- Index selection uses `dispatchPickByIndices` for scalar and Binary arrays.
  List and Struct rejection is covered with checked-allocator cleanup checks.
- Cancellation tests cover canceled/expired contexts at entry, operator errors
  with partial results, and cancellation during execution or assembly. A last
  operator may complete successfully; a following operator checks cancellation
  at entry.
- Error tests verify preservation of errors received by the framework, not
  service-wide retry behavior. OOM/allocator-exhaustion was not fault-injected.

Run the framework regression from the repository root with the native library
environment configured:

```sh
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/util/function/chain/...
```

## Key implementation files

- `internal/util/function/chain/operator_execution.go`: selection types, validation,
  common execution and assembly.
- `internal/util/function/chain/chain.go`: unified dispatch and intermediate ownership.
- `internal/util/function/chain/operator_{map,filter,sort,limit,group_by}.go`:
  selection/computed-column producers.
- `internal/util/function/chain/operator_merge.go`: separate multi-input entry.
- `internal/util/function/chain/pruning.go`: liveness pruning and schema retention.
- `internal/util/function/chain/operator_execution_test.go`: contracts, ownership,
  error injection and export-order regression.
- `internal/util/function/chain/operator_expansion_test.go`: test-only expansion
  and subsequent-operator coverage.
- `internal/querynodev2/tasks/l1_function_chain.go`: existing provenance consumer;
  inspected, unchanged by this design.
