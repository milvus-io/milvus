# FunctionChain runtime schema and operator consistency

- Related designs: [transformations](20261010-function-chain-transform-stages.md), [POST tables](20261011-function-chain-post-tables.md)
- Related issues: #46565, #52319
- Scope: runtime validation and type preservation in the experimental FunctionChain implementation

## Runtime contract

Arrow carries the actual input/output schemas. Users do not register a second
function signature or declare intermediate schemas. Each operator resolves
dependencies against its current input, including dynamic UDF outputs. Optional
`col(..., data_type=...)` hints are checked at execution, including produced fields.

Checks cover field uniqueness, supported physical types, logical type metadata,
dimensions, nested nullability, array/schema agreement, logical group row counts
and source lineage. Typed empty arrays receive the same checks. Missing columns
and incompatible user bindings are parameter errors; malformed function output
retains a typed function failure without publishing partial results.

## Type coverage

Tests enumerate the pinned protocol's 28 concrete field types directly. `None`
remains an absent optional hint and is rejected as a declared field type.

| Milvus family | Runtime representation |
| --- | --- |
| Bool, Int8/16/32/64, Float, Double | Corresponding Arrow scalar |
| String, VarChar, Text | String; LargeString is accepted by applicable operators |
| JSON, Geometry, Mol | Binary with distinct logical type metadata |
| Timestamptz, Date, Time | Canonical wire integers with logical type metadata |
| Decimal, UUID | Canonical wire fixed binary with logical type parameters |
| FloatVector | Dimensioned list of float components |
| BinaryVector, Float16Vector, BFloat16Vector, Int8Vector | Dimensioned fixed binary with distinct logical tags |
| SparseFloatVector | Encoded sparse binary rows |
| Array, ArrayOfVector | List with element type, nullability and vector dimensions |
| Struct, ArrayOfStruct | Struct or List of Struct with recursively typed children |

Field metadata carries `milvus.data_type` and `milvus.field_schema`, with
compatible dimension/element annotations. Recursive `TypeSchema` preserves
nested arrays. Contradictory annotations fail, including on empty arrays. Byte
width alone does not identify FP16/BF16/JSON/Geometry/UUID. Unannotated binary
remains ordinary Arrow binary in TABLE; ambiguous storage conversion needs a
destination binding. Schema coverage does not introduce collection/storage
capabilities for types only reserved in the protocol. TABLE can also carry
ordinary unsigned and temporal/decimal Arrow scalars independently of storage.

## Operator consistency

Map, filter, select, sort, rerank group_by and aggregate share column
resolution. Exact names precede dotted/quoted Struct path interpretation.
Parent null masks propagate to projected child schema and validity. Invalid
explicit tie-break fields fail rather than being ignored. Legacy parameter-based
group_by/sort dependencies are included before requery planning and pruning.

Sort, grouping and MIN/MAX share comparison type checks. Encoded
JSON/Geometry/Mol/Decimal/vector values are rejected as comparison operands;
UUID comparison uses bytes. SUM/AVG reject logical Date/Time/Timestamptz
integers. COUNT/FIRST/LAST/COLLECT_LIST and custom aggregation preserve supported
complex payloads. After validating a function's declared and runtime output,
non-TABLE Map normalizes numeric `$score` outputs to Float32 before downstream
ranking and export. The cast preserves groups, NULL validity and user metadata,
while rebinding the score's physical and Milvus logical schemas together.
Ordinary payload columns and independent TABLE outputs keep their original types;
TABLE cannot rewrite reserved system columns. Intermediate NaN/Inf and final score
validity policies remain a separate, unresolved rerank contract.

FIRST/LAST/MIN/MAX preserve source types; COLLECT_LIST preserves element
metadata; EXPLODE transfers it to extracted rows. Generated/aliased fields
discard persisted field IDs in shorthand and full metadata. POST TABLE output
schema remains independent of the collection. TABLE can consume stored
ArrayOfVector fields and Struct vector children; HITS keeps existing endpoint
restrictions. INSERT binds Struct children to destination types/IDs/nullability
before allocating IDs or appending to WAL.

## Python transport

Runtime 0.4.0 adds execution version 4. The
[query expansion extension](20261012-function-chain-query-expansion.md) adds
runtime 0.5.0/version 5 for query-origin and PRE-row mappings, preserving the
same complete schema transport. Schema-aware calls require exact worker
acknowledgement so an old worker cannot silently drop metadata. Versions 0/2/3
and existing array signatures remain supported; public protobuf fields are
unchanged. `context.input_schema` describes selected positional arguments,
including repeated references.

The [minimal call-context extension](20261012-function-chain-call-context.md)
uses runtime 0.6.0/version 6 when host request scope is attached, retaining
complete Arrow schemas and expanded query provenance.

A UDF can return an array sequence or a self-describing `pyarrow.RecordBatch` /
`Table`. POST still returns one result per current input group. Complete schemas,
metadata and nullability survive IPC and must agree across groups. Returning an
input array by identity retains its metadata. Newly produced ambiguous binary
values use a record/table field annotation, without a separate function schema.

```python
def transform_batch(self, params, batches, *, context):
    field = context.input_schema.field(0)
    return [
        pa.record_batch([columns[0]], schema=pa.schema([field]))
        for columns in batches
    ]
```

## Verification

Tests cover every protocol enum, complex/vector wire round-trips, NULL parent
masks, empty groups, invalid metadata/type/dimension bindings, real gRPC schema
transport and old-worker acknowledgements. Combinations include aggregate →
collect → explode → sort → select, dynamic map aliases and custom aggregation,
with checked Arrow allocation cleanup and lineage. The delivery report records final
package/race and installed-worker results. Historical repository-wide C++
prerequisite failures are not evidence of a passing repository-wide Go gate.
