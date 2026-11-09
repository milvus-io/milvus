# Timestamptz as Clustering Key

- **Issue**: [#54361](https://github.com/milvus-io/milvus/issues/54361)
- **Author(s)**: @simonhearne
- **Status**: Draft
- **Date**: 2026-10-16
- **Component**: Proxy (DDL validation, plan parser), QueryNode (delegator segment pruner)

## TL;DR

Allow a `Timestamptz` field to be the collection's clustering key. The write and
compaction path already treats Timestamptz as int64 microseconds and needs no
change. The query path does: today a Timestamptz literal can reach the
delegator's segment pruner with a non-int64 `GenericValue`, which panics the
QueryNode. The type whitelist currently hides that panic. This change therefore
ships three parts together:

1. Allow `DataType_Timestamptz` in `typeutil.IsClusteringKeyType`.
2. Make the plan parser reject non-int64 literals for Timestamptz columns.
3. Make the pruner's literal conversion return an error instead of panicking,
   and stop the IN-list pruner from silently dropping unconvertible values.

## Motivation

Time-range filters (`ts >= ISO '…' and ts < ISO '…'`) are a common access
pattern. A clustering key on the timestamp lets clustering compaction lay out
segments by time, and lets the delegator skip whole segments whose min/max
range cannot match the filter.

Today `IsClusteringKeyType` (`pkg/util/typeutil/schema.go:714`) allows only
Int8/16/32/64, Float, Double, VarChar, String and FloatVector. Create collection
(`internal/proxy/ddl/task.go:290`) and add field (`task.go:940`) reject a
Timestamptz clustering key. The workaround is a duplicate INT64 epoch column.
That costs storage, needs the application to keep both columns in sync, and
gives up the timezone-aware `ISO '…'` filter syntax on the clustering column.

## Non-goals

- **Pruning INTERVAL comparisons.** `ts + INTERVAL 'P1D' > ISO '…'` parses to
  `TimestamptzArithCompareExpr`. The pruner has no case for it, so `ParseExpr`
  returns `nil` and the query searches all segments, which is correct. Shifting
  the stats range by the interval to prune these is a possible follow-up.
- **Tuple IN on Timestamptz.** `tuple_term_filter.go` rejects Timestamptz
  explicitly and stays unchanged.
- **Tightening raw integer literals.** See [Open questions](#open-questions).
- **Reviving `FilterSegmentsOnScalarField`** (`segment_pruner.go:297`). It holds
  a `todo: add float/double/timestmaptz pruner` comment but has no non-test
  callers. The live path is `ParseExpr` → `PruneByScalarField`. Removing it is
  a separate cleanup.

## Current state

### Write and compaction path: already compatible

Timestamptz is stored as int64 microseconds since the epoch, so ordering is by
absolute instant and independent of timezone. Every clustering-compaction step
already handles it as int64:

| Step | Site | Behaviour |
|---|---|---|
| Arrow type / row deserialize (scalar analyze) | `internal/storage/serde.go:366` | `arrow.Int64`, returns `int64` |
| Column value extraction (mix/split) | `clustering_compactor.go:816` | `*array.Int64` case |
| Sort and bucket split | `field_value.go:1095` (`NewScalarFieldValue`) | `Int64FieldValue` |
| Min/max field stats, partition stats (de)serialization | `field_stats.go:102/311/369` | handled with Int64 |
| Default value for null rows | `storage.GetDefaultValue` | `GetTimestamptzData()` (int64) |

The datacoord trigger (`compaction_policy_clustering.go`), the C++ core and the
RESTful layer have no clustering-key type checks.

### Query path: unguarded panic

`PruneSegments` (`delegator.go:486`, `:859`; enabled by
`queryNode.enableSegmentPrune`, default `false`) parses the filter for the
clustering key field. It converts each literal with
`storage.NewScalarFieldValueFromGenericValue` (`field_value.go:1035`). The
Timestamptz branch does an unchecked `gVal.Val.(*planpb.GenericValue_Int64Val)`.

The parser does not guarantee that shape. `castValue`
(`internal/parser/planparserv2/utils.go:283`) returns any value unchanged for
Timestamptz. Probing the parser on master gives:

| Filter on a Timestamptz column | Plan literal |
|---|---|
| `ts > ISO '2025-01-01T00:00:00Z'` | `int64_val` |
| `ts in [1, 2, 3]`, `1 < ts < 100` | `int64_val` |
| `ts > 123`, `ts == "x"`, `ts > 1.5` | rejected at parse time |
| `ts in ["a", "b"]` | **`string_val`** (`TermExpr`) |
| `"a" < ts < "z"` | **`string_val`** (`BinaryRangeExpr`) |
| `ts > {p}` with string template `p` | **`string_val`** (`UnaryRangeExpr`) |

With a Timestamptz clustering key and pruning enabled, the last three panic in
`scalar_pruner.go`. Neither the pruner, the delegator, nor the QueryNode gRPC
server recovers that panic. Int64 keys are not exposed: `castValue` rejects
non-integer literals for integer types before the plan is built.

## Design

### 1. Clustering key gate

Add `schemapb.DataType_Timestamptz` to the scalar list in `IsClusteringKeyType`
and its doc comment. Both validation sites (create collection, add field) use
this function, so neither needs its own change. `clustering.GetClusteringKeyField`
and the datacoord policy need no change.

### 2. Parser: reject non-int64 literals at the source

In `castValue`, accept only integer literals for Timestamptz:

```go
if typeutil.IsTimestamptzType(dataType) && IsInteger(value) {
    return value, nil
}
```

Any other shape falls through to `castValue`'s existing
`merr.WrapErrQueryPlanMsg("cannot cast value to …")` rejection, which is the
error every other type mismatch already gets.

`castValue` is the shared conversion point for term literals, unary-range
literals and template filling (`fill_expression_value.go`). Inline binary
ranges (`lo < ts < hi`) are checked separately by `castRangeValue`, which gets
the same rule by adding Timestamptz to its integer case. Together the two
changes cover all three panic shapes.

**Error class.** The blame test gives Input: the request's own literal is what
fails. `ErrQueryPlan` is an input error and is not retriable, so no `retry.Do`
consumer is affected.

**Behaviour change for collections without a Timestamptz clustering key.** The
string, float, bool and array literal shapes listed above become parse errors
on every Timestamptz column, not only clustering keys. Segcore asserts
`kInt64Val` for Timestamptz literals (see [Open questions](#open-questions),
item 2), so these queries already fail at evaluation time. The change only
moves the failure earlier and classifies it as user input.

### 3. Pruner: return an error instead of panicking, and never drop IN values

**3a. Checked conversion.** In `NewScalarFieldValueFromGenericValue`, use
comma-ok type assertions for every branch, not only Timestamptz; Int64, Float
and the string branches have the same unchecked pattern. A shape mismatch
returns `merr.WrapErrServiceInternalMsg`. After part 2 this branch is reachable
only through a parser bug, so the System class applies. The existing
out-of-range returns for Int8/16/32 keep `ErrParameterInvalid`.

**3b. IN-list correctness.** `ParseTermExpr` (`scalar_pruner.go:278`) currently
*skips* any value whose conversion fails. That is correct for an out-of-range
integer, because no row can equal it. For a type mismatch it would be wrong:
dropping `y` from `ts in [x, y]` lets the pruner remove segments that contain
`y`, and the query silently returns fewer results. Today a mismatch panics
instead, so 3a alone would turn a crash into wrong results. `ParseTermExpr`
must keep skipping only `ErrParameterInvalid` and return any other error.
`PruneSegments` already handles a parse error by logging and searching every
segment.

`NewScalarFieldValueFromGenericValue` has no callers outside `scalar_pruner.go`,
so these changes are contained.

### Compactor

No change. `clusteringScalarValue`, the scalar analyze deserializer and
`splitClusterByScalarValue` all yield and compare `int64` for Timestamptz (see
[Current state](#write-and-compaction-path-already-compatible)). A dedicated
test covers this, because no test exercises it today.

## Compatibility

- **Existing collections:** unaffected. The clustering key is fixed at create
  time or add-field time, so only new schemas can opt in.
- **Rolling upgrade:** a proxy that accepts the new schema may coexist with an
  old QueryNode. The old QueryNode would prune Timestamptz keys with the
  unchecked conversion, but only parser-produced literals reach it. Plans from a
  new proxy are already sanitized by part 2. Plans from an old proxy can carry
  the bad shapes, but an old proxy also rejects the schema, so the collection
  cannot exist until the proxy is upgraded. Upgrade order is therefore not
  constrained.
- **Downgrade:** a collection created with a Timestamptz clustering key and then
  downgraded to a version without part 2 exposes the panic again, if pruning is
  enabled. Part 2 does not depend on parts 1 and 3. Landing or backporting it
  first narrows that window.
- **2.6 backport:** verified on master only. Re-check `castValue`, the pruner
  and `fill_expression_value.go` on the 2.6 branch before backporting.

## Test plan

**Unit tests:**
- `pkg/util/typeutil`: flip the negative assertion at `schema_test.go:7318`.
- `internal/proxy/ddl`: in `TestClusteringKey`, accept Timestamptz on create
  collection and on add field, and still reject a second clustering key.
- `internal/parser/planparserv2`:
  - Reject `ts in ["a"]`, `"a" < ts < "z"`, `ts > 1.5`, `ts in [true]`, and a
    string template value bound to a Timestamptz column.
  - Keep accepting ISO literals and raw int64 literals.
- `internal/storage`: `NewScalarFieldValueFromGenericValue` returns an error
  instead of panicking for each type/shape mismatch, Timestamptz included.
- `internal/querynodev2/delegator`:
  - Timestamptz key with ISO literal range, equality and IN filters: prunes
    exactly the segments whose `FieldStats` min/max cannot match.
  - IN list with one malformed value: no segment is pruned.
  - INTERVAL filter: no segment is pruned.
- `internal/datanode/compactor`: clustering compaction with a Timestamptz key,
  including null rows and a default value, produces buckets ordered by int64
  microseconds, with partition stats min/max matching the data.

**Integration test:** create a collection with a Timestamptz clustering key,
insert across several time ranges, run clustering compaction, enable
`queryNode.enableSegmentPrune`, and check that:
- range queries return the same results as with pruning disabled;
- `QueryNodeSegmentPruneRatio` shows segments skipped.

**Fault injection:** before part 3, the pruner test with a hand-built
`string_val` Timestamptz plan must panic; after, it must fall back to no
pruning. This pins the regression.

## Open questions

1. **Raw integer literals.** `ts in [1,2,3]` and `1 < ts < 100` parse today and
   are read as microseconds, while `ts > 123` is rejected as a cross-type
   comparison. This design keeps accepting `Int64Val` so as not to break current
   queries. Should a later change require ISO literals only, for consistency?
2. **Segcore behaviour for the rejected shapes.** *Resolved by reading source.*
   Segcore executes Timestamptz unary, term and range filters as `int64_t`, and
   `GetValueFromProtoInternal<int64_t>` (`internal/core/src/exec/expression/Utils.h:236`)
   asserts `kInt64Val`. The rejected shapes therefore already fail at query
   time with a system-classified segcore error. Part 2 moves that failure to
   parse time and classifies it as `ErrQueryPlan` (input). It removes no
   working behaviour.

## Rejected alternatives

- **Gate change only.** This makes the pruner panic reachable from user filters.
- **Pruner guard only (part 3 without part 2).** This stops the crash, but leaves
  malformed plans flowing to segcore and leaves the source of the bad value in
  place. It contradicts fixing the value where it is constructed.
- **`recover()` in `PruneSegments`.** This hides the invariant violation and
  would mask future bugs in the same path.
- **Keep the exclusion and document an INT64 epoch column.** This works today,
  at the cost of a duplicate column and the loss of the ISO filter syntax.
