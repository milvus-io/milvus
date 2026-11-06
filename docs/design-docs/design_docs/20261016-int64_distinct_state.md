# MEP: Single-column exact distinct state for Int64

- **Created:** 2026-10-16
- **Author(s):** @pardeep-singh
- **Status:** Draft
- **Component:** QueryNode (segcore `exec/operator/query-agg`)
- **Related Issues:** #53848 (DT01), #53849 (DT02), #53797, #53801

## Summary

Add `Int64DistinctState`, a local building block that collects the exact set
of distinct values of one Int64 column across batches. It keeps at most one
NULL, skips rows that are filtered out or invisible, enforces explicit entry
and memory limits, and checks for cancellation. Later work reuses it: query
DISTINCT execution (#53797) and the mergeable COUNT(DISTINCT Int64)
accumulator (#53849, DT02).

This is the Stanford CS146S catalog project DT01. It does not change query
behavior, plans, or any public API. Nothing calls the component yet.

## Motivation

The tracker #53801 splits exact DISTINCT into components. The shared design
(public API, plan representation, distributed state protocol, resource
policy) is still pending; @AyushKashyapII offered on #53801 to draft it.
Every option in that design needs the same small core: a per-column exact set
of values with clear NULL semantics, a hard resource bound and a
cancellation point. Building and testing that core alone lets the later
pieces focus on plan and reduce semantics. It also keeps the core free of
any decision the shared design has yet to make.

## Scope

In scope:

- One type (Int64) and one column.
- Batch input with an optional validity bitmap and an optional
  filter/visibility bitmap.
- Exact deduplication across batches, collapsing NULLs, explicit limits and
  cancellation.
- An export form that a merge can consume.

Out of scope (from the #53848 boundaries, or owned by other issues):

- Other types, composite keys, and key encoding (#53797).
- Planner, operator, Proxy and StreamingNode integration; the public API and
  SDKs (#53797, #53799).
- Merging two states and the COUNT(DISTINCT) accumulator (#53849).
- Spill to disk, ordering, and LIMIT/OFFSET pushdown.
- Index acceleration (#53800).

## Interface

`internal/core/src/exec/operator/query-agg/Int64DistinctState.h`:

```cpp
struct Int64DistinctStateLimits {
    int64_t max_entries;       // distinct non-NULL values
    int64_t max_memory_bytes;  // hash-table bytes reserved
};

struct Int64DistinctSnapshot {
    std::vector<int64_t> values;  // unique, ascending
    bool has_null = false;
};

class Int64DistinctState {
 public:
    explicit Int64DistinctState(const Int64DistinctStateLimits& limits,
                                milvus::OpContext* op_ctx = nullptr);

    void AddBatch(const int64_t* values, int64_t num_rows,
                  const TargetBitmapView* valid,      // nullptr: no NULLs
                  const TargetBitmapView* excluded);  // nullptr: all rows count

    int64_t DistinctCount() const;   // non-NULL values + (has NULL ? 1 : 0)
    int64_t NonNullCount() const;    // COUNT(DISTINCT x) input
    bool HasNull() const;
    const std::vector<int64_t>& Values() const;  // first-seen order, no copy
    Int64DistinctSnapshot ExportSnapshot() const;  // sorted copy

    int64_t MemoryUsageBytes() const;
    static int64_t ProjectedBytesForCapacity(int64_t capacity);
    bool Failed() const;
};
```

A caller holding a `ColumnVector` passes
`col->RawAsValues<int64_t>()`, `col->size()` and
`TargetBitmapView(col->GetValidRawData(), col->size())`, so the component does
not depend on `common/Vector.h`. The state is movable, not copyable, and
not thread-safe. Each pipeline driver owns its own state.

## Semantics

**Rows that count.** A row counts unless its bit in `excluded` is set.
`excluded` uses the exec pipeline's polarity: in the
`FilterBitsNode`/`MvccNode` output, a set bit means filtered out, deleted, or
invisible at the query timestamp. An excluded row adds neither its value nor
a NULL. If the caller has already compacted the batch to the visible rows,
it passes `nullptr`.

**NULL.** A counted row whose `valid` bit is clear is NULL. Its value slot is
ignored. Every NULL row collapses into one NULL entry: `HasNull()` becomes
true and `DistinctCount()` includes it once. This matches the result-DISTINCT
rule in #53801, where equal NULL-containing tuples collapse. `NonNullCount()`
excludes it, which matches COUNT(DISTINCT x).

**Equality.** Int64 values are equal when their bits are equal. Every value,
including `INT64_MIN` and `INT64_MAX`, is an ordinary key. No sentinel values
are reserved.

**Empty input.** No batches, empty batches, and batches whose rows are all
excluded give `DistinctCount() == 0` and `HasNull() == false`.

## Resource bounds

The table is `ankerl::unordered_dense::set<int64_t>`, already used in `exec/`.
It keeps values in one contiguous vector plus an 8-byte bucket array, so its
footprint can be computed exactly:
`values.capacity() * 8 + bucket_count * 8`. The state reserves capacity
itself, doubling from 64, so it knows how much memory each growth step will
take before allocating:

- `max_entries` is exact. Inserting a new value when the state already holds
  `max_entries` values fails. Repeated values and NULL never use budget.
- `max_memory_bytes` is checked before growth.
  `ProjectedBytesForCapacity(c)` mirrors the table's bucket sizing, and the
  tests check it against the real allocation. If doubling would exceed the
  budget, the state reserves the largest capacity that fits. It rejects the
  value only when no capacity above the current size fits. So rejection
  happens exactly when the exact answer does not fit, and
  `MemoryUsageBytes() <= max_memory_bytes` always holds.
- A limit failure throws `SegcoreError` with
  `ErrorCode::InsufficientResource`. The values are never truncated or
  approximated.

The caller picks the limits. A later issue will derive them from query
configuration once the shared design sets the resource policy. Construction
fails if `max_memory_bytes` is smaller than the empty table, 32 bytes.

## Cancellation

If an `OpContext` is supplied, `AddBatch` checks
`op_ctx->cancellation_token` at the start of every batch, including an empty
one, and then every 1024 rows. This follows the interval used by
`search-groupby/GroupMembership.cpp`. A cancellation throws `SegcoreError`
with `ErrorCode::FollyCancel`, the same code that `segcore::CheckCancellation`
raises.

## Failure model

Any exception during `AddBatch` puts the state into `Failed()`. That
includes a limit failure, a cancellation, and an allocation failure. The
table memory is released at once, and every later read or `AddBatch` throws
`UnexpectedError`. Rows of the failing batch may have been partly applied,
so the state refuses to answer rather than return a partial result that
looks exact. Argument errors are found before any row is consumed and leave
the state unchanged: a missing values pointer, a negative row count, or a
bitmap whose size does not match.

## How DT02 (#53849) builds on it

DT02 adds the union and the final count. This design gives it what it needs
without fixing the protocol early:

- **Mergeable export.** `Values()`, `HasNull()` and `ExportSnapshot()` hold
  the complete state: the unique values plus one NULL flag. The values form a
  valid input batch, so a union can be built by feeding one state's values
  into another under the receiver's limits. Merging `{1, 2}` with `{1, 3}`
  gives 3 values, not 4. A test covers this, and a randomized harness check
  confirms that any split of the input reproduces the single-input result.
  DT02 may add an in-place `MergeFrom(const Int64DistinctState&)` that skips
  the copy.
- **Per-group state.** The `Aggregate` accumulator slot is fixed-width, so
  DT02 can store a pointer to one `Int64DistinctState` per group and
  expression, and free it in `Aggregate::destroy`. The state is movable and
  owns its memory for this purpose.
- **COUNT semantics.** `NonNullCount()` is COUNT(DISTINCT x): it excludes
  NULL and is zero for all-NULL or empty input. A finalized count is never
  merged; only states are.

The wire format for shipping states between segments, QueryNodes and Proxy
belongs to the shared design (#53801/#53799). The sorted snapshot is a
natural payload for it, but this design does not fix one.

## Compatibility

The change is additive: a new source file, a new test, and this document.
Sources and `*Test.cpp` files under `internal/core/src` are picked up by
existing globs (`add_source_at_current_directory_recursively` and
`unittest/CMakeLists.txt`), so no build files change. Query behavior is
unchanged.

## Validation

`Int64DistinctStateTest.cpp` compares the state with a sort/unique oracle in
these cases:

- repeated values and Int64 extrema;
- duplicates across batches;
- NULL collapse, including garbage values under NULL rows;
- all-NULL input;
- excluded and all-excluded rows;
- empty input;
- 50 rounds of random batches.

It also covers:

- the memory projection against the real table;
- the exact entry limit, including a limit of zero;
- the memory limit, with a check that a rejection happens only when the
  exact answer does not fit;
- cancellation and the release of state on failure;
- malformed batches and invalid limits;
- snapshot re-ingest as a union.

## References

- #53848: [Stanford DT01] Build a single-column exact distinct-state component
- #53849: [Stanford DT02] Build a mergeable COUNT(DISTINCT Int64) accumulator
- #53797: Implement query DISTINCT execution in segcore
- #53801: Support exact DISTINCT in Milvus queries (tracking issue)
