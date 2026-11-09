# MEP: Variance and standard deviation query aggregates

- **Created:** 2026-10-27
- **Author(s):** @sfjchen
- **Status:** Draft
- **Component:** Proxy | QueryNode
- **Related Issues:** #54390

## Summary

Adds `var_pop`, `var_samp`, `stddev_pop` and `stddev_samp` to Query aggregation, with and without GROUP BY. Like `avg`, each function expands into primitive plan aggregates that segcore computes and the reducers merge. 2 new primitives, `mean` and `m2`, carry the variance state `(mean, count, m2)` with the existing `count`.

## Motivation

Applications can get count, sum, min, max and avg per group but have to retrieve every value to measure spread. A naive `sum(x^2) - sum(x)^2/n` loses all precision for large offsets with small variance, so the state must be mergeable and numerically stable.

## Public Interfaces

- Output field expressions `var_pop(f)`, `var_samp(f)`, `stddev_pop(f)`, `stddev_samp(f)`, case-insensitive, for Int8, Int16, Int32, Int64, Float and Double fields (the types `avg` accepts). Other types fail with `aggregation operator var_pop does not support data type ...`.
- Result type is always Double, nullable.
- `plan.proto`: `AggregateOp.m2 = 5`, `AggregateOp.mean = 6`.
- A repeated aggregate expression in `output_fields` is planned once, and every occurrence reads the same state.
- Search aggregation is unchanged and still rejects these names.

```python
client.query("products", filter="price > 0", group_by_fields=["brand"],
             output_fields=["brand", "count(price)", "stddev_samp(price)"])
```

## Design Details

### State layout

`NewAggregate` expands each function into 3 adjacent plan aggregates on the same field: `mean`, `count`, `m2`. `count` is the existing primitive, so count stays an exact Int64. `m2` is the sum of squared deviations from the mean. The 4 functions share this state and differ only at finalization.

segcore keeps 1 `(count, shift, mean, m2)` accumulator per primitive, 32 bytes per group. `mean` and `m2` are the same accumulator and emit different fields, so the 2 columns always describe 1 state. `shift` is the group's first value, and the accumulator works on deviations from it, which keeps their precision under a large offset. Grouped rows fold in 1 at a time (Welford). A global aggregation computes each batch's mean and M2 in 2 passes and folds the batch in with the same Chan formula as the merge below, which avoids a division per row. Each input converts to double. A group without non-NULL values reports count 0 and NULL mean and m2.

### Merge

`GroupAggReducer` merges partial results in the QueryNode across segments, in the delegator, in the proxy and chunk by chunk in the Query ReduceStream. `Row.Merge` treats `[mean, count, m2]` as 1 slot group and applies Chan's formula to the whole group:

```
n     = n_a + n_b
delta = mean_b - mean_a
mean  = mean_a + delta * n_b / n
m2    = m2_a + m2_b + delta^2 * n_a * n_b / n
```

A side with count 0 is skipped or replaced, so empty partial states never divide by 0. That covers segcore's NULL state and the empty partial a QueryNode without segments encodes. Other aggregates merge column by column as before, and a failed merge now fails the query instead of being dropped. The reducer rejects a plan where `mean`, `count` and `m2` of a field are not adjacent in that order, a count that is negative or would overflow Int64, and a NULL or non-numeric mean or m2 with count > 0.

### Finalization

`AggregationFieldMap.Finalize` builds every user output field, for all 3 proxy paths (`MilvusAggReducer`, the reduce and remap operators of the query pipeline). It reads the function name back from the output expression and checks it against the number of state columns: 2 for `avg`, 3 for a variance function, 1 otherwise. A variance field gets `m2 / n` (population) or `m2 / (n - 1)` (sample), then the square root for `stddev_*`.

- count 0 (empty or all-NULL input): NULL.
- count 1: population functions return 0, sample functions NULL.
- A negative, NaN or NULL m2 with count > 0 is an internal error, not clamped. Every Chan term is a square or a product of non-negative factors, so a valid state is never negative.

### Precision

- Values convert to double before accumulation. Int64 values above 2^53 round to the nearest double, so the result is the variance of the rounded values: `2^53 + 1` and `2^53 + 3` give var_pop 4, not 1.
- Each partial state carries its mean as a double, so data whose spread is a few units in the last place of its magnitude loses accuracy across segments: `1e16` to `1e16 + 4` split over 2 segments gives var_pop 5, not 2.
- No sum is carried, so an Int64 column whose sum overflows still has a finite variance: 10 nanosecond epoch timestamps near 1.7e18 work.
- Large offsets: 2 segments of 1e7 values near 1e9 with standard deviation 1e-3 give var_pop within 1e-6 relative of a reference on exact deviations.
- Insert and upsert reject NaN and Inf in Float and Double fields, so input is finite. An M2 that exceeds the double range becomes +Inf, in segcore and in the merge, and never turns into NaN.
- Tests compare with exact or 2-pass references within 1e-9 relative, the large-offset case within 1e-6, and with pandas within 1e-6 relative because Float inputs are single precision.

## Compatibility, Deprecation, and Migration Plan

No persisted state. During a rolling upgrade a QueryNode without `mean` and `m2` rejects the plan, so the query fails until all nodes run the new version. Existing aggregates and their wire layout are unchanged.

## Test Plan

- Go reducer: fixtures merged across uneven splits, empty partial states and both merge orders, global and grouped, against an exact rational reference. Fixtures cover [1, 2, 3], singleton, constant, mixed sign, a 1e9 offset, Int64 above 2^53, near ±2^62, epoch nanoseconds and 0 with 1.5e154, whose squared delta overflows while M2 does not. Plus the 2 x 1e7 large-offset merge, +Inf and NaN states, invalid layouts, count overflow, a NULL mean, a NaN or negative m2 after an empty state and a failed column merge.
- Query ReduceStream: the state streams in 1-row chunks through 2 shards, global and grouped, with an empty group.
- Go finalization: NULL and count rules for all 4 functions, +Inf, invalid state errors, repeated output fields, a column count that does not match the function, and both proxy pipeline paths (with and without ORDER BY).
- C++: `M2AggregateTest` drives the accumulator through `addSingleGroupRawInput` and `addRawInput` against a 2-pass reference for mean and M2, Double, Float and Int64 input, NULLs, Int64 rounding, the 2e7-value large offset, epoch nanoseconds, overflow to +Inf and a squared delta that overflows while M2 does not. The 6 input types share 1 template.
- E2E: `test_query_aggregation.py` compares grouped and filtered global results with pandas `var`/`std` (`ddof` 0 and 1), checks the NULL rules across a sealed and a growing segment mixed with `count` and `avg`, repeated output fields and epoch nanoseconds.
- Mutation checks: breaking the Chan term, the count-0 skip, the mean precision, the count overflow check, the sample denominator, the layout check, the +Inf rule, the merge error propagation, the m2 validation, the overflow-safe term order or a per-column merge in the global or grouped reducer each fails a test.

## Rejected Alternatives

- **Sum of squares state.** `sum(x^2) - sum(x)^2/n` cancels catastrophically for large offsets.
- **A sum column instead of mean.** The merge would rebuild each mean as `sum / count`, whose rounding is larger than the difference of the means under a large offset, and an Int64 sum overflows although the variance is finite.
- **1 aggregate owning 3 slots through the `NewState/UpdateState/Terminate` API with a state merge.** `FromPB` builds 1 aggregate per plan column and a segcore aggregate emits 1 column, so the plan still lists 3 primitives. Moving the Query reducer to slot groups is a separate change that `avg` would share.
- **A compound column.** `FieldData` columns hold 1 scalar per row. Packing the triple into bytes would need new serialization in segcore and every reducer.

## References

- Chan, Golub, LeVeque, "Algorithms for computing the sample variance", 1983.
- PostgreSQL statistical aggregates: https://www.postgresql.org/docs/current/functions-aggregate.html
