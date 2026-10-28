# MEP: Variance and standard deviation query aggregates

- **Created:** 2026-10-27
- **Author(s):** @sfjchen
- **Status:** Draft
- **Component:** Proxy | QueryNode
- **Related Issues:** #54390

## Summary

Adds `var_pop`, `var_samp`, `stddev_pop` and `stddev_samp` to Query aggregation, with and without GROUP BY. Like `avg`, each function expands into primitive plan aggregates that segcore computes and the reducers merge. 1 new primitive, `m2`, carries the variance state.

## Motivation

Applications can get count, sum, min, max and avg per group but have to retrieve every value to measure spread. A naive `sum(x^2) - sum(x)^2/n` loses all precision for large offsets with small variance, so the state must be mergeable and numerically stable.

## Public Interfaces

- Output field expressions `var_pop(f)`, `var_samp(f)`, `stddev_pop(f)`, `stddev_samp(f)`, case-insensitive, for Int8, Int16, Int32, Int64, Float and Double fields (the types `avg` accepts). Other types fail with `aggregation operator var_pop does not support data type ...`.
- Result type is always Double, nullable.
- `plan.proto`: `AggregateOp.m2 = 5`.
- Search aggregation is unchanged and still rejects these names.

```python
client.query("products", filter="price > 0", group_by_fields=["brand"],
             output_fields=["brand", "count(price)", "stddev_samp(price)"])
```

## Design Details

### State layout

`NewAggregate` expands each function into 3 adjacent plan aggregates on the same field: `sum`, `count`, `m2`. `sum` and `count` are the existing primitives, so count stays an exact Int64. `m2` is the sum of squared deviations from the mean. The 4 functions share this state and differ only at finalization.

segcore keeps a `(count, mean, m2)` accumulator, 24 bytes per group, and emits only `m2`. Grouped rows fold in 1 at a time (Welford). A global aggregation computes each batch's mean and M2 in 2 passes and folds the batch in with the same Chan formula as the merge below, which avoids a division per row. Each input converts to double. Count and sum come from their own aggregates over the same rows, so all 3 columns skip the same NULLs. A group without non-NULL values reports count 0 and NULL sum and m2.

### Merge

`GroupAggReducer` merges partial results column by column, in the QueryNode across segments, in the delegator and in the proxy. `m2` cannot merge alone, so `Row.Merge` treats `[sum, count, m2]` as 1 slot group and applies Chan's formula before the sum and count columns are added:

```
delta = sum_b / n_b - sum_a / n_a
m2    = m2_a + m2_b + delta^2 * n_a * n_b / (n_a + n_b)
```

A side with count 0 is skipped or replaced, so empty partial states never divide by 0. That covers both NULL states from segcore and the zero states `EmptyAggResult` reports for a QueryNode without segments. The reducer rejects a plan where `m2` does not directly follow the `sum` and `count` of its field, a state with a negative count or a non-numeric slot, and an Int64 sum that would overflow.

### Finalization

The proxy reads the function name back from the output expression, as the feature-usage hook already does, and computes `m2 / n` (population) or `m2 / (n - 1)` (sample), then the square root for `stddev_*`.

- count 0 (empty or all-NULL input): NULL.
- count 1: population functions return 0, sample functions NULL.
- A negative or NULL m2 with count > 0 is an internal error, not clamped. Every Chan term is a square or a product of non-negative factors, so a valid state is never negative.

### Precision

- Values convert to double before accumulation. Int64 values above 2^53 round to the nearest double, so the result is the variance of the rounded values: `2^53 + 1` and `2^53 + 3` give var_pop 4, not 1.
- The merge takes each side's mean as sum / count in double, the same rounding `avg` has.
- An Int64 sum that overflows fails the query: segcore checks each segment, as it does for `sum`, and the reducer checks each merge.
- Insert and upsert reject NaN and Inf in Float and Double fields, so input is finite. An M2 that exceeds the double range becomes +Inf.
- Tests compare with exact or 2-pass references within 1e-9 relative, and with pandas within 1e-6 relative because Float inputs are single precision.

## Compatibility, Deprecation, and Migration Plan

No persisted state. During a rolling upgrade a QueryNode without `m2` rejects the plan, so the query fails until all nodes run the new version. Existing aggregates and their wire layout are unchanged.

## Test Plan

- Go reducer: fixtures merged across uneven splits, empty partial states and both merge orders, global and grouped, against an exact rational reference. Fixtures cover [1, 2, 3], singleton, constant, mixed sign, a 1e9 offset, Int64 above 2^53 and near ±2^62. Plus invalid slot groups and Int64 sum overflow.
- Go finalization: NULL and count rules for all 4 functions, invalid state errors, the field map for mixed `avg` and variance requests, and both proxy pipeline paths (with and without ORDER BY).
- C++: `M2AggregateTest` drives the accumulator through `addSingleGroupRawInput` and `addRawInput`, in batches of 1, 2 or 3 and all rows, against a 2-pass reference for Double, Float and Int64 input, NULLs and Int64 rounding. The 6 input types share 1 template.
- E2E: `test_query_aggregation.py` compares grouped and filtered global results with pandas `var`/`std` (`ddof` 0 and 1), and checks the NULL rules across a sealed and a growing segment, mixed with `count` and `avg`.
- Mutation checks: reversing the merge order, dropping the Chan term, rejecting count-0 partial states, the overflow check, the sample denominator or the slot-group validation each fail at least 1 Go test.

## Rejected Alternatives

- **Sum of squares state.** `sum(x^2) - sum(x)^2/n` cancels catastrophically for large offsets.
- **A mean column instead of sum.** It needs a second new primitive and a weighted merge for the mean too. Reusing `sum` keeps Int64 means exact and adds 1 primitive.
- **A `VarianceAggregate` owning 3 columns through the `NewState/UpdateState/Terminate` slot API.** `FromPB` builds 1 aggregate per plan column, so the reducer would need a regrouping pass. The adjacency check gives the same guarantee with no new interface.
- **A compound column.** `FieldData` columns hold 1 scalar per row. Packing the triple into bytes would need new serialization in segcore and every reducer.

## References

- Chan, Golub, LeVeque, "Algorithms for computing the sample variance", 1983.
- PostgreSQL statistical aggregates: https://www.postgresql.org/docs/current/functions-aggregate.html
